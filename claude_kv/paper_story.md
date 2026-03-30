# MassKV 论文故事线

最后更新：2026-03-23

## 一句话

利用真实负载中 key 的前缀结构，将 KV 索引内存从 O(N) 降到 O(P)，并建立以前缀聚合度 α 为核心参数的解析性能模型，使系统在任意规模下的读延迟、写代价和内存开销均可预测。

---

## 1. Observation：真实负载的 key 不是无意义字节流

时序数据库（device_id + timestamp）、对象存储（bucket/user/path）、社交图谱（user_id + relation + target）、电商（merchant + product + sku）——超过 80% 的生产 KV 负载的 key 具有可识别的前缀结构。

但现有系统（RocksDB、LevelDB、PebblesDB）把 key 当作 opaque bytes，索引大小与 KV 对数量 N 线性相关。

---

## 2. Problem：O(N) 索引在大规模下不可持续

### 内存墙

| 规模 N | RocksDB Index+Filter（实测 16B key, 64B value） |
|---|---|
| 1M | 3.18 MB |
| 100M | 178 MB |
| 1B | 2.15 GB |
| 10B | ~21.5 GB |
| 1T | ~2.15 TB |

索引必须驻留内存才能保证性能。当 N → 万亿，O(N) 索引撑爆任何单节点内存。

### 延迟不确定性

RocksDB 依赖 Bloom Filter 做概率性过滤：
- 单层假阳性率 1%，7 层累积后无效 I/O 概率显著增加
- p99/p999 延迟取决于 compaction 状态、block cache 命中率等运行时因素
- DBA 无法预测给定配置下的延迟上界

### 分布式不是答案

"加机器"能解决规模问题，但解决不了可预测性问题——网络延迟、leader 切换、跨节点协调引入更多不确定性。对于要求严格延迟 SLA 的场景（金融交易、实时推荐），单节点的确定性优势不可替代。

---

## 3. Insight：α 是统一控制变量

定义前缀聚合度 α = P/N（P 为独立前缀数）。

利用前缀结构，索引内存从 O(N) 降到 O(P) = O(αN)。但更深层的发现是——α 不只是一个参数，它同时控制了系统的三个核心维度：

```
α 大 → 内存大（route index = P × 33B）
     → SSD I/O 少（每个 prefix 下 key 少，TinyDirect 占比高）
     → 但 route 查找慢（Masstree 更大，cache miss）
     → 写代价高（compaction 涉及更多 prefix 的索引更新）

α 小 → 内存小
     → SSD I/O 多（subtree 更深）
     → route 查找快
     → 写代价低
```

这意味着：
1. 读延迟不是 α 的单调函数，存在最优 α*
2. 给定 α 和内存预算 M，系统的读延迟上界、写代价、内存占用都是可解析计算的
3. 性能是可预测的，不依赖运行时状态

---

## 4. Design：MassKV 架构

### 两层 LSM（去掉多层读放大）

```
MemTable (Masstree, 内存)
    ↓ flush
L0 (ring buffer, 最多 32 棵树)
    ↓ compaction
L1 (sorted, 双层索引)
```

只有两层，不存在 RocksDB 的 L0→L1→...→Lmax 多层穿透问题。

### 双层索引（route layer + subtree layer）

```
Route Layer (Masstree, 内存)
  prefix → descriptor
    ├─ TinyDirect:     descriptor 即 leaf_value → 1 次 SSD I/O
    ├─ NormalPack:      pack page → leaf_value → 2 次 SSD I/O
    └─ NormalSubtree:   B+Tree traversal → leaf_value → ⌈log_F(R)⌉+1 次 SSD I/O

leaf_value → (kv_block_ptr, offset, count) → PointQueryWindow
```

### Swap 机制（内存可控）

当 route layer 内存超预算时，将 Masstree 冷叶子节点下沉到 SSD：
- 内部节点不动，查找路径不变
- 冷叶子替换为 ColdLeafStub（24B），指向 SSD page
- 读路径遇到冷叶子时多 1 次 SSD I/O

### 增量索引更新（Delta Batch）

Compaction 产出 L1DeltaBatch，按 prefix 粒度增量更新索引：
- 小变更：COW patch（复用未变叶子页）
- 大变更：bulk rebuild

---

## 5. Analysis：解析性能模型

### 点查 I/O 上界

给定 α、N、M：

| 条件 | I/O 上界 |
|---|---|
| M ≥ αN × 33B（全热），TinyDirect | 1 |
| M ≥ αN × 33B（全热），NormalPack | 2 |
| M ≥ αN × 33B（全热），NormalSubtree | ⌈log₁₀₀₀(1/α)⌉ + 1 ≤ 4 |
| M < αN × 33B（swap 触发） | 上述 + 1 |

**关键性质**：上界是确定性的（worst-case），不是概率性的（expected）。

### Descriptor 模式分布

每个 prefix 下的 key 数 R = N/P = 1/α：
- R ≤ 128（单 KV block 容纳）→ TinyDirect
- 128 < R ≤ ~1000 → NormalPack
- R > ~1000 → NormalSubtree

α = 0.01 → R = 100 → 大部分 TinyDirect
α = 0.001 → R = 1000 → 开始出现 NormalSubtree

### 写代价模型

```
T_index_update ∝ P_affected × cost_per_prefix(R)
```

α 大 → P_affected 大 → 写代价高（已被实验数据验证：α 从 0.01→0.10，index update 从 37s→681s）

### 内存模型

```
Route Index Memory = P × 33B = αN × 33B
```

线性关系，已被实验数据验证。

---

## 6. Evaluation 计划

### 核心实验

| 实验 | 目的 | 自变量 |
|---|---|---|
| 读 vs α | 找到最优 α*，验证 I/O 上界 | α = 0.001~0.50，N=100M |
| 写 vs α | 验证写代价随 α 单调递增 | α = 0.001~0.20，N=10M |
| 规模不变性 | 证明延迟只取决于 α 不取决于 N | N = 1M/10M/100M，α=0.01 |
| Swap 阶梯 | 证明 swap 前后延迟是阶梯跳变 | M = 32MB~512MB，N=100M |
| 理论 vs 实测 | 验证解析模型误差 < 10% | 全部 (α, N) 组合 |

### 对比实验

| 对比对象 | 重点指标 |
|---|---|
| RocksDB | p99/p999 tail latency 稳定性对比 |
| RocksDB | 索引内存对比（同规模下） |
| RocksDB | 不同 compaction 阶段的延迟波动 vs MassKV 的恒定延迟 |

---

## 7. Contribution 总结

1. 提出 prefix-aware 索引架构，将索引复杂度从 O(N) 降到 O(P)
2. 揭示前缀聚合度 α 对读/写/内存的三角约束关系，证明存在最优 α*
3. 建立以 α 和 M 为参数的解析性能模型，点查 I/O 上界确定性 ≤ 4（全热）/ ≤ 5（swap）
4. 实现 MassKV，在 10⁶ 到 10⁹ 规模下验证理论模型与实测误差 < 10%

### 与现有工作的本质区别

| | RocksDB | MassKV |
|---|---|---|
| 索引内存 | O(N) | O(P) |
| 延迟保证 | 概率性（Bloom Filter） | 确定性（结构性） |
| 性能可预测性 | 依赖运行时状态 | 给定 α, M 可解析计算 |
| 调参 | DBA 经验 | 公式推导 |

---

## 8. 风险与待解决问题

- α* 的存在性需要实验一严格验证
- 前缀结构假设的适用范围需要在 related work 中充分论证
- swap 机制的 leaf-level 实现（design_swap_v2.md）尚未完成
- 需要补 p999/p9999 延迟指标
- 需要 RocksDB 对比实验基线
