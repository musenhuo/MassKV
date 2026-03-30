# L1 Hybrid B+Tree 设计方案 V6（增量补丁驱动 + Leaf-Stream Bulk-Load）

## 1. 版本定位

V6 在 V5 的 `prefix -> root_page_ptr` 直达读路径基础上，收敛写路径与 compaction 索引更新逻辑：

- 去掉“基于全量 L1 元数据的二次推导”主路径
- 由数据合并阶段直接产出 `prefix` 级增量补丁
- 索引更新阶段按更新密度在 `CoW` 与 `bulk-load` 之间自适应选择
- bulk-load 改为 `leaf-stream` 模式，不再依赖全量 KV block 元数据读取

## 2. 问题复盘（V5 的写瓶颈根因）

当前 compaction 后段耗时偏高，核心在于：

1. 合并阶段与索引更新阶段信息传递太粗，只传“新增/删除表”。
2. 索引阶段需要再次遍历当前有效 L1 表全集，按 prefix 重新推导受影响范围。
3. prefix 比例越高，二次推导与重建成本近似线性放大。

V6 目标是把“已在合并阶段可得的信息”直接传到索引阶段，避免重复推导。

## 3. 目标与不变约束

### 3.1 目标

- 写路径中索引更新改为增量驱动
- 显著降低 compaction 后段 `index update` 时间
- 保持点查主路径不变：`prefix -> root_page_ptr -> suffix 下钻 -> leaf 直达 KV block`

### 3.2 不变约束

- layer0 必须保持 Masstree（内存路由）
- layer1 必须保持 SSD 驻留 B+Tree（16KB 页）
- 叶子 payload 仍为 `kv_block_ptr + offset + count`
- 发布与恢复必须保持原子可恢复语义

## 4. 主线一：增量补丁驱动索引更新

### 4.1 核心思想

数据合并阶段不再只产出“新旧表集合”，而是直接产出“索引增量补丁批次（DeltaBatch）”。  
索引更新阶段只消费该批次，不再做全量 L1 元数据二次推导。

### 4.2 跨阶段传递结构（逻辑定义）

以 `prefix` 为粒度组织补丁：

- 批次头：
  - `batch_id`
  - `l1_seq`
  - `checksum`
- 前缀补丁项：
  - `prefix`
  - `old_root_page_ptr`
  - `old_generation`
  - `old_record_count`
  - `ops[]`
- 单条操作（op）：
  - `type`：`add/delete/replace`
  - `suffix` 或 `suffix_range`
  - `kv_block_ptr`
  - `offset`
  - `count`

其中 `kv_block_ptr/offset/count` 与当前 leaf value 编码完全对齐。

### 4.3 执行流程（写路径后半段）

1. 合并阶段生成新数据块并同时记录 prefix 增量（形成 `DeltaBatch`）。
2. 索引阶段按 `prefix` 取补丁并读取该前缀当前根指针。
3. 根据更新密度选更新模式（CoW 或 bulk-load）。
4. 产出每个 prefix 的 `new_root_page_ptr` 与 `reclaim list`。
5. 与 manifest 在同一原子批次发布新根映射与版本号。
6. 提交成功后执行旧页回收；失败则按批次协议回滚/重放。

### 4.4 密度决策规则（V6）

定义：

`density = changed_records / max(1, old_record_count)`

推荐阈值（首版）：

- `density <= 5%`：优先 CoW
- `density >= 20%`：优先 bulk-load
- `(5%, 20%)`：结合“触达叶页比例”二次决策  
  - 触达页低：CoW  
  - 触达页高：bulk-load

热前缀可放宽 CoW 阈值，但必须记录治理指标并可回退。

## 5. 主线二：Leaf-Stream Bulk-Load 改造

### 5.1 现状问题

bulk-load 路径中仍存在对外部元数据的额外依赖，导致不必要读取与推导。

### 5.2 V6 改造目标

bulk-load 仅基于两类输入：

1. 原子树叶子记录流（按 suffix 有序）
2. 当前 `DeltaBatch` 的 prefix 补丁流

不再要求全量读取该子树对应的 KV block 元数据。

### 5.3 Leaf-Stream 逻辑

1. 顺序扫描旧树叶子页，得到旧记录流（有序）。
2. 将补丁流按 suffix 排序。
3. 对两条流做 merge（类似归并）：
  - `add`：插入新记录
  - `delete`：删除目标记录
  - `replace`：替换目标记录 payload
4. 生成新有序记录流后，一次性 bulk-load 构造新树。

这样 bulk-load 仍是整树重建，但输入准备成本显著下降。

## 6. 一致性与 durable 协议（V6）

### 6.1 原子提交边界

必须保证以下信息同批次提交：

- manifest 的表增删与版本推进
- `prefix -> new_root_page_ptr` 发布
- L1 hybrid state 快照（用于恢复）

### 6.2 崩溃恢复要求

- 基于 `batch_id + l1_seq` 做幂等重放
- 若检测到未完成批次：
  - 若根映射未发布：回滚中间页
  - 若根映射已发布：补齐状态并继续回收

### 6.3 回收策略

- 提交成功后再回收旧页
- CoW 场景仅回收不共享页（差集回收）

## 7. 可观测性与实验指标（新增）

V6 必须新增以下实测指标：

- `delta_prefix_count`
- `delta_ops_count`
- `index_update_total_ms`
- `index_update_cow_ms`
- `index_update_bulk_ms`
- `cow_prefix_count`
- `bulk_prefix_count`
- `leaf_stream_merge_ms`
- `rebuild_fallback_count`

目标不是理论估算，而是线上实测拆分。

## 8. 分阶段落地计划

### 阶段 A：协议与管道打通（先做）

- 增加 `DeltaBatch` 结构
- 合并阶段生成 prefix 增量补丁
- 索引阶段改为消费补丁，不再扫描全量 L1 表做二次推导

### 阶段 B：密度决策与双执行器

- 接入 `density` 决策器
- 接通 CoW 路径与 bulk-load 路径分流

### 阶段 C：Leaf-Stream Bulk-Load

- 实现叶子记录流 + 补丁流 merge
- 替换现有 bulk-load 输入准备逻辑

### 阶段 D：协议收口与性能回归

- 打通 durable 提交与恢复
- 运行 1M/10M 写入回归，验证 compaction 后段耗时下降

## 9. 与 V5 的关系

V6 不改动 V5 的核心读路径语义，只改写路径后段与索引更新机制。  
因此 V6 是“写路径优化版”，不是“读路径重设计版”。

## 10. 验收标准（V6）

1. 正确性：
  - 点查/范围查与 V5 一致
  - 崩溃恢复后索引状态一致
2. 性能：
  - `compaction_total_time_ms` 随 prefix 比例增长斜率显著下降
  - `ingest_throughput_ops` 在 10M 写入场景可观提升
3. 工程：
  - 支持开关回退（可切回 V5 旧路径）
  - 关键指标在结果报告中自动输出

