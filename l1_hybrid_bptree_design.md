# L1 混合 B+Tree 设计方案

## 1. 设计目标

本文档用于定义 FlowKV 中新的 L1 混合索引模块的第一阶段实现目标。

这里的目标不是替换整个 FlowKV 的索引体系，而是做一件更聚焦的事情：

- 保持 `Memtable` 和 `L0` 不变
- 替换当前 `Version` 中 L1 的易失路由索引
- 在内存中引入一个轻量级路由层
- 在该路由层之下引入一个专用于 L1 的 B+Tree 层
- 让 L1 的更新方式与 FlowKV 的 compaction 保持一致，而不是走传统 OLTP 风格的在线细粒度维护

该设计后续要支撑论文写作，因此必须明确假设、边界和正确性约束。

## 2. 当前系统中的 L1 基线行为

当前 `Version` 中维护了两部分与 L1 相关的数据：

- `level1_tables_`：保存 `TaggedPstMeta` 的向量
- `level1_tree_`：一个 `Index*`

它的实际语义并不是“普通 KV 索引”，而是“PST 路由索引”：

- 索引键：`PSTMeta::MaxKey()`
- 索引值：该 PST 在 `level1_tables_` 中的下标
- 查找语义：对 `max_key` 执行 `lower_bound(key)`
- 命中后仍需二次校验：`table.MinKey() <= key <= table.MaxKey()`

这套逻辑被以下路径依赖：

- `Version::Get()`
- `Version::GetLevel1Iter()`
- `Version::PickOverlappedL1Tables()`
- `Version::L1TreeConsistencyCheckAndFix()`

因此，当前 L1 索引本质上是一个“有序 PST 元数据路由器”，而不是一个通用 Value 索引。

## 3. 新模块的边界

建议在 `lib/` 下新建目录：

- `lib/hybrid_l1/`

该目录中的模块只负责一件事：L1 路由。

第一阶段范围：

- 只服务 `Version` 的 L1
- 只索引 L1 中的 PST 元数据
- 只支持 `FLOWKV_KEY16`
- 支持点查路由和有序范围枚举
- 更新由 compaction 驱动

第一阶段不做：

- 替换 memtable
- 替换 L0
- 把该模块做成全局通用索引抽象
- 做一个支持任意在线插删分裂合并的 SSD B+Tree
- 持久化这棵路由树本身

## 4. 核心设计选择

第一版不应该从“全功能并发可变 B+Tree”开始。

更合理的起点应该顺着 FlowKV 当前的数据生命周期来设计：

- L1 的变化主要发生在 compaction 之后
- compaction 的输出天然是有序的
- 有序输出非常适合 bulk-load 构建树结构

因此推荐的模型是：

- 构建可批量加载的、不可变或近似不可变的 subtree
- 构建完成后原子发布
- 旧 subtree 延迟回收

这比实现一个支持任意在线 `insert/split/delete/merge` 的并发 B+Tree 更符合当前系统，也更容易保证正确性。

## 5. 总体架构

新的 L1 混合路由结构包含两个逻辑层次。

### 5.1 路由层（RouteLayer）

位置：

- DRAM

结构：

- 一个轻量的有序路由结构
- 第一版可以继续使用 Masstree，也可以使用更简单的有序路由结构

职责：

- 根据 key 的前缀或范围边界，将查找路由到某个 subtree
- 定位第一个可能命中的 subtree
- 支持跨 subtree 的顺序遍历

为避免与 FlowKV 全局的 `L0/L1` 混淆，本文内部统一使用：

- `RouteLayer`：L1 模块内部的内存路由层
- `SubtreeLayer`：L1 模块内部的 B+Tree 层

### 5.2 子树层（SubtreeLayer）

位置：

- 逻辑上面向 SSD
- 第一阶段可以先作为易失结构存在于内存中
- 但节点布局从一开始就应按页组织，为后续 SSD 化做准备

结构：

- 高扇出的 B+Tree
- 内部节点存 fence key 与 child 指针
- 叶子节点存有序的 PST 路由记录

职责：

- 执行 `lower_bound`
- 返回有序候选 `table_idx`
- 支持范围枚举，供 overlap 检测和 scan 使用

## 6. 数据模型

### 6.1 Key 类型

第一阶段假设 `FLOWKV_KEY16`。

原因：

- 该方案的理论基础依赖于“16B key 被拆为 prefix + suffix”
- 如果 key 只有 8B，那么“前 8 字节做路由”会直接耗尽整个 key，层次设计意义明显下降

### 6.2 路由记录

B+Tree 叶子不应该存真实 value，而应该存路由元数据。

建议的叶子记录：

```text
struct L1RouteEntry {
    KeyType min_key;
    KeyType max_key;
    uint32_t seq_no;
    uint32_t table_idx;
}
```

理由：

- `table_idx` 用于回到 `level1_tables_`
- `min_key` 用于范围校验与 overlap 判断
- `max_key` 是当前查找语义的自然搜索键
- `seq_no` 可用于未来的重复项判定与版本规则

备选方案：

- 叶子中直接存 `TaggedPstMeta*`

第一阶段不推荐：

- 让树直接拥有 manifest 或 `Version` 内部元数据对象的指针生命周期

原因：

- 在 subtree 重建与旧版本回收时，更容易产生悬挂和生命周期管理问题

### 6.3 路由分区

`RouteLayer` 不应该为每一个 PST 单独建路由项。

更合理的方式是：

- 一个路由分区覆盖一个连续 key-range
- 一个分区下面挂一棵 subtree
- subtree 内包含多个 `L1RouteEntry`

建议的分区描述：

```text
struct RoutePartition {
    KeyType route_min;
    KeyType route_max;
    SubtreeRoot* subtree;
    uint64_t generation;
}
```

## 7. 为什么不直接做一个全局可变 B+Tree

采用分区化 subtree 的好处有三点：

- rebuild 的代价被局部化，只重建受影响的 key-range
- 发布简单，只需替换某些 route partition 的指针
- 后续若落到 SSD，每棵 subtree 都可以自然映射为一段连续布局

这比一个单体可变 B+Tree 更贴合“prefix 路由到 subtree”的理论设计。

## 8. 模块对外接口

新的 L1 模块不应该强行塞进现有通用 `Index` 接口。

它应该先定义一个 L1 专用接口。

建议主类：

```text
class L1HybridIndex;
```

建议提供的操作：

- `LookupCandidate(key) -> optional<table_idx>`
- `LookupCandidates(key, limit) -> ordered table_idx list`
- `RangeQuery(start, end) -> ordered table_idx list`
- `BuildPartition(sorted_entries) -> PartitionBuildResult`
- `PublishPartitions(changeset)`
- `DebugValidate()`

面向 compaction 的关键接口：

- `ApplyCompactionDelta(removed_tables, added_tables)`

该接口内部可以决定：

- 局部修补某个 partition
- 重建若干 partition
- 或者在第一版中直接重建整个 L1 模块

## 9. 查询语义

### 9.1 点查

对查找 key `k`：

1. 通过 `RouteLayer` 找到第一个可能包含 `k` 的 partition
2. 在该 partition 对应的 `SubtreeLayer` 中按 `max_key` 做 `lower_bound(k)`
3. 取出候选 `table_idx`
4. 校验 `min_key <= k <= max_key`
5. 如果首个候选不满足校验，则继续在该 subtree 内向后扫描
6. 如果该 subtree 扫描完仍未命中，则必要时进入下一个 partition

这个逻辑保持了当前 `Version::Get()` 的正确性模式。

### 9.2 有序候选扫描

对于 `GetLevel1Iter()` 或 overlap 选择这类场景：

1. 先定位第一个与目标范围相交的 partition
2. 在 subtree 的叶子层按 key 顺序扫描
3. 当前叶子结束后跳到下一叶子
4. 当前 partition 结束后跳到下一个 partition

这样可以保持当前系统依赖的“按 `max_key` 升序返回候选 PST”的语义。

## 10. 更新语义

### 10.1 更新来源

L1 的主要变化来自 compaction。

每次 compaction 天然会产生：

- 一批失效的旧表
- 一批新生成的输出表

这是重建 route partition 的最佳时机。

### 10.2 推荐更新协议

对一次 compaction：

1. 收集被删除与新增的 L1 route entries
2. 依据 key range 确定受影响的 route partition
3. 在读路径之外构建新的 subtree 实例
4. 原子替换 partition 中的 subtree 指针
5. 旧 subtree 在 grace period 之后延迟回收

### 10.3 为什么不直接用 `Put/Delete`

如果直接在线对 B+Tree 执行 `Put/Delete`，就必须解决：

- 并发 split/merge 正确性
- 读写同步
- compaction 过程中 iterator 的稳定性
- 更复杂的 crash/recovery 语义

第一阶段完全没有必要承担这些复杂度。

## 11. 并发模型

第一阶段建议采用读优化的发布模型，而不是完全可变树上的细粒度并发控制。

推荐模型：

- 读者不对 subtree 内部持重锁
- 后台构建线程在读路径之外生成新版本 subtree
- 发布时原子交换 partition 指针
- 回收通过 epoch 或 RCU 风格的延迟释放完成

这比“在线维护一个通用并发 B+Tree”更符合 FlowKV 当前的 compaction 驱动更新模式。

## 12. 与 `Version` 的集成方式

`Version` 不应再把 L1 当作普通 `Index*`。

建议调整为：

- `level0_trees_` 继续保持 `Index*`
- `level1_tree_` 替换为 `L1HybridIndex*`

原因：

- L0 依旧是当前简单 PST ring 路由结构
- L1 将具备不同的数据模型和更新协议
- 如果强行塞进 `Index`，会扭曲接口，使实现逻辑不自然

需要修改的 `Version` 接口包括：

- `InsertTableToL1()`
- `DeleteTableInL1()`
- `Get()`
- `GetLevel1Iter()`
- `PickOverlappedL1Tables()`
- `L1TreeConsistencyCheckAndFix()`

## 13. 与 Manifest / Recovery 的关系

第一阶段不建议持久化这棵混合路由树本身。

恢复规则建议保持简单：

- manifest 仍然是 L1 PST 元数据的真值源
- 恢复时先重建 `level1_tables_`
- 再由恢复后的 `TaggedPstMeta` 重建 L1 混合路由索引

原因：

- 正确性更容易论证
- 工程风险更低
- 路由树属于可导出元数据，不是必须持久化的主数据

这与当前 `Version` 的易失索引重建思路是一致的。

## 14. 与论文论点的关系

论文中的表述必须比下面这些说法更克制、更严谨：

- “确定性的 O(1) 读延迟”
- “零写放大”

更稳妥的说法应该是：

- 在明确的数据分布与 prefix 假设下，L1 路由的 I/O 次数被小常数上界所约束
- 索引维护开销被折叠进已有 compaction，而不是额外引入在线 in-place index maintenance 开销
- 在选定的 prefix 划分模型下，DRAM 占用主要与 route partition 数量相关，而不是直接与 L1 中所有表项数量线性相关

## 15. 第一阶段实现约束

第一版应主动接受以下约束：

- 仅支持 `FLOWKV_KEY16`
- `RouteLayer` 先采用固定 prefix 宽度
- `SubtreeLayer` 先做易失版本
- 只支持 bulk-load
- 不支持在线页分裂/合并
- 不要求 L1 模块兼容现有 `Index` 多态

这些约束是合理的，因为它们能显著降低工程噪音，让研究假设先变得可验证。

## 16. 编码前必须讨论清楚的问题

在开始实现之前，至少要先定下这几个问题：

1. `RouteLayer` 的前缀宽度到底是固定 64 bit，还是做成可配置 `b` bit
2. 一个 route partition 是严格对应一个 prefix 值，还是对应一个更大的 key bucket
3. subtree 叶子里只存 `table_idx`，还是直接存完整 `L1RouteEntry`
4. 点查时首个候选失败后，是只在当前 subtree 内继续扫，还是允许跨 partition 继续找
5. 第一版发布机制先用“粗粒度锁 + 原子指针交换”，还是直接引入 epoch 回收
6. 第一版是“局部 partition 重建”，还是为了先保证正确性直接“整棵 L1 重建发布”

## 17. 推荐实现顺序

在设计确认后，建议按以下顺序编码：

1. 定义 `lib/hybrid_l1/` 的核心接口与数据结构
2. 实现一个可 bulk-load 的内存版 subtree B+Tree
3. 实现 route partition 与发布逻辑
4. 与 `Version` 对接，仅替换 L1 路由
5. 增加与旧 L1 逻辑对拍的验证工具
6. 在逻辑稳定后，再考虑 subtree 页布局如何落到 SSD

## 18. 最终建议

第一版正确的方向应该是：

- 一个专用的 L1 模块
- 一个内存中的路由层
- 若干可 bulk-load 的 B+Tree subtree
- 通过 compaction 驱动更新
- 通过原子发布切换版本
- 通过 manifest 重建恢复

不建议：

- 继续沿用 `hmasstree` 这条路线
- 强迫新模块套进当前通用 `Index` 接口
- 一开始就引入复杂的第三方并发 B+Tree 作为核心实现

这样的设计在系统逻辑上更自洽，也更适合作为后续论文方法部分的基础。
