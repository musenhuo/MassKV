# L1 混合 B+Tree 设计方案 V3

## 1. 文档定位

本文档是在 [l1_hybrid_bptree_design.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design.md) 与 [l1_hybrid_bptree_design_v2.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v2.md) 基础上的第三版设计文档。

V3 的目标不是继续描述当前已经完成的原型，而是明确后续演进到方向 B 时的设计冻结点：

- `RouteLayer` 不再停留在固定 key-range 路由语义
- `RouteLayer` 进入真正的 prefix routing
- `SubtreeLayer` 采用“先 B1、后 B2”的两阶段收敛路线
- 对大 prefix bucket 的更新代价，引入 CoW 作为正式备选更新协议

从本版开始，后续开发若涉及方向 B，应优先以 V3 为准。

## 2. V3 的目标设计

方向 B 的目标语义如下：

- 16B key 被拆为前 8B `prefix` 与后 8B `suffix`
- `RouteLayer` 的 Masstree 负责 `prefix -> partition/subtree`
- 相同 `prefix` 的 key 被路由到同一个 `SubtreeLayer`
- `SubtreeLayer` 在该 prefix 范围内负责后续查找与范围扫描

这意味着系统层的双层结构变成：

- `RouteLayer`：prefix routing
- `SubtreeLayer`：prefix-local B+Tree

## 3. partition 的新语义

在 V3 中，`partition` 不再是固定 key-range 分区，而是 prefix bucket。

第一版方向 B 建议采用：

- 一个 partition 对应一个 `prefix`

即：

- `partition = prefix bucket`
- `RouteLayer` 上的 Masstree key = `prefix`
- `RouteLayer` value = `partition_id`

这比“一个 partition 覆盖多个 prefix”的方案更直接，也更容易和论文中的“前 8B 路由、后 8B 子树查找”叙事对齐。

## 4. 大 partition 的更新代价与 CoW

### 4.1 问题

方向 B 的一个现实问题是：

- 若某个 prefix 很热
- 或某个 prefix 下累计的 subtree 很大

则“整棵 prefix subtree 重建后发布”的代价可能过高。

### 4.2 设计判断

因此，V3 明确：

- 对小到中等规模 prefix subtree，仍优先采用当前的 `bulk-load + publish`
- 对明显过大的 prefix subtree，可以引入 CoW（Copy-on-Write）更新协议

CoW 的作用不是替代整个方向 B，而是作为：

- prefix-local subtree 的增量更新手段

### 4.3 CoW 在方向 B 中的定位

CoW 只用于：

- 不原地修改旧 subtree
- 沿更新路径复制节点
- 构造新版本 subtree
- 原子替换 prefix 对应的 subtree root
- 旧版本延迟回收

也就是说，CoW 作用于：

- `SubtreeLayer` 内部

而不是：

- `RouteLayer`

### 4.4 当前建议

V3 建议先不在第一步直接实现 CoW，而是：

1. 先把方向 B 的 prefix routing 路径做正确
2. 先以 bulk-load 的 prefix subtree 跑通正确性与性能基线
3. 再把 CoW 作为“大 prefix subtree 更新优化”单独引入

因此，CoW 在 V3 中被定义为：

- **方向 B 的正式优化路线**
- **不是第一步必须落地的基础语义**

### 4.5 CoW 的选择逻辑

V3 不采用自适应学习，而采用固定的 rule-based policy。

每次 prefix subtree 更新前，系统先计算：

- `existing_record_count`
- `target_record_count`
- `changed_record_count`
- `changed_leaf_span`

然后用固定阈值选择：

- `BulkLoadRebuild`
- `CowPatch`

第一版规则如下：

- 若 subtree 过小，直接 `BulkLoadRebuild`
- 若不存在旧 subtree，直接 `BulkLoadRebuild`
- 若 `changed_record_count` 超阈值，直接 `BulkLoadRebuild`
- 若 `changed_ratio` 超阈值，直接 `BulkLoadRebuild`
- 若 `changed_leaf_span` 超阈值，直接 `BulkLoadRebuild`
- 其他情况走 `CowPatch`

这个策略的定位是：

- 可解释
- 可复现
- 便于后续做消融实验

而不是：

- 在线学习
- 动态调参系统

### 4.6 当前 CoW 落地形态

当前代码已经实现的 CoW 形态是：

- 先用 rule-based policy 选择 `BulkLoadRebuild` 或 `CowPatch`
- `CowPatch` 当前采用 **完整的 immutable CoW 版本**

具体来说：

- 可复用任意位置的未变化叶节点，而不再局限于前后叶段
- 对新旧版本 child 序列完全一致的内部节点，直接复用旧内部节点
- 只为变化叶段和无法复用的内部节点生成新对象

因此，当前 CoW 的实现边界是：

- 已经具备真实叶节点复用与内部节点复用
- 采用 immutable subtree 上的“精确 child 序列复用”策略
- 还没有走到可变 B+Tree 上的逐层原地 path-copy 设计

这条路线的优点是：

- 在不重写整个 subtree 实现的前提下，把完整 CoW 主路径接入主系统
- 与当前 immutable subtree 结构兼容
- 能为后续更细粒度 path-copy 或并行优化继续留出接口

## 5. SubtreeLayer 的两阶段路线：B1 -> B2

### 5.1 B1

第一阶段先采用 B1：

- `RouteLayer` 已按 prefix 路由
- `partition` 已是 prefix bucket
- `SubtreeLayer` 仍保留当前 `SubtreeRecord { min_key, max_key, seq_no, table_idx }`
- subtree 内部仍保留现有的 `max_key + range check` 路由语义

这样做的好处：

- 只改 RouteLayer 与 Rebuilder 的分桶语义
- 尽量保留当前 subtree、recovery、四层回归成果
- 能更快形成方向 B 的第一版可运行系统

### 5.2 B2

第二阶段再收紧为 B2：

- prefix 已固定
- subtree 内部显式按 suffix 组织
- 点查更接近“前缀确定后做标准后缀查找”
- range query 进入 prefix-local suffix 扫描模型

当前实现进展：

- `SubtreeRecord` 已增加 prefix-local 路由字段：
  - `route_prefix`
  - `route_min_suffix`
  - `route_max_suffix`
- `Rebuilder` 已从“跨 prefix 复制全局记录”收敛为“为每个触达 prefix 生成裁剪后的 local fragment”
- `subtree_bptree` 已从 `max_key` 排序切到按 local route key 排序
- `L1HybridIndex` 的点查询、范围查询和结构校验已优先使用 local fragment 语义：
  - 点查询按 `route_prefix + suffix` 命中
  - 范围查询按 prefix-local suffix 区间扫描
  - 校验按 `MatchesLocalFragment(prefix)` 验证 fragment 裁剪正确性
- `L1HybridIndex` 已区分两套导出口径：
  - table-level `ExportAll()`：给现有上层 PST 元数据视图与兼容逻辑使用
  - local-fragment 导出：给 B2 结构检查与调试使用
- `Version` 的一致性修复路径已开始切到 local-fragment 视图：
  - `L1TreeConsistencyCheckAndFix()` 现在按 prefix-local fragment 重叠关系执行修复
- subtree 页编码/恢复已同步支持这些 B2 字段

这意味着：

- 当前系统已经进入 **B2 的第一阶段实现**
- 但仍保留全局 `min_key/max_key` 作为 PST 元数据和上层去重/校验依据
- 还没有进入更激进的 CoW 或并行 scan 优化阶段

### 5.3 当前冻结结论

V3 明确采用：

- **先 B1**
- **后 B2**

即：

- 第一版方向 B 不要求 subtree 立即完全 suffix 化
- 先把 prefix routing 做正确
- 再视实验数据决定是否继续推进到 B2

## 6. 什么情况下会跨 prefix

方向 B 中，“跨 prefix”是一个必须说清楚的问题。

### 6.1 点查询

对单个 key 的点查询：

- 正常情况下不会跨 prefix
- 因为 key 的前 8B 唯一确定一个 prefix bucket

因此，V3 建议点查询语义为：

- 默认只进入一个 prefix subtree
- 不做跨 prefix fallback

### 6.2 范围查询

范围查询会跨 prefix，典型情况包括：

1. `[start, end]` 的起点和终点前 8B 不相同
2. 查询区间覆盖多个相邻 prefix bucket
3. 长范围 scan 自然横跨多个 prefix

举例：

- `start = (prefix=0x10, suffix=...)`
- `end   = (prefix=0x13, suffix=...)`

那么该范围查询会覆盖：

- `prefix=0x10`
- `prefix=0x11`
- `prefix=0x12`
- `prefix=0x13`

### 6.3 为什么这很重要

因为跨 prefix 会导致：

- 需要访问多个 subtree
- 若串行执行，需要 subtree 切换
- 若并行执行，需要多路 merge

因此，V3 明确：

- 点查询不跨 prefix
- 范围查询允许跨 prefix

## 7. 范围查询的 V3 约束

在方向 B 下，范围查询第一版建议：

- 先采用串行 prefix-by-prefix 扫描
- 暂不把并行 range scan 作为基础语义

原因：

- 先保证正确性与可验证性
- 避免在 prefix routing 尚未稳定时引入额外 merge/调度复杂度

并行 range scan 在 V3 中的定位是：

- **后续优化选项**

而不是：

- **方向 B 第一版的必要组成**

当前实现已经进入该优化项的第一版：

- `L1HybridIndex::RangeScan()` 增加了可开关的并行路径
- 仅当跨 prefix 分区数超过阈值时才并行
- 并行执行各 partition 的 local suffix 扫描
- 上层用 `RecordRouteKeyLess` 做多路有序 merge
- 小范围/少分区查询仍保持串行，避免调度开销反噬

## 8. V3 的实施优先级

### 8.1 必须先做

1. prefix/suffix 抽象工具
2. `partition` 语义从 range bucket 改为 prefix bucket
3. `RouteLayer` 改成 `prefix -> partition` 的 Masstree 路由
4. `Rebuilder` 改成按 prefix 分桶
5. 点查询改成单 prefix subtree 语义
6. 范围查询改成串行 prefix-by-prefix 扫描

当前已完成：

- prefix/suffix 抽象工具
- `partition` 语义已切到 prefix bucket
- `RouteLayer` 已切到 `prefix -> partition` 的 Masstree 路由
- `Rebuilder` 已切到按 prefix 分桶
- 对跨 prefix 的 `SubtreeRecord`，B1 当前采用“复制到所有触达 prefix bucket”的兼容语义
- 点查询已收紧为单 prefix subtree 语义
- 范围查询已收紧为串行 prefix-by-prefix 扫描语义
- `SubtreeRecord` 已进入 B2 第一阶段：prefix-local fragment + local suffix 边界
- `subtree_bptree` 已改为按 local route key 组织
- subtree 页编码/恢复已同步到 B2 字段

当前未完成：

- 更纯的 B2 仍未完全完成：
  - 上层导出/调试路径仍以全局 `min_key/max_key` 为主
  - 尚未引入并行 range scan
  - 尚未做 prefix bucket 自适应治理

### 8.2 建议第二步做

1. `Validate()` 改成 prefix bucket 不变式
2. recovery 重建改成 prefix bucket 语义
3. 四层正确性回归改到 prefix routing 版本

### 8.3 后续优化做

1. CoW 版 prefix subtree 更新
2. subtree 从当前 B2 第一阶段继续收紧到更纯的 suffix-local 组织
3. 并行 range scan + 多路 merge

## 9. V3 的优化路线

V3 中已经正式纳入、需要后续持续跟进的优化项如下：

### 9.1 大 prefix subtree 的 CoW 更新

- 目标：降低热点 prefix / 大 subtree 的重建代价
- 位置：`SubtreeLayer`
- 语义：不原地更新、路径复制、原子替换 root、旧版本延迟回收

### 9.2 从 B1 收敛到 B2

- 目标：把 subtree 从“prefix 路由 + 现有 `SubtreeRecord` 语义”推进到更纯的 suffix-local 组织
- 前提：先有稳定的 prefix routing 正确性与基线实验

### 9.3 并行 range scan

- 目标：缓解跨 prefix 范围查询的 subtree 切换成本
- 方式：多 subtree 并行扫描 + k-way merge
- 定位：优化项，而不是方向 B 第一阶段的基础语义

### 9.4 prefix bucket 自适应治理

- 目标：处理 prefix 热点倾斜、单 bucket 过大、更新放大等问题
- 可选策略：
  - 热点 prefix 监控
  - 超大 prefix bucket 的 CoW 化
  - 后续必要时做更细粒度 split

当前实现先采用轻量治理，而不是结构拆分：

- 每个 `RoutePartition` 维护静态 bucket 健康状态：
  - `hot_prefix`
  - `prefer_cow`
  - `prefer_parallel_scan`
- 这些状态由固定 record-count 阈值计算得到
- 更新路径据此放宽热点 bucket 的 CoW 触发条件
- 范围查询据此在热点 bucket 参与时更积极启用并行扫描

这条轻量治理路线的定位是：

- 不改变 `partition = prefix bucket` 的结构定义
- 不引入 bucket split/merge
- 先用最小代价处理 prefix skew

## 10. V3 的当前结论

方向 B 的正式结论如下：

- `RouteLayer` 进入 prefix routing
- `partition` 的正式语义为 prefix bucket
- `SubtreeLayer` 先采用 B1，再按需要收紧到 B2
- 对大 prefix subtree 的更新代价，CoW 是正式优化路线，但不是第一步必做项
- 点查询默认不跨 prefix
- 范围查询允许跨 prefix，第一版先串行实现

当前代码状态进一步收敛为：

- prefix/suffix 抽象工具已落地
- `partition` 已按 prefix bucket 组织
- `RouteLayer` 已按 prefix 做 Masstree 路由
- `Rebuilder` 已按 prefix 分桶
- `SubtreeRecord` 已进入 B2 第一阶段：prefix-local fragment + local suffix 边界
- `subtree_bptree` 已按 local route key 组织
- `L1HybridIndex` 已按 local suffix 语义执行点查询、范围扫描和结构校验
- `CowPatch` 已进入完整 immutable CoW：任意叶段复用 + 内部节点复用
