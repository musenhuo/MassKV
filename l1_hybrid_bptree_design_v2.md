# L1 混合 B+Tree 设计方案 V2

## 1. 文档定位

本文档是 [l1_hybrid_bptree_design.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design.md) 的第二版设计文档。

V1 的职责是定义第一阶段原型目标；V2 的职责是基于当前已经落地的原型，明确：

- 哪些能力已经是当前正式基线
- 哪些能力仍然属于过渡实现
- 哪些能力必须在大规模实验和论文评测前继续补齐
- 后续开发应沿什么顺序推进

本文档不推翻 V1，而是在 V1 的基础上收敛出“当前原型版”向“正式实验版”过渡的设计边界。

## 2. 当前实现基线

截至目前，FlowKV 的 L1 已经不是单层易失路由索引，而是一个专用的双层路由结构：

- `RouteLayer`：由 Masstree 支撑的内存路由层
- `SubtreeLayer`：每个 partition 下的一棵只读 `L1SubtreeBPTree`

当前已经落地的核心模块如下：

- [lib/hybrid_l1/l1_hybrid_index.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_index.h)
- [lib/hybrid_l1/l1_hybrid_index.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_index.cc)
- [lib/hybrid_l1/l1_hybrid_rebuilder.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_rebuilder.h)
- [lib/hybrid_l1/l1_hybrid_rebuilder.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_rebuilder.cc)
- [lib/hybrid_l1/route_layout.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_layout.h)
- [lib/hybrid_l1/route_layout.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_layout.cc)
- [lib/hybrid_l1/route_partition.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_partition.h)
- [lib/hybrid_l1/subtree_bptree.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_bptree.h)
- [lib/hybrid_l1/subtree_bptree.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_bptree.cc)
- [lib/hybrid_l1/subtree_page.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page.h)
- [lib/hybrid_l1/subtree_page.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page.cc)
- [lib/hybrid_l1/subtree_page_store.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page_store.h)
- [lib/hybrid_l1/subtree_page_store.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page_store.cc)

## 3. V2 的设计目标

V2 不再讨论“是否做双层结构”，而是默认双层结构已经成立。V2 关注的是三件事：

1. 把当前原型中仍然模糊的边界收紧。
2. 把后续实验所需的功能闭环补齐。
3. 把论文中要声称的最终方案与过渡实现区分清楚。

## 4. 当前正式基线

以下能力在 V2 中视为当前正式基线：

- FlowKV 的 L1 是专用 PST 路由索引，而不是通用 KV 索引。
- L1 内部采用 `RouteLayer + SubtreeLayer` 双层结构。
- `RouteLayer` 当前已由 Masstree 实现 route key 到 partition 的定位。
- `SubtreeLayer` 是面向 PST 路由的只读 bulk-load B+Tree。
- `SubtreeLayer` 当前不支持在线 `insert/delete/split/merge`；更新通过 compaction 驱动的 bulk-load 与 partition 发布完成。
- L1 更新采用 partition 级局部重建，而不是整棵全局树原地维护。
- subtree 发布采用延迟回收模型。
- subtree 已具备页式物理布局与真实介质持久化接口。
- recovery 以 manifest 为真值源，通过批量重建恢复 `L1HybridIndex`。
- `Delete` 已在 memtable、flush、compaction、L1 路由与 recovery 的点路径上形成稳定正确性基线。

## 5. 当前仍属过渡实现的部分

以下能力虽然已经存在，但在 V2 中仍视为过渡实现，而不是最终论文版定稿：

### 5.1 RouteLayer 的固定 key-range 分区

当前 `RouteLayer` 已由 Masstree 实现，但 partition 的切分语义仍是固定 key-range。该方案工程上已可工作，但是否作为最终论文版路由策略，需要在 V2 中明确冻结。

### 5.2 查询主路径仍以内存 subtree 为主

当前 subtree 页式表示已经存在，且已支持落盘与恢复；但运行时查询仍走内存 `L1SubtreeBPTree`。如果后续论文需要强调页式 subtree 的运行价值，需要在 V2 中明确是否把页式访问提升为正式路径。

### 5.3 延迟回收当前采用 `shared_ptr`

当前方案足以支撑正确性和原型验证；是否要演进为更低开销的 epoch/RCU 风格机制，留待后续实验数据决定。

## 6. V2 必须补齐的功能闭环

要进入正式大规模正确性实验和后续性能实验，V2 必须明确并推动以下闭环：

### 6.1 删除语义闭环

当前 `Delete` 的点路径语义已经收口，并已进入稳定 smoke/ctest 基线。V2 仍需继续推进：

- 将 `Delete` 从当前第一层随机 stress 继续扩展到更大规模与更多分布
- 将 `Delete` 纳入第二到第四层结构一致性、长时间运行与差分回归
- 继续验证删除语义在大规模长时间运行下不会出现旧值复活

### 6.2 结构一致性回归闭环

除端到端值语义外，还必须形成稳定的结构一致性检查。当前第二层结构回归已建立一条稳定基线，已覆盖：

- `L1HybridIndex::Validate()`
- `ExportAll()` 导出顺序与记录去重
- 导出 `SubtreeRecord` 与 `TaggedPstMeta` 的字段一致性
- `PickOverlappedL1Tables()` 对导出记录的结构可达性
- delete 后多轮 flush/compaction 的结构一致性
- recovery 后的结构一致性

后续仍需继续扩展：

- `L1HybridIndex::Validate()`
- partition 边界约束
- subtree 记录有序性
- overlap 路由语义
- recovery 前后结构一致性

### 6.3 长时间运行闭环

V2 需要覆盖。当前第三层长时间运行回归已建立一条稳定基线，已覆盖：

- 多轮随机 `Put/Delete/Get`
- 每轮 `BGFlush -> BGCompaction`
- 每轮 shadow model 对拍
- 每轮 L1 结构一致性检查
- 每隔固定 round 的 `recover=true` 重启验证
- 多轮 delete、局部重建、recovery 交替后的长期稳定性

后续仍需继续扩展：

- 多轮 flush / compaction
- 多轮 partition 局部重建
- 多轮重启恢复
- 周期性随机查询

目标是证明双层结构在长时间运行后没有累积性错误。

### 6.4 差分回归闭环

V2 需要建立与原始 L1 行为的差分对拍基线。当前第四层差分回归已建立一条稳定基线，已覆盖：

- 当前双层 L1 与原始单层 L1 基线语义的点查候选选择一致性
- 当前双层 L1 与原始单层 L1 基线语义的 overlap 选择一致性
- delete 后多轮 flush/compaction 下的差分一致性
- recovery 后的差分一致性

后续仍可继续扩展：

- 候选 PST 选择语义
- overlap 选择语义
- recovery 后的查询语义

## 7. V2 的最终实验版冻结点

在进入性能实验前，必须冻结以下问题：

1. 最终实验版是否继续使用“Masstree 路由 + 固定 key-range partition”这一路线。
2. 最终实验版查询是否仍以内存 subtree 为主。
3. `Delete` 是否已从当前第一层随机正确性基线扩展到完整四层回归。
4. 当前 subtree 页式持久化在论文中承担的角色：
   - 仅作为物理布局与恢复接口
   - 还是作为后续页式访问路径的基础

## 8. 后续开发顺序

V2 建议的开发顺序如下：

1. 先把已进入第一层随机 stress 的 `Delete` 语义继续扩展进完整四层正确性与回归框架。
2. 再完成四层正确性与回归实验框架：
   - 端到端语义回归
   - 结构一致性回归
   - 长时间运行回归
   - 差分回归
3. 在正确性边界冻结后，再进入性能实验。
4. 最后决定是否继续推进更强的 `RouteLayer` 或页式查询主路径。

## 9. V2 对论文写作的意义

V1 更像“设计原型说明”；V2 才是“实验版系统设计约束”。

V2 的作用是把当前工程状态映射为论文可表述的系统边界，避免后续出现以下混淆：

- 已实现原型与最终实验版混淆
- 过渡策略与最终设计混淆
- 正确性基线未冻结就开始性能实验

## 10. 当前结论

当前双层 L1 原型已经基本完成，V1 的核心目标已经大体落地。

当前最合理的推进方式不是继续向 V1 里堆内容，而是以 V2 为新的设计基线：

- V1 保留为第一阶段原型设计
- V2 作为后续开发、回归实验、性能实验和论文系统描述的当前工作文档
