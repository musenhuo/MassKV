# FlowKV L1 Hybrid Progress 20260302_073800

## 当前维护规则

- 若继续推进方向 B（prefix routing），请转为维护：
  - [l1_hybrid_bptree_design_v3.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v3.md)
  - [FlowKV_L1_Hybrid_V3_Progress_20260303_090524.md](/home/zwt/yjy/FlowKV/FlowKV_L1_Hybrid_V3_Progress_20260303_090524.md)

- 本文档只保留当前阶段仍然成立的结果性信息。
- 不再保留已经完成的过程性待办、历史性计划、重复性的实施步骤。
- 每次新的实现、验证、设计收敛完成后，都直接把“当前状态”覆盖更新到本文档。
- 每次继续开发前，都应先读取本文档，再决定下一步工作。
- 当前设计基线已扩展为 V1 + V2 双文档：
  - V1 用于保留第一阶段原型设计
  - V2 用于作为当前继续开发与实验冻结的设计基线
  - V3 用于作为方向 B（prefix routing）演进基线

## 当前目标边界

- 当前阶段只改 L1，不改 memtable 与 L0。
- 当前路由层 `layer0` 暂不改动。
- 当前实现聚焦于 L1 内部的子树层 `layer1`，即专用 B+Tree。
- 当前方案按 `FLOWKV_KEY16` 语义设计。

## 当前设计文档基线

- V1 设计文档：
  - [l1_hybrid_bptree_design.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design.md)
- V2 设计文档：
  - [l1_hybrid_bptree_design_v2.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v2.md)
- V3 设计文档：
  - [l1_hybrid_bptree_design_v3.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v3.md)
- 当前后续开发、正确性回归、功能冻结与实验边界，以最新路线对应的设计文档为准。
- 当前若进入方向 B（prefix routing）开发，以 V3 为准。

## 当前设计结论

- `hmasstree` 不再作为本轮实现路线。
- 当前 L1 不是通用 KV 索引，而是 PST 路由索引。
- L1 子树层不是通用 B+Tree，而是面向 PST 路由的专用只读子树。
- 当前子树层采用 `bulk-load + lookup + range scan`，不实现在线 `insert/delete/split/merge`。
- 当前排序语义保持与既有 L1 一致：按 `max_key` 做 lower-bound，再检查 `min_key <= key <= max_key`。

## 当前已落地的创新设计

### 创新点 1：L1 被重构为专用 PST 路由索引

- 当前实现不再把 FlowKV 的 L1 当作通用 KV 索引使用。
- 当前 L1 的抽象语义被明确收敛为：
  - 索引对象是 `TaggedPstMeta`
  - 路由键是 `max_key`
  - 查询语义是 `lower_bound(max_key)` 加范围校验
- 这使 L1 的结构设计开始直接服务 `Version::Get()`、`GetLevel1Iter()`、`PickOverlappedL1Tables()` 的真实访问模式。

### 创新点 2：FlowKV L1 内部实现了双层结构

- 当前 L1 已实现为：
  - `RouteLayer`
  - `SubtreeLayer`
- `RouteLayer` 现在由 Masstree 负责按 route key 将查询路由到具体 partition。
- `SubtreeLayer` 负责在 partition 内执行有序 PST 路由。
- 当前实现不是单棵全局树，而是“分区路由 + 分区子树”的双层索引系统。

### 创新点 3：子树层是面向 PST 路由的只读 bulk-load B+Tree

- 当前 `subtree` 不是通用可变 B+Tree。
- 当前叶子记录模型是：
  - `min_key`
  - `max_key`
  - `seq_no`
  - `table_idx`
- 当前子树只支持：
  - `BulkLoad`
  - `Lookup`
  - `RangeScan`
- 当前不实现在线 `insert/delete/split/merge`，而是与 compaction 输出的有序数据生命周期保持一致。

### 创新点 4：L1 更新采用分区局部重建，而不是全局重建

- 当前 `L1HybridIndex` 已支持基于固定 route partition 的局部重建。
- 当前 L1 变化时，只重建受影响 partition 下的 subtree。
- 当前更新代价被绑定到受影响 key-range，而不是整个 L1 大小。
- 这是当前设计中最核心的结构性收益之一。

### 创新点 5：发布机制采用可安全读取的延迟回收模型

- 当前 partition 持有的 subtree 通过 `shared_ptr` 发布。
- 当前查询路径会先捕获 subtree 引用，再执行 lookup / range scan。
- 当前 partition 发布新 subtree 后，旧 subtree 会在最后一个读者释放引用后自动回收。
- 这使局部重建与在线读取之间具备稳定的生命周期边界。

### 创新点 6：子树从一开始就具备页式物理布局

- 当前 `subtree` 已定义独立的页式布局，而不是只有内存节点。
- 当前页集合由：
  - manifest
  - internal pages
  - leaf pages
  组成。
- 当前 leaf page 和 internal page 的字段布局已经独立实现。
- 这为后续 SSD 化、空间测量、恢复路径验证提供了统一的物理边界。

### 创新点 7：子树页集合已经接入真实持久化介质

- 当前 `SubtreePageSet` 已可写入 `SegmentAllocator/SortedSegment`。
- 当前持久化布局采用连续 page run：
  - 首页为 manifest
  - 后续为 node pages
- 当前已经支持真实介质上的：
  - page set 写入
  - page set 读回
  - subtree 恢复
  - page run 回收

### 创新点 8：recovery 采用 manifest 真值源加批量重建模型

- 当前 recovery 不以持久化 subtree 本身作为真值源。
- 当前恢复策略是：
  - manifest 扫描得到有效 L1 `TaggedPstMeta`
  - 批量重建 `L1HybridIndex`
  - 恢复正确的 `l1_seq_`
- 当前 recovery 后可继续执行新的 flush / compaction。
- 这使恢复路径保持简单，同时保证了 L1 双层结构在系统重启后的可持续运行。

## 当前代码状态

### 已落地模块

- [lib/hybrid_l1](/home/zwt/yjy/FlowKV/lib/hybrid_l1)
- [l1_hybrid_index.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_index.h)
- [l1_hybrid_index.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_index.cc)
- [l1_hybrid_rebuilder.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_rebuilder.h)
- [l1_hybrid_rebuilder.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/l1_hybrid_rebuilder.cc)
- [route_layout.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_layout.h)
- [route_layout.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_layout.cc)
- [route_partition.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/route_partition.h)
- [subtree_record.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_record.h)
- [subtree_bptree.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_bptree.h)
- [subtree_bptree.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_bptree.cc)
- [subtree_page.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page.h)
- [subtree_page.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page.cc)
- [subtree_page_store.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page_store.h)
- [subtree_page_store.cc](/home/zwt/yjy/FlowKV/lib/hybrid_l1/subtree_page_store.cc)

### 当前双层结构能力

当前 `hybrid_l1` 已经不是单棵 subtree，而是：

- `RouteLayer`：由 Masstree 支撑的内存路由索引
- `SubtreeLayer`：每个 partition 下的一棵 `L1SubtreeBPTree`

当前代码职责已经拆分为：

- `subtree_bptree.*`：单棵 B+Tree 子树
- `route_partition.h`：分区元数据
- `route_layout.*`：Masstree-backed 的 route key 到 partition 定位规则
- `l1_hybrid_rebuilder.*`：全量/局部重建逻辑
- `l1_hybrid_index.*`：双层索引对外门面

当前发布方式为：

- partition 中的 subtree 通过 `shared_ptr` 发布
- 查询路径会先捕获当前 subtree 引用，再执行查找
- partition 局部重建发布新 subtree 后，旧 subtree 会在最后一个读者释放引用后自动回收

当前能力包括：

- `BulkLoad`
- `BulkLoadFromTables`
- `LowerBound`
- `LookupCandidate`
- `LookupCandidates`
- `RangeScan`
- `ExportAll`
- `Validate`
- `ExportPageSet`
- `ImportPageSet`

### 当前页式布局与持久化骨架

- 当前 `subtree` 已具备独立的页式物理布局定义
- 当前页集合由 manifest 与 node pages 组成
- 当前 node page 已区分 `internal` 与 `leaf`
- 当前 leaf page 保存：
  - `high_key`
  - `prev/next page id`
  - 有序 `SubtreeRecord`
- 当前 internal page 保存：
  - `high_key`
  - `child_page_id`
  - `child_high_key`
- 当前恢复路径已支持：
  - 从页集合校验结构
  - 从 leaf 链按序提取记录
  - 通过 `bulk-load` 恢复为内存 subtree
- 当前查询路径仍使用内存 subtree，不直接访问页表示

### 当前真实持久化接入状态

- 当前 `subtree` 页集合已可写入 `SegmentAllocator/SortedSegment`
- 当前持久化入口使用连续 page run：
  - 首页保存 manifest
  - 后续页面保存 node pages
- 当前已支持：
  - 从真实持久化页集合读回 `SubtreePageSet`
  - 从读回页集合恢复内存 subtree
  - 回收已占用的 subtree page run

### 当前 recovery 接入状态

- 当前恢复真值源仍为 manifest，而不是持久化 subtree 本身
- 当前 L1 recovery 已改为：
  - manifest 扫描阶段先收集有效 L1 `TaggedPstMeta`
  - 恢复结束后一次性批量重建 `L1HybridIndex`
- 当前 `Version` 已在 recovery 时恢复正确的 `l1_seq_`
- 当前 recovery 后可继续执行新的 flush/compaction，而不会复用旧的 L1 seq 编号

### 当前 RouteLayer 形态

- 当前 route layer 由 Masstree 路由索引和 `RoutePartition` 元数据共同组成
- 每个 partition 覆盖一段连续的 `max_key` 路由区间
- 每个 partition 挂一棵 subtree
- 当前 route 定位通过 Masstree 上的 `route_max_key -> partition_id` 映射完成
- 当前 partition 切分规则仍为固定 key-range 分区
- 相同 `max_key` 的记录不会被切裂到不同 partition

### 当前记录模型

叶子记录使用：

- `min_key`
- `max_key`
- `seq_no`
- `table_idx`

### 当前树结构

- 内部节点保存 child 与 `high_key`
- 叶子节点保存按 `max_key` 有序的 `SubtreeRecord`
- 叶子之间通过 `prev/next` 连接

## 当前接入状态

### `Version` 接入状态

L1 路由已经切换到新的双层结构：

- `level1_tree_` 已改为 `L1HybridIndex`
- `InsertTableToL1()` / `DeleteTableInL1()` 已接入局部分区重建
- `Version::Get()` 已通过 subtree 执行 L1 路由
- `GetLevel1Iter()` 已通过 subtree 返回候选 PST
- `PickOverlappedL1Tables()` 已通过 subtree 执行范围路由

### 当前接入策略

- 当前仍是保守版接入
- L1 已支持按受影响 partition 的局部重建
- 已引入基于 `shared_ptr` 的延迟回收机制

## 当前验证状态

### 子树层测试

- [hybrid_l1_test.cpp](/home/zwt/yjy/FlowKV/tests/hybrid_l1_test.cpp)

已验证：

- bulk-load 语义
- lookup 语义
- overlap 候选顺序
- 相同 `max_key` 下的 `seq_no` 排序
- range scan
- 无序输入拒绝
- `BulkLoadFromTables` 的有效项装载
- 双层结构下的跨 partition lookup
- 双层结构下的跨 partition range scan
- 相同 `max_key` 记录不会跨 partition 被切裂
- 按 partition 局部重建后，未受影响 partition 的查询结果保持正确
- partition 发布新 subtree 后，旧 subtree 在持有引用时仍可继续安全读取
- subtree 页集合导出、校验、恢复的 round-trip 语义成立
- subtree 页集合损坏后可被校验逻辑拒绝
- subtree 页集合写入真实 `SortedSegment` 后，可跨 allocator reopen 读回并恢复
- subtree 页集合对应的持久化 page run 可被回收

### `Version` 级对拍测试

- [version_l1_selection_test.cpp](/home/zwt/yjy/FlowKV/tests/version_l1_selection_test.cpp)

已验证：

- `GetLevel1Iter()` 的候选 PST 选择
- `PickOverlappedL1Tables()` 的范围重叠选择
- 删除旧 L1 表并重建 subtree 后，与基线结果一致
- 跨固定 route partition 的查询与删除后，结果仍与基线一致

### 最小 DB 路径验证

- [db_l1_route_smoke_test.cpp](/home/zwt/yjy/FlowKV/tests/db_l1_route_smoke_test.cpp)

已验证：

- `Put`
- `BGFlush`
- `BGCompaction`
- `Get`
- compaction 后经由新 L1 路由正常读取
- 缺失 key 正常 miss

### recovery 系统级验证

- [db_l1_recovery_smoke_test.cpp](/home/zwt/yjy/FlowKV/tests/db_l1_recovery_smoke_test.cpp)

已验证：

- 首次运行后经 flush/compaction 生成 L1 数据
- 关闭数据库并以 `recover=true` 重启
- manifest 恢复后 L1HybridIndex 正常重建
- recovery 后旧数据可正确读回
- recovery 后继续执行新的 `Put -> BGFlush -> BGCompaction`
- recovery 后继续 compaction 时，更新 key 与新增 key 都能正确读回

### `Delete` 系统级正确性验证

- [db_delete_correctness_smoke_test.cpp](/home/zwt/yjy/FlowKV/tests/db_delete_correctness_smoke_test.cpp)

已验证：

- memtable 中的 tombstone 会遮蔽旧值，而不是继续向 L0/L1 回落
- `Delete` 经 `BGFlush` 后仍保持缺失语义
- `Delete` 经 `BGCompaction` 进入 L1 后仍保持缺失语义
- 删除后重新插入，同一 key 可被新值正确读回
- 二次删除后，旧值不会被恢复
- `recover=true` 重启后，删除语义仍可保持
- recovery 后重新插入，同一 key 可再次被正确读回

### 第一层端到端语义回归实验

- [correctness_e2e_semantics_stress.cpp](/home/zwt/yjy/FlowKV/experiments/correctness_regression/01_end_to_end_semantics/correctness_e2e_semantics_stress.cpp)

已验证：

- 固定随机种子的端到端随机 workload
- shadow model 对拍
- `Put/Get`
- `Delete`
- 覆盖写
- 周期性 `BGFlush`
- 周期性 `BGCompaction`
- recovery 后继续写入与 compaction

当前稳定覆盖边界：

- 当前稳定实验入口已覆盖 `Put/Get/Delete/覆盖写/recovery`
- `Delete` 已补成独立系统级 smoke 基线，并纳入稳定 `ctest`
- `Delete` 已并入第一层随机 stress 入口

### 第二层结构一致性回归实验

- [l1_structure_consistency_regression.cpp](/home/zwt/yjy/FlowKV/experiments/correctness_regression/02_structure_consistency/l1_structure_consistency_regression.cpp)

已验证：

- `L1HybridIndex::Validate()`
- `ExportAll()` 的记录顺序合法性
- 导出 `SubtreeRecord` 与对应 `TaggedPstMeta` 的 `min/max/seq_no` 一致性
- `PickOverlappedL1Tables()` 对导出记录的结构可达性
- delete 后多轮 flush/compaction 下的结构一致性
- recovery 后的结构一致性

### 第三层长时间运行回归实验

- [l1_long_running_cycle_regression.cpp](/home/zwt/yjy/FlowKV/experiments/correctness_regression/03_long_running_cycles/l1_long_running_cycle_regression.cpp)

已验证：

- 多轮随机 `Put/Delete/Get`
- 每轮 `BGFlush -> BGCompaction`
- 每轮 shadow model 对拍
- 每轮 L1 结构一致性检查
- 每隔固定 round 的 `recover=true` 重启验证
- 多轮 delete、局部重建、recovery 交替后的长期稳定性

### 第四层差分回归实验

- [l1_differential_regression.cpp](/home/zwt/yjy/FlowKV/experiments/correctness_regression/04_differential_regression/l1_differential_regression.cpp)

已验证：

- 当前双层 L1 与原始单层 L1 基线语义的点查候选选择一致性
- 当前双层 L1 与原始单层 L1 基线语义的 overlap 选择一致性
- delete 后多轮 flush/compaction 下的差分一致性
- recovery 后的差分一致性

### 当前验证结论

- `hybrid_l1` 子树层逻辑成立
- `hybrid_l1` 双层结构逻辑成立
- 固定 route partition 下的局部重建逻辑成立
- `shared_ptr` 发布下的延迟回收语义成立
- subtree 页式布局与持久化接口骨架成立
- subtree 页式表示接入真实持久化介质成立
- `Version` 接入后的 L1 选表逻辑成立
- 最小 DB 主路径上的读写与 compaction 后读取成立
- recovery 后的 L1 批量重建与继续 compaction 逻辑成立
- `Delete` 在 memtable、flush、compaction、L1 路由、recovery 下的点路径语义成立
- 第一层端到端语义回归实验入口成立，并已覆盖 `Delete`
- 第二层结构一致性回归实验入口成立
- 第三层长时间运行回归实验入口成立
- 第四层差分回归实验入口成立

## 当前构建状态

已接入：

- [CMakeLists.txt](/home/zwt/yjy/FlowKV/CMakeLists.txt)
- [tests/CMakeLists.txt](/home/zwt/yjy/FlowKV/tests/CMakeLists.txt)

当前用于验证的构建方式：

- `build_hybrid_check`
- `FLOWKV_KEY16=ON`
- `USE_HMASSTREE=OFF`
- `KV_SEPARATION=OFF`

当前测试状态：

- `hybrid_l1_test` 可稳定通过
- `version_l1_selection_test` 可稳定通过
- `db_l1_recovery_smoke_test` 可稳定通过
- `db_delete_correctness_smoke_test` 可稳定通过
- `correctness_e2e_semantics_stress` 可稳定通过
- `l1_structure_consistency_regression` 可稳定通过
- `l1_long_running_cycle_regression` 可稳定通过
- `l1_differential_regression` 可稳定通过
- `db_l1_route_smoke_test` 可作为手动 smoke 使用
- `ctest --test-dir build_hybrid_check -R 'hybrid_l1_test|version_l1_selection_test|db_l1_recovery_smoke_test|db_delete_correctness_smoke_test|correctness_e2e_semantics_stress|l1_structure_consistency_regression|l1_long_running_cycle_regression|l1_differential_regression'` 已稳定通过

## 当前总进度

### 已完成

- L1 子树层设计收敛
- `lib/hybrid_l1` 双层原型实现
- `Version` 的双层 L1 路由接入
- 基于固定 route partition 的局部重建
- 基于 `shared_ptr` 发布的延迟回收
- subtree 页式物理布局与持久化接口骨架
- subtree 页式表示接入真实持久化介质
- recovery 场景下的系统级接入与验证
- `Delete` 系统级正确性闭环与专项 smoke 验证
- `experiments/correctness_regression/` 四层验证目录结构组织完成
- 第一层端到端语义回归实验入口，并已纳入 `Delete`
- 第二层结构一致性回归实验入口
- 第三层长时间运行回归实验入口
- 第四层差分回归实验入口
- 子树层单元测试
- `Version` 级选择对拍
- 最小 DB 路径验证

### 当前所处阶段

当前处于：

- **L1 双层内存原型、局部重建、延迟回收、真实介质接入、recovery 与 Delete 系统级正确性闭环已完成，且四层正确性回归已全部建立稳定基线**

### 仍未完成

- 第二层到第四层回归实验的具体实现
- 更大规模的数据正确性与回归验证
- 性能测试与论文实验数据采集

## 当前参考文档

- [l1_hybrid_bptree_design.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design.md)
