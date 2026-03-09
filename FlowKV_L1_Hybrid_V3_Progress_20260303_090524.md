# FlowKV L1 Hybrid V3 Progress 20260303_090524

## 当前维护规则

- 本文档用于方向 B（prefix routing）路线的唯一进度跟踪。
- 后续若继续推进方向 B，只维护：
  - [l1_hybrid_bptree_design_v3.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v3.md)
  - [FlowKV_L1_Hybrid_V3_Progress_20260303_090524.md](/home/zwt/yjy/FlowKV/FlowKV_L1_Hybrid_V3_Progress_20260303_090524.md)
- 旧的 `V1/V2` 进度口径不再作为方向 B 的当前跟踪基线。
- 每次继续开发前，应先读取本文件与 `V3`。

## 当前方向 B 冻结结论

- `RouteLayer` 将进入真正的 prefix routing。
- `partition` 的正式语义改为 prefix bucket。
- `SubtreeLayer` 采用“先 B1、后 B2”的路线。
- CoW 被纳入方向 B 的正式优化路线，但不是第一步必须落地的基础语义。
- 点查询默认不跨 prefix。
- 范围查询允许跨 prefix，第一版先串行 prefix-by-prefix 扫描。

## 当前实现状态

- 当前系统已进入方向 B 的第一版实现：
  - `partition` 已切换为 prefix bucket
  - `RouteLayer` 已切换为 `prefix -> partition` 的 Masstree 路由
  - `Rebuilder` 已切换为按 prefix 分桶
  - `SubtreeLayer` 已进入 B2 第一阶段实现
- `SubtreeRecord` 已带有 prefix-local fragment 字段：
  - `route_prefix`
  - `route_min_suffix`
  - `route_max_suffix`
- `Rebuilder` 已按触达 prefix 生成裁剪后的 local fragment，而不再只复制全局记录。
- `L1HybridIndex` 已进一步收紧到 local suffix-first 语义：
  - 点查询按 `route_prefix + suffix` 命中
  - 范围查询按 prefix-local suffix 区间扫描
  - 结构校验按 local fragment 正确性验证
  - 已新增 local-fragment 导出接口，结构回归改用这套口径
- `Version` 的一致性修复路径也已开始收紧到 local-fragment 口径：
  - `L1TreeConsistencyCheckAndFix()` 改为按 prefix-local fragment 重叠执行修复
  - 第三层长时间运行回归也已切到 local-fragment 结构检查
- prefix bucket 轻量治理已落地：
  - 每个 bucket 维护 `hot_prefix / prefer_cow / prefer_parallel_scan`
  - 这些状态按固定 record-count 阈值生成
  - 热点 bucket 会放宽 CoW 选择条件
  - 热点 bucket 参与的范围查询会更积极进入并行扫描
- CoW 的规则式选择框架已经接入更新主路径：
  - `BulkLoadRebuild`
  - `CowPatch`
  - 当前 `CowPatch` 已落地为完整 immutable CoW
  - 可复用任意未变化叶段
  - 对 child 序列完全一致的内部节点直接复用
- 点查询已收紧为单 prefix subtree 语义。
- 范围查询已进入“串行为默认、并行为优化”的阶段：
  - 默认仍保持 prefix-by-prefix 串行扫描语义
  - 当跨 prefix 分区数超过阈值时，启动并行 subtree 扫描
  - 上层通过有序 merge 保持 `RecordRouteKeyLess` 顺序
- 当前回归基线已显式覆盖多 prefix 数据，而不再只停留在 `prefix=0` 场景。

## 方向 B 第一大步

第一大步定义为：建立统一的 prefix/suffix 抽象工具，并把方向 B 的 key 语义从文档层收敛到代码层。

当前已完成：

- 新增 prefix/suffix 抽象工具：
  - [prefix_suffix.h](/home/zwt/yjy/FlowKV/lib/hybrid_l1/prefix_suffix.h)
- 已补最小测试覆盖：
  - [hybrid_l1_test.cpp](/home/zwt/yjy/FlowKV/tests/hybrid_l1_test.cpp)
- 当前 8 个稳定回归测试保持通过
- `partition` 语义已从 range bucket 切到 prefix bucket
- `RouteLayer` 已切到 `prefix -> partition` 的 Masstree 路由
- `Rebuilder` 已切到按 prefix 分桶

当前第一大步的边界：

- subtree 已进入 B2 第一阶段，但仍保留全局 `min_key/max_key` 用于上层去重、导出与校验
- recovery/query 大框架保持稳定
- CoW 已进入完整 immutable 版本：任意叶段复用 + 内部节点复用
- 并行 range scan 已进入第一版实现
- prefix bucket 轻量治理已进入实现

## 下一步主线

按 V3 的“必须先做”顺序，接下来应执行：

1. 继续扩大四层正确性回归的多 prefix 覆盖强度
2. 继续把 B2 收敛到更纯的 suffix-local 组织，尤其是减少对全局 `min_key/max_key` 的上层依赖
3. 最后再进入性能评测

## 当前优化路线

以下优化项已纳入方向 B 的正式后续跟踪：

1. 大 prefix subtree 的 CoW 更新
2. subtree 从当前 B2 第一阶段继续收敛
3. 并行 range scan + 多路 merge
4. prefix bucket 自适应治理

## 当前实验目录状态

- `experiments/` 已按论文实验方向完成目录规整：
  - `correctness_regression/`
  - `performance_evaluation/`
  - `ablation_studies/`
  - `common/`
- `performance_evaluation/` 已预留以下主实验目录：
  - `01_point_lookup/`
  - `02_range_query/`
  - `03_compaction_update/`
  - `04_space_overhead/`
  - `05_recovery_runtime/`
  - `06_long_running_mixed/`
- `ablation_studies/` 已预留以下消融目录：
  - `01_no_prefix_routing/`
  - `02_no_cow/`
  - `03_no_parallel_range_scan/`
  - `04_no_bucket_governance/`
  - `05_b1_vs_b2/`
- 当前处于“实验目录与实验分组已冻结，等待逐组落地程序与脚本”的状态

## 当前性能实验冻结口径

- key/value 大小固定为 `16B / 16B`
- KV block 固定为 `4KB`
- subtree leaf page 固定为 `4KB`
- subtree internal node 固定为 `16KB`
- 主实验数据规模固定为：
  - `10^6`
  - `10^7`
  - `10^8`
- 扩展实验预留：
  - `10^9`
- 点查询操作数固定为每组 `1,000,000 ops`
- 范围查询操作数固定为每个范围档位 `20,000 ops`
- prefix 基数固定三档：
  - `0.5N`
  - `0.1N`
  - `0.01N`
- 数据分布固定至少覆盖：
  - `uniform`
  - `prefix-skew`
- 实验数据目录固定使用独立 SSD 挂载点：
  - `/mnt/nvme0`
- 当前 `2.2T` 可用空间足够覆盖 `10^6 ~ 10^9` 主/扩展实验，并可谨慎尝试部分 `10^10` 级展示实验；不足以支撑 `10^11+` 的完整实验

## 当前性能实验实现状态

- `01_point_lookup/` 的第一版 benchmark 已落地：
  - [point_lookup_benchmark.cpp](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/point_lookup_benchmark.cpp)
- 当前引擎持久化点查询路径已切到真实 `16B value`：
  - `FLOWKV_KEY16` 下 WAL 统一为固定 `64B` 槽位
  - flush / compaction / PST query / recovery 已对齐到 `16B value`
  - 小规模 benchmark sanity run 已验证 `16B value` 可正确读回
- 当前 benchmark 已支持：
  - `direction_b_full`
  - `uniform / prefix-skew`
  - 三档 `prefix_count`
  - `80% hit / 20% miss`
  - 单线程 / 多线程
- 当前输出指标已固定为：
  - `avg_latency_ns`
  - `p99_latency_ns`
  - `throughput_ops`
  - `rss_bytes`
  - `l1_index_bytes_estimated`
- 已补实验专用 L1 内存估算接口：
  - subtree 估算
  - route/partition/governance 估算
- 已完成小规模 sanity run，确认：
  - 数据构建路径可运行
  - `16B value` 查询 hit/miss 正确
  - 指标输出格式可直接用于后续结果采集
- 已完成第一批正式 point lookup 结果采集：
  - `direction_b_full`
  - `10^6`
  - `uniform`
  - `1 thread`
  - prefix ratio = `0.5N / 0.1N / 0.01N`
  - 结果已落到：
    - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260304_065044/RESULTS.md)
  - 图表已生成：
    - average latency
    - P99 latency
    - L1 index memory

## 当前回归基线

当前稳定回归集合仍保持通过：

- `hybrid_l1_test`
- `version_l1_selection_test`
- `db_l1_recovery_smoke_test`
- `db_delete_correctness_smoke_test`
- `correctness_e2e_semantics_stress`
- `l1_structure_consistency_regression`
- `l1_long_running_cycle_regression`
- `l1_differential_regression`
