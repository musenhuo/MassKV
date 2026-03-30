# FlowKV L1 Hybrid V4 Progress 20260306_073226

## 当前维护规则

- 本文档是方向 B 在 V4（磁盘驻留查询）路线下的唯一进度跟踪。
- 后续只维护：
  - [l1_hybrid_bptree_design_v4.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v4.md)
  - [FlowKV_L1_Hybrid_V4_Progress_20260306_073226.md](/home/zwt/yjy/FlowKV/FlowKV_L1_Hybrid_V4_Progress_20260306_073226.md)
- 性能实验结果文档输出固定采用 `20260308_112413` 模板：
  - 必含配置范围、原始结果表、指标分析、结构改动前后对比、图表索引、完整 CSV dump。
  - 内存开销为强制项：`RSS`、`L1 Index Memory`、`Memory Overhead Table`（route/subtree/cache/governance 拆分）。
  - 若缺少“改动对比”或“完整指标”，视为结果记录不合规，需要补齐后再进入下一阶段。

## 当前有效结果

- 新增收敛（`2026-03-10`，SSD-only 主路径）：
  - 结构收敛：
    - `RoutePartition` 已移除 `subtree_pages_cache` 字段，仅保留 `subtree_store` 句柄。
    - `L1HybridIndex` 构造强制要求 `segment_allocator != nullptr`。
  - 查询路径收敛：
    - `LookupCandidate/LookupCandidates` 仅走页级磁盘路径。
    - `RangeScan` 仅走页级磁盘路径（失败即返回失败，不再回退整树导入）。
  - 统计口径收敛：
    - `l1_governance_bytes` 不再重复计入（已包含在 `route_partition_bytes`）。
  - 验证：
    - 编译通过：`hybrid_l1`、`hybrid_l1_test`、`version_l1_selection_test`
    - 运行通过：
      - `./build_hybrid_check/tests/hybrid_l1_test`
      - `./build_hybrid_check/tests/version_l1_selection_test`
  - 说明：
    - 全量构建仍会被 `hmasstree` 目标中的历史重复定义问题阻塞（与本次收敛无关）。

- 新增点查询实验（`20260309_091827`，10M + 连续数组只读快照后复测）：
  - 配置：
    - `key_count=10,000,000`
    - `query_count=1,000,000`
    - `distribution=uniform`
    - `threads=1`
    - `build_mode=fast_bulk_l1`
    - `prefix_ratio=0.1N/0.05N/0.01N`
    - `use_direct_io=1`
    - `warmup_queries=0`
    - `subtree_cache_capacity=256`
    - `subtree_cache_max_bytes=256MB`
    - `bitmap_persist_every=1024`
    - `pst_nowait_poll=0`
  - 结果：
    - `0.1N`: `avg=168976ns`, `p99=2063142ns`, `throughput=5907.57 ops/s`
    - `0.05N`: `avg=135316ns`, `p99=1830452ns`, `throughput=7372.34 ops/s`
    - `0.01N`: `avg=87495ns`, `p99=227266ns`, `throughput=11399.6 ops/s`
    - `avg_io_l1_pages_per_query` 三档均为 `1`
    - `avg_io_pst_reads_per_query` 三档约 `0.994~0.997`
  - 对比上一组可比实验（`20260308_132322`）：
    - I/O 次数基本不变（路径一致）
    - `l1_index_bytes_estimated` 降幅：
      - `0.1N`: `-29.69%`
      - `0.05N`: `-29.69%`
      - `0.01N`: `-38.35%`
  - 产物路径：
    - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260309_091827/RESULTS.md)
    - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260309_091827/results.csv)
    - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260309_091827/plots)
- 新增（2026-03-09，连续数组 + 32B 对齐 + 只读结构）：
  - `L1HybridIndex` 已新增只读发布快照 `PublishedSnapshot`：
    - `routes`：`alignas(32)` 的连续描述符数组 `PublishedRoutePartition`
    - `page_refs`：共享连续页引用池
  - 点查/范围查磁盘路径已改为读取快照切片（`SubtreePageStoreView`），不再依赖每个 partition 的 `pages vector`。
  - publish 后会释放 partition 自带 `pages vector`；在 `Clear/BulkLoad/Rebuild` 前按需 materialize，保证 CoW/回收流程兼容。
  - 本地验证：
    - 编译通过：`point_lookup_benchmark`
    - 测试通过：`hybrid_l1_test`、`version_l1_selection_test`
    - 小规模 sanity（`100k keys/10k prefixes/50k queries`）：
      - `avg_latency_ns=217918`
      - `p99_latency_ns=450484`
      - `avg_io_l1_pages_per_query=1`
      - `avg_io_pst_reads_per_query=0.99568`
      - `l1_index_bytes_estimated=1,922,480`
- 新增（2026-03-09，路由层回归）：
  - `FixedRouteLayout` 路由层已恢复 `Masstree`（`prefix -> partition_idx`）。
  - 点路由 `FindPartitionByKey` 已回到 masstree 查询路径。
  - 范围路由 `CollectPartitionsForRange` 已回到 masstree 范围扫描路径。
  - 约束明确：双层结构核心不变，`layer0` 必须保持 masstree 路由结构。
- 新增（2026-03-09，内存压缩 V1 + 10M 验证）：
  - `SubtreePageStoreHandle` 收紧为 `32B`（去除 `page_count/record_count` 常驻字段，`page_size` 改 `uint16`）。
  - `RoutePartition` 收紧为 `72B`（`generation` 改 `uint32`）。
  - 10M 点查（`prefix_count=1,000,000`, `query_count=300,000`, `fast_bulk_l1`）结果：
    - `l1_index_bytes_estimated=187,052,480`
    - `l1_route_partition_bytes=72,000,000`
    - `l1_route_index_estimated_bytes=64,000,000`
    - `l1_subtree_bytes=48,000,000`
  - 结论：较前一版有下降，但距离 `<50MB` 目标仍有明显差距，需要进入“紧凑描述符 + 共享页引用池”阶段。
- 新增（2026-03-09，内存收敛）：
  - `RoutePartition` 已压缩（移除 `route_key`、`record_count` 改 `uint32`、`subtree_pages_cache` 改按需指针）。
  - `SubtreeStoredPageRef` 已压缩（`16B -> 8B`，`segment_id` 改 `uint32`，导入快照增加越界检查）。
  - `L1HybridIndex` 在 `BulkLoad/BulkLoadFromTables/Rebuild/Import/Clear` 后会 `shrink_to_fit` 收紧路由容器。
  - 结构尺寸实测：
    - `sizeof(RoutePartition)=88`（之前 `144`）
    - `sizeof(SubtreeStoredPageRef)=8`（之前 `16`）
  - 快速 sanity（`1M keys / 100k prefixes`）：
    - `l1_index_bytes_estimated=15,552,480`
    - `l1_route_partition_bytes=8,800,000`
    - `l1_subtree_bytes=6,400,000`
- 已将 `RoutePartition` 从常驻 subtree 指针切到磁盘句柄：
  - `SubtreePageStoreHandle subtree_store`
- `L1HybridIndex` 查询路径已改为按需加载 subtree page set 再查询。
- `Version` 初始化 `L1HybridIndex` 时已传入 `SegmentAllocator`，支持磁盘句柄读取。
- `Rebuilder` 构建 partition 后会先尝试持久化 page set，仅保存句柄。
- `SubtreePageStore` 已改为非连续页持久化：
  - 句柄切到 `vector<{segment_id, page_id}>`。
  - `Persist/Load/Destroy` 已按离散页工作（支持跨 segment）。
- `L1HybridIndex` 已引入可控 subtree page cache：
  - 键：`prefix + generation`
  - 策略：线程安全 LRU
  - 约束：`capacity + max_bytes`
  - 生命周期：`Clear/BulkLoad/Rebuild` 自动失效
- subtree cache 可观测性已接入：
  - `subtree_cache_requests/hits/misses/hit_rate`
  - point benchmark 与批量脚本均可采集
- `subtree_pages_cache` 已收紧：
  - 仅 `segment_allocator == nullptr`（debug/unit-test）时可用。
  - allocator 已接入时 `Persist` 失败直接抛错。
- 页级 CoW 持久化路径已接入：
  - `PersistCow`：复用未变化页，写入变化页。
  - `DestroyUnshared`：发布后回收旧版本独占页。
- CoW 统计已接入：
  - `SubtreePageStore` 累积 `persist_calls/reused_pages/written_pages/reused_bytes/written_bytes`。
  - `L1HybridIndex::EstimateMemoryUsage` 与 point benchmark 已可导出上述指标。
- point benchmark 已补齐 SSD 外存参数化：
  - `build_mode`（`online` / `fast_bulk_l1`）
  - `use_direct_io`
  - `warmup_queries`
  - `enable_subtree_cache/subtree_cache_capacity/subtree_cache_max_bytes`
- point benchmark 已接入 benchmark-only 快速建库路径：
  - `build_mode=fast_bulk_l1`
  - 直接批量构建 L1 PST + 一次性重建 L1 hybrid 索引
  - 跳过在线 `Put -> Flush -> Compaction` 中间过程，用于大规模读性能实验的数据准备加速
- `run_point_lookup_batch.py` 已补齐：
  - `l1_cow_*` 结果采集
  - 失败 case 的 raw 日志落盘
  - `--run-id` 覆盖固定结果目录
  - `--bitmap-persist-every` 透传（控制 SSD allocator 位图批量持久化）
- point benchmark 已接入 IO 诊断指标：
  - `avg_io_total_per_query`
  - `avg_io_total_top1pct_latency`
  - `avg_io_l1_pages_per_query`
  - `avg_io_l1_pages_top1pct_latency`
  - `avg_io_pst_reads_per_query`
  - `avg_io_pst_reads_top1pct_latency`
- 点查路径已收紧为页级磁盘遍历：
  - `LookupCandidate/LookupCandidates` 主路径改为 `root->leaf` 逐页读取（由句柄内 root 元数据直达）。
  - 仅 legacy 快照（无 root 元数据）回退一次 manifest 读取。
  - 仅在页级解码异常时回退旧整树加载路径（容错回退）。
  - 已新增回归测试覆盖该行为（见 `hybrid_l1_test` 新用例）。
- 范围查询路径已收紧为页级磁盘遍历：
  - `ScanSinglePartition` 先走页级路径（句柄 root 元数据直达 + leaf chain）。
  - legacy 快照（无 root 元数据）回退一次 manifest 读取。
  - 仅在页级解码异常时回退旧整树加载路径（容错回退）。
  - 已新增回归测试覆盖该行为（见 `hybrid_l1_test` 新用例）。
- manifest 句柄快照协议已接入：
  - compaction 收尾持久化 L1 hybrid state。
  - recover 优先加载 snapshot，失败自动回退重建。
- manifest 快照协议已增强：
  - snapshot 落盘带 envelope（`magic/version/full seq/payload_len/checksum`）。
  - recover 加载时执行 seq/checksum 校验；保留 legacy payload-only 兼容读取。
- manifest root 原子提交协议已落地：
  - snapshot 区改为双槽（slot0/slot1）切换。
  - 每次先写 inactive slot，再更新 super 元数据切换 active slot。
  - 已新增双槽切换回归测试（inactive slot 损坏不影响 active slot 读取）。
- SSD allocator（`SegmentAllocator`）已新增位图批量持久化开关：
  - 环境变量 `FLOWKV_BITMAP_PERSIST_EVERY`（默认 `1`）。
  - benchmark 参数 `bitmap_persist_every` 已打通。
  - 目标：降低建数阶段 allocator 位图写放大，缩短大规模数据生成耗时。
- 性能实验“快速建库（自底向上重建式）”已抽离为公共接口：
  - 新增 [fast_bulk_l1_builder.h](/home/zwt/yjy/FlowKV/experiments/common/fast_bulk_l1_builder.h)
  - 新增 [fast_bulk_l1_builder.cpp](/home/zwt/yjy/FlowKV/experiments/common/fast_bulk_l1_builder.cpp)
  - 统一接口：`BuildFastBulkL1Dataset`
- 点查询 benchmark 已改为调用公共接口，不再内嵌 fast bulk 构建实现：
  - [point_lookup_benchmark.cpp](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/point_lookup_benchmark.cpp)
- 公共接口已提供统一的数据分布与值生成函数：
  - `PrefixDistribution`
  - `UsedCountForPrefix`
  - `KeyForLogicalIndex`
  - `DeterministicValue16`
- `experiments` 文档已同步“统一建库接口”约束：
  - [experiments/common/README.md](/home/zwt/yjy/FlowKV/experiments/common/README.md)
  - [experiments/performance_evaluation/README.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/README.md)
  - [01_point_lookup/README.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/README.md)
- 修复了 fast bulk 路径暴露出的分配器复用缺陷：
  - `SegmentAllocator::AllocSortedSegment` 现在只复用“页大小匹配”的 cached segment。
  - 避免 PST（`4KB`）段被误复用于 subtree（`16KB`）页写入。
  - 变更文件：
    - [segment_allocator.h](/home/zwt/yjy/FlowKV/db/allocator/segment_allocator.h)
- 新增一次读流程的细化说明文档（根目录）：
  - [FlowKV_Read_Path_Detailed_20260308_124958.md](/home/zwt/yjy/FlowKV/FlowKV_Read_Path_Detailed_20260308_124958.md)
- subtree 句柄已补齐 root 直达元数据，并接入快照编码：
  - `SubtreePageStoreHandle` 新增 `flags/root_page_id/page_count/record_count`。
  - `Persist/PersistCow` 会把 manifest 中的 root 元数据写入句柄。
  - `Version` 的 L1 hybrid snapshot 编码升级为 `format=2`（保留 `format=1` 兼容导入）。
  - 查询路径优先使用句柄元数据，避免每次点查固定读取 subtree manifest 页。

## 当前验证结果

- 编译通过：
  - `point_lookup_benchmark`
  - `hybrid_l1_test`
  - `db_l1_recovery_smoke_test`
- 运行通过：
  - `hybrid_l1_test`
  - `db_l1_recovery_smoke_test`
  - `manifest_l1_snapshot_test`
  - 小规模 point lookup sanity
  - `TestHybridIndexDiskPointLookupUsesPagePath`
  - `ctest -R "hybrid_l1_test|manifest_l1_snapshot_test|db_l1_recovery_smoke_test"`

新增验证（2026-03-08，root 直达）：

- `point_lookup_benchmark` 小规模 sanity（`key=5000, prefix=500, fast_bulk_l1`）：
  - `avg_io_l1_pages_per_query=1`（由 `manifest+root->leaf` 的约 `2` 页降为 `1` 页）
  - `avg_io_total_per_query=2.8298`
- 结论：
  - 新数据路径下，点查已不再固定读取 subtree manifest 页；仅 legacy 快照会回退 manifest 一次读取。

小规模 sanity（`key=2000, prefix=200`）当前输出：

- `avg_latency_ns=26405.2`
- `p99_latency_ns=74613`
- `throughput_ops=37124.1`
- `l1_index_bytes_estimated=42520`

新增 sanity（`key=50000, prefix=5000, uniform, 1 thread`）：

- `avg_latency_ns=30062.6`
- `p99_latency_ns=53870`
- `throughput_ops=33124.5`
- `l1_index_bytes_estimated=1529112`

新增 sanity（`key=30000, prefix=3000, uniform, 1 thread`，page cache 指标已输出）：

- `avg_latency_ns=25882`
- `p99_latency_ns=58785`
- `throughput_ops=38391`
- `l1_index_bytes_estimated=855496`
- `l1_subtree_cache_bytes=49440`

新增 sanity（`key=20000, prefix=2000, uniform, 1 thread`，cache hit/miss 指标已输出）：

- `avg_latency_ns=23093.6`
- `p99_latency_ns=56109`
- `throughput_ops=42921.3`
- `l1_subtree_cache_requests=2000`
- `l1_subtree_cache_hits=0`
- `l1_subtree_cache_misses=2000`
- `l1_subtree_cache_hit_rate=0`

新增 sanity（`key=20000, prefix=2000, uniform, 1 thread`，CoW+snapshot协议接入后）：

- `avg_latency_ns=24683.7`
- `p99_latency_ns=59622`
- `throughput_ops=40182.1`
- `l1_subtree_cache_requests=4000`
- `l1_subtree_cache_hits=0`
- `l1_subtree_cache_misses=4000`
- `l1_subtree_cache_hit_rate=0`

新增 sanity（`key=20000, prefix=2000, uniform, 1 thread`，CoW统计导出已接通）：

- `avg_latency_ns=24467.1`
- `p99_latency_ns=56413`
- `throughput_ops=40555.3`
- `l1_cow_persist_calls=0`
  - `l1_cow_reused_pages=0`
  - `l1_cow_written_pages=0`
  - `l1_cow_page_reuse_ratio=0`
- 新增回归验证（2026-03-06）：
  - `TestHybridIndexDiskRangeScanUsesPagePath` 通过
  - `TestSnapshotDualSlotAtomicSwitch` 通过
  - `ctest -R "hybrid_l1_test|manifest_l1_snapshot_test"` 通过

首轮点查询正式实验（覆盖原 `20260304_065044` 目录，SSD 外存口径）：

- 配置：
  - `key_count=1,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.5N/0.1N/0.01N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
  - `pool_size=256GB`
- 结果：
  - `0.5N`: `avg=145712ns`, `p99=378233ns`, `throughput=6851 ops/s`
  - `0.1N`: `avg=138971ns`, `p99=351948ns`, `throughput=7186 ops/s`
  - `0.01N`: `avg=129372ns`, `p99=316601ns`, `throughput=7717 ops/s`
  - `subtree_cache_hit_rate` 三档均为 `0`
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260304_065044/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260304_065044/results.csv)
  - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260304_065044/plots)

新增点查询实验（`20260307_065159`，与 `065044` 同级）：

- 配置：
  - `key_count=1,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.5N/0.1N/0.01N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
  - `pool_size=256GB`
- 结果：
  - `0.5N`: `avg=106264ns`, `p99=313308ns`, `throughput=9391.61 ops/s`
  - `0.1N`: `avg=106563ns`, `p99=315138ns`, `throughput=9369.22 ops/s`
  - `0.01N`: `avg=102077ns`, `p99=294938ns`, `throughput=9775.11 ops/s`
  - `subtree_cache_hit_rate` 三档均为 `0`
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_065159/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_065159/results.csv)
  - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_065159/plots)

新增 IO 归因验证实验（`20260307_072449`）：

- 配置：
  - `key_count=1,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
- 结果：
  - `avg_latency_ns=112822`
  - `p99_latency_ns=267151`
  - `avg_io_total_per_query=2.99899`
  - `avg_io_total_top1pct_latency=3`
  - `avg_io_l1_pages_per_query=2`
  - `avg_io_l1_pages_top1pct_latency=2`
  - `avg_io_pst_reads_per_query=0.998995`
  - `avg_io_pst_reads_top1pct_latency=1`
- 结论：
  - top1% 高延迟样本与总体样本 I/O 次数一致，p99 抬高主因更接近 SSD 尾部抖动，而非路径 hop 变长。
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_072449/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_072449/results.csv)

新增更大规模点查询实验（`20260307_084226`）：

- 配置：
  - `key_count=2,000,000`
  - `query_count=200,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
- 结果：
  - `avg_latency_ns=114383`
  - `p99_latency_ns=302925`
  - `throughput_ops=8727.17`
  - `avg_io_total_per_query=2.99908`
  - `avg_io_total_top1pct_latency=3`
  - `avg_io_l1_pages_per_query=2`
  - `avg_io_l1_pages_top1pct_latency=2`
  - `avg_io_pst_reads_per_query=0.99908`
  - `avg_io_pst_reads_top1pct_latency=1`
- 结论：
  - 更大规模下，top1% 与总体 I/O 次数仍一致，p99 抬高主因仍偏向 SSD 尾部抖动。
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_084226/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260307_084226/results.csv)

SSD allocator 建数提速对照（2026-03-07）：

- 实验配置（快速 A/B）：
  - `key_count=500,000`
  - `prefix_count=50,000`
  - `query_count=1`（以建数耗时为主）
  - `distribution=uniform`
  - `threads=1`
  - `use_direct_io=1`
  - `db_dir=/tmp/flowkv_alloc_cmp`
- 对照结果（`/usr/bin/time` 总耗时）：
  - `bitmap_persist_every=1`：`elapsed_sec=15.45`
  - `bitmap_persist_every=1024`：`elapsed_sec=14.14`
- 结论：
  - 在该规模下总耗时下降约 `8.5%`，瓶颈方向与“allocator 位图频繁落盘”一致。

新增点查询实验（`20260308_112413`，10M + fast_bulk_l1）：

- 配置：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `build_mode=fast_bulk_l1`
  - `prefix_ratio=0.1N/0.05N/0.01N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
  - `bitmap_persist_every=1024`
  - `pool_size=256GB`
- 结果：
  - `0.1N`: `avg=138954ns`, `p99=2170254ns`, `throughput=7182.04 ops/s`
  - `0.05N`: `avg=163498ns`, `p99=1959191ns`, `throughput=6104.23 ops/s`
  - `0.01N`: `avg=116047ns`, `p99=354893ns`, `throughput=8591.97 ops/s`
  - `subtree_cache_hit_rate` 三档均为 `0`
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_112413/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_112413/results.csv)
  - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_112413/plots)

新增实现结果（2026-03-08，PST 4KB + 无分段 key）：

- `PDataBlock` 默认从 `16KB` 切换为 `4KB`（`FLOWKV_KEY16` 下每块最多 `128` 条 KV）。
- PST 元数据已移除分段采样键：
  - 删除 `key_256/key_512/key_768` 字段与相关访问函数。
- 点查询路径已改为“单块直查”：
  - `Version::Get` 在 L0/L1 都直接调用 `PointQuery(datablock_ptr_, ..., entry_num_)`。
  - 去掉 `256/512/768` 分段偏移选择逻辑。
- `DataBlockReader` 点查读取粒度已与 `PDataBlock` 对齐：
  - `kPointReadBufSize = sizeof(PDataBlock)`（当前为 `4KB`）。
- 编译验证：
  - `db/datablock_reader.o`
  - `db/pst_builder.o`
  - `db/pst_reader.o`
  - `db/compaction/version.o`
  - 均已编译通过。

新增实现结果（2026-03-08，B+Tree leaf value 44/12/8 位段）：

- `SubtreeRecord` 新增 `leaf_value`（64-bit）：
  - 高 `44` 位：`kv_block_ptr`（4KB block id）
  - 中 `12` 位：`offset`
  - 低 `8` 位：`count`
- `leaf_value` 已接入 subtree page 编解码持久化。
- subtree page 编码版本提升为 `v2`，用于区分新旧记录布局。
- `L1HybridRebuilder::BuildPartition` 已接入 block 扫描回填：
  - 按 prefix 提取每个 block 的精确 `offset/count`。
  - 同一 block 复用窗口缓存，避免重复扫描。
- `Version::Get`（L1 路径）已接入窗口化点查：
  - 先按 `leaf_value` 做局部 `PointQuery`。
  - 局部 miss 自动回退整块 `PointQuery`，保证正确性。
- 编译/回归：
  - `cmake --build build_hybrid_check --target hybrid_l1_test` 通过
  - `./build_hybrid_check/tests/hybrid_l1_test` 通过
  - `./build_hybrid_check/tests/db_l1_route_smoke_test` 通过

新增点查询实验（`20260308_125643`，10M + fast_bulk_l1）：

- 配置：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `build_mode=fast_bulk_l1`
  - `prefix_ratio=0.1N/0.05N/0.01N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
  - `bitmap_persist_every=1024`
- 结果：
  - `0.1N`: `avg=182612ns`, `p99=2038296ns`, `throughput=5462.10 ops/s`
  - `0.05N`: `avg=169325ns`, `p99=1859625ns`, `throughput=5888.83 ops/s`
  - `0.01N`: `avg=114188ns`, `p99=461856ns`, `throughput=8725.33 ops/s`
  - `avg_io_l1_pages_per_query` 三档均为 `1`
  - `avg_io_pst_reads_per_query` 分别为 `1.85351 / 1.71169 / 1.39747`
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_125643/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_125643/results.csv)
  - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_125643/plots)

新增实现结果（2026-03-08，O_DIRECT 对齐修复）：

- L1 window 点查已改为“块基址 + 入口窗口”语义，不再构造块内字节偏移：
  - `Version::Get` 改为调用 `PSTReader::PointQueryWindow(block_addr, start_entry, entry_count)`。
  - 避免 `O_DIRECT` 下 `offset % 4096 != 0` 的非对齐读失败。
- `DataBlockReader` 已新增 O_DIRECT 对齐守卫：
  - 非对齐请求自动回退到 buffered fd，不再直接失败。
  - 删除热路径 `read wrong` stdout 打印，改为统计计数。
- 新增可观测性指标：
  - `pst_direct_unaligned_fallbacks`
  - `pst_short_reads`
- 小规模 sanity（`key=50000, prefix=5000, query=10000, direct_io=1`）验证：
  - `avg_io_l1_pages_per_query=1`
  - `avg_io_pst_reads_per_query=0.995`
  - `pst_direct_unaligned_fallbacks=0`
  - `pst_short_reads=0`

新增点查询实验（`20260308_132322`，10M + 对齐修复后复测）：

- 配置：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `build_mode=fast_bulk_l1`
  - `prefix_ratio=0.1N/0.05N/0.01N`
  - `use_direct_io=1`
  - `warmup_queries=0`
  - `subtree_cache_capacity=256`
  - `subtree_cache_max_bytes=256MB`
  - `bitmap_persist_every=1024`
- 结果：
  - `0.1N`: `avg=132558ns`, `p99=2092805ns`, `throughput=7530.23 ops/s`
  - `0.05N`: `avg=133467ns`, `p99=1806312ns`, `throughput=7475.15 ops/s`
  - `0.01N`: `avg=89281.8ns`, `p99=272378ns`, `throughput=11175.7 ops/s`
  - `avg_io_l1_pages_per_query` 三档均为 `1`
  - `avg_io_pst_reads_per_query` 三档约 `0.994~0.997`
  - `pst_direct_unaligned_fallbacks=0`（三档）
  - `pst_short_reads=0`（三档）
  - raw 日志 `read wrong` 行数为 `0`
- 对比结论：
  - 相比修复前异常 run（`20260308_125643`），avg 延迟明显回落，I/O 计数恢复到理论路径（约 `1` 次 L1 页 + `1` 次 PST 读）。
  - p99 在 `0.1N/0.05N` 档仍偏高，当前主要受 SSD 随机读尾延迟影响。
- 产物路径：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_132322/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_132322/results.csv)
  - [plots](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_132322/plots)

新增实现结果（2026-03-09，PST NOWAIT 轮询开关）：

- `DataBlockReader` 新增同步非阻塞轮询实验模式：
  - 通过 `preadv2(..., RWF_NOWAIT)` 轮询读取。
  - 环境变量开关：`FLOWKV_PST_NOWAIT_POLL=0/1`。
  - 保留自动回退：若内核/文件系统不支持 NOWAIT（`EOPNOTSUPP/ENOSYS/EINVAL`），自动回退到 `pread`。
- benchmark 参数打通：
  - 新增 `--pst-nowait-poll`
  - 新增输出指标：`pst_nowait_eagain_retries`、`pst_nowait_unsupported_fallbacks`

新增点查询实验（2026-03-09，NOWAIT A/B，`10M, uniform, 0.1N, 1 thread`）：

- baseline（`pst_nowait_poll=0`）：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260309_062140/RESULTS.md)
  - `avg=134443ns`, `p99=2105919ns`, `throughput=7424.89`
- treatment（`pst_nowait_poll=1`）：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260309_062523/RESULTS.md)
  - `avg=158066ns`, `p99=2060153ns`, `throughput=6312.69`
- 诊断：
  - 两组 `avg_io_total_per_query` 均为 `1.99689`（I/O 次数未变）
  - `pst_nowait_eagain_retries=0`
  - `pst_nowait_unsupported_fallbacks=0`
- 结论：
  - 本轮证据不支持“同步阻塞 I/O 是 p99 达到 ms 级的主因”。

## 当前边界与限制

- snapshot 只携带 partition 句柄状态，缺少更细粒度的一致性诊断字段。
- 双槽提交后，单槽最大 snapshot 空间为 `32MB`（原保留区 `64MB` 的一半）。

## 下一步

1. 重新组织 `01_point_lookup`：
   - warm-cache 口径补跑
   - 与当前 cold-I/O 结果做成对照表
2. 在实验文档中新增磁盘占用指标，并开展 warm/cold 对照实验。
3. 增加 CoW 命中率/写放大统计（复用页与新写页比例）。
4. 做 `RangeScan` 性能实验（warm/cold + 不同 prefix 比例）并补图。
