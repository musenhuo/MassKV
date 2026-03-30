# FlowKV L1 Hybrid V5 Progress 20260310_085023

## 当前维护规则

- 从当前时刻开始，V5 为唯一主线。
- 后续仅持续维护：
  - [l1_hybrid_bptree_design_v5.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v5.md)
  - [FlowKV_L1_Hybrid_V5_Progress_20260310_085023.md](/home/zwt/yjy/FlowKV/FlowKV_L1_Hybrid_V5_Progress_20260310_085023.md)
- V4 及更早文档保留归档，不再作为执行基线。

## 当前已落地结果（2026-03-10）

- 读路径结构收敛：
  - `PublishedSnapshot` 移除 `page_refs` 池。
  - `PublishedRoutePartition` 改为保存 `root_page_ptr/page_count/page_size`。
  - 点查与范围查均改为 `root_page_ptr` 起始的页级遍历。
- 子树页协议收敛：
  - internal child 与 leaf prev/next 指针改为 `u64` 绝对地址。
  - 保留 `page_id` 作为逻辑稳定标识（用于导入/校验/CoW）。
- Subtree 存储层收敛：
  - `SubtreePageStoreHandle` 删除 `pages vector`。
  - `Persist/PersistCow` 改为页分配后重写绝对地址指针再落盘。
  - `Destroy/DestroyUnshared` 改为从 `root_page_ptr` 遍历收集页后回收。
- 恢复协议收敛：
  - `Version` 的 L1 hybrid snapshot 协议升级为 `format=3`。
  - 持久化字段改为 `root_page_ptr/manifest_page_ptr/page_count/record_count`，不再写 page_ref 列表。

## 本次验证结果

- 编译通过（定向目标）：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test`
- 运行通过：
  - `./build_hybrid_check/tests/hybrid_l1_test`
  - `./build_hybrid_check/tests/version_l1_selection_test`
  - `./build_hybrid_check/tests/db_l1_route_smoke_test`
- 说明：
  - 全量 `cmake --build` 仍会被 `hmasstree` 既有重复定义问题阻塞（与 V5 主线改动无关）。

## 当前阶段结论

- “prefix 直接路由到 layer1 根页绝对地址”的设计已经在代码主路径落地。
- 读路径已不依赖页表式中间结构，满足你提出的收敛目标。
- 下一阶段可直接进入 V5 基线下的点查性能复测与内存开销再拆分。

## 新增实验结果（2026-03-10，10M 点查）

- 运行目录：
  - [20260310_090035](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260310_090035)
- 配置：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=fast_bulk_l1`
  - `use_direct_io=1`
  - `pool_size=64 GiB`
- 核心结果：
  - `0.1N`: `avg=180,449ns`, `p99=2,142,539ns`, `l1_index=184,052,480B`
  - `0.05N`: `avg=129,431ns`, `p99=1,719,173ns`, `l1_index=92,053,760B`
  - `0.01N`: `avg=113,409ns`, `p99=389,991ns`, `l1_index=18,466,560B`
  - 三档均满足：`avg_io_l1_pages_per_query=1`，`avg_io_pst_reads_per_query≈1`
- 结果文档：
  - [RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260310_090035/RESULTS.md)
  - [results.csv](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260310_090035/results.csv)

## 新增口径收敛（2026-03-10）

- 点查实验内存统计口径已收敛为“L1 读路径索引开销”：
  - `l1_index_bytes_estimated = route_index_estimated_bytes + subtree_bytes + subtree_cache_bytes (+ governance_bytes)`
  - 不再把 `route_partition_bytes` 计入 `l1_index_bytes_estimated`
- `route_partition_bytes` 保留为独立列，仅用于控制面/恢复态结构观测。
- `experiments/performance_evaluation` 与 `01_point_lookup` 的 README 已同步更新为该口径。

## 新增读路径收敛（2026-03-10）

- 路由层查询已从 `prefix -> partition_idx -> root_page_ptr` 收敛为：
  - `prefix -> root_page_ptr`（Masstree value 直接承载根页绝对地址）
- 读路径已去掉 `partitions` 的 prefix 一致性二次校验。
- 点查与范围查的 subtree 遍历均改为“给定 `root_page_ptr` 直接页级遍历”。
- 定向验证通过：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test`

## 新增点查实验（2026-03-10，10M）

- 新结果目录：
  - [20260310_093411](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260310_093411)
- 实验口径：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=fast_bulk_l1`
- 结果要点：
  - `avg_io_l1_pages_per_query=1`（三档一致）
  - `avg_io_pst_reads_per_query≈1`（三档一致）
  - `l1_index_bytes_estimated` 已按“L1 读路径索引开销”统计，不计 `route_partition_bytes`

## 新增页大小收敛（2026-03-10）

- L1 subtree B+Tree 页大小明确固定为 `16KB`（leaf/internal 统一）。
- `Version` 初始化 L1HybridIndex 时显式设置 `subtree_page_size=16KB`。
- `ImportPersistedState` 增加校验：若导入句柄页大小非 `16KB`，拒绝导入以避免读路径页粒度不一致。

## 新增 suffix 语义收敛（2026-03-11）

- subtree layer1 内部节点与叶子定位已改为 `suffix` 语义：
  - internal `high_keys` 改为 `RouteSuffix`
  - leaf `high_key` 改为 `RouteSuffix`
  - 磁盘查询路径 `root_page_ptr -> leaf` 改为按 `suffix` 比较下钻
- subtree 页格式已升级，页头/分隔键按 `suffix` 编码。
- 已验证：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test`
  - `point_lookup_benchmark` 小规模 sanity run

## 新增直接物理块收敛（2026-03-11）

- L1 点查主路径已去掉 `table_idx -> level1_tables_ -> PSTMeta` 依赖。
- subtree leaf record 现在承担最终物理块定位职责：
  - `kv_block_ptr`
  - `offset`
  - `count`
- `Version::Get` 在 L1 分支中直接执行：
  - `prefix -> root_page_ptr`
  - `root->leaf (suffix)`
  - `kv_block_ptr + offset + count`
  - `PST block window binary search`
- 读路径不再为了获取 `datablock_ptr_ / entry_num_` 回查 `level1_tables_`。
- 控制面 `level1_tables_ / table_idx` 仍暂时保留，供 compaction / overlap / 调试接口使用；已不属于点查主路径。

## 新增点查实验（2026-03-11，10M）

- 新结果目录（单目录落盘）：
  - [20260311_064737](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260311_064737)
- 实验口径：
  - `key_count=10,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=fast_bulk_l1`
  - `use_direct_io=1`
  - `pool_size=256 GiB`
- 核心结果：
  - `0.1N`: `avg=137,069ns`, `p99=2,143,809ns`, `throughput=7,282.21ops/s`, `l1_index=128,050,432B`
  - `0.05N`: `avg=128,219ns`, `p99=1,682,324ns`, `throughput=7,780.18ops/s`, `l1_index=64,051,712B`
  - `0.01N`: `avg=102,188ns`, `p99=314,543ns`, `throughput=9,754.63ops/s`, `l1_index=12,864,512B`
  - 三档均满足：`avg_io_l1_pages_per_query=1`，`avg_io_pst_reads_per_query≈1`
- 相比 [20260310_093411](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260310_093411)：
  - `avg_latency_ns` 三档均下降
  - `throughput_ops` 三档均提升
  - `p99` 在 `0.05N` 明显下降，在 `0.1N/0.01N` 轻微回升

## 新增实验脚本收敛（2026-03-11）

- `run_point_lookup_batch.py` 已调整为“单时间标识”行为：
  - 传入 `--run-id` 时，结果目录与 `db_root` 子目录统一使用同一 `run_id`
  - 避免同一次实验出现额外时间戳标识
- 同时修复 `datetime.utcnow()` 弃用警告，改为 `datetime.now(datetime.UTC)`。

## 新增点查实验（2026-03-11，100M）

- 结果目录：
  - [20260311_071438](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260311_071438)
- 实验口径：
  - `key_count=100,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=fast_bulk_l1`
  - `use_direct_io=1`
  - `pool_size=1 TiB`
- 核心结果：
  - `0.1N`: `avg=126,114ns`, `p99=409,980ns`, `throughput=7,907.81ops/s`, `l1_index=1,280,050,432B`
  - `0.05N`: `avg=104,623ns`, `p99=276,506ns`, `throughput=9,537.28ops/s`, `l1_index=640,051,712B`
  - `0.01N`: `avg=65,969.7ns`, `p99=194,734ns`, `throughput=15,115.1ops/s`, `l1_index=128,064,512B`
  - 三档均满足：`avg_io_l1_pages_per_query=1`，`avg_io_pst_reads_per_query≈1`

## 新增实验产物自动化（2026-03-11）

- 新增脚本：
  - [generate_point_lookup_report.py](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/generate_point_lookup_report.py)
- 作用：
  - 自动生成 `plots/*.png`
  - 自动生成 `RESULTS.md`（含固定章节与完整 CSV dump）
  - 自动尝试选择同规模同配置上一组结果做 `Change Comparison`
- 批跑脚本已接入自动报告：
  - [run_point_lookup_batch.py](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/run_point_lookup_batch.py)
  - 默认行为：每次实验完成自动产出 `results.csv + meta.json + plots + RESULTS.md`
  - 保留 `--skip-report=1` 作为显式跳过开关（默认不跳过）

## 新增点查实验（2026-03-11，100M，极端倾斜，fanout=1000）

- 结构参数调整：
  - `subtree internal_fanout` 默认值已收敛为 `1000`
- 结果目录：
  - [20260311_094812](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260311_094812)
- 实验口径：
  - `key_count=100,000,000`
  - `query_count=1,000,000`
  - `distribution=prefix-skew-extreme`
  - `prefix_ratio=0.0005`（`prefix_count=50,000`）
  - `threads=1`
  - `build_mode=fast_bulk_l1`
  - `use_direct_io=1`
  - `pool_size=1 TiB`
- 核心结果：
  - `avg=94,610.9ns`
  - `p99=270,531ns`
  - `throughput=10,545.6ops/s`
  - `avg_io_l1_pages_per_query=1.96532`
  - `avg_io_pst_reads_per_query=0.975143`
  - `avg_io_total_per_query=2.94046`（接近“2 层 subtree + 1 次 PST”目标）
  - `l1_index_bytes_estimated=6,451,712B`

## 新增路由层键收敛（2026-03-11）

- 路由层（layer0）键已收敛为 `8B prefix`：
  - `route_layout` 不再使用 `Key16{prefix, suffix}` 作为路由键。
  - `MakeRouteKey(prefix)` 现在返回 `RoutePrefix(uint64)` 本身。
- 为兼容 `FLOWKV_KEY16` 主编译配置，`MasstreeWrapper` 增加了 `uint64` 路由接口：
  - `insert(uint64_t, ValueHelper&)`
  - `search(uint64_t, uint64_t&)`
  - `scan(uint64_t, uint64_t, std::vector<uint64_t>&, std::vector<uint64_t>&)`
- 验证通过：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test`

## 新增内存口径收敛（2026-03-11）

- `l1_index_bytes_estimated` 已收敛为“仅统计 route Masstree”：
  - `MemoryUsageStats::ReadPathBytes()` 仅返回 `route_index_estimated_bytes`
  - `route_partition/subtree/subtree_cache/governance` 均不再并入 `L1 Index Memory (bytes)` 主指标
- 文档已同步更新：
  - [experiments/performance_evaluation/README.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/README.md)
  - [experiments/performance_evaluation/01_point_lookup/README.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/README.md)

## 新增点查实验（2026-03-11，100M，uniform，新内存口径）

- 结果目录：
  - [20260311_102527](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260311_102527)
- 实验口径：
  - `key_count=100,000,000`
  - `query_count=1,000,000`
  - `distribution=uniform`
  - `threads=1`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=fast_bulk_l1`
  - `use_direct_io=1`
  - `pool_size=1 TiB`
- 核心结果：
  - `0.1N`: `avg=126,812ns`, `p99=383,510ns`, `throughput=7,867.63ops/s`, `l1_index=640,000,000B`
  - `0.05N`: `avg=86,653ns`, `p99=223,616ns`, `throughput=11,511.9ops/s`, `l1_index=320,000,000B`
  - `0.01N`: `avg=98,340.5ns`, `p99=284,224ns`, `throughput=10,141.9ops/s`, `l1_index=64,000,000B`
  - 三档均满足：`avg_io_l1_pages_per_query=1`，`avg_io_pst_reads_per_query≈1`

## 新增对齐硬约束（2026-03-11）

- 路由索引内存估算改正：
  - `kRouteMasstreeBytesPerEntry: 64 -> 32`
  - `l1_route_index_estimated_bytes = prefix_count * 32`（按论文近似口径）
- 尺寸/编码静态约束已落地：
  - `RoutePrefix/RouteSuffix` 固定 `8B`
  - `FLOWKV_KEY16` 下 `KeyType` 固定 `16B`
  - `leaf_value` 位布局固定 `44/12/8`，总计 `64 bits`
  - `kKvBlockShift=12`（固定 `4KB` block）
  - `count bits` 与 `PDataBlock::MAX_ENTRIES` 容量校验
- 结构约束已落地：
  - `L1HybridIndex` 强制 subtree page size 固定 `16KB`（非 16KB 直接拒绝初始化）
- 设计文档新增“路径+尺寸+结构对齐检查”章节：
  - [l1_hybrid_bptree_design_v5.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v5.md)
- 口径 sanity（小规模）：
  - `key_count=100,000`，`prefix_count=10,000`
  - 输出：`l1_route_index_estimated_bytes=320,000`，`l1_index_bytes_estimated=320,000`
  - 证明当前主指标已按 `32B/prefix` 生效（不再是历史 `64B/prefix`）

## 新增在线写性能实验模块（2026-03-12 12:09:23 UTC）

- 03 实验槽位已收敛为“在线写性能（compaction-focused）”：
  - [experiments/performance_evaluation/03_compaction_update/README.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/README.md)
- 新增在线写 benchmark（固定 `build_mode=online`）：
  - [write_online_benchmark.cpp](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp)
  - 实测输出包含：
    - `avg_put_latency_ns` / `p99_put_latency_ns`
    - `put_path_throughput_ops` / `ingest_throughput_ops`
    - `flush_*` / `compaction_*`（次数与时间）
    - `compaction_time_ratio`
    - `l1_route_index_measured_bytes`
    - `l1_cow_*`
- 新增批跑与报告链路：
  - [run_write_online_batch.py](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/run_write_online_batch.py)
  - [generate_write_online_report.py](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/generate_write_online_report.py)
  - [plot_write_online_results.py](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/plot_write_online_results.py)
- 构建系统已接入：
  - [experiments/performance_evaluation/CMakeLists.txt](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/CMakeLists.txt)
  - [experiments/performance_evaluation/03_compaction_update/CMakeLists.txt](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/CMakeLists.txt)
- 小规模 sanity 已完成（`write_ops=20,000`）：
  - [results/sanity_20260312](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/sanity_20260312)
  - 已生成：`raw + results.csv + RESULTS.md + plots`

## 新增 compaction 提交期单次重建收敛（2026-03-13）

- 问题：
  - compaction 提交阶段每插入/删除一个 L1 表都会立即触发一次 `RebuildLevel1Partitions`，导致重复重建。
- 实现：
  - 在 `Version` 增加 L1 批更新窗口：
    - `BeginL1BatchUpdate()`
    - `EndL1BatchUpdate()`
    - 批窗口内只累积 `changed_route_keys`，窗口结束后统一执行一次重建。
  - 关键改动文件：
    - [version.h](/home/zwt/yjy/FlowKV/db/compaction/version.h)
    - [version.cpp](/home/zwt/yjy/FlowKV/db/compaction/version.cpp)
    - [compaction.cpp](/home/zwt/yjy/FlowKV/db/compaction/compaction.cpp)
  - `CleanCompaction` 与 `CleanCompactionWhenUsingSubCompaction` 已接入批窗口。
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`
  - 在线写 sanity：
    - [results/sanity_batch_rebuild_once_20260313](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/sanity_batch_rebuild_once_20260313)
  - 输出链路完整：`raw + results.csv + RESULTS.md + plots`

## 新增在线写实验（2026-03-13，1,000,000 writes，uniform）

- 结果目录：
  - [20260313_082428](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/20260313_082428)
- 实验口径：
  - `write_ops=1,000,000`
  - `distribution=uniform`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=online`
  - `use_direct_io=1`
- 核心结果：
  - `0.1N` (`prefix_count=100,000`)：
    - `avg_put_latency_ns=299.801`
    - `p99_put_latency_ns=848`
    - `ingest_throughput_ops=202,821`
    - `compaction_total_time_ms=3,639.760`
    - `compaction_time_ratio=0.7382`
    - `l1_route_index_measured_bytes=4,243,792`
  - `0.05N` (`prefix_count=50,000`)：
    - `avg_put_latency_ns=243.892`
    - `p99_put_latency_ns=367`
    - `ingest_throughput_ops=368,259`
    - `compaction_total_time_ms=1,451.190`
    - `compaction_time_ratio=0.5344`
    - `l1_route_index_measured_bytes=2,126,304`
  - `0.01N` (`prefix_count=10,000`)：
    - `avg_put_latency_ns=200.096`
    - `p99_put_latency_ns=401`
    - `ingest_throughput_ops=472,925`
    - `compaction_total_time_ms=1,014.380`
    - `compaction_time_ratio=0.4797`
    - `l1_route_index_measured_bytes=366,560`

## 新增写实验并发参数收敛（2026-03-13）

- 03 在线写 benchmark 支持并发写参数：
  - `write_online_benchmark.cpp` 新增 `--threads`
  - `run_write_online_batch.py` 新增 `--threads` 并透传到 benchmark
  - `generate_write_online_report.py` 的 Run Scope 新增 `threads` 展示
- 多线程写入实现：
  - 每个 worker 在线程内创建并使用自己的 `MYDBClient(tid)`，避免跨线程持有 client
  - 写入仍按 `flush_batch` 分批，每批写后执行 `BGFlush`，并按阈值触发 `BGCompaction`

## 新增在线写实验（2026-03-13，10,000,000 writes，4 threads，uniform）

- 结果目录：
  - [20260313_083734](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/20260313_083734)
- 实验口径：
  - `write_ops=10,000,000`
  - `threads=4`
  - `distribution=uniform`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=online`
  - `use_direct_io=1`
- 核心结果：
  - `0.1N` (`prefix_count=1,000,000`)：
    - `avg_put_latency_ns=420.783`
    - `p99_put_latency_ns=1,556`
    - `put_path_throughput_ops=2,376,520`
    - `ingest_throughput_ops=18,133.8`
    - `compaction_total_time_ms=537,507`
    - `compaction_time_ratio=0.9747`
  - `0.05N` (`prefix_count=500,000`)：
    - `avg_put_latency_ns=395.442`
    - `p99_put_latency_ns=1,453`
    - `put_path_throughput_ops=2,528,810`
    - `ingest_throughput_ops=35,684.2`
    - `compaction_total_time_ms=268,386`
    - `compaction_time_ratio=0.9577`
  - `0.01N` (`prefix_count=100,000`)：
    - `avg_put_latency_ns=407.615`
    - `p99_put_latency_ns=1,218`
    - `put_path_throughput_ops=2,453,300`
    - `ingest_throughput_ops=118,616`
    - `compaction_total_time_ms=74,521.2`
    - `compaction_time_ratio=0.8839`

## 新增在线写实验（2026-03-13，10,000,000 writes，1 thread，uniform）

- 结果目录：
  - [20260313_085729](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/20260313_085729)
- 实验口径：
  - `write_ops=10,000,000`
  - `threads=1`
  - `distribution=uniform`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=online`
  - `use_direct_io=1`
- 核心结果：
  - `0.1N` (`prefix_count=1,000,000`)：
    - `avg_put_latency_ns=240.406`
    - `p99_put_latency_ns=543`
    - `put_path_throughput_ops=4,159,630`
    - `ingest_throughput_ops=18,274.1`
    - `compaction_total_time_ms=536,172`
    - `compaction_time_ratio=0.9798`
  - `0.05N` (`prefix_count=500,000`)：
    - `avg_put_latency_ns=233.212`
    - `p99_put_latency_ns=532`
    - `put_path_throughput_ops=4,287,950`
    - `ingest_throughput_ops=34,346.4`
    - `compaction_total_time_ms=265,983`
    - `compaction_time_ratio=0.9136`
  - `0.01N` (`prefix_count=100,000`)：
    - `avg_put_latency_ns=295.478`
    - `p99_put_latency_ns=543`
    - `put_path_throughput_ops=3,384,350`
    - `ingest_throughput_ops=114,144`
    - `compaction_total_time_ms=75,547.3`
    - `compaction_time_ratio=0.8623`

## 新增后台 compaction 线程收敛（2026-03-13）

- 目标：
  - 将后台 compaction 线程并发固定为 `4`（不再跟随 `RANGE_PARTITION_NUM=12`）。
- 改动：
  - [db.cpp](/home/zwt/yjy/FlowKV/db/db.cpp)
  - `MYDB::MYDB()` 与 `MYDB::MYDB(const MYDBConfig&)` 中：
    - `compaction_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM)`
    - 改为 `compaction_thread_pool_->SetBackgroundThreads(4)`
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`

## 新增 compaction 落盘批量化（2026-03-13）

- 目标：
  - 减少 `PDataBlock(4KB)` 逐块 `pwrite` 的系统调用开销。
- 改动：
  - [datablock_writer.h](/home/zwt/yjy/FlowKV/db/datablock_writer.h)
    - 新增 `PendingBlockWrite` 队列与批量阈值 `128KB`
    - 新增 `QueueCurrentBlockWrite` / `FlushPendingWrites`
  - [datablock_writer.cpp](/home/zwt/yjy/FlowKV/db/datablock_writer.cpp)
    - `Flush()` 改为入队而非立即 `pwrite`
    - 达到 `128KB` 时触发批量落盘（连续物理地址合并为单次大写）
    - `PersistCheckpoint()` 先强制刷空 pending 队列，再关闭 segment
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`

## 新增 manifest 批量提交（2026-03-13）

- 目标：
  - 将 compaction clean 阶段的 `AddTable/DeleteTable/UpdateVersion` 从“逐条立即刷盘”改为“内存页聚合后统一提交”。
- 改动：
  - [manifest.h](/home/zwt/yjy/FlowKV/db/compaction/manifest.h)
    - 新增批处理接口：`BeginBatchUpdate / CommitBatchUpdate / AbortBatchUpdate`
    - 新增 batch 页缓存与 shadow super 元数据
  - [manifest.cpp](/home/zwt/yjy/FlowKV/db/compaction/manifest.cpp)
    - `AddTable/DeleteTable` 支持 batch 模式（按 4KB 页缓存并标脏）
    - `UpdateL0Version/UpdateL1Version` 在 batch 模式仅更新 shadow super，提交时一次写 super
    - `CommitBatchUpdate`：先写脏 manifest 页，再写 super（提交点）
  - [compaction.cpp](/home/zwt/yjy/FlowKV/db/compaction/compaction.cpp)
    - `CleanCompaction` 与 `CleanCompactionWhenUsingSubCompaction` 接入 manifest batch 提交
- 说明：
  - 这是 durable 提交路径的第一阶段收敛（“页聚合 + super 最后写”）。
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`
  - 在线写小规模 sanity 通过（`20,000` 写入，含 flush+compaction）

## 新增 manifest durable 协议（2026-03-13）

- 目标：
  - 将 manifest batch commit 从“页聚合提交”收敛到“可恢复的 durable 提交协议”。
- 改动：
  - [manifest.h](/home/zwt/yjy/FlowKV/db/compaction/manifest.h)
    - manifest 空间布局新增事务区：`ManifestTxnLogSize=64MB`
    - `ManifestSize` 扩展为：`super + L0 + L1 + flush_log + l1_hybrid_state + manifest_txn`
  - [manifest.cpp](/home/zwt/yjy/FlowKV/db/compaction/manifest.cpp)
    - `CommitBatchUpdate` 改为三阶段：
      - 先写 txn payload（脏页列表 + 可选 super 页）
      - 再写 txn header（`prepared`）
      - 再 apply 到真实 manifest 页与 super，最后清空 txn header
    - 新增 `ReplayPendingBatchTxn`：
      - recover 启动时扫描 txn header
      - 若存在 `prepared` 未完成事务，则按 redo 方式重放脏页与 super
      - 重放完成后清空 txn header
    - 构造函数 recover 路径已接入事务重放逻辑
    - `flush_log` 偏移口径统一为 `FlushLogSize`
- 效果：
  - compaction clean 阶段的 manifest 批量更新具备“崩溃后可重放完成”的提交语义。
  - 对原有写路径调用点无侵入（`compaction.cpp` 仍通过 `Begin/CommitBatchUpdate` 触发）。
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`
  - 运行通过：`sanity_20260313_durable`（`20,000` 在线写，uniform，1 thread）
    - 结果目录：
      - [sanity_20260313_durable](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/sanity_20260313_durable)

## 新增 durable 完善（2026-03-13，第二阶段）

- 改动：
  - [manifest.cpp](/home/zwt/yjy/FlowKV/db/compaction/manifest.cpp)
    - `CommitBatchUpdate` 新增两处 `fdatasync` 屏障：
      - prepare（txn payload + header）后立即落盘屏障
      - apply（真实页 + super + clear header）后立即落盘屏障
    - recover replay 中：
      - 清理非法 txn header 后增加落盘屏障
      - 重放成功并清理 header 后增加落盘屏障
    - fresh init（`recover=false`）路径显式清空 txn header，避免旧事务残留污染后续恢复
    - 修复 `GetFlushLog` 临时缓冲区未释放问题
- 新增测试：
  - [manifest_batch_txn_replay_test.cpp](/home/zwt/yjy/FlowKV/tests/manifest_batch_txn_replay_test.cpp)
  - [tests/CMakeLists.txt](/home/zwt/yjy/FlowKV/tests/CMakeLists.txt)
  - 用例通过“手工注入 prepared 事务 -> recover 自动重放 -> 校验页与 super 更新 -> 校验 txn header 被清空”验证 durable replay 闭环
- 验证：
  - 构建通过：`flowkv`、`write_online_benchmark`、`manifest_batch_txn_replay_test`
  - 运行通过：
    - `./build_hybrid_check/tests/manifest_batch_txn_replay_test`
    - `sanity_20260313_durable_v2`（在线写 20,000）
      - [sanity_20260313_durable_v2](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/sanity_20260313_durable_v2)

## 新增 durable 故障注入恢复（2026-03-13，第三阶段）

- 改动：
  - [manifest.cpp](/home/zwt/yjy/FlowKV/db/compaction/manifest.cpp)
    - 新增测试 failpoint：
      - `FLOWKV_MANIFEST_TXN_FAILPOINT=after_prepare_sync`
      - `FLOWKV_MANIFEST_TXN_FAILPOINT=after_apply_before_clear`
    - 用于在 `CommitBatchUpdate` 关键阶段模拟进程崩溃（`_exit(99)`）
- 新增测试：
  - [manifest_durable_crash_recovery_smoke_test.cpp](/home/zwt/yjy/FlowKV/tests/manifest_durable_crash_recovery_smoke_test.cpp)
  - [tests/CMakeLists.txt](/home/zwt/yjy/FlowKV/tests/CMakeLists.txt)
  - 测试流程：
    - 子进程写入 + flush + compaction，命中 `after_prepare_sync` failpoint 崩溃
    - 父进程重启 DB（`recover=true`）并校验关键键值可读
    - 验证 manifest 事务重放在真实崩溃场景下生效
- 验证：
  - 构建通过：
    - `manifest_batch_txn_replay_test`
    - `manifest_durable_crash_recovery_smoke_test`
  - 运行通过：
    - `./build_hybrid_check/tests/manifest_batch_txn_replay_test`
    - `./build_hybrid_check/tests/manifest_durable_crash_recovery_smoke_test`

## 新增 durable 双崩溃窗口覆盖（2026-03-13，第四阶段）

- 改动：
  - [manifest_durable_crash_recovery_smoke_test.cpp](/home/zwt/yjy/FlowKV/tests/manifest_durable_crash_recovery_smoke_test.cpp)
    - 由单场景扩展为双场景：
      - `after_prepare_sync`
      - `after_apply_before_clear`
    - 每个场景均执行：
      - 子进程触发 failpoint 崩溃
      - 首次恢复校验 key 可读
      - 二次恢复校验 txn 已清理、恢复幂等
- 验证：
  - 构建通过：
    - `manifest_durable_crash_recovery_smoke_test`
  - 运行通过：
    - `./build_hybrid_check/tests/manifest_durable_crash_recovery_smoke_test`
    - `ctest -R "manifest_(batch_txn_replay|durable_crash_recovery_smoke)_test"`

## 新增 durable crash-fuzz 回归（2026-03-13，第五阶段）

- 新增测试：
  - [manifest_durable_crash_fuzz_test.cpp](/home/zwt/yjy/FlowKV/tests/manifest_durable_crash_fuzz_test.cpp)
  - [tests/CMakeLists.txt](/home/zwt/yjy/FlowKV/tests/CMakeLists.txt)
- 覆盖策略：
  - 固定随机种子，多轮（`10` 轮）随机选择 failpoint：
    - `after_prepare_sync`
    - `after_apply_before_clear`
  - 每轮均执行：
    - 子进程注入崩溃
    - 父进程首次恢复校验数据
    - 二次恢复幂等校验
- 验证：
  - 构建通过：
    - `manifest_durable_crash_fuzz_test`
  - 运行通过：
    - `./build_hybrid_check/tests/manifest_durable_crash_fuzz_test`
    - `ctest -R "manifest_(batch_txn_replay|durable_crash_recovery_smoke|durable_crash_fuzz)_test"`

## 新增在线写实验（2026-03-13，10,000,000 writes，1 thread，uniform，复跑）

- 结果目录：
  - [20260313_104446](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/03_compaction_update/results/20260313_104446)
- 实验口径：
  - `write_ops=10,000,000`
  - `threads=1`
  - `distribution=uniform`
  - `prefix_ratio=0.1N / 0.05N / 0.01N`
  - `build_mode=online`
  - `use_direct_io=1`
  - `db_root=/mnt/nvme0/flowkv_exp/performance_evaluation/03_compaction_update/dbfiles`
- 核心结果：
  - `0.1N` (`prefix_count=1,000,000`)：
    - `avg_put_latency_ns=241.524`
    - `p99_put_latency_ns=550`
    - `put_path_throughput_ops=4,140,380`
    - `ingest_throughput_ops=18,764.7`
    - `compaction_total_time_ms=521,056`
    - `compaction_time_ratio=0.977748`
  - `0.05N` (`prefix_count=500,000`)：
    - `avg_put_latency_ns=236.617`
    - `p99_put_latency_ns=542`
    - `put_path_throughput_ops=4,226,240`
    - `ingest_throughput_ops=37,655`
    - `compaction_total_time_ms=252,154`
    - `compaction_time_ratio=0.949486`
  - `0.01N` (`prefix_count=100,000`)：
    - `avg_put_latency_ns=284.988`
    - `p99_put_latency_ns=514`
    - `put_path_throughput_ops=3,508,920`
    - `ingest_throughput_ops=146,937`
    - `compaction_total_time_ms=52,935.4`
    - `compaction_time_ratio=0.777817`
