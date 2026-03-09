# 01 Point Lookup

## 目标

- 验证方向 B 在点查询上的平均延迟与尾延迟收益
- 量化点查询收益对应的内存开销
- 为后续原始单层 L1 与各类消融实验预留统一 benchmark 入口

## 当前冻结的实验口径

- key size = `16B`
- value size = `16B`
- 当前点查询 benchmark 已按真实持久化 `16B value` 路径运行：
  - WAL 为 `FLOWKV_KEY16` 固定 `64B` 槽位
  - flush / compaction / PST query / recovery 均已打通 `16B value`
  - 当前点查询结果可直接用于 `16B key + 16B value` 的论文主实验
- V4 路径补充：
  - RouteLayer 内存驻留
  - Subtree 查询路径已切到磁盘句柄按需加载（含 LRU cache）
  - 点查主路径已收紧为 `root->leaf` 逐页 I/O（优先使用句柄内 root 元数据）
  - 仅 legacy 快照（无 root 元数据）回退一次 manifest 读取
  - benchmark-only 快速建库模式：
    - `build_mode=fast_bulk_l1`
    - 统一通过 `experiments/common/fast_bulk_l1_builder.h` 的 `BuildFastBulkL1Dataset` 接口执行
    - 直接批量写 L1 PST + 一次性重建 L1 hybrid 索引
    - 跳过在线 `Put -> Flush -> Compaction` 中间过程，主要用于读性能实验的数据准备加速
  - benchmark 已支持 SSD 外存口径参数：
    - `build_mode`
    - `use_direct_io`
    - `warmup_queries`
    - `enable_subtree_cache / subtree_cache_capacity / subtree_cache_max_bytes`
    - `bitmap_persist_every`（SegmentAllocator 位图批量持久化间隔）

## 当前输出指标

- `avg_latency_ns`
- `p99_latency_ns`
- `throughput_ops`
- `avg_io_total_per_query`
- `avg_io_total_top1pct_latency`
- `avg_io_l1_pages_per_query`
- `avg_io_l1_pages_top1pct_latency`
- `avg_io_pst_reads_per_query`
- `avg_io_pst_reads_top1pct_latency`
- `rss_bytes`
- `l1_index_bytes_estimated`
- `l1_subtree_cache_bytes`
- `l1_subtree_cache_requests`
- `l1_subtree_cache_hits`
- `l1_subtree_cache_misses`
- `l1_subtree_cache_hit_rate`
- `l1_cow_persist_calls`
- `l1_cow_reused_pages`
- `l1_cow_written_pages`
- `l1_cow_reused_bytes`
- `l1_cow_written_bytes`
- `l1_cow_page_reuse_ratio`
- `pst_direct_unaligned_fallbacks`
- `pst_short_reads`
- `pst_nowait_eagain_retries`
- `pst_nowait_unsupported_fallbacks`

其中：

- `rss_bytes` 来自进程 RSS
- `l1_index_bytes_estimated` 来自实验专用的 L1 内存估算接口
- `l1_subtree_cache_bytes` 表示 L1 subtree query cache 当前估算占用
- `l1_subtree_cache_*` 用于量化 warm/cold 与缓存收益
- `l1_cow_*` 用于量化 CoW 复用率与写放大

额外实验配置字段（由 benchmark 输出）：

- `build_mode`
- `use_direct_io`
- `warmup_queries`
- `enable_subtree_cache`
- `subtree_cache_capacity`
- `subtree_cache_max_bytes`
- `bitmap_persist_every`
- `pst_nowait_poll`

结果文档规范（强制）：

- `results.csv` 必须保留 benchmark 输出的完整字段。
- `RESULTS.md` 必须采用 [20260308_112413/RESULTS.md](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260308_112413/RESULTS.md) 的组织格式。
- `RESULTS.md` 必须包含完整指标（至少附上完整 CSV dump），不能只保留摘要指标。
- `RESULTS.md` 必须包含以下固定章节（按顺序）：
  - `Run Scope`
  - `Raw Result Table`
  - `Memory Overhead Table`
  - `Initial Observations`
  - `Change Comparison`（若本次涉及实现/结构改动则必填）
  - `Figures`
  - `Raw Files`
  - `Complete Metrics (CSV Dump)`
- `Raw Result Table` 必须包含内存列：
  - `RSS (bytes)`
  - `L1 Index Memory (bytes)`（`l1_index_bytes_estimated`）
- `Memory Overhead Table` 必须至少包含：
  - `l1_route_partition_bytes`
  - `l1_route_index_estimated_bytes`
  - `l1_subtree_bytes`
  - `l1_subtree_cache_bytes`
  - `l1_governance_bytes`
- `Initial Observations` 至少给出 3 条结论：
  - 延迟/吞吐趋势
  - I/O 路径与计数一致性
  - 内存/缓存/CoW 或诊断指标变化
- `Change Comparison` 必须量化与上一组可比实验的差异：
  - 至少包含 `avg_latency_ns / p99_latency_ns / throughput_ops`
  - 若改动涉及查询路径，必须同时比较 `avg_io_l1_pages_per_query / avg_io_pst_reads_per_query`
  - 若改动涉及 O_DIRECT 或读取可靠性，必须比较 `pst_direct_unaligned_fallbacks / pst_short_reads`

## 当前工作负载

- 总点查询数：`1,000,000`
- 命中比例：`80% hit / 20% miss`
- 线程数：
  - `1`
  - `16`

## 当前数据维度

- 数据规模：
  - `10^6`
  - `10^7`
  - `10^8`
- prefix 基数：
  - `0.1N`
  - `0.05N`
  - `0.01N`
- 数据分布：
  - `uniform`
  - `prefix-skew`

## 当前实现状态

- 第一版 benchmark 程序：`point_lookup_benchmark.cpp`
- 当前先支持：
  - `direction_b_full`
- 当前已完成 `16B value` 持久化查询路径适配，并通过：
  - `db_l1_recovery_smoke_test`
  - `db_delete_correctness_smoke_test`
  - 小规模 point benchmark sanity run
- V4 当前实现说明：
  - `SubtreePageStore` 已支持非连续页持久化与跨 segment 句柄
  - `L1HybridIndex` 已支持可控 subtree page cache（LRU）
  - `PersistCow` + `DestroyUnshared` 已接入页级 CoW 发布路径
  - manifest 已支持 L1 hybrid 句柄快照恢复协议（含 seq/checksum envelope 校验）
  - `subtree_pages_cache` 仅用于 allocator 未接入的 debug/unit-test 场景
- 后续可继续接入：
  - 原始单层 L1
  - 去掉 prefix routing 的消融版
  - 去掉 CoW 的消融版

## 当前建议执行顺序

先跑：

- `10^6`
- `uniform`
- `0.1N`
- `1 thread`

确认结果口径稳定后，再扩到其余规模、prefix 基数、分布与线程数。

## SSD 外存首轮基线（本轮覆盖）

- `use_direct_io = 1`
- `warmup_queries = 0`
- `enable_subtree_cache = 1`
- `subtree_cache_capacity = 256`
- `subtree_cache_max_bytes = 256MB`

## 当前已完成实验

- 第一批正式点查询实验已完成：
  - `direction_b_full`
  - `10^6`
  - `uniform`
  - `1 thread`
  - （历史口径）prefix ratio = `0.5N / 0.1N / 0.01N`
  - （当前口径）prefix ratio = `0.1N / 0.05N / 0.01N`
- 数据目录位于：
  - `/mnt/nvme0/flowkv_exp/performance_evaluation/01_point_lookup/dbfiles`
- 结果目录位于：
  - [20260304_065044](/home/zwt/yjy/FlowKV/experiments/performance_evaluation/01_point_lookup/results/20260304_065044)
- 当前已生成：
  - 原始文本输出
  - `results.csv`
  - 结果汇总 `RESULTS.md`
  - 三张图：
    - average latency
    - P99 latency
    - L1 index memory
