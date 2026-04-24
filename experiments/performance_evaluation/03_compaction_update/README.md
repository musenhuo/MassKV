# 03 Online Write Performance

## 目标

- 评估在线写路径（`Put -> WAL -> memtable -> Flush -> Compaction -> L1 hybrid rebuild`）性能
- 重点量化 compaction 在总写入时间中的占比与代价
- 为后续与 RocksDB 的写性能对比提供统一口径

## 冻结配置

- key size = `16B`
- value size = `16B`
- 写入操作数（每组）= `1,000,000`
- 后台治理阈值：
  - `l0_compaction_trigger`：达到后后台触发 compaction
  - `l0_write_stall_threshold`：达到后前台写阻塞，直到 L0 树数回落到阈值以下
- 写路径固定：
  - `build_mode=online`
  - 维护模式口径：
    - `maintenance_mode=background`：默认与官方推荐口径，前台持续写，后台自动触发 flush/compaction（FlowKV 原生风格）
    - `maintenance_mode=manual`：仅用于调试拆分 flush/compaction 代价，不作为正式吞吐主结果
    - `maintenance_mode=overlap`：已兼容映射为 `background`，不再作为独立模式
- prefix ratio 三档：
  - `0.1N`
  - `0.05N`
  - `0.01N`
- 分布：
  - `uniform`
  - `prefix-skew`

## 指标口径（实测）

以下指标全部来自运行时计时/计数，不使用理论估算：

- `avg_put_latency_ns`
- `p99_put_latency_ns`
- `put_path_throughput_ops`（已对齐为“前台 Put 阶段墙钟吞吐”）
- `foreground_put_throughput_ops`
- `foreground_put_phase_time_ms`
- `drain_wait_time_ms`
- `ingest_throughput_ops`
- `end_to_end_throughput_ops`
- `total_ingest_time_ms`
- `flush_count`
- `flush_total_time_ms`
- `flush_avg_time_ms`
- `flush_max_time_ms`
- `compaction_count`
- `compaction_total_time_ms`
- `compaction_avg_time_ms`
- `compaction_max_time_ms`
- `compaction_time_ratio`
- `rss_bytes`
- `rss_before_drain_wait_bytes`
- `rss_after_drain_wait_bytes`
- `rss_drain_wait_delta_bytes`（after - before）
- `l1_route_index_measured_bytes`
- `l1_cow_*`（页面复用/写入相关计数）
- `delta_prefix_count`
- `delta_ops_count`
- `effective_delta_prefix_count`
- `effective_delta_ops_count`
- `index_update_total_ms`
- `index_update_cow_ms`
- `index_update_bulk_ms`
- `cow_prefix_count`
- `bulk_prefix_count`
- `tiny_descriptor_count`
- `normal_pack_count`
- `tiny_hit_ratio`
- `dirty_pack_pages`
- `pack_write_bytes`
- `leaf_stream_merge_ms`
- `rebuild_fallback_count`

## 输出规范

- 每次运行必须同时产出：
  - `raw/*.txt`
  - `results.csv`
  - `RESULTS.md`
  - `plots/*.png`
- `RESULTS.md` 需要包含：
  - `Run Scope`
  - `Raw Result Table`
    - 吞吐字段以 `Foreground Throughput` 为主口径
    - `End-to-End Throughput` 仅作参考
  - `Compaction Detail Table`
  - `Initial Observations`
  - `Change Comparison`
  - `Figures`
  - `Raw Files`
  - `Complete Metrics (CSV Dump)`

## 入口

- benchmark:
  - `write_online_benchmark.cpp`
- batch runner:
  - `run_write_online_batch.py`
- report generator:
  - `generate_write_online_report.py`

## Phase 2 验收脚本

- `run_phase2_acceptance.sh`
  - 作用：
    - 构建并运行 Phase 2 关键测试（`version_l1_selection_test`、`db_l1_*`、`manifest_*`、durable crash tests）。
    - 采集每项测试的峰值 RSS（`/usr/bin/time`）。
    - 校验默认路径关键日志：`tableless recovery`、`record-only`、是否触发 direct table materialization fallback。
    - 输出汇总：
      - `summary.csv`
      - `PHASE2_ACCEPTANCE.md`
      - `logs/` 与 `time/`
  - 用法：
    - `./experiments/performance_evaluation/03_compaction_update/run_phase2_acceptance.sh`
    - 默认复用当前仓库的 `build/`
    - 可选传入 build 目录：`./.../run_phase2_acceptance.sh build_phase2_record_only`
    - 若传入目录的 `CMakeCache.txt` 属于其他源码树，脚本会直接失败，避免误跑到别的仓库
  - 可选开关：
    - `FLOWKV_PHASE2_ACCEPTANCE_RUN_BENCH=1`：追加小规模 `write_online_benchmark` 双模式对比（phase2_default vs records_disabled）。
