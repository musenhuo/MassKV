# FlowKV L1 Hybrid V6 进度（更新于 2026-03-18 05:16:10 UTC）

## 已实现结果（当前有效）

### 1) 增量补丁驱动主线（阶段 A）

- `DeltaBatch` 结构与 compaction 产出已落地：
  - `lib/hybrid_l1/l1_delta_batch.h`
  - `db/compaction/compaction.cpp`
- `Version` 已支持 batch 内消费增量补丁并触发受影响 prefix 重建：
  - `db/compaction/version.cpp`

### 2) 密度驱动双路径（阶段 B）

- `Version -> L1HybridIndex -> L1HybridRebuilder` 已支持携带 `DeltaBatch`。
- `ChooseUpdateMode` 已按 `delta_op_count` 计算变更密度并在 CoW / bulk-load 之间决策。

### 3) Leaf-stream bulk-load 初版（阶段 C）

- `BuildRecordsFromDelta(...)` 已接入 bulk-load 分支（开关控制）：
  - `FLOWKV_L1_ENABLE_LEAF_STREAM_BULKLOAD=1`

### 4) V6 观测指标已打通（本次新增）

- `L1HybridRebuilder` 增加运行期实测指标累计：
  - `delta_prefix_count`
  - `delta_ops_count`
  - `index_update_total_ms`
  - `index_update_cow_ms`
  - `index_update_bulk_ms`
  - `cow_prefix_count`
  - `bulk_prefix_count`
  - `leaf_stream_merge_ms`
  - `rebuild_fallback_count`
- 写实验 03 已接入这些指标输出与报告展示：
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
  - `experiments/performance_evaluation/03_compaction_update/run_write_online_batch.py`
  - `experiments/performance_evaluation/03_compaction_update/generate_write_online_report.py`
  - `experiments/performance_evaluation/03_compaction_update/README.md`

### 5) Leaf-stream 删除匹配已收紧（本次新增）

- 删除条件从“范围重叠 + 可选块指针”收紧为：
  - 先要求记录被删除范围完整包含；
  - 若带 `kv_block_ptr`，还要求 `kv_block_ptr` 相同；
  - 且要求 suffix 窗口精确匹配，减少误删面。
- 代码位置：
  - `lib/hybrid_l1/l1_hybrid_rebuilder.cc`（`MatchesDeleteOp`）

### 6) durable 提交协议已收口到同一批次（本次新增）

- compaction 提交流程已改为：
  - 先在 batch 内完成表增删、版本推进、L1 重建；
  - 再在同一 batch 内写入 L1 hybrid snapshot；
  - 最后一次 `CommitBatchUpdate()` 原子提交。
- `Manifest::PersistL1HybridState(...)` 已支持 `batch_active`：
  - snapshot 页写入 batch 脏页集合；
  - super 元数据在 batch super 中更新；
  - 与 manifest 元数据同事务落盘。
- `Manifest::ClearL1HybridState()` 也支持 batch 模式，不再破坏事务边界。
- durable 回归测试已通过（非性能实验）：
  - `manifest_batch_txn_replay_test`
  - `manifest_durable_crash_recovery_smoke_test`
  - `manifest_durable_crash_fuzz_test`

### 7) 10M 在线写入实验已完成（本次新增）

- 实验目录：
  - `experiments/performance_evaluation/03_compaction_update/results/20260318_050353`
- 本次配置：
  - online build、uniform、`write_ops=10,000,000`、`threads=1`、`prefix_ratio=0.1`
- 核心实测结果：
  - `ingest_throughput_ops=14325.4`
  - `avg_put_latency_ns=251.168`
  - `p99_put_latency_ns=556`
  - `compaction_total_time_ms=680051`
  - `compaction_time_ratio=0.9742`
  - `l1_route_index_measured_bytes=28778544`

## 当前总进度（V6）

- 设计实现进度：约 **96%**
- 剩余核心工作：
  1. 基于新指标继续扩展写实验（线程数、prefix 比例）并确认 `compaction_total_time_ms` 与 `index_update_*` 的变化关系。
