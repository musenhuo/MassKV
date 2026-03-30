# MassKV 当前进展

最后更新：2026-03-22

## 已完成

### 核心架构
- [x] 基础 LSM 引擎（Memtable → L0 → L1）
- [x] PST (Persistent Sorted Table) 读写路径
- [x] WAL (LogWriter/LogReader)
- [x] SegmentAllocator (PM 空间管理)
- [x] Version (volatile) + Manifest (persistent) 元数据层
- [x] 分区并行 Flush / Compaction (RANGE_PARTITION_NUM=12)
- [x] KV 分离模式 (INDEX_LOG_MEMTABLE + KV_SEPARATE)

### Hybrid L1 Index (V4→V7)
- [x] V4: RouteLayer (Masstree) + SubtreeLayer (B+Tree)，SSD 主路径，快照发布
- [x] V5: 绝对地址协议，leaf_value 直接编码 (kv_block_ptr, offset, count)，format=3 恢复
- [x] V6: L1DeltaBatch 增量 patch 驱动更新，密度驱动 CoW/bulk-load 决策，leaf-stream bulk-load
- [x] V7: patch 粒度收紧，TinyDirect + NormalPack descriptor 双模式，迟滞阈值，route swap
- [x] SubtreePageStore：CoW 持久化，pwritev 批量写，page magic 校验
- [x] L1HybridRebuilder：BulkLoad / ApplyDeltaBatch / ApplyCowUpdate
- [x] L1HybridIndex：LRU subtree cache，MemoryUsageStats，PublishedSnapshot
- [x] Manifest 集成：PersistL1HybridState / LoadL1HybridState (64MB 区域)
- [x] Version 集成：ExportL1HybridState / ImportL1HybridState / ApplyDeltaBatch
- [x] CompactionJob 集成：输出 L1DeltaBatch，编码 PrefixWindow
- [x] RSS 控制第一阶段（2026-03-22）：写路径 memtable 软触发+硬门控、O(1) memtable 计数、去除 Version 大 reserve、读写 benchmark 采样前 `malloc_trim(0)`

### 实验框架
- [x] 01_point_lookup benchmark（fast_bulk_l1 + online 两种构建模式）
- [x] 03_compaction_update benchmark（online write，V7 指标）
- [x] FastBulkL1Builder（绕过在线写路径直接构建 L1）
- [x] 批量运行脚本 (run_*.py)、报告生成 (generate_*.py)、绘图 (plot_*.py)
- [x] 01 批跑脚本失败清理机制：失败/中断自动删除 `db_dir`，避免 SSD 残留堆积

### 文档
- [x] CLAUDE.md 更新至 V7（含 Hybrid L1 完整章节、实验规格、设计演进表）
- [x] claude_kv/ 目录建立

## 未完成 / 进行中

### V7 剩余工作
- [ ] Route swap 实验归因 + drain phase 优化
- [ ] 200M+ 规模验证（目前最大跑过 100M）
- [ ] RocksDB 写性能对比实验
- [ ] Ablation studies（prefix routing、CoW、parallel scan 各自贡献）

### 实验
- [ ] 01_point_lookup：10M/100M uniform + prefix-skew 全矩阵（prefix ratio × scale）
- [ ] 03_compaction_update：100M 规模验证
- [ ] 02_range_query benchmark（尚未建立）
- [ ] 空间放大 / 恢复时间实验

### 工程
- [ ] key=0 时 compaction 报错 bug
- [ ] Manifest PM 空间占用异常大问题
- [ ] 多线程点查性能验证（目前实验均为 1 thread）

## 已知性能基准（供参考）

### 写性能口径说明

论文与实验均以 **`foreground_put_throughput_ops`（纯前台 Put 阶段墙钟吞吐）** 为主指标。

- `foreground_put_throughput_ops`：前台 Put 阶段墙钟吞吐，**论文主口径**
- `end_to_end_throughput_ops` / `ingest_throughput_ops`：含后台 flush/compaction drain 等待，**仅参考**
- `avg_put_latency_ns` / `p99_put_latency_ns`：前台 Put 延迟，**论文主口径**

### 点查性能（01_point_lookup，fast_bulk_l1，1M queries，80% hit，1 thread）

| 结果目录 | 规模 | prefix ratio | avg latency | p99 latency | throughput | l1_pages/query |
|---|---|---|---|---|---|---|
| 20260321_100m_swap64m_rebuilt | 100M | 0.10N | 98,800 ns | 284,000 ns | 10,092 ops/s | 0.063 |
| 20260321_100m_swap64m_rebuilt | 100M | 0.05N | 80,900 ns | 520,000 ns | 12,333 ops/s | 0.750 |
| 20260321_101550 | 100M | 全矩阵 | **外部后台任务 killed，中途中止** | — | — | — |

### 写性能（03_compaction_update，online，background）

| 结果目录 | 规模 | threads | prefix ratio | 前台吞吐 (ops/s) | avg put (ns) | p99 put (ns) | drain wait (ms) |
|---|---|---|---|---|---|---|---|
| 20260320_200m_0p01N_t1_quickfix_pool256g | 200M | 1 | 0.01N uniform | **2,391,870** | 375.7 | 703 | 349,046 |
| 20260320_0730_t16 | 10M | 16 | 0.01N uniform | **11,755,600** | — | — | — |
