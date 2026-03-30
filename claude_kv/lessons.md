# MassKV 错误记录与自我纠正

最后更新：2026-03-23

本文件记录我（Claude）或用户在开发过程中犯的错误、踩的坑、以及纠正方式。
每条记录格式：**[日期] [类别] 描述 → 纠正方式**

---

## 科研开发原则（用户明确，硬约束）

### [2026-03-23] 以创新设计推进为第一目标，不做保守回退
**要求**：
- 遇到新设计暴露的问题，优先做并行/结构优化与问题修复，**不要直接回退到旧串行路径**。
- 当前目标是论文与科研验证，重点是“设计创新 + 可复现实验”，不是生产级极致稳定系统。
- 避免写大量“未雨绸缪”的兜底代码；只实现当前实验/论文主线所需的必要修复。
- 代码改动遵循“最小可行 + 可解释 + 可测量”，先跑通并证明，再按实验结果迭代。

**执行方式**：
- 当新方案失败时，先定位根因并修复主路径；若确需临时降级，仅作短时排障并在修复后立即撤回。
- 不擅自引入会显著增加内存开销或偏离故事主线的结构；涉及主结构变动先与用户对齐。

---

## 工具使用

### [2026-03-21] Write 工具参数缺失
**错误**：调用 Write 工具时未填写 `file_path` 或 `content` 参数，导致无限错误循环。
**纠正**：Write 工具必须同时提供 `file_path` 和 `content`，两者缺一不可。对于大文件，先 Write 核心内容，再用 Edit 追加。

---
## 代码理解

### [2026-03-21] hybrid_l1 编译开关误判
**错误**：最初以为 `hybrid_l1` 由某个 CMake 选项控制，类似 `USE_HMASSTREE`。
**纠正**：`hybrid_l1` 在 CMakeLists.txt 中无条件编译并链接到 `flowkv`，不受任何 option 控制。

### [2026-03-21] RoutePrefix 在 8B key 模式下的含义
**错误**：误以为 8B key 模式下 prefix 是 key 的高位。
**纠正**：8B key 模式下 `ExtractPrefix` 始终返回 0（单一路由域），`ExtractSuffix` = `bswap64(key)`。只有 `FLOWKV_KEY16` 模式下才有真正的 prefix = `key.hi`，suffix = `key.lo`。

---

## 设计决策

### [2026-03-21] SubtreeRecord.leaf_value 不存真实 value
**注意**：`SubtreeRecord` 的 `leaf_value` 不是 KV 的 value，而是一个 64 位打包指针，编码 `(44-bit kv_block_ptr | 12-bit offset | 8-bit count)`，直接指向 PST 数据块内的窗口，用于读路径跳过 B+Tree 全量扫描。

---

## 实验

### [2026-03-21] 100M 点查后台任务被外部 kill 后残留爆盘
**错误**：把 100M 点查放到 IDE 后台任务执行，任务被外部 `killed` 后没有自动清理 `/mnt/nvme0/.../dbfiles`，留下数百 GB 残留，后续实验连续失败。
**纠正**：
- `run_point_lookup_batch.py` 增加失败/中断自动清理：
  - 子进程非 0 退出时自动删除当前 `db_dir`（`--cleanup-failed-db=1` 默认开启）
  - 捕获 `SIGTERM/SIGINT`，先终止子进程，再清理 `db_dir`
  - 每个 ratio 成功结束后也会清理 `db_dir`（当 `--keep-db-files=0`）
- 失败后第一动作必须执行 `dbfiles` 残留清理，再重跑。

### [2026-03-21] 写性能指标口径错误
**错误**：在 progress.md 中用 `ingest_throughput`（端到端含 drain wait）作为写性能基准。
**纠正**：论文与实验的写性能主指标是 `foreground_put_throughput_ops`（纯前台 Put 阶段墙钟吞吐）。`ingest_throughput` / `end_to_end_throughput_ops` 包含后台 flush/compaction drain 等待，不代表前台写能力。
**规则**：凡是提到"写性能"、"写吞吐"，默认指 `foreground_put_throughput_ops`。提到 drain/端到端时需显式说明。

### [2026-03-21] 03_compaction_update README 明确的吞吐口径
**记录**：`03_compaction_update/README.md` 明确写道：
- `put_path_throughput_ops` 已对齐为"前台 Put 阶段墙钟吞吐"
- `RESULTS.md` 的 Raw Result Table 以 `Foreground Throughput` 为主口径
- `End-to-End Throughput` 仅作参考
这是冻结的实验规范，不能随意改动。

---

## 待观察

- key=0 时 compaction 报错的根因尚未定位
- Manifest PM 空间占用异常大的根因尚未定位
