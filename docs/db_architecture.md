# FlowKV `db/` 架构说明（长期维护版）

本文档描述 FlowKV 在 `db/` 目录下的模块边界、依赖关系、典型读写/后台流程，以及关键入口函数索引（带源码锚点链接），用于后续维护与排障。

> 约定：本文默认以“用户线程 = `MYDBClient` 发起请求、后台线程 = flush/compaction 线程池执行任务”的视角描述。

## 1. 模块清单与职责边界

### 1.1 对外 API / 入口层

- 公共接口声明：[`include/db.h`](../include/db.h)
  - `class MYDB`：DB 生命周期、后台调度、模式切换等。
  - `class MYDBClient`：面向用户线程的 Put/Delete/Get/Scan。

- 实现：
  - `MYDB` 主要实现：[`db/db.cpp`](../db/db.cpp)
  - `MYDBClient` 主要实现：[`db/db_client.cpp`](../db/db_client.cpp)

### 1.2 内存索引（memtable index）

- `MYDB` 内部维护多个 memtable index（宏分支可能是 Masstree/HOT 等），用户写入会先落到日志，再更新 memtable index。
- Flush 会扫描 memtable index（`Scan2` / `ScanByRange`）得到有序 KV 列表，并构建 L0 PST。

### 1.3 日志（WAL / Value 回读）

- 写日志：[`db/log_writer.cpp`](../db/log_writer.cpp)
  - 追加 Put/Delete 记录，返回 `log_ptr`（偏移/编码后的地址）。
- 读日志：[`db/log_reader.cpp`](../db/log_reader.cpp)
  - 在 `INDEX_LOG_MEMTABLE` 或 `KV_SEPARATE` 分支下，Get/Flush 可能需要依据 `ValuePtr` 从日志回读 value。
- 日志段（LogSegment）：[`db/allocator/segment.h`](../db/allocator/segment.h)
  - 以 `logbuffersize` 缓冲批量 `pwrite`，并在 segment header 里维护尾指针等元数据。

### 1.4 持久化表（PST）与 DataBlock

- PST 元信息：[`db/table.h`](../db/table.h)
  - `PSTMeta`：包含 `datablock_ptr_`、`min_key_`、`max_key_`、`entry_num_`，以及 `key_256/key_512/key_768` 等采样 key（用于加速范围定位/分层查找）。
  - `TaggedPstMeta`：附带 `level`、`manifest_position` 等运行时/管理信息。

- PST 构建：[`db/pst_builder.cpp`](../db/pst_builder.cpp)
  - `PSTBuilder::AddEntry` 将 KV 写入 DataBlockWriter；空间不足时由上层触发 `Flush()` 生成 `PSTMeta`。

- PST 读取：[`db/pst_reader.cpp`](../db/pst_reader.cpp)
  - `PSTReader::PointQuery` 调用 DataBlockReader 做二分查找。
  - `RowIterator`（定义在 `pst_reader.h`）用于在 compaction/scan 中跨多个 PST 顺序迭代。

- DataBlock 写入：[`db/datablock_writer.cpp`](../db/datablock_writer.cpp)
  - 通过 `SegmentAllocator` 分配 `SortedSegment` 页，写入固定大小 `PDataBlock`。

- DataBlock 读取：[`db/datablock_reader.cpp`](../db/datablock_reader.cpp)
  - 支持遍历与二分查找（当前实现以固定 8-byte key/value 为主）。

### 1.5 元数据与版本视图（Manifest / Version）

- Version（内存视图）：[`db/compaction/version.h`](../db/compaction/version.h)
  - 维护 L0 tree ring（head/tail/read_tail）与 L1 tables。
  - 提供 `Get()`、`GetLevel1Iter()`、L0/L1 的插入/挑选等。

- Manifest（持久化元数据）：[`db/compaction/manifest.h`](../db/compaction/manifest.h)
  - 持久化 `AddTable/DeleteTable` 等元数据变更。
  - 维护 flush log（用于崩溃一致性：当 flush 正在删除 log segments 时，先记录待删除列表）。

### 1.6 后台任务（Flush / Compaction）

- Flush：[`db/compaction/flush.cpp`](../db/compaction/flush.cpp)
  - `FlushJob` 将 memtable index 的内容固化为 L0 PST，并更新 Manifest/Version。

- Compaction：[`db/compaction/compaction.cpp`](../db/compaction/compaction.cpp)
  - `CompactionJob` 选取若干 L0 tree 与重叠 L1 tables 做归并，输出新 PST，并清理旧表。

### 1.7 空间管理（SegmentAllocator / Segment）

- SegmentAllocator：[`db/allocator/segment_allocator.h`](../db/allocator/segment_allocator.h)
  - 统一管理 segment 位图、log segment 备份位图、sorted segment 复用 cache。
  - 负责分配/回收 LogSegment 与 SortedSegment。

- Segment 实现：[`db/allocator/segment.h`](../db/allocator/segment.h)
  - `LogSegment`：顺序追加日志。
  - `SortedSegment`：页式分配、位图管理，可用于 DataBlock 等。

## 2. 模块依赖图（文字版）

下面是“谁依赖谁”的主路径（从上到下依赖更底层）：

- `MYDBClient` → `LogWriter` / `LogReader` / memtable index / `Version` / `PSTReader`
- `MYDB` → `Version` / `Manifest` / `SegmentAllocator` / thread pools / `FlushJob` / `CompactionJob`
- `FlushJob` → memtable index →（必要时 `LogReader`）→ `PSTBuilder` → `DataBlockWriter` → `SegmentAllocator` → file fd
- `CompactionJob` → `Version` → `PSTReader`/`RowIterator` → `PSTBuilder`/`PSTDeleter` → `SegmentAllocator`
- `Version` → `Index`（L0/L1 索引）+ `PSTReader`
- `Manifest` → fd 持久化区域（L0/L1 meta + flush log）

## 3. 关键流程时序（文字版）

### 3.1 Put（写入路径）

```
用户线程: MYDBClient::Put
  1) StartWrite()：观察 active memtable 是否已切换
  2) LogWriter::WriteLogPut()：追加 WAL，得到 log_ptr
  3) memtable index Put/PutValidate：写入 (key -> ValuePtr/value)
  4) 递增 memtable put 计数
后台线程: MYDB::MayTriggerFlushOrCompaction（周期触发或写满触发）
  5) memtable 达到阈值时触发 BGFlush
```

要点：
- 在 `INDEX_LOG_MEMTABLE` 分支下，memtable 里保存的是编码后的 `ValuePtr`，Get/Flush 需要 `LogReader` 回读 value。

### 3.2 Get（读路径）

```
用户线程: MYDBClient::Get
  1) GetFromMemtable：从最新 memtable 往旧的查
     - 命中：若存的是 ValuePtr，则 LogReader::ReadLogForValue 回读 value
  2) 未命中：Version::Get
     - 通过 PSTReader/DataBlockReader 在 L0/L1 查找
     - 若 KV_SEPARATE：Version 返回 ValuePtr，再由 LogReader 回读 value
```

### 3.3 Flush（memtable → L0 PST）

```
后台线程: MYDB::BGFlush
  1) 切换 active memtable（新的 memtable 开始接收写入）
  2) FlushJob::subrunParallel（按分区并行）
     - memtable_index ScanByRange 得到 keys/values
     - 必要时 LogReader 回读 value
     - PSTBuilder::AddEntry / Flush 生成 PSTMeta
  3) Manifest::AddTable 持久化元信息
  4) Version::InsertTableToL0 把新 PST 纳入 L0 tree
  5) 回收本次 log group 的 log segments（并用 manifest flush log 保障崩溃一致性）
```

### 3.4 Compaction（L0 → L1 合并）

```
后台线程: MYDB::BGCompaction
  1) CompactionJob::PickCompaction
     - Version::PickLevel0Trees 选 L0 输入
     - Version::PickOverlappedL1Tables 选重叠 L1 输入
  2) CompactionJob::RunSubCompactionParallel（按分区并行）
     - RowIterator 跨 PST 迭代，多路归并输出
     - PSTBuilder 写出新 PST
  3) CleanCompaction...：更新 Version/Manifest、删除旧 PST（PSTDeleter）
```

### 3.5 Recovery（恢复路径概览）

```
启动: MYDB 构造 + recover=true
  1) SegmentAllocator 恢复位图/识别已用 log segments
  2) Manifest::RecoverVersion 恢复 L0/L1 PST 元数据与 Version 视图
  3) MYDB::RecoverLogAndMemtable 扫描未固化日志，重建 memtable
```

## 4. 关键入口函数索引（源码锚点）

### 4.1 MYDB（后台与调度）

- 恢复：[`MYDB::RecoverLogAndMemtable`](../db/db.cpp#L211)
- 周期触发 flush/compaction：[`MYDB::MayTriggerFlushOrCompaction`](../db/db.cpp#L340)
- 触发 compaction 判定：[`MYDB::MayTriggerCompaction`](../db/db.cpp#L389)
- flush 后台任务：[`MYDB::BGFlush`](../db/db.cpp#L445)
- compaction 后台任务：[`MYDB::BGCompaction`](../db/db.cpp#L520)
- Scan 前等待后台完成：[`MYDB::WaitForFlushAndCompaction`](../db/db.cpp#L579)

### 4.2 MYDBClient（读写路径）

- Put：[`MYDBClient::Put`](../db/db_client.cpp#L38)
- Delete：[`MYDBClient::Delete`](../db/db_client.cpp#L81)
- Get：[`MYDBClient::Get`](../db/db_client.cpp#L116)
- memtable 命中路径：[`MYDBClient::GetFromMemtable`](../db/db_client.cpp#L140)
- Scan：[`MYDBClient::Scan`](../db/db_client.cpp#L176)
- 写路径切换观察：[`MYDBClient::StartWrite`](../db/db_client.cpp#L283)

### 4.3 Flush / Compaction

- Flush 主流程：[`FlushJob::run`](../db/compaction/flush.cpp#L38)
- Flush 并行分区：[`FlushJob::subrunParallel`](../db/compaction/flush.cpp#L213)
- Compaction 选取输入：[`CompactionJob::PickCompaction`](../db/compaction/compaction.cpp#L47)
- Compaction 单线程归并：[`CompactionJob::RunCompaction`](../db/compaction/compaction.cpp#L77)
- Compaction 并行分区：[`CompactionJob::RunSubCompactionParallel`](../db/compaction/compaction.cpp#L220)
- Compaction 清理：[`CompactionJob::CleanCompactionWhenUsingSubCompaction`](../db/compaction/compaction.cpp#L439)
- Compaction 回滚：[`CompactionJob::RollbackCompaction`](../db/compaction/compaction.cpp#L497)

## 5. 关键数据结构索引

本节用于“从概念直接跳到定义处”，便于理解编码/落盘格式与各模块的边界。

- 对外入口对象
  - [`MemTableStates`](../include/db.h#L30)：memtable 的状态机（ACTIVE/FREEZE/EMPTY 等）
  - [`MYDB`](../include/db.h#L49)：DB 对象（持有 allocator/version/manifest、调度后台任务）
  - [`MYDBClient`](../include/db.h#L289)：用户线程视角的读写入口（Put/Delete/Get/Scan）

- memtable/value 编码与索引抽象
  - [`ValuePtr` + `ValueHelper`](../include/db_common.h#L56-L104)：memtable value 的编码与 PutValidate 辅助信息（受宏分支影响）
  - [`Index`](../include/db_common.h#L108)：memtable/L0/L1 使用的索引抽象（Get/Put/Scan/ScanByRange 等；Version 会依赖 Scan 的 lower_bound 起扫语义）

- PST 与索引元信息
  - [`PSTMeta`](../db/table.h#L7)：单个 PST 的范围与落盘位置（min/max/entry_num/datablock_ptr 等）
  - [`TaggedPstMeta`](../db/table.h#L25)：为 PSTMeta 附加 level/manifest_position 等管理信息
  - [`TaggedPtr`](../db/table.h#L37)：用于索引中编码“manifest idx + 指针”等信息的位域结构

- 落盘日志格式
  - [`LSN`](../include/log_format.h#L15)：日志序列号（不同模式下用于校验/排序）
  - [`OpCode` / `LogType`](../include/log_format.h#L29)：日志操作类型与记录类别
  - [`LogEntry32`/`LogEntry64`/`LogEntryVar64`](../include/log_format.h#L57)：日志记录布局（包含 valid/lsn/key/value 等字段）

- Version / Manifest（元数据与版本视图）
  - [`TreeMeta`](../db/compaction/version.h#L22)：L0 tree 的范围与统计信息（min/max/size 等）
  - [`Version`](../db/compaction/version.h#L31)：L0/L1 的内存视图与查询入口（Get/GetLevel1Iter/Pick*）
  - [`ManifestSuperMeta`](../db/compaction/manifest.h#L22)：manifest 超级块（含 flush log 元信息）
  - [`Manifest`](../db/compaction/manifest.h#L35)：PST 元数据的持久化与恢复入口（AddTable/DeleteTable/RecoverVersion 等）

- 空间与段管理
  - [`SegmentAllocator`](../db/allocator/segment_allocator.h#L12)：统一分配/回收 log segment 与 sorted segment
  - [`LogSegment`](../db/allocator/segment.h#L59)：顺序追加日志的 segment（header + buffered pwrite）
  - [`SortedSegment`](../db/allocator/segment.h#L189)：页式分配的 segment（DataBlock 等落盘单元基于它分配页面）

- 读写块/表抽象
  - [`DataBlockWriter`](../db/datablock_writer.h#L17)：DataBlock 页的分配与写入
  - [`DataBlockReader`](../db/datablock_reader.h#L16)：DataBlock 的读取、遍历与二分查找
  - [`PSTReader`](../db/pst_reader.h#L15)：对 PST 的点查与迭代封装（内部使用 DataBlockReader）

- 分区信息
  - [`PartitionInfo`](../include/db_common.h#L258)：范围分区的 min/max key，用于 flush/compaction 的按分区并行

## 6. 维护建议（最小集合）

### 6.1 索引扫描契约（务必遵守）

- `Index::Scan(start_key, n, out)` 必须等价于“lower_bound(start_key) 起扫”：返回满足 `entry_key >= start_key` 的前 n 条记录，且按 key 升序。
- `Index::Scan2` 的起扫位置与顺序必须与 `Scan` 一致，但同时返回 key 与 value。
- `Index::ScanByRange(start, end, ...)` 必须返回 `start <= entry_key <= end` 的所有记录（按 key 升序）。

该契约直接影响 `Version` 如何从 L0/L1 索引定位候选 PST（例如 `FindTableByIndex/ScanIndexForTables`），如果某个索引实现把 Scan 做成“仅等值命中”或“无序返回”，会导致读路径返回错误结果。

- 新增/修改数据布局时：优先从 `PSTMeta`（[`db/table.h`](../db/table.h)）与 `DataBlockReader/Writer`（[`db/datablock_reader.cpp`](../db/datablock_reader.cpp)、[`db/datablock_writer.cpp`](../db/datablock_writer.cpp)）入手，保证落盘格式自洽。
- 调整一致性语义时：重点核对 Flush 删除 log segments 的顺序与 `Manifest` flush log（[`db/compaction/manifest.h`](../db/compaction/manifest.h)）。
- 排查读写延迟时：先区分是 memtable 命中、PST 命中还是 log 回读路径（Get/Flush 在不同宏分支下差异很大）。
