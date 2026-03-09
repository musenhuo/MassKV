# FlowKV 项目架构报告

## 一、项目概述

FlowKV 是一个面向高带宽 SSD（及持久内存）的多层级 Key-Value 存储引擎。采用类 LSM-tree 的分层架构，核心数据路径为：

```
写入 → WAL日志 → Memtable(内存索引) → Flush → L0(BufferStore) → Compaction → L1(StableStore)
```

论文术语与代码术语对照：

| 论文术语 | 代码术语 |
|---------|---------|
| Chunk | Segment（4MB段） |
| Logical Sorted Table (LST) | PST（Persistent Sorted Table） |
| FastStore | Memtable（内存索引） |
| BufferStore | Level 0（L0，环形缓冲区） |
| StableStore | Level 1（L1，有序表向量） |
| Manifest | Version（易失）+ Manifest（持久） |

---

## 二、目录结构总览

```
FlowKV/
├── include/                    # 公共头文件（接口、类型、配置）
│   ├── db.h                    # MYDB / MYDBClient 类声明
│   ├── db_common.h             # KeyType, ValuePtr, Index 抽象接口
│   ├── config.h                # 编译期常量（MAX_MEMTABLE_NUM 等）
│   ├── log_format.h            # 日志条目格式（LogEntry32/64）
│   └── slice.h                 # Slice 类型（字节串视图）
│
├── db/                         # 核心引擎实现
│   ├── db.cpp                  # MYDB 单例（全局资源、后台线程）
│   ├── db_client.cpp           # MYDBClient（每线程读写句柄）
│   ├── log_writer.h/cpp        # WAL 写入器
│   ├── log_reader.h/cpp        # WAL 读取器
│   ├── pst_builder.h/cpp       # PST 构建器
│   ├── pst_reader.h/cpp        # PST 读取器（点查 + 迭代）
│   ├── pst_deleter.h/cpp       # PST 删除器（回收数据块）
│   ├── datablock_writer.h/cpp  # 数据块写入（16KB块）
│   ├── datablock_reader.h/cpp  # 数据块读取（二分查找）
│   ├── table.h                 # PSTMeta / TaggedPstMeta 定义
│   ├── blocks/
│   │   └── fixed_size_block.h  # PDataBlock16K 数据块格式
│   ├── allocator/
│   │   ├── segment_allocator.h # 段分配器（管理整个存储池）
│   │   ├── segment.h           # LogSegment / SortedSegment
│   │   └── bitmap.h            # 位图（差异化持久）
│   └── compaction/
│       ├── version.h/cpp       # Version（易失元数据：L0环形缓冲+L1有序表）
│       ├── manifest.h/cpp      # Manifest（持久元数据：超级块+刷写日志）
│       ├── flush.h/cpp         # FlushJob（Memtable → L0）
│       └── compaction.h/cpp    # CompactionJob（L0 + L1 → 新L1）
│
├── lib/                        # 索引库
│   ├── index_masstree.h        # MasstreeIndex（原版 Masstree 适配器）
│   ├── index_hmasstree.h       # HMasstreeIndex（H-Masstree 内存适配器）
│   ├── index_hmasstree_external.h  # HMasstreeExternalIndex（外存适配器）
│   ├── masstree/               # 原版 Masstree（纯内存，cache-line优化）
│   └── hmasstree/              # H-Masstree（改造版，支持外存）
│
├── util/                       # 工具类
│   ├── lock.h                  # SpinLock
│   └── binary_search.h         # 二分查找模板
│
├── benchmarks/                 # 基准测试
└── lib/ThreadPool/             # 线程池（预编译 .so）
```

---

## 三、核心引擎层

### 3.1 关键类型（`include/db_common.h`）

#### KeyType

```
默认 8 字节：typedef uint64_t KeyType;
16字节模式：struct Key16 { uint64_t hi, lo; }  // 需 cmake -DFLOWKV_KEY16=ON
```

比较时使用大端序（`__builtin_bswap64`），使字节序比较等价于数值比较。

#### ValuePtr（位压缩指针，8字节）

```
无 KV 分离模式：
  ┌─valid(1)─┬──ptr(33)──┬──lsn(30)──┐  共64位
  │  有效位   │ 64B对齐偏移 │ 日志序列号  │
  └──────────┴───────────┴───────────┘

KV 分离模式：
  ┌─valid(1)─┬──ptr(34)──┬──lsn(29)──┐
  └──────────┴───────────┴───────────┘
```

`ptr` 字段编码了日志条目在存储池中的逻辑位置，通过公式还原为文件偏移：
```
file_offset = (ptr / LogNumperBlock) * 16KB + (ptr % LogNumperBlock) * LOG_ENTRY_SIZE
```

#### Index 抽象接口

```cpp
class Index {
    virtual void ThreadInit(int thread_id);
    virtual uint64_t Get(KeyType key);              // 点查
    virtual void Put(KeyType key, ValueHelper &vh);  // 插入/更新
    virtual void PutValidate(KeyType key, ValueHelper &vh); // 带LSN校验的插入
    virtual void Delete(KeyType key);
    virtual int Scan(KeyType key, int cnt, std::vector<uint64_t> &vec);
    virtual int Scan2(KeyType key, int cnt, std::vector<KeyType> &kvec, std::vector<uint64_t> &vvec);
    virtual int ScanByRange(KeyType start, KeyType end, ...);
};
```

所有索引实现（MasstreeIndex、HMasstreeIndex、HMasstreeExternalIndex）都实现此接口。

### 3.2 编译期配置（`include/config.h`）

| 常量 | 值 | 含义 |
|-----|---|------|
| `MAX_MEMTABLE_NUM` | 4 | Memtable 轮转槽位数 |
| `MAX_MEMTABLE_ENTRIES` | 500,000 | 触发 Flush 的条目阈值 |
| `MAX_L0_TREE_NUM` | 32 | L0 环形缓冲区最大树数 |
| `MAX_USER_THREAD_NUM` | 64 | 最大并发客户端线程数 |
| `RANGE_PARTITION_NUM` | 12 | 并行 Flush/Compaction 的分区数 |
| `SEGMENT_SIZE` | 4MB | 段大小 |

编译模式（由 CMake 设置）：
- `INDEX_LOG_MEMTABLE` + `KV_SEPARATE`：Memtable 存日志指针，值单独存储
- `BUFFER_WAL_MEMTABLE`：值内联在 WAL 条目中
- `USE_HMASSTREE`：使用 H-Masstree 替代原版 Masstree
- `FLOWKV_KEY16`：16 字节 key 模式

### 3.3 MYDB 单例（`include/db.h`, `db/db.cpp`）

MYDB 是整个引擎的入口，持有所有全局资源：

```
MYDB（单例）
├── SegmentAllocator*           // 存储池段分配器
├── mem_index_[4]               // 4个 Memtable 索引槽位（轮转使用）
├── memtable_states_[4]         // 每个槽位的状态：ACTIVE / FREEZE / EMPTY
├── current_memtable_idx_       // 当前活跃的 Memtable 槽位
├── Version*                    // 易失元数据（L0 + L1）
├── Manifest*                   // 持久元数据
├── partition_info_[12]         // 键空间分区边界
├── lsn_map_[256]               // 哈希分桶的 LSN 计数器（原子操作）
├── thread_pool_                // 通用后台线程池（4线程）
├── flush_thread_pool_          // 并行 Flush 线程池（12线程）
├── compaction_thread_pool_     // 并行 Compaction 线程池（12线程）
├── bgwork_trigger_             // 后台轮询线程（每100ms检查一次）
└── client_list_[64]            // 活跃客户端列表
```

#### 初始化流程

1. 创建 `SegmentAllocator`，打开存储池文件
2. 创建第一个 Memtable 索引（`mem_index_[0]`），状态设为 ACTIVE
3. 创建 `Version` 和 `Manifest`
4. 若为恢复模式：回放 Manifest 重建 Version，回放 WAL 重建 Memtable
5. 创建 3 个线程池
6. 计算 12 个分区的键范围边界（均分 64 位键空间）
7. 启动后台轮询线程

#### 后台触发逻辑（每 100ms）

```
MayTriggerFlushOrCompaction()
  ├── 采样写入比例，自适应调整模式
  ├── 检查 Memtable 大小 ≥ 阈值？ → 触发 BGFlush
  └── 检查 L0 树数量 ≥ 阈值？ → 触发 BGCompaction
```

### 3.4 MYDBClient（`db/db_client.cpp`）

每个用户线程持有一个 MYDBClient 实例：

```
MYDBClient
├── thread_id_                  // 逻辑线程ID
├── db_                         // 指向 MYDB 单例
├── log_writer_                 // 每线程 WAL 写入器
├── log_reader_                 // 每线程日志读取器
├── pst_reader_                 // 每线程 PST 读取器
├── current_memtable_idx_       // 本地缓存的活跃 Memtable 索引
└── put_num_in_current_memtable_[4]  // 每个 Memtable 的写入计数
```

#### 写入路径（Put）

```
MYDBClient::Put(key, value)
  1. StartWrite() — 同步本地 Memtable 索引，若发生轮转则切换日志段
  2. GetLSN(key) — 从哈希分桶的原子计数器获取 LSN
  3. LogWriter::WriteLogPut() — 追加日志条目到 WAL 段，返回池内偏移
  4. 编码 ValuePtr{valid=1, ptr=编码后偏移, lsn}
  5. mem_index_[idx]->PutValidate() — CAS 方式插入 Memtable
  6. 递增本地写入计数
```

#### 读取路径（Get）

```
MYDBClient::Get(key)
  1. GetFromMemtable() — 从新到旧扫描所有非 EMPTY 的 Memtable
     ├── 找到 valid=1 → 读取值（KV分离模式从日志读，否则内联）
     ├── 找到 valid=0 → 墓碑，返回未找到
     └── 未找到 → 继续
  2. Version::Get() — 搜索 L0（从新到旧），再搜索 L1
     └── PSTReader::PointQuery() → DataBlockReader::BinarySearch()
```

---

## 四、存储层

### 4.1 段分配器（`db/allocator/`）

整个存储池是一个大文件，被划分为固定大小的 4MB 段（Segment）。段分配器管理这些段的分配与回收。

#### 存储池文件布局

```
┌──────────┬──────────┬─────┬──────────┬────────────────┬──────────────────┐
│ Segment 0│ Segment 1│ ... │ Segment N│ segment_bitmap │ log_segment_bitmap│
└──────────┴──────────┴─────┴──────────┴────────────────┴──────────────────┘
 ←────────── pool_size ──────────────→  ←── 追加在池数据之后 ──→
```

#### SegmentAllocator（`segment_allocator.h`）

```
SegmentAllocator
├── pool_path_, pool_size_      // 池文件路径和大小
├── fd                          // 池文件描述符
├── segment_bitmap_             // 全局段分配位图（1位/段）
├── log_segment_bitmap_         // 日志段专用位图（用于快速恢复）
├── data_segment_cache_         // 未满的 SortedSegment 复用队列
├── log_segment_group_[4]       // 每个 Memtable 对应的日志段ID列表
└── log_seg_num_, sort_seg_num_ // 计数器
```

关键方法：
- `AllocLogSegment(group_id)` — 分配日志段，同时在两个位图中标记
- `AllocSortedSegment()` — 优先从复用队列取，否则分配新段
- `FreeSegment()` — 释放段，清除位图
- `RecoverLogSegmentAndGetId()` — 恢复时从日志位图获取所有日志段ID

#### BitMap（`bitmap.h`）

双位图差异化持久方案：
- `bitmap_` — 当前分配状态
- `history_bitmap_` — 上次持久化时的快照
- `PersistToSSDOnlyAlloc()` — 只持久化新分配的位（bitmap XOR history）
- `PersistToSSDOnlyFree()` — 只持久化新释放的位

这种差异化持久避免了并发分配/释放时的覆盖问题。

#### LogSegment（`segment.h`）

```
LogSegment（4MB）
├── Header（8字节，位压缩）
│   ├── segment_status : 2    // Free/Available/Using/Closed
│   ├── segment_block_type : 6
│   ├── objects_tail_offset : 24  // 已写入字节数
│   └── magic : 32
├── buf_[16KB]                // DRAM 写缓冲区
├── tail_                     // 当前写位置
└── buffer_offset_            // 缓冲区内偏移
```

写入流程：数据先写入 16KB DRAM 缓冲区，缓冲区满后通过 `pwrite` 刷到文件。一个 4MB 段可容纳约 255 个 16KB 块。

#### SortedSegment（`segment.h`）

```
SortedSegment（4MB，PAGE_SIZE=16KB）
┌──────────┬──────────┬──────────┬──────────┬─────┐
│ Header   │ Bitmap   │ Page 0   │ Page 1   │ ... │
│ (16KB)   │ (16KB)   │ (16KB)   │ (16KB)   │     │
└──────────┴──────────┴──────────┴──────────┴─────┘
 EXTRA=2 pages           PAGE_NUM = 4MB/16KB - 2 = 254 pages
```

每个 SortedSegment 内部有自己的页分配位图，支持单页和批量分配。未满的段会被放入复用队列。

### 4.2 WAL 日志（`db/log_writer.h`, `db/log_reader.h`）

#### 日志条目格式（`include/log_format.h`）

```
LogEntry32（8B key 模式下 24 字节）：
┌─valid(1)─┬─lsn(31)─┬─key_sz(16)─┬─value_sz(16)─┬──key(64)──┬──value/addr(64)──┐
└──────────┴─────────┴────────────┴──────────────┴──────────┴─────────────────┘

LogEntry64（64 字节）：更大的内联值空间（48字节）
LogEntryVar64：变长，使用柔性数组存储任意大小的值
```

#### LogWriter

每个客户端线程拥有一个 LogWriter：
- `WriteLogPut(key, value, lsn)` — 值 ≤ 8B 用 LogEntry32 内联，更大用 LogEntryVar64
- 返回值是池内绝对偏移 = `segment_id * SEGMENT_SIZE + segment内偏移`
- 段满时自动关闭当前段、分配新段

#### LogReader

- `ReadLogForValue(key, valueptr, buf)` — 从 ValuePtr 解码文件偏移，pread 读取日志条目，提取值
- `ReadLogFromSegment(seg_id, output)` — 批量读取整个段的所有条目（恢复用）

### 4.3 数据块（`db/blocks/`, `db/datablock_writer.h`, `db/datablock_reader.h`）

#### PDataBlock16K（`fixed_size_block.h`）

```
8B key 模式：
  MAX_ENTRIES = 1024
  Entry { uint64_t key; uint64_t value; }  // 16 字节/条目
  总大小 = 1024 × 16 = 16KB

16B key 模式：
  MAX_ENTRIES = 682
  Entry { uint64_t key_hi, key_lo; uint64_t value; }  // 24 字节/条目
```

数据块是 PST 的物理存储单元。条目按 key 排序存储，未满部分用 `INVALID_PTR` 哨兵填充。

#### DataBlockWriter

- 维护一个 `PDataBlockWrapper` 缓冲区
- `AddEntry()` 向缓冲区添加条目
- `Flush()` 将整个 16KB 块 pwrite 到 SortedSegment 的一个页中
- 段满时自动切换到新段

#### DataBlockReader

- `BinarySearch(offset, key, value_out, entry_num)` — 只读前 4KB（256条目），二分查找
- `ReadDataBlock(offset)` — 读完整 16KB 块（带偏移缓存，避免重复读）
- `TraverseDataBlock(offset, results)` — 遍历块内所有有效条目

### 4.4 PST（Persistent Sorted Table）

#### PSTMeta（`db/table.h`）

```
PSTMeta
├── datablock_ptr_    // 数据块的文件偏移（0 = 无效）
├── min_key_, max_key_  // 键范围
├── key_256, key_512, key_768  // 采样键（1/4, 1/2, 3/4 位置）
├── seq_no_           // 序列号
└── entry_num_        // 条目数
```

采样键的作用：点查时先用采样键将 1024 条目缩小到 256 条目的分区，再做二分查找，减少实际 I/O 量。

#### TaggedPstMeta

```
TaggedPstMeta
├── meta              // PSTMeta
├── level             // 0 或 1（100000 = NotOverlappedMark，表示可直接复用）
└── manifest_position // 在 Manifest 中的槽位索引
```

#### PSTBuilder（`db/pst_builder.h`）

- `AddEntry(key, value)` — 委托给 DataBlockWriter
- `Flush()` — 刷写数据块，填充 PSTMeta（偏移、键范围、采样键）
- 一个 PST = 一个 16KB 数据块，最多 1024 条目

#### PSTReader（`db/pst_reader.h`）

- `PointQuery()` — 委托给 DataBlockReader::BinarySearch
- `GetIterator()` — 返回迭代器，将整个块加载到内存中的 vector
- `RowIterator` — 跨多个 PST 的归并迭代器（Compaction 用）

---

## 五、Compaction 系统（`db/compaction/`）

### 5.1 Version（`version.h`）— 易失元数据

Version 维护 L0 和 L1 的运行时状态：

```
Version
├── L0 环形缓冲区
│   ├── level0_trees_[32]           // 每个槽位一棵 Masstree 索引
│   ├── level0_table_lists_[32]     // 每棵树的 PST 列表
│   ├── level0_tree_meta_[32]       // 每棵树的键范围摘要
│   ├── l0_head_                    // 最旧的树（Compaction 消费端）
│   ├── l0_tail_                    // 最新的树（Flush 写入端）
│   └── l0_read_tail_               // 读可见边界（Flush 完成后更新）
│
├── L1 有序表向量
│   ├── level1_tables_[]            // 所有 L1 PST（预留 650万槽位）
│   ├── level1_tree_                // 单棵 Masstree 索引（按 max_key 索引）
│   └── level1_free_list_           // 回收的空闲槽位
│
└── pst_reader_                     // 用于点查的 PST 读取器
```

#### L0 环形缓冲区

```
 l0_head_                              l0_tail_
    ↓                                     ↓
┌───┬───┬───────┬───────┬───────┬───────┬───┬───┐
│nil│nil│ tree1 │ tree2 │ tree3 │ tree4 │nil│nil│
└───┴───┴───────┴───────┴───────┴───────┴───┴───┘
          oldest ──────────────→ newest
```

- 每棵 L0 树包含一组 PST，通过 Masstree 按 max_key 索引
- `l0_read_tail_` 与 `l0_tail_` 分离，确保读者不会看到正在构建的树
- 满条件：`(l0_tail_ + 1) % 32 == l0_head_`

#### Version::Get() 读取流程

```
1. 遍历 L0 树（从新到旧）：
   对每棵树：Scan(key, 1) 找到 max_key ≥ key 的 PST
   验证 min_key ≤ key
   用采样键缩小范围 → PSTReader::PointQuery()

2. 搜索 L1：
   Scan(key, 1) 找到 max_key ≥ key 的 PST
   同样验证 + 采样键 + 点查
```

### 5.2 Manifest（`manifest.h`）— 持久元数据

#### 文件布局

```
┌──────────────┬──────────────┬──────────────┬──────────┐
│ SuperBlock   │ L0 Meta Array│ L1 Meta Array│ Flush Log│
│ (4KB)        │ (~51MB)      │ (~512MB)     │          │
└──────────────┴──────────────┴──────────────┴──────────┘
```

#### ManifestSuperMeta（4KB 超级块）

```
├── l0_min_valid_seq_no   // L0 最小有效序列号（更小的是垃圾）
├── l1_current_seq_no     // L1 最新已提交的 Compaction 序列号
├── l0_tail, l1_tail      // 元数据数组的写入位置
└── flush_log             // 刷写日志（崩溃一致性）
    ├── is_valid : 1      // 是否有待处理的段释放
    └── length : 63       // 待释放的段ID数量
```

#### 崩溃一致性协议（Flush Log）

解决的问题：Flush 完成后释放日志段，若中途崩溃，日志段可能成为孤儿。

```
1. AddFlushLog(segment_ids) — 写入待释放段ID列表，设 is_valid=1
2. 逐个释放日志段
3. ClearFlushLog() — 设 is_valid=0
恢复时：若 is_valid=1，重放释放操作
```

#### 恢复流程

1. 读超级块，获取 `l0_min_valid_seq_no` 和 `l1_current_seq_no`
2. 扫描 L0 元数据：`seq ≥ min_valid` 的 PST 重建到 L0 树，更旧的删除
3. 扫描 L1 元数据：`seq ≤ current_seq` 的 PST 插入 L1，更新的删除（未完成的 Compaction）
4. `L1TreeConsistencyCheckAndFix()` — 清理崩溃 Compaction 留下的重叠 PST

### 5.3 FlushJob（`flush.h`）— Memtable → L0

#### 单线程 Flush（`run()`）

```
1. Scan 冻结的 Memtable，获取所有排序的 KV 对
2. 在 L0 环形缓冲区分配新树槽位
3. 遍历排序的 key：
   ├── 确定所属分区（partition_info_）
   ├── 跨分区时刷写当前 PST
   └── PSTBuilder::AddEntry()，满时刷写
4. 更新 l0_read_tail_，使新树对读者可见
5. 释放日志段（通过 Flush Log 保证崩溃一致性）
```

#### 并行 Flush（`subrunParallel()`）

```
1. 分配 L0 树槽位
2. 派发 12 个任务到 flush_thread_pool_
   每个任务：ScanByRange(min_key, max_key) → 只处理本分区的 key
3. 等待所有任务完成
4. 顺序合并：持久化 PST 元数据，插入 L0 树
5. 更新可见性，释放日志段
```

### 5.4 CompactionJob（`compaction.h`）— L0 + L1 → 新 L1

#### 输入选择（`PickCompaction()`）

```
inputs_[0]     = 最新 L0 树的 PST 列表
inputs_[1]     = 次新 L0 树
...
inputs_[N-1]   = 最旧 L0 树
inputs_[N]     = 与 L0 键范围重叠的 L1 PST
```

#### K路归并（`RunCompaction()`）

使用最小堆进行 K 路归并排序：

```
1. 为每个输入创建 RowIterator
2. 将每个迭代器的首个 key 推入最小堆
3. 循环弹出最小 key：
   ├── 重复 key：row_id 最大的（最新数据）胜出
   ├── 非重叠 PST 优化：整个 PST 可直接复用（零拷贝）
   │   标记 level = NotOverlappedMark(100000)
   └── 重叠情况：逐条读取，写入新 PST
4. 刷写最后一个不完整的 PST
```

#### 清理（`CleanCompaction()`）

```
阶段1：安装输出到 L1
  - 持久化新 PST 元数据到 Manifest
  - 插入 L1 树
  - 更新 L1/L0 版本号

阶段2：删除旧 L1 输入
  - 从 L1 树移除
  - 非 NotOverlappedMark 的 PST：删除数据块
  - 从 Manifest 删除

阶段3：删除旧 L0 树
  - FreeLevel0Tree()（删除 Masstree，清理表列表）
  - 删除数据块，从 Manifest 删除
```

#### 并行 Compaction

与并行 Flush 类似，12 个分区各自独立归并，最后顺序合并结果。

---

## 四、存储层

### 4.1 段分配器（`db/allocator/`）

整个存储池是一个大文件，被划分为固定大小的 4MB 段（Segment）。段分配器管理这些段的分配与回收。

#### 存储池文件布局

```
┌──────────┬──────────┬─────┬──────────┬────────────────┬──────────────────┐
│ Segment 0│ Segment 1│ ... │ Segment N│ segment_bitmap │ log_segment_bitmap│
└──────────┴──────────┴─────┴──────────┴────────────────┴──────────────────┘
 ←────────── pool_size ──────────────→  ←── 追加在池数据之后 ──→
```

#### SegmentAllocator（`segment_allocator.h`）

```
SegmentAllocator
├── pool_path_, pool_size_      // 池文件路径和大小
├── fd                          // 池文件描述符
├── segment_bitmap_             // 全局段分配位图（1 bit/段）
├── log_segment_bitmap_         // 日志段专用位图（恢复时快速定位）
├── data_segment_cache_         // 未满的 SortedSegment 复用队列
├── log_segment_group_[4]       // 每个 Memtable 对应的日志段ID列表
└── log_seg_num_, sort_seg_num_ // 统计计数器
```

关键操作：
- `AllocLogSegment(group_id)` — 分配日志段，同时在两个位图中标记，加入对应 Memtable 的段组
- `AllocSortedSegment()` — 优先从 `data_segment_cache_` 复用未满段，否则分配新段
- `FreeSegment()` — 清除位图标记，回收段
- `RecoverLogSegmentAndGetId()` — 恢复时从 `log_segment_bitmap_` 读取所有日志段ID

#### BitMap（`bitmap.h`）

双位图差异化持久方案：

```
bitmap_         — 当前分配状态（内存中实时更新）
history_bitmap_ — 上次持久化时的快照

PersistToSSDOnlyAlloc()  — 只持久化新分配的位（bitmap_ XOR history_bitmap_ 中新增的1）
PersistToSSDOnlyFree()   — 只持久化新释放的位（新增的0）
```

这种设计避免了并发分配/释放时的全量写入冲突。

#### LogSegment（`segment.h`）

```
LogSegment（4MB）
├── Header (8B)                 // status(2) + type(6) + tail_offset(24) + magic(32)
├── 写缓冲区 buf_ (16KB DRAM)   // 累积写入，满后 pwrite 刷盘
├── tail_                       // 当前写入位置
└── 数据区域                     // 16KB header 之后的空间
```

写入流程：`Append(data, size)` → 拷贝到 buf_ → buf_ 满时 pwrite 16KB → 推进 tail_。
段满条件：`tail_ >= off + SEGMENT_SIZE`（4MB）。

#### SortedSegment（`segment.h`）

```
SortedSegment（4MB，PAGE_SIZE=16KB）
├── Header (16KB)               // 段头
├── BitMap (16KB)               // 页内分配位图
└── 数据页 [0..253]             // 254 个 16KB 页
    每页存放一个 PDataBlock（PST 的数据块）
```

- `AllocatePage()` — 从内部位图分配一个 16KB 页
- `RecyclePage()` — 回收页（Compaction 清理时使用）
- 未满的段可以 `Freeze()` 后放入复用队列

### 4.2 WAL 日志（`db/log_writer.h`, `db/log_reader.h`）

#### 日志条目格式（`include/log_format.h`）

```
LogEntry32（24B / 8B-key，32B / 16B-key）：
┌─valid(1)─┬─lsn(31)─┬─key_sz(16)─┬─value_sz(16)─┬──key(8/16B)──┬──value(8B)──┐
└──────────┴─────────┴────────────┴──────────────┴─────────────┴────────────┘

LogEntry64（64B）：更大的内联 value 空间（48B / 8B-key）
LogEntryVar64：柔性数组，支持任意大小 value
```

#### LogWriter

每线程一个实例，写入流程：
1. 构造 LogEntry（小值用 LogEntry32，大值用 LogEntryVar64）
2. 调用 `LogSegment::Append()` 追加到当前段
3. 段满时自动关闭旧段、分配新段
4. 返回值：池内绝对字节偏移（`segment_id * SEGMENT_SIZE + segment_offset`）

#### LogReader

- `ReadLogForValue(key, valueptr, buf)` — 从 ValuePtr 解码文件偏移，pread 读取日志条目，提取 value
- `ReadLogFromSegment(seg_id, output)` — 批量读取整个段的所有条目（恢复用）

### 4.3 PST（Persistent Sorted Table）

PST 是 FlowKV 的最小持久化有序数据单元。当前实现中，一个 PST = 一个 16KB 数据块。

#### 数据块格式（`db/blocks/fixed_size_block.h`）

```
PDataBlock16K（16KB，4096B 对齐）：
┌─────────────────────────────────────────────┐
│ Entry[0]: { key(8B), value(8B) }    = 16B   │  8B-key 模式：
│ Entry[1]: { key(8B), value(8B) }            │  最多 1024 个条目
│ ...                                          │
│ Entry[1023]                                  │  16B-key 模式：
│ (未满部分填充 INVALID_PTR 哨兵)               │  最多 682 个条目
└─────────────────────────────────────────────┘
```

#### PSTMeta（`db/table.h`）

```cpp
struct PSTMeta {
    uint64_t datablock_ptr_;    // 数据块的文件偏移（0 = 无效）
    uint64_t min_key_, max_key_; // 键范围
    uint64_t key_256, key_512, key_768; // 采样键（1/4, 1/2, 3/4 位置）
    uint32_t seq_no_;           // 序列号
    uint16_t entry_num_;        // 有效条目数
};
```

采样键的作用：点查时先用采样键将 1024 个条目缩小到 ~256 个条目的分区，再做二分查找，减少实际 I/O 读取量。

#### TaggedPstMeta

```cpp
struct TaggedPstMeta {
    PSTMeta meta;
    size_t level;               // 0=L0, 1=L1, 100000=NotOverlappedMark（可直接复用）
    size_t manifest_position;   // 在 Manifest 中的槽位索引
};
```

#### PSTBuilder（`db/pst_builder.h`）

构建流程：`AddEntry(key, value)` → 填充 DataBlockWrapper → 满时 `Flush()` → 写入 16KB 到 SortedSegment 页 → 返回 PSTMeta。

#### PSTReader（`db/pst_reader.h`）

- `PointQuery(addr, key, ...)` — 二分查找，只读前 4KB（256 条目）即可定位
- `GetIterator(addr)` — 加载整个块到内存，返回顺序迭代器
- `RowIterator` — 跨多个 PST 的归并迭代器（Compaction 使用）

#### DataBlockReader 缓存策略

```
ReadBuf(offset)      — 读取前 4KB/8KB（256 条目），按 offset 缓存
ReadDataBlock(offset) — 读取完整 16KB，按 offset 缓存
```

同一个 PST 的重复查询命中缓存，避免重复 I/O。

---

## 五、Compaction 系统（`db/compaction/`）

### 5.1 Version（`version.h/cpp`）— 易失元数据

Version 维护 L0 和 L1 的运行时状态，纯内存结构。

#### L0 环形缓冲区

```
level0_trees_[32]        — 每个槽位一棵 Masstree 索引（key=PST.max_key → 槽位索引）
level0_table_lists_[32]  — 每棵树对应的 PST 列表
level0_tree_meta_[32]    — 每棵树的键范围摘要

三个指针管理环形缓冲区：
┌───┬───┬───────┬───────┬───────┬───────┬───┬───┐
│nil│nil│ tree1 │ tree2 │ tree3 │ tree4 │nil│nil│
└───┴───┴───────┴───────┴───────┴───────┴───┴───┘
              ↑                           ↑
           l0_head_                    l0_tail_
          (最旧，Compaction消费)      (最新，Flush写入)

l0_read_tail_ — 读可见边界（Flush 完成后才推进，避免读到半成品）
```

#### L1 有序表向量

```
level1_tables_[]    — 扁平向量（预留 650 万槽位），带空闲链表复用
level1_tree_        — 单棵 Masstree 索引（key=PST.max_key → 向量索引）
level1_free_list_   — 回收的空闲槽位
```

#### Version::Get() 读取流程

```
1. 遍历 L0 树（从新到旧）：
   对每棵树调用 Scan(key, 1) 做 lower_bound
   → 找到候选 PST → 验证 MinKey ≤ key
   → 用采样键缩小范围 → PSTReader::PointQuery()

2. 搜索 L1：
   对 L1 树调用 Scan(key, 1) 做 lower_bound
   → 同样的验证和查询流程
```

### 5.2 Manifest（`manifest.h/cpp`）— 持久元数据

#### 文件布局

```
┌──────────────┬──────────────────┬──────────────────┬──────────┐
│ SuperBlock   │ L0 PSTMeta 数组   │ L1 PSTMeta 数组   │ 刷写日志  │
│ (4KB)        │ (~51MB)          │ (~512MB)          │          │
└──────────────┴──────────────────┴──────────────────┴──────────┘
```

#### SuperBlock（4KB）

```cpp
struct ManifestSuperMeta {
    uint32_t l0_min_valid_seq_no;  // L0 有效序列号下界
    uint32_t l1_current_seq_no;    // L1 最新已提交序列号
    size_t l0_tail, l1_tail;       // 数组尾指针
    FlushLog { is_valid:1, length:63 }; // 刷写日志状态
};
```

#### 崩溃一致性：刷写日志协议

解决的问题：Flush 写完 PST 后、释放日志段之前崩溃，会导致日志段泄漏。

```
1. AddFlushLog(segment_ids)  — 写入待释放的段ID列表，设 is_valid=1（意图记录）
2. 逐个释放日志段
3. ClearFlushLog()           — 设 is_valid=0

恢复时：若 is_valid=1 → 重放释放操作
```

#### 恢复流程

1. 读取 SuperBlock 获取序列号边界
2. 扫描 L0 数组：`seq ≥ l0_min_valid_seq_no` 的 PST 重建到 L0 树
3. 扫描 L1 数组：`seq ≤ l1_current_seq_no` 的 PST 重建到 L1
4. `L1TreeConsistencyCheckAndFix()` — 清理崩溃中断的 Compaction 留下的重叠 PST

### 5.3 FlushJob（`flush.h/cpp`）— Memtable → L0

#### 单线程流程

```
FlushJob::run()
  1. memtable_index_->Scan2() — 提取所有 KV 对（已排序）
  2. version_->AddLevel0Tree() — 在环形缓冲区分配新槽位
  3. 遍历排序后的 key：
     ├── 确定所属分区（partition_info_）
     ├── 跨分区时 Flush 当前 PST
     ├── PSTBuilder::AddEntry() → 满时 Flush
     └── 每个 PST：持久化到 Manifest + 插入 L0 树
  4. UpdateLevel0ReadTail() — 使新树对读者可见
  5. 释放日志段（通过刷写日志协议保证崩溃安全）
```

#### 并行 Flush（`subrunParallel`）

```
协调线程：
  1. 分配 L0 树槽位
  2. 派发 12 个分区任务到 flush_thread_pool_
  3. 等待所有任务完成
  4. 顺序合并结果：持久化 PST 元数据 + 插入 L0 树
  5. 使树可见 + 释放日志段

每个分区工作线程 subrun(partition_id)：
  1. 创建独立的 PSTBuilder
  2. ScanByRange(min_key, max_key) 只扫描本分区范围
  3. 构建 PST，结果存入 partition_outputs_[partition_id]
```

### 5.4 CompactionJob（`compaction.h/cpp`）— L0 + L1 → 新 L1

#### 输入选择（PickCompaction）

```
inputs_[0]     = 最新 L0 树的 PST 列表
inputs_[1]     = 次新 L0 树
...
inputs_[N-1]   = 最旧 L0 树
inputs_[N]     = 与 L0 键范围重叠的 L1 PST 列表
```

#### K 路归并（RunCompaction / RunSubCompactionParallel）

使用最小堆进行 K 路归并排序：

```
初始化：每个输入行创建 RowIterator，将首个 key 推入堆
循环：
  弹出最小 key
  ├── 重复 key 处理：row_id 大的（更新的）胜出
  ├── 非重叠 PST 优化：若整个 PST 无重叠，直接复用（零拷贝）
  │   → 标记 level = NotOverlappedMark
  └── 重叠情况：读取实际 KV → PSTBuilder 构建新 PST
```

非重叠 PST 优化是关键性能点：如果一个 PST 的键范围与其他输入行不重叠，可以直接复用其数据块，无需读取和重写。

#### 清理（CleanCompaction）

```
阶段1：安装输出到 L1
  → 持久化新 PST 到 Manifest → 插入 L1 树 → 更新 L1 版本号

阶段2：删除旧 L1 输入
  → 从 L1 树移除 → 回收数据块（非 NotOverlappedMark 的）→ 从 Manifest 删除

阶段3：删除旧 L0 树
  → FreeLevel0Tree() → 回收数据块 → 从 Manifest 删除
```

---

## 六、原版 Masstree（`lib/masstree/`）

原版 Masstree 是一棵纯内存的并发 B+ 树/Trie 混合结构，由 MIT 的 Kohler、Mao、Morris 设计。FlowKV 将其用作 Memtable 和 L0/L1 的内存索引。

### 6.1 文件结构

```
lib/masstree/
├── masstree.hh              # 顶层 basic_table<P> 定义
├── masstree_struct.hh       # 核心节点定义：node_base, internode, leaf
├── masstree_key.hh          # Key 抽象：ikey/suffix/shift（层间跳转）
├── masstree_get.hh          # find_unlocked()（无锁读）、find_locked()（写路径查找）
├── masstree_insert.hh       # find_insert()、make_new_layer()、finish_insert()
├── masstree_remove.hh       # 删除逻辑
├── masstree_split.hh        # 节点分裂
├── masstree_scan.hh         # 正向/反向扫描状态机
├── masstree_tcursor.hh      # 游标：unlocked_tcursor（读）、tcursor（写）
├── nodeversion.hh           # OCC 版本号 + 锁协议
├── kpermuter.hh             # 排列编码（leaf 的逻辑排序）
├── ksearch.hh               # 二分/线性搜索
├── btree_leaflink.hh        # B^link 双向叶链表
├── stringbag.hh             # 变长 key 后缀存储
├── string_slice.hh          # 字节序感知的 key 切片提取
├── kvthread.hh/cc           # 线程局部内存分配 + RCU epoch 管理
├── config.h                 # CACHE_LINE_SIZE=64, 最大 key 长度=255
└── masstree_wrapper.h       # FlowKV 适配层：MasstreeWrapper
```

### 6.2 核心设计：Layer 机制

Masstree 的核心创新是将 B+ 树和 Trie 结合。Key 被切分为 8 字节的 slice（称为 ikey），每一层（layer）的 B+ 树索引一个 8 字节 slice：

```
对于 24 字节 key "AAAAAAAABBBBBBBBCCCCCCCC"：

Layer 0 的 B+ 树：索引前 8 字节 "AAAAAAAA"
    ↓ leaf slot 指向下一层
Layer 1 的 B+ 树：索引中间 8 字节 "BBBBBBBB"
    ↓ leaf slot 指向下一层
Layer 2 的 B+ 树：索引后 8 字节 "CCCCCCCC"
    ↓ 存储实际 value
```

对于 FlowKV 默认的 8 字节 key，只有 Layer 0，不会创建子层。
对于 16 字节 key（`FLOWKV_KEY16`），最多 2 层。

#### Layer 跳转的实现

Leaf 节点的 `keylenx_[p]` 字段编码了 slot 类型：
- `0..8` — key 长度恰好这么多字节，完全在 ikey 中
- `64`（`ksuf_keylenx`）— key 有后缀，存在 stringbag 中
- `128`（`layer_keylenx`）— 该 slot 指向下一层子树（不是 value）

```cpp
// leafvalue 是一个 union：
union {
    node_base<P>* n;   // 指向下一层子树根（当 keylenx == 128）
    value_type v;      // 实际 value（uint64_t）
    uintptr_t x;       // 原始位
} u_;
```

当两个 key 共享相同的 8 字节前缀但后缀不同时，`make_new_layer()` 会：
1. 创建新的子树（一系列单条目 leaf 节点）
2. 将原 slot 的 `keylenx` 设为 128
3. 将 `lv_` 设为新子树的根指针

### 6.3 节点结构

#### Internode（内部节点）

```
internode<P>（继承 node_base<P> : nodeversion<P>）
├── nkeys_ (uint8)           // 当前 key 数量（0..15）
├── height_ (uint32)         // 树高
├── ikey0_[15] (uint64×15)   // 15 个 8 字节 key = 120B
├── child_[16] (ptr×16)      // 16 个子指针 = 128B
└── parent_ (ptr)            // 父指针

总大小约 280 字节，64B 对齐
```

使用简单排序数组，`key_upper_bound` 定位子指针。

#### Leaf（叶节点 / Border 节点）

```
leaf<P>（继承 node_base<P> : nodeversion<P>）
├── extrasize64_ (int8)      // 额外 64B 块数（内联后缀用）
├── modstate_ (uint8)        // 插入/删除/层删除 状态
├── keylenx_[15] (uint8×15)  // 每 slot 的 key 长度/类型指示
├── permutation_ (uint64)    // 排列编码（逻辑排序）
├── ikey0_[15] (uint64×15)   // 每 slot 的 8 字节 ikey = 120B
├── lv_[15] (leafvalue×15)   // 每 slot 的 value 或层指针 = 120B
├── ksuf_ (ptr)              // 外部 key 后缀 bag 指针
├── next_, prev_ (ptr×2)     // B^link 双向链表
├── parent_ (ptr)            // 父指针
└── iksuf_[0]                // 柔性数组：内联后缀存储

总大小约 320 字节，向上取整到 64B 边界
```

#### 关键设计：Permuter（排列编码）

`kpermuter` 将 15 个 slot 的逻辑排序编码在一个 64 位整数中：

```
[bits 63-60] slot[14] | ... | [bits 7-4] slot[0] | [bits 3-0] size
```

插入时：
1. 将新 KV 写入空闲物理 slot（对读者不可见，因为不在排列中）
2. 原子更新排列字（将新 slot 插入正确的逻辑位置）

读者看到的要么是旧排列（不含新 key），要么是新排列（含新 key），永远不会看到半写状态。这是实现无锁读的关键。

### 6.4 并发控制：OCC（乐观并发控制）

#### 版本号布局（`nodeversion.hh`，32 位）

```
[31]    isleaf_bit      — 节点类型标志
[30]    root_bit        — 是否为根节点
[29]    deleted_bit     — 节点已删除
[28]    unused
[9-27]  vsplit (19位)   — 分裂计数器
[3-8]   vinsert (6位)   — 插入计数器
[2]     splitting_bit   — 正在分裂（脏位）
[1]     inserting_bit   — 正在插入（脏位）
[0]     lock_bit        — 排他锁
```

#### 读协议（无锁）

```
1. stable() — 自旋等待脏位清零（inserting/splitting），然后 acquire_fence
2. 执行读操作
3. has_changed(old_v) — 检查版本号是否变化，若变化则重试
```

#### 写协议

```
1. lock() — CAS 设置 lock_bit
2. mark_insert() / mark_split() — 设置脏位（通知读者自旋/重试）
3. 执行修改
4. unlock() — 递增计数器，清除脏位，release_fence
```

### 6.5 树遍历

#### reach_leaf() — 从根到叶

```
1. 从根开始，获取稳定版本号
2. 循环遍历内部节点：
   a. 预取节点（prefetch 4 个 cache line）
   b. 搜索 key → 确定子指针
   c. 获取子节点稳定版本
   d. 若父节点未变 → 下降
   e. 若父节点已变（并发分裂）→ 重试
3. 返回叶节点 + 版本号
```

使用双缓冲节点跟踪（`n[2]`, `v[2]`, `sense ^= 1`），可以检测并发修改而无需加锁。

#### advance_to_key() — B^link 叶追踪

到达叶节点后，若该叶已分裂，沿 `next_` 指针追踪到包含目标 key 的叶。

#### find_unlocked() — 无锁点查

```
1. reach_leaf(key) → 叶 + 版本
2. 读排列，lower_bound 搜索
3. 若找到：读 value，检查后缀匹配
4. 若叶已变 → advance_to_key，重试
5. 若 match < 0（层指针）→ shift key，跟随层指针，重试
6. 返回结果
```

### 6.6 Scan 扫描

扫描使用状态机实现（`masstree_scan.hh`）：

```
状态：scan_emit | scan_find_next | scan_down | scan_up | scan_retry

scan_emit:
  调用 scanner.visit_value() → 返回 false 则停止
  推进到下一个 slot

scan_find_next:
  读取当前叶的下一个 slot
  ├── 有效且非层 → scan_emit
  ├── 是层指针 → 压栈(root, leaf)，scan_down
  ├── 叶内无更多 slot → 沿 next_ 到下一叶
  └── 无下一叶 → scan_up

scan_down:
  shift key（前进 8 字节），在子树中重新搜索

scan_up:
  弹栈(root, leaf)，unshift key，回到父层继续
```

`node_stack_` 存储每层的 (root, leaf) 对，支持跨层扫描。

### 6.7 FlowKV 适配层

#### MasstreeWrapper（`masstree_wrapper.h`）

```cpp
struct table_params : public Masstree::nodeparams<15, 15> {
    typedef uint64_t value_type;  // value 是 uint64_t（ValuePtr）
    // ...
};
```

Key 编码：
- 8B key：直接传入 `uint64_t` 字节（Masstree 内部做大端转换）
- 16B key：`Key16::ToBigEndianBytes()` 转为 16 字节缓冲区

#### MasstreeIndex（`lib/index_masstree.h`）

实现 `Index` 抽象接口，委托给 `MasstreeWrapper`：
- `Get(key)` → `mt_->search(key, val)`
- `Put(key, vh)` → `mt_->insert(key, vh)`
- `Scan(key, cnt, vec)` → `mt_->scan(key, cnt, vec)`

### 6.8 Cache 友好性总结

原版 Masstree 的 cache 优化策略：
1. 节点大小 64B 对齐，`prefetch_full()` 预取 4 个 cache line
2. width=15 使用线性搜索（比二分更 cache 友好）
3. Permuter 编码在单个 `uint64_t` 中，原子读写
4. 读路径完全无锁，无 cache line 争用
5. 写路径只锁单个叶节点，锁粒度极细

---

## 六、原版 Masstree（`lib/masstree/`）

原版 Masstree 是一棵纯内存的并发 B+ 树/Trie 混合结构，由 MIT 的 Kohler、Mao、Morris 设计。FlowKV 将其用作 Memtable 和 L0/L1 的内存索引。

### 6.1 文件结构

```
lib/masstree/
├── masstree.hh              # 顶层 basic_table<P>，根指针
├── masstree_struct.hh       # 核心节点定义：node_base, internode, leaf, leafvalue
├── masstree_key.hh          # Key 抽象：ikey 切片 + shift（层下降）
├── masstree_get.hh          # find_unlocked()（无锁读）、find_locked()（写路径查找）
├── masstree_insert.hh       # find_insert()、make_new_layer()、finish_insert()
├── masstree_remove.hh       # 删除逻辑
├── masstree_split.hh        # 节点分裂：leaf::split_into()、internode::split_into()
├── masstree_scan.hh         # 扫描状态机：scanstackelt、forward/reverse_scan_helper
├── masstree_tcursor.hh      # 游标：unlocked_tcursor（读）、tcursor（写）
├── nodeversion.hh           # OCC 版本号 + 锁协议
├── kpermuter.hh             # 排列编码（leaf 的逻辑排序）
├── ksearch.hh               # 二分/线性搜索
├── btree_leaflink.hh        # B^link 双向叶链表
├── stringbag.hh             # 变长 key 后缀存储
├── string_slice.hh          # 字节序感知的 key 切片提取
├── kvthread.hh/cc           # 线程局部内存分配器 + RCU epoch 管理
├── config.h                 # CACHE_LINE_SIZE=64 等
└── masstree_wrapper.h       # FlowKV 适配层：MasstreeWrapper
```

### 6.2 核心设计：Trie of B+ Trees

Masstree 的核心创新是将长 key 按 8 字节切片分层索引。每一层（layer）是一棵完整的 B+ 树，索引 key 的一个 8 字节片段。

```
Key = "AAAAAAAABBBBBBBBCCCCCCCC"（24字节）

Layer 0 B+ 树：索引 ikey = "AAAAAAAA"（字节 0-7）
    └── leaf slot → 指向 Layer 1 子树根
        Layer 1 B+ 树：索引 ikey = "BBBBBBBB"（字节 8-15）
            └── leaf slot → 指向 Layer 2 子树根
                Layer 2 B+ 树：索引 ikey = "CCCCCCCC"（字节 16-23）
                    └── leaf slot → 实际 value
```

对于 FlowKV 默认的 8 字节 key，key 完全装入一个 ikey，不会创建子层——树退化为单层 B+ 树。
对于 16 字节 key（`FLOWKV_KEY16`），最多 2 层。

### 6.3 节点结构

#### Internode（内部节点）

```
internode<P> : node_base<P> : nodeversion<P>

字段：
├── nkeys_ (uint8)           // 当前 key 数量（0..15）
├── height_ (uint32)         // 树高
├── ikey0_[15] (uint64×15)   // 15 个 key 槽位（120B）
├── child_[16] (ptr×16)      // 16 个子指针（128B）
└── parent_ (ptr)            // 父指针

总大小 ≈ 280 字节，64B 对齐
搜索方式：width ≤ 16 时用线性搜索（cache 友好）
```

#### Leaf（叶节点 / Border 节点）

```
leaf<P> : node_base<P> : nodeversion<P>

字段：
├── extrasize64_ (int8)      // 额外 64B 块数（内联后缀用）
├── modstate_ (uint8)        // 修改状态
├── keylenx_[15] (uint8×15)  // 每槽位 key 长度/类型指示
│   ├── 0..8   → key 完全装入 ikey
│   ├── 64     → key 有后缀（存在 stringbag 中）
│   └── 128    → 此槽位指向下一层子树（layer 指针）
├── permutation_ (uint64)    // 排列编码（逻辑排序）
├── ikey0_[15] (uint64×15)   // 15 个 8 字节 ikey（120B）
├── lv_[15] (leafvalue×15)   // 15 个值槽位（120B）
│   └── union { value_type v; node_base* n; }  // 值 或 层指针
├── ksuf_ (ptr)              // 外部后缀 bag 指针
├── next_, prev_ (ptr)       // B^link 双向链表
├── parent_ (ptr)            // 父指针
└── iksuf_[0]                // 柔性数组：内联后缀

总大小 ≈ 320 字节，向上取整到 64B 边界
```

#### leafvalue（联合体）

```cpp
union {
    node_base<P>* n;   // 指向下一层子树根（keylenx == 128 时）
    value_type v;      // 实际值（FlowKV 中为 uint64_t / ValuePtr）
    uintptr_t x;       // 原始位
};
```

同一个槽位可以存储用户值或子树指针，这是 layer 机制的关键。

### 6.4 Permuter：无锁读的核心

`kpermuter`（`kpermuter.hh`）将 leaf 的逻辑排序编码在一个 64 位整数中：

```
width=15 时的 permutation_ 布局：
┌─slot[14](4b)─┬─...─┬─slot[1](4b)─┬─slot[0](4b)─┬─size(4b)─┐
│   高4位       │     │             │             │  低4位    │
└──────────────┴─────┴─────────────┴─────────────┴──────────┘
```

插入流程：
1. 将新 KV 写入一个空闲物理槽位（此时读者看不到，因为不在排列中）
2. `fence()` 保证写入顺序
3. 原子更新 `permutation_`，将新槽位插入正确的逻辑位置

读者要么看到旧排列（不含新 key），要么看到新排列（含新 key），永远不会看到半写状态。

### 6.5 并发控制：OCC（乐观并发控制）

#### 版本号布局（`nodeversion.hh`，32位）

```
┌─isleaf(1)─┬─root(1)─┬─deleted(1)─┬─unused(1)─┬─vsplit(19)─┬─vinsert(6)─┬─splitting(1)─┬─inserting(1)─┬─lock(1)─┐
│  bit 31   │ bit 30  │  bit 29    │  bit 28   │ bits 9-27  │ bits 3-8   │   bit 2      │   bit 1      │  bit 0  │
└───────────┴─────────┴────────────┴───────────┴────────────┴────────────┴──────────────┴──────────────┴─────────┘
```

#### 读协议（无锁）

```
1. stable() — 自旋等待 inserting/splitting 位清零，acquire_fence
2. 执行读操作
3. has_changed(old_v) — 检查版本号是否变化，若变化则重试
```

#### 写协议

```
1. lock() — CAS 设置 lock_bit
2. mark_insert() / mark_split() — 设置 dirty 位（通知读者自旋）
3. 执行修改
4. unlock() — 递增 vinsert/vsplit 计数器，清除 dirty 位，release_fence
```

### 6.6 树遍历

#### reach_leaf()（从根到叶）

```
1. 从根开始，获取稳定版本号
2. 循环遍历 internode：
   a. prefetch 节点
   b. 搜索 key → 确定子指针
   c. 获取子节点稳定版本
   d. 若父节点未变 → 下降
   e. 若父节点已变（并发分裂）→ 重试
3. 返回叶节点 + 版本号
```

使用双缓冲节点跟踪（`n[2]`, `v[2]`, `sense ^= 1`）检测并发修改。

#### advance_to_key()（B^link 追链）

叶节点分裂后，通过 `next_` 指针追踪到正确的叶：

```cpp
while (!v.deleted() && (next = n->safe_next())
       && compare(ka.ikey(), next->ikey_bound()) >= 0) {
    n = next;
    v = n->stable_annotated(...);
}
```

#### find_unlocked()（无锁点查）

```
1. reach_leaf(key) → 叶 + 版本
2. 读 permutation，lower_bound 搜索
3. 若找到：读值，检查后缀匹配
4. 若叶已变 → advance_to_key，重试
5. 若匹配到 layer → shift key，跟随层指针，重试
6. 返回结果
```

### 6.7 Scan（扫描）

扫描使用状态机（`masstree_scan.hh`）：

```
状态：scan_emit | scan_find_next | scan_down | scan_up | scan_retry

scan_emit:
  调用 scanner.visit_value() → 返回 false 则停止
  推进到下一个槽位

scan_find_next:
  读取下一个槽位
  ├── 有效且非 layer → scan_emit
  ├── 是 layer → push 栈，scan_down
  ├── 叶末尾 → 跟随 next_ 到下一叶
  └── 无下一叶 → scan_up

scan_down:
  shift key（进入子层），从子树根重新开始

scan_up:
  pop 栈，unshift key（回到父层），继续 find_next
```

`node_stack_` 存储 `(root, leaf)` 对，支持跨层扫描。

### 6.8 FlowKV 适配（`masstree_wrapper.h`, `index_masstree.h`）

```cpp
// MasstreeWrapper 配置
struct table_params : public Masstree::nodeparams<15, 15> {
    typedef uint64_t value_type;  // 值类型 = ValuePtr
};

// MasstreeIndex 实现 Index 接口
class MasstreeIndex : public Index {
    MasstreeWrapper *mt_;
    // Get → mt_->search()
    // Put → mt_->insert()
    // Scan → mt_->scan() with Scanner callback
};
```

Key 编码：8B key 直接传入（Masstree 内部做字节序转换），16B key 先转大端序再传入。

### 6.9 Cache-Line 优化总结

| 优化点 | 机制 |
|-------|------|
| 节点大小 | 64B 对齐，prefetch 4 条 cache line |
| 搜索方式 | width ≤ 16 用线性搜索（避免分支预测失败） |
| Permuter | 单个 uint64 原子更新，无锁读 |
| 版本号 | 32 位，单 cache line 内 |
| B^link | 分裂时不需要锁父节点 |

这些优化在纯内存场景下非常有效，但对外存场景意义不大（SSD 延迟 ~10μs >> cache miss ~100ns）。

---

## 七、H-Masstree 外存架构（`lib/hmasstree/`）

H-Masstree 是对原版 Masstree 的改造，目标是支持将节点持久化到外存（SSD），并按需加载。当前实现是一个"全外存"方案——所有节点统一处理，没有区分 Layer 0 和 Layer 1。

### 7.1 文件结构

```
lib/hmasstree/
├── 核心 Masstree（从原版 fork 并修改）
│   ├── masstree.hh              # basic_table<P>（增加 root_handle_）
│   ├── masstree_struct.hh       # node_base/internode/leaf（增加 handle 字段）
│   ├── masstree_get.hh          # find_unlocked/locked（增加 handle 解析逻辑）
│   ├── masstree_insert.hh       # find_insert（增加外存感知）
│   ├── masstree_split.hh        # 分裂时维护 handle
│   ├── masstree_scan.hh         # 扫描（使用指针遍历叶链表）
│   ├── btree_leaflink.hh        # 叶链表（增加 handle 链接）
│   └── (其余文件与原版基本一致)
│
├── 外存存储层（新增文件）
│   ├── node_handle.h            # NodeHandle（8字节逻辑地址）+ PackedPage（4KB页）
│   ├── external_node.h          # 外存节点布局 + 类型别名
│   ├── node_serializer.h        # 序列化/反序列化
│   ├── node_cache.h/cpp         # NodeCache（分片哈希表 + Clock 淘汰）
│   ├── index_storage.h/cpp      # IndexStorageManager（段式分配 + 页 I/O）
│   ├── node_resolver.h          # NodeResolver（handle → 内存指针的桥接）
│   ├── node_factory.h           # NodeFactory（创建节点时分配 handle）
│   ├── scan_context.h           # ScanContext（扫描时的页 pin 管理）
│   └── external_index.h         # ExternalStorageManager（集成层）
│
└── 适配层
    ├── hmasstree_wrapper.h/cc   # HMasstreeWrapper（FlowKV API 适配）
    ├── lib/index_hmasstree.h    # HMasstreeIndex（纯内存适配器）
    └── lib/index_hmasstree_external.h  # HMasstreeExternalIndex（外存适配器）
```

### 7.2 NodeHandle — 逻辑地址（`node_handle.h`）

NodeHandle 是一个 8 字节的逻辑地址，与原始指针大小相同，用于替代裸指针实现外存寻址：

```
64 位布局：
┌─valid(1)─┬─node_type(3)─┬──page_id(37)──┬─slot_index(3)─┬─generation(20)─┐
│  有效位   │ LEAF/INTERNODE│ 支持1370亿页  │  页内槽位0-7  │  ABA防护计数器  │
└──────────┴──────────────┴──────────────┴──────────────┴───────────────┘
```

区分 handle 和裸指针的方法：Linux 用户态指针的 bit 63 为 0，而有效 NodeHandle 的 bit 63 为 1。

`AtomicNodeHandle` 封装了 `std::atomic<uint64_t>`，支持原子 load/store/CAS。

### 7.3 PackedPage — 页打包（`node_handle.h`）

每个 4KB 页打包 8 个节点槽位：

```
PackedPage（4096 字节）
┌──────────────────────────────────────────────────┐
│ PageHeader (64B)                                  │
│   magic(8B) + page_id(8B) + slot_bitmap(1B)      │
│   + slot_types[8](8B) + reserved(39B)            │
├──────────────────────────────────────────────────┤
│ Slot 0 (504B) │ Slot 1 (504B) │ ... │ Slot 7 (504B) │
└──────────────────────────────────────────────────┘
(4096 - 64) / 8 = 504 字节/槽位
```

### 7.4 外存节点布局（`external_node.h`, `node_serializer.h`）

#### ExternalInternodeLayout（504 字节）

```
┌─nkeys(1B)─┬─reserved(3B)─┬─height(4B)─┬─version(8B)─┐  头部 16B
├─ikey0[15] (120B)──────────────────────────────────────┤  键
├─children[16] (128B, NodeHandle)───────────────────────┤  子节点 handle
├─parent (8B, NodeHandle)───────────────────────────────┤  父 handle
├─padding (232B)────────────────────────────────────────┤
└───────────────────────────────────────────────────────┘  共 504B
```

#### ExternalLeafLayout（504 字节）

```
┌─version(8B)─┬─permutation(8B)─┬─extrasize64(1B)─┬─modstate(1B)─┬─keylenx[15](15B)─┬─reserved(5B)─┐  头部 40B
├─ikey0[15] (120B)─────────────────────────────────────────────────────────────────────────────────────┤  键
├─lv[15] (120B, union{uint64_t raw, NodeHandle layer_handle})──────────────────────────────────────────┤  值/层handle
├─next(8B)─┬─prev(8B)─┬─parent(8B)─┬─phantom_epoch(8B)────────────────────────────────────────────────┤  导航
├─padding (192B)───────────────────────────────────────────────────────────────────────────────────────┤
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘  共 504B
```

关键设计决策：
- Key 后缀（ksuf）不序列化——变长 key 的后缀信息在持久化时丢失
- Layer 指针序列化为子树根的 `self_handle_`，而非内存指针
- Internode 的子指针只存 handle，裸指针在反序列化时设为 nullptr（按需加载）

### 7.5 内存节点的额外字段

H-Masstree 在原版节点结构上增加了 handle 字段：

```
Internode 额外字段（+144B）：
├── child_handles_[16]  (128B)  // 每个子节点的 NodeHandle
├── parent_handle_ (8B)
└── self_handle_ (8B)

Leaf 额外字段（+32B）：
├── self_handle_ (8B)
├── next_handle_ (8B)
├── prev_handle_ (8B)
└── parent_handle_ (8B)
```

这是"双表示"设计：内存中同时维护裸指针（快速遍历）和 handle（持久化），两者需要保持同步。

### 7.6 IndexStorageManager — 磁盘 I/O 层（`index_storage.h`）

管理外存文件的段式分配和页级 I/O：

```
IndexStorageManager
├── fd_                         // 存储文件描述符
├── superblock_                 // 超级块（256B，存储在 page 0）
│   ├── magic, version
│   ├── next_page_id            // 单调递增的页ID计数器
│   ├── root_handle_raw         // 持久化的根节点 handle
│   └── checksum
├── segments_[]                 // 段列表（每段 4MB = 1024 个 4KB 页）
│   └── IndexSegment
│       ├── page_bitmap[1024]   // 页分配位图
│       └── free_pages (queue)  // 空闲页队列
├── free_slots_                 // 按 NodeType 分类的空闲槽位队列
└── segments_with_space_        // 有空闲页的段队列
```

节点分配流程：
1. `allocate_node_slot(type)` — 先查 `free_slots_[type]` 复用已有页的空闲槽位
2. 若无可复用 → `allocate_page()` 分配新 4KB 页，返回 `(page_id, slot_index=0)`
3. 构造 `NodeHandle::make_leaf(page_id, slot, generation=0)`

### 7.7 NodeCache — 缓存层（`node_cache.h`）

分片哈希表 + Clock 淘汰算法：

```
NodeCache
├── shards_[64]                 // 64 个分片
│   └── Shard
│       ├── mutex (shared_mutex)
│       ├── pages (unordered_map<page_id, CachedPage>)
│       ├── clock_list          // Clock 淘汰列表
│       └── clock_hand          // Clock 指针
├── storage_ (IndexStorageManager*)
├── evict_callback_             // 淘汰回调（通知 NodeResolver 失效指针）
├── thread_epochs_[128]         // 每线程 epoch 状态（64B 对齐避免伪共享）
└── global_epoch_               // 全局 epoch
```

#### CachedPage

```
CachedPage
├── page_ptr (PackedPage*)      // 4KB 对齐的页数据
├── page_id
├── pin_count (atomic uint32)   // 引用计数（防止被淘汰）
├── evict_requested (atomic bool)
├── reference_bit (atomic uint8) // Clock 算法位
└── is_dirty (atomic bool)      // 脏页标记
```

#### 核心操作

- `get_or_load(handle, pin)` — 快速路径：shared lock 查哈希表；慢路径：exclusive lock + pread 4KB
- `evict_pages(target_bytes)` — Clock 扫描：reference_bit=0 且 pin_count=0 的页被淘汰
- `flush_all()` — 将所有脏页写回存储
- Pin 协议（双重检查）：`try_pin()` 先检查 `evict_requested`，再递增 `pin_count`，再次检查

#### Epoch 保护

防止 use-after-free：
- 读线程进入临界区时记录当前 epoch
- 淘汰操作延迟到所有线程都推进到淘汰请求之后的 epoch

### 7.8 NodeResolver — Handle 到指针的桥接（`node_resolver.h`）

每线程一个实例，维护 handle → 内存指针的映射：

```
NodeResolver<P>
├── cache_ (NodeCache*)
├── mutex_ (shared_mutex)
└── handle_to_node_ (unordered_map<uint64_t, node_type*>)
```

#### resolve(handle, ti) — 按需加载

```
1. 快速路径：shared lock 查 handle_to_node_ 映射
2. 慢路径：load_and_register(handle, ti)
   a. cache_->get_or_load(handle, pin=true) — 获取页（可能触发磁盘读）
   b. 从页的 slot_types[] 确定节点类型（权威来源）
   c. pool_allocate() 分配内存节点
   d. deserialize_leaf/internode() 反序列化
   e. 对 internode：所有 child_[i] = nullptr（按需加载）
   f. 双重检查 + exclusive lock 注册到映射
```

#### on_page_evicted(page_id) — 淘汰回调

当 NodeCache 淘汰一个页时，遍历 `handle_to_node_` 移除所有属于该页的条目。

### 7.9 NodeFactory — 节点创建（`node_factory.h`）

创建新节点时自动分配 handle：

```
make_leaf(ksuf_capacity, phantom_epoch, ti):
  1. leaf::make() — 原版 Masstree 分配器
  2. storage_->allocate_node_slot(LEAF) → (page_id, slot_index)
  3. NodeHandle::make_leaf(page_id, slot, generation=0)
  4. leaf->self_handle_ = handle
  返回 (leaf*, NodeHandle)
```

### 7.10 ScanContext — 扫描支持（`scan_context.h`）

扫描操作需要遍历叶链表，每个 `next` 指针都是 NodeHandle：

```
ScanContext
├── cache_ (NodeCache*)
├── thread_id_
└── pinned_pages_ (vector<CachedPage*>)  // 扫描期间累积的 pin

resolve_next<P>(current_leaf):
  1. 读取 current_leaf->safe_next_h()（next handle）
  2. cache_->get_or_load(handle, pin=true)
  3. 从页槽位获取叶节点指针
  4. 将页加入 pinned_pages_

release_all():  // 析构时调用
  逐个 unpin 所有累积的页
```

### 7.11 ExternalStorageManager — 集成层（`external_index.h`）

顶层编排器，组合 cache + storage + 后台刷写：

```
ExternalStorageManager
├── cache_ (NodeCache)
├── storage_ (IndexStorageManager)
├── stats_ (命中率、I/O 统计)
└── 后台刷写线程（定期 flush_all + advance_epoch）
```

配置项：
| 参数 | 默认值 | 含义 |
|-----|-------|------|
| `storage_path` | `/tmp/hmasstree_index.dat` | 存储文件路径 |
| `cache_size_mb` | 256 | 缓存内存预算 |
| `cache_shards` | 64 | 缓存分片数 |
| `flush_interval_ms` | 1000 | 后台刷写间隔 |
| `max_memory_mb` | 512 | 淘汰触发阈值 |

### 7.12 HMasstreeWrapper — 适配层（`hmasstree_wrapper.h`）

在外存模式下增加的关键方法：

```
init_external_storage(config):
  创建 ExternalStorageManager → 设置淘汰回调

thread_init_external(tid):
  设置线程局部的 IndexStorageRegistry / NodeCacheRegistry / NodeResolver / ScanContext

serialize_all_nodes():
  DFS 遍历整棵树 → 逐节点序列化到页槽位 → 标记脏页 → flush

restore_from_storage():        // 懒加载
  只加载根节点，其余按需加载

restore_from_storage_full():   // 全量加载
  加载根节点 → load_children_recursive() 递归加载所有子节点
```

### 7.13 线程局部注册表

所有外存组件通过 `thread_local` 注册表访问，避免修改 Masstree 的函数签名：

```
IndexStorageRegistry   — thread_local IndexStorageManager*
NodeCacheRegistry      — thread_local NodeCache*
NodeResolverRegistry<P> — thread_local NodeResolver<P>*
ScanContextRegistry    — thread_local ScanContext*
NodeFactoryRegistry<P> — thread_local NodeFactory<P>*
```

### 7.14 端到端数据流

#### 写入（Put）

```
HMasstreeExternalIndex::Put()
  → ScanContextGuard 激活线程上下文
  → HMasstreeWrapper::insert() → Masstree tcursor::find_insert()
  → 需要新节点时（分裂等）：
    NodeFactory::make_leaf() → allocate_node_slot() → 创建 NodeHandle
    节点存活在 DRAM，handle 是其逻辑地址
  → 后台刷写线程定期 flush_all() 写回脏页
```

#### 读取（Get）

```
HMasstreeExternalIndex::Get()
  → ScanContextGuard
  → Masstree find_unlocked() → reach_leaf()
  → 遇到 child_[i] == nullptr 但 child_handles_[i] 有效：
    NodeResolver::resolve(handle)
      快速路径：查 handle_to_node_ 映射
      慢路径：cache->get_or_load() → pread 4KB → 反序列化 → 注册
  → 解析后的指针缓存回 child_[i]，下次直接使用
```

#### 持久化

```
serialize_all_nodes()
  → DFS 遍历 → 每个节点序列化到对应页槽位
  → 标记页为脏
  → persist_tree_structure()（根 handle 写入超级块）
  → flush_all()（所有脏页写回磁盘）
```

#### 恢复

```
restore_from_storage()
  → 从超级块读取根 handle
  → 只加载根节点到内存
  → 其余节点按需加载（通过 NodeResolver）
```

### 7.15 当前方案的问题分析

1. **所有节点一视同仁**：频繁访问的上层节点（尤其是 Layer 0）也可能被淘汰，导致反复加载
2. **504B 节点 vs 4KB I/O**：一次 pread 读 4KB，但同一页的 8 个节点之间没有逻辑关联（父子大概率不在同一页），空间局部性差
3. **width=15 的 fanout 对外存太小**：树高 4-5 层，每次点查需要 4-5 次潜在的磁盘 I/O
4. **resolve() 路径上的锁竞争**：shared_mutex + hash map 查找在热路径上开销显著
5. **双表示的同步开销**：每个节点同时维护裸指针和 handle，分裂/插入时都需要同步更新
6. **cache-line 优化失效**：外存节点加了 handle 字段后，内存布局不再 cache-line 对齐

---

## 八、分层 Masstree 设计方案

### 8.1 核心思路

利用 Masstree 天然的 Layer 分界，将内存和外存的边界恰好放在 Layer 边界上：

```
┌─────────────────────────────────────────────────┐
│  Layer 0（常驻内存）                              │
│  索引 16B key 的前 8 字节                         │
│  保持原版 Masstree 结构                           │
│  cache-line 优化、OCC 无锁读、permuter            │
│                                                   │
│  leaf slot: keylenx=128 时                        │
│  lv_[p] 存储 → Layer 1 子树的入口（handle 或指针） │
└───────────────────┬─────────────────────────────┘
                    │ Layer 跳转边界
                    ▼
┌─────────────────────────────────────────────────┐
│  Layer 1（外存，按需加载）                         │
│  索引 16B key 的后 8 字节                         │
│  SSD 友好的大节点 B+ 树（4KB/节点）                │
│  高 fanout（~250），树高 2-3 层                    │
│  排序数组 + 二分查找（无需 permuter）              │
└─────────────────────────────────────────────────┘
```

### 8.2 Layer 0：保持原版结构

Layer 0 常驻内存，理想情况下完全保持原版 Masstree 的结构和优化：

- 节点大小不变（~320B leaf, ~280B internode），64B 对齐
- OCC 并发控制 + permuter 无锁读完整保留
- 不需要 handle、不需要序列化，零额外开销
- 对 16B key，Layer 0 只索引前 8 字节，规模可控

唯一的修改点：Layer 0 的 leaf 中 `keylenx == 128` 的 slot，其 `lv_[p]` 需要存储 Layer 1 子树的入口。可以是：
- 一个 NodeHandle（通过 bit 63 区分）
- 或一个指向已加载的 Layer 1 根节点的指针

在 `find_unlocked()` 检测到 layer 跳转时，触发从内存到外存的切换。

### 8.3 Layer 1：SSD 友好的大节点 B+ 树

Layer 1 针对 SSD 特性重新设计：

#### 节点大小：4KB（对齐 SSD 页）

```
Layer1 Leaf（4KB）：
┌─header(64B)──────────────────────────────────┐
│ entry_count, min_key, max_key, next_handle,  │
│ prev_handle, checksum                         │
├─entries[~250]────────────────────────────────┤
│ { key(8B), value(8B) } × ~250               │  8B key: (4096-64)/16 ≈ 252
└──────────────────────────────────────────────┘

Layer1 Internode（4KB）：
┌─header(64B)──────────────────────────────────┐
│ key_count, height, min_key, max_key          │
├─keys[~250](8B each)──────────────────────────┤
├─children[~251](8B NodeHandle each)───────────┤
└──────────────────────────────────────────────┘  8B key: (4096-64)/(8+8) ≈ 252
```

#### 优势

| 对比项 | 当前 H-Masstree | Layer 1 大节点 |
|-------|----------------|---------------|
| 节点大小 | 504B（8个/4KB页） | 4KB（1个/页） |
| Fanout | 15 | ~250 |
| 树高（100万条目） | ~5 层 | ~2-3 层 |
| 点查 I/O 次数 | ~5 次 | ~2-3 次 |
| 空间局部性 | 差（同页节点无关联） | 好（整个节点一次读入） |
| 搜索方式 | 线性搜索（cache优化） | 二分查找（节点内搜索开销可忽略） |

#### 并发模型选项

1. **Latch-based（读写锁）**：节点加载到内存后加锁操作，写回时持有锁。比 OCC 简单，适合外存
2. **Immutable**：如果 Layer 1 主要存放 compaction 产出的只读数据，可以完全不需要并发控制
3. **Copy-on-Write**：修改时创建新页，原子替换指针，天然支持并发读

### 8.4 Layer 间桥接机制

```
Layer 0 leaf slot (keylenx=128)
    │
    │ lv_[p] 存储 Layer1RootEntry：
    │   ├── NodeHandle root_handle  // Layer 1 子树根的外存地址
    │   ├── node_base* cached_root  // 已加载的根指针（缓存）
    │   └── 状态标志                 // COLD / WARM / HOT
    │
    ▼
find_unlocked() 检测到 layer 跳转：
    if (lv_[p].cached_root != nullptr)
        直接使用缓存的根指针 → Layer 1 查找
    else
        从外存加载根节点 → 缓存到 lv_[p].cached_root → Layer 1 查找
```

### 8.5 整体架构图

```
                    ┌──────────────────────────────────┐
                    │         MYDBClient::Get()         │
                    └──────────┬───────────────────────┘
                               │
                    ┌──────────▼───────────────────────┐
                    │     Memtable (原版 Masstree)      │
                    │     纯内存，cache-line 优化        │
                    └──────────┬───────────────────────┘
                               │ miss
                    ┌──────────▼───────────────────────┐
                    │     Version::Get()                │
                    │     L0 环形缓冲区 → L1 有序表      │
                    └──────────┬───────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                  │
    ┌─────────▼──────────┐            ┌─────────▼──────────┐
    │  L0/L1 PST 索引     │            │  L0/L1 PST 数据     │
    │  (分层 Masstree)    │            │  (16KB 数据块)       │
    │                     │            │                     │
    │  Layer 0: 内存      │            │  PSTReader::        │
    │  原版结构，常驻      │            │  PointQuery()       │
    │                     │            │                     │
    │  Layer 1: 外存      │            │  DataBlockReader::  │
    │  4KB大节点B+树      │            │  BinarySearch()     │
    │  按需加载           │            │                     │
    └─────────────────────┘            └─────────────────────┘
```

### 8.6 需要进一步确认的问题

1. **Key 分布特征**：前 8 字节的分布是否均匀？这决定了 Layer 0 的规模
2. **读写比例**：Layer 1 是否需要支持 in-place update，还是主要由 compaction 产出（只读）
3. **内存预算**：Layer 0 常驻内存的上限是多少？这决定了是否需要对 Layer 0 也做淘汰
4. **Layer 1 的持久化时机**：是每次写入都持久化，还是批量/后台持久化

---

## 九、全局数据流总览

### 9.1 写入路径（端到端）

```
用户调用 MYDBClient::Put(key, value)
│
├─ 1. StartWrite()
│     同步本地 memtable 索引；若发生轮转，LogWriter 切换到新段组
│
├─ 2. GetLSN(key)
│     哈希到 256 个桶之一，原子递增桶计数器
│
├─ 3. LogWriter::WriteLogPut(key, value, lsn)
│     ├─ 构造 LogEntry32（小值）或 LogEntryVar64（大值）
│     ├─ LogSegment::Append() → 写入 16KB DRAM 缓冲区
│     ├─ 缓冲区满 → pwrite 16KB 到存储池文件
│     └─ 返回池内绝对偏移
│
├─ 4. 编码 ValuePtr
│     将偏移编码为 {valid=1, ptr=逻辑条目索引, lsn}
│
├─ 5. mem_index_[idx]->PutValidate(key, ValuePtr)
│     ├─ Masstree: find_locked() → 定位叶节点并加锁
│     ├─ CAS 比较 LSN，新值胜出
│     └─ 更新 permuter，unlock
│
└─ 6. 递增本地写入计数
```

### 9.2 读取路径（端到端）

```
用户调用 MYDBClient::Get(key)
│
├─ 1. GetFromMemtable()
│     从新到旧遍历 4 个 Memtable 槽位（跳过 EMPTY）
│     ├─ mem_index_[i]->Get(key) → Masstree find_unlocked()（无锁）
│     ├─ 找到 valid=1 → LogReader 读值 → 返回
│     ├─ 找到 valid=0 → 墓碑 → 返回未找到
│     └─ 未找到 → 继续下一个 Memtable
│
├─ 2. Version::Get(key) — L0 搜索
│     从新到旧遍历 L0 环形缓冲区中的每棵树
│     ├─ L0 Masstree Scan(key, 1) → lower_bound 找候选 PST
│     ├─ 验证 MinKey ≤ key
│     ├─ 用采样键（key_256/512/768）缩小到 256 条目分区
│     └─ PSTReader::PointQuery() → DataBlockReader::BinarySearch()
│         ├─ ReadBuf() 读取前 4KB（带缓存）
│         └─ 二分查找 → 返回 8B value
│
└─ 3. Version::Get(key) — L1 搜索
      L1 Masstree Scan(key, 1) → 同样的验证 + 采样键 + 点查流程
```

### 9.3 Flush 路径

```
后台轮询线程（每 100ms）
│
├─ MayTriggerFlushOrCompaction()
│   GetMemtableSize() ≥ 500K？
│
├─ BGFlush()
│   ├─ 创建新 Masstree 放入下一个槽位，设为 ACTIVE
│   ├─ 原子切换 current_memtable_idx_
│   ├─ sleep 100μs（等待飞行中的写入完成）
│   │
│   ├─ FlushJob::subrunParallel()
│   │   ├─ 分配 L0 树槽位
│   │   ├─ 12 个分区并行：ScanByRange → PSTBuilder → 输出 PST 列表
│   │   ├─ 顺序合并：Manifest::AddTable() + Version::InsertTableToL0()
│   │   └─ UpdateLevel0ReadTail()（使新树对读者可见）
│   │
│   ├─ 释放日志段（通过 Flush Log 保证崩溃安全）
│   └─ 设旧 Memtable 为 EMPTY，删除旧 Masstree
│
└─ MayTriggerCompaction()
```

### 9.4 Compaction 路径

```
L0 树数量 ≥ 阈值（默认 4）
│
├─ CompactionJob::PickCompaction()
│   ├─ 收集所有 L0 树的 PST 列表（从新到旧）
│   ├─ 计算总键范围 [min_key, max_key]
│   └─ 收集与该范围重叠的 L1 PST
│
├─ RunSubCompactionParallel()
│   12 个分区并行 K 路归并：
│   ├─ 每个分区创建独立的 RowIterator + PSTBuilder
│   ├─ 最小堆归并，重复 key 取最新
│   ├─ 非重叠 PST 直接复用（零拷贝）
│   └─ 输出新 PST 列表
│
└─ CleanCompaction()
    ├─ 阶段1：新 PST → Manifest + L1 树 + 更新版本号
    ├─ 阶段2：旧 L1 PST → 从树移除 + 回收数据块 + 从 Manifest 删除
    └─ 阶段3：旧 L0 树 → FreeLevel0Tree + 回收 + 从 Manifest 删除
```

### 9.5 恢复路径

```
MYDB 构造（recover=true）
│
├─ SegmentAllocator 恢复
│   读取 segment_bitmap_ 和 log_segment_bitmap_
│
├─ Manifest::RecoverVersion()
│   ├─ 读超级块 → l0_min_valid_seq_no, l1_current_seq_no
│   ├─ 扫描 L0 数组 → 重建 L0 树（seq ≥ min_valid）
│   ├─ 扫描 L1 数组 → 重建 L1（seq ≤ current_seq）
│   └─ L1TreeConsistencyCheckAndFix()
│
├─ RedoFlushLog()
│   若 flush_log.is_valid → 重放日志段释放
│
└─ RecoverLogAndMemtable()
    ├─ 获取所有日志段 ID
    ├─ 逐段 ReadLogFromSegment() → 批量读取 LogEntry
    └─ 逐条 Put 到 mem_index_[0] → 重建 Memtable
```

---

## 附录 A：关键常量速查表

| 常量 | 值 | 定义位置 | 含义 |
|-----|---|---------|------|
| `SEGMENT_SIZE` | 4MB | `db_common.h` | 段大小 |
| `MAX_MEMTABLE_NUM` | 4 | `config.h` | Memtable 轮转槽位数 |
| `MAX_MEMTABLE_ENTRIES` | 500,000 | `config.h` | Flush 触发阈值 |
| `MAX_L0_TREE_NUM` | 32 | `config.h` | L0 环形缓冲区容量 |
| `MAX_USER_THREAD_NUM` | 64 | `config.h` | 最大并发线程数 |
| `RANGE_PARTITION_NUM` | 12 | `config.h` | 并行分区数 |
| `logbuffersize` | 16KB | `segment.h` | 日志写缓冲区大小 |
| `LOG_ENTRY_SIZE` | 24B (8B key) / 32B (16B key) | `db_common.h` | 日志条目大小 |
| `PDataBlock MAX_ENTRIES` | 1024 (8B key) / 682 (16B key) | `fixed_size_block.h` | 数据块最大条目数 |
| `CACHE_LINE_SIZE` | 64B | `masstree/config.h` | Cache line 大小 |
| Masstree `width` | 15 | `masstree_struct.hh` | 节点 fanout |
| `PackedPage NODES_PER_PAGE` | 8 | `node_handle.h` | 每 4KB 页的节点槽位数 |
| `NODE_SLOT_SIZE` | 504B | `node_handle.h` | 外存节点序列化大小 |

## 附录 B：关键文件索引

| 功能 | 文件 | 核心类/结构 |
|-----|------|-----------|
| 引擎入口 | `include/db.h`, `db/db.cpp` | `MYDB` |
| 客户端句柄 | `db/db_client.cpp` | `MYDBClient` |
| 公共类型 | `include/db_common.h` | `KeyType`, `ValuePtr`, `Index` |
| WAL 写入 | `db/log_writer.h` | `LogWriter` |
| WAL 读取 | `db/log_reader.h` | `LogReader` |
| 日志格式 | `include/log_format.h` | `LogEntry32`, `LogEntry64` |
| PST 构建 | `db/pst_builder.h` | `PSTBuilder` |
| PST 读取 | `db/pst_reader.h` | `PSTReader`, `RowIterator` |
| 数据块格式 | `db/blocks/fixed_size_block.h` | `PDataBlock16K` |
| PST 元数据 | `db/table.h` | `PSTMeta`, `TaggedPstMeta` |
| 段分配 | `db/allocator/segment_allocator.h` | `SegmentAllocator` |
| 段类型 | `db/allocator/segment.h` | `LogSegment`, `SortedSegment` |
| 位图 | `db/allocator/bitmap.h` | `BitMap` |
| 易失元数据 | `db/compaction/version.h` | `Version` |
| 持久元数据 | `db/compaction/manifest.h` | `Manifest` |
| Flush | `db/compaction/flush.h` | `FlushJob` |
| Compaction | `db/compaction/compaction.h` | `CompactionJob` |
| 原版 Masstree | `lib/masstree/masstree_struct.hh` | `internode`, `leaf` |
| Masstree 适配 | `lib/index_masstree.h` | `MasstreeIndex` |
| H-Masstree 节点 | `lib/hmasstree/masstree_struct.hh` | `internode`, `leaf`（+handle） |
| 逻辑地址 | `lib/hmasstree/node_handle.h` | `NodeHandle`, `PackedPage` |
| 外存布局 | `lib/hmasstree/external_node.h` | `ExternalLeafLayout`, `ExternalInternodeLayout` |
| 序列化 | `lib/hmasstree/node_serializer.h` | `serialize_leaf`, `deserialize_leaf` |
| 节点缓存 | `lib/hmasstree/node_cache.h` | `NodeCache`, `CachedPage` |
| 磁盘 I/O | `lib/hmasstree/index_storage.h` | `IndexStorageManager` |
| Handle 解析 | `lib/hmasstree/node_resolver.h` | `NodeResolver` |
| 节点工厂 | `lib/hmasstree/node_factory.h` | `NodeFactory` |
| 扫描上下文 | `lib/hmasstree/scan_context.h` | `ScanContext` |
| 外存集成 | `lib/hmasstree/external_index.h` | `ExternalStorageManager` |
| H-Masstree 适配 | `lib/index_hmasstree_external.h` | `HMasstreeExternalIndex` |
