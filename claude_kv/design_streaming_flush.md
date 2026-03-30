# Streaming Flush：零拷贝 + 并行 Flush 设计

最后更新：2026-03-21

## 一、问题

原 flush 路径存在三个性能瓶颈：

| 问题 | 影响 |
|------|------|
| `Scan2`/`ScanByRange` 全量复制到临时 vector | memtable 有 N 条 entry 就分配 2×N 的临时内存（keys + values），flush 期间内存翻倍 |
| `is_flushing_` 全局单 flag | 同一时间只能 flush 一个 memtable，前台写入被迫等待 |
| Masstree 释放后内存不归还 OS | glibc malloc 不主动 trim，RSS 只增不减 |

## 二、Streaming Flush（零拷贝遍历）

### 核心思路

不再 `Scan2` 全量复制，而是在 Masstree scan 的 `visit_value` 回调中直接调用 `PSTBuilder::AddEntry`，边遍历边写盘。

### 接口设计

在 `Index` 基类新增两个虚函数：

```cpp
// 遍历所有 entry
virtual void ForEachEntry(std::function<bool(KeyType, ValueType)> callback);

// 遍历 [start, end] 范围内的 entry
virtual void ForEachEntryInRange(const KeyType start, const KeyType end,
                                 std::function<bool(KeyType, ValueType)> callback);
```

### Masstree 实现

新增两个 Scanner 结构体，在 `visit_value` 中直接调用用户 callback：

- `ScannerForEach`：全量遍历，callback 返回 false 终止
- `ScannerForEachRange`：带 end key 边界检查

对应 `MasstreeWrapper` 新增 `for_each()` 和 `for_each_range()` 方法。

### Flush 路径改动

**`FlushJob::run()`**（单线程 streaming flush）：

```
memtable_index_->ForEachEntry([&](KeyType k, uint64_t val) {
    // 1. encode key -> Slice
    // 2. read log value (INDEX_LOG_MEMTABLE) 或直接用 val
    // 3. partition boundary check -> FlushPST()
    // 4. pst_builder_.AddEntry(key, value)
    return true;
});
```

**`FlushJob::subrun()`**（分区 streaming flush）：

```
memtable_index_->ForEachEntryInRange(min_key, max_key, [&](KeyType k, uint64_t val) {
    // 同上，但不需要 partition boundary check
    return true;
});
```

### 收益

| 指标 | 改动前 | 改动后 |
|------|--------|--------|
| flush 临时内存 | O(N) × 16B | O(1)，仅栈上变量 |
| 内存分配次数 | vector resize 多次 | 零次 |
| cache 友好性 | 先全量扫描再遍历（两遍） | 单遍遍历 |

## 三、并行 Flush

### 核心改动

1. `is_flushing_: atomic<bool>` → `flushing_count_: atomic<int>` 计数器
2. memtable 切换提前到 `MayTriggerFlushOrCompaction` 触发点
3. BGFlush 用 streaming `run()` 独立执行，不依赖共享 `flush_thread_pool_`

### 触发逻辑

```
MayTriggerFlushOrCompaction:
  if memtable_size >= threshold:
    if flushing_count_ < MAX_MEMTABLE_NUM - 1:
      if next_memtable is EMPTY:
        flushing_count_++
        创建新 memtable，切换 current_memtable_idx_
        Schedule BGFlush(target_memtable_idx)
```

关键点：
- memtable 切换是快速操作（微秒级），在触发点同步完成
- flush 本身是慢操作（毫秒~秒级），异步执行
- 最多 `MAX_MEMTABLE_NUM - 1 = 3` 个 flush 并行（1 个 slot 留给 active memtable）

### BGFlush(target_memtable_idx)

```
1. CheckSpaceForL0Tree，失败则 flushing_count_--
2. usleep(100) 等待 in-flight writer
3. Persist client buffered log
4. FlushJob::run() streaming flush
5. 清理：state=EMPTY, delete memtable, ClearLogGroup
6. flushing_count_--
7. malloc_trim(0)
```

### 为什么用 run() 而不是 subrunParallel()

`subrunParallel()` 内部调用 `flush_thread_pool_->WaitForJobsAndJoinAllThreads()`，这是全局 barrier。多个 BGFlush 共享同一个 pool 会互相阻塞。streaming `run()` 不依赖 thread pool，天然支持并行。

### WaitForFlushAndCompaction

```cpp
while (flushing_count_.load() > 0 || is_l0_compacting_.load())
    sleep(1);
```

## 四、malloc_trim

BGFlush 完成后调用 `malloc_trim(0)`，强制 glibc 将 Masstree 释放的内存归还 OS。

原因：Masstree 节点通过 `malloc` 分配，`delete` 后 glibc 不一定归还给 OS（保留在 free list 中）。对于 flush 这种一次性释放大量内存的场景，`malloc_trim(0)` 可以显著降低 RSS。

## 五、改动文件清单

| 文件 | 改动 |
|------|------|
| `include/db_common.h` | Index 基类新增 `ForEachEntry`/`ForEachEntryInRange` 虚函数 |
| `lib/masstree/masstree_wrapper.h` | 新增 `ScannerForEach`/`ScannerForEachRange` + `for_each()`/`for_each_range()` |
| `lib/index_masstree.h` | MasstreeIndex 实现 `ForEachEntry`/`ForEachEntryInRange` |
| `db/compaction/flush.cpp` | `run()` 和 `subrun()` 改用 streaming callback |
| `include/db.h` | `is_flushing_` → `flushing_count_`，FlushArgs 携带 target idx，BGFlush 签名 |
| `db/db.cpp` | 并行 flush 触发逻辑、BGFlush 拆分、malloc_trim |
