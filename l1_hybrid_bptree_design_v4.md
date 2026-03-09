# L1 Hybrid B+Tree 设计方案 V4（磁盘驻留查询）

## 1. 文档定位

V4 是在 V3 基础上的方向切换版本：  
系统目标从“内存驻留 subtree 查询”切换为“磁盘驻留 subtree 查询”。

从本版开始，后续开发与实验基线只看：

- [l1_hybrid_bptree_design_v4.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v4.md)
- `FlowKV_L1_Hybrid_V4_Progress_<timestamp>.md`

## 2. V4 核心目标

- `RouteLayer` 继续内存驻留（prefix -> partition），保持小体量路由。
- `SubtreeLayer` 改成磁盘驻留，查询时按需加载 page set。
- 去掉 `RoutePartition` 对常驻 `L1SubtreeBPTree` 的强依赖。
- 保留 `B2` 的 prefix-local fragment 语义。

## 3. 数据结构调整

### 3.1 RoutePartition

`RoutePartition` 从：

- `std::shared_ptr<const L1SubtreeBPTree> subtree`

切换为：

- `SubtreePageStoreHandle subtree_store`（磁盘句柄）
- `std::shared_ptr<const SubtreePageSet> subtree_pages_cache`（仅回退路径）

其中：

- `subtree_store` 是 V4 的正式路径
- `subtree_pages_cache` 仅用于 allocator 未接入时的 debug/unit-test 路径

### 3.2 查询路径

`L1HybridIndex` 查询不再直接访问常驻 subtree 指针，而是：

1. 通过 route 找到 partition
2. 先查 `L1HybridIndex` 内部 subtree page cache（LRU）
3. cache miss 时根据 `subtree_store` 从 `SubtreePageStore` 加载并 `ImportPageSet`
4. 在还原树上执行点查/范围查，并按容量/字节上限回收 cache

当前收紧（2026-03-06 更新）：

- 点查主路径已改为页级读取：
  - 优先使用 `subtree_store` 句柄内的 root 元数据直达根页
  - 再按 `root -> internal -> leaf` 逐页读取
  - 叶内使用 `RecordRouteKeyLess` 做 lower_bound 后顺序匹配
  - legacy 快照（无 root 元数据）回退一次 manifest 读取
- `RangeScan` 已切到页级读取：
  - 优先使用句柄 root 元数据定位根页
  - 再定位首个覆盖 `start` 的 leaf
  - 再沿 leaf 链逐页扫描到 `end`
  - legacy 快照（无 root 元数据）回退一次 manifest 读取
- 仅在页级解码失败时才回退到整树加载路径（容错回退）。

## 4. 构建与更新路径

`Rebuilder` 在构建 partition 时：

1. 先构建临时 subtree
2. `ExportPageSet`
3. `SubtreePageStore::Persist` 持久化
4. 仅保存 `subtree_store` 句柄

当前策略：

- allocator 已接入时采用严格磁盘驻留：`Persist` 失败直接抛错，不再静默回退
- allocator 未接入时允许 `subtree_pages_cache`（debug/unit-test）

## 5. CoW 与更新策略（V4 当前口径）

V4 已接入页级 CoW 发布路径：

- `ApplyCowUpdate` 会加载旧 subtree page set，生成目标 page set。
- `SubtreePageStore::PersistCow` 复用“页内容未变”的旧页引用，只写入变更页。
- 发布后通过 `DestroyUnshared(old, new)` 回收旧版本独占页，保留共享页。
- 已新增 CoW 可观测统计：
  - `cow_persist_calls`
  - `cow_reused_pages / cow_written_pages`
  - `cow_reused_bytes / cow_written_bytes`

当前限制：

- 仍属于“构树后页面级 diff/复用”路径，不是增量原地结构修改。
- manifest 双槽提交将 L1 snapshot 可用空间收紧为单槽 `32MB`（`64MB/2`）。

## 6. 当前实现边界（必须明确）

V4 当前已经落地：

- partition 磁盘句柄化
- 查询按需加载 subtree page set
- 点查已切到 root->leaf 逐页 I/O（不再整树加载）
- `L1HybridIndex` 可控 page cache（LRU，按 `prefix+generation` 键）
- `Version` 已传入 `SegmentAllocator` 给 `L1HybridIndex`
- `SubtreePageStoreHandle` 已支持跨 segment 页位置引用（`segment_id + page_id` 列表）
- `SubtreeStoredPageRef` 已压缩为 8B（`uint32 segment_id + uint32 page_id`）
- 页级 CoW 存储发布（`PersistCow + DestroyUnshared`）
- manifest L1 hybrid 句柄快照协议（持久化/恢复）
- manifest 快照 envelope 校验协议（`magic/version/full-seq/checksum`）
- manifest root 原子提交协议（双槽切换，inactive slot 写入后再切 active slot）
- `RangeScan` 页级化（`root -> leaf` 路径 + leaf 链扫描）

V4 当前仍未完成：

- 针对 snapshot 失配场景的更细粒度校验/诊断信息

## 7. 实验口径变化

因为查询已切到“按需加载 subtree”路径：

- 点查询延迟应从微秒级内存路径，转向更接近 I/O 级延迟
- `subtree_bytes`（内存常驻）不再作为主开销项
- 需要新增/强调 `subtree_disk_bytes` 或 page-run 占用统计
- 大规模建数阶段需要关注 SSD allocator 位图落盘写放大，建议启用批量位图持久化参数：
  - `FLOWKV_BITMAP_PERSIST_EVERY`（benchmark 参数 `bitmap_persist_every`）
- 已新增 subtree cache 可观测指标：
  - `subtree_cache_requests`
  - `subtree_cache_hits`
  - `subtree_cache_misses`
  - `subtree_cache_hit_rate`
- 点查询 benchmark 已支持 SSD 口径参数化：
  - `build_mode`（`online` / `fast_bulk_l1`）
  - `use_direct_io`
  - `warmup_queries`
  - `enable_subtree_cache/subtree_cache_capacity/subtree_cache_max_bytes`
  - `bitmap_persist_every`

## 8. 下一步优先级

1. 开始重新跑 `01_point_lookup`，区分 warm-cache 与 cold-I/O。
2. 引入 cache 命中率统计，并纳入性能实验报告。
3. 做 CoW 命中率与写放大统计（复用页比例、新增页比例）。

## 9. 最新落地（2026-03-06）

- `SubtreePageStore` 已去掉 contiguous page run 依赖：
  - 由单段句柄改为 `vector<{segment_id, page_id}>` 页位置集合。
  - `Persist/Load/Destroy` 已切到离散页协议。
- 新句柄当前作用域：
  - 支持单个 segment 内非连续页。
  - 已支持跨 segment 页位置引用。
- `L1HybridIndex` 已新增 subtree page cache：
  - 线程安全 LRU。
  - 容量与字节上限可配置。
  - 在 `Clear/BulkLoad/Rebuild` 时主动失效，避免旧快照残留。
- 已接入 cache 命中率统计：
  - 在 `EstimateMemoryUsage` 导出 `requests/hits/misses`。
  - 已打通到 `point_lookup_benchmark` 输出。
- 已接入 CoW 复用统计：
  - 在 `EstimateMemoryUsage` 导出 `cow_persist_calls/reused/written`。
  - 已打通到 `point_lookup_benchmark` 输出。
- `subtree_pages_cache` 已收紧为 debug-only：
  - 仅 `segment_allocator == nullptr` 时允许走该路径。
  - allocator 已接入时若 `Persist` 失败会直接报错。
- 页级 CoW 与回收已落地：
  - `PersistCow` 复用未变化页，写入变化页。
  - `DestroyUnshared` 只回收旧版本独占页。
- manifest 句柄协议已落地：
  - compaction 结束后持久化 L1 hybrid state snapshot。
  - recover 读取 snapshot 并导入；失败时自动回退到重建树。
- manifest 句柄协议已增强：
  - snapshot 数据区增加 envelope（`magic/version/seq/payload_len/checksum`）。
  - recover 时会校验 seq 与 checksum，防止误恢复损坏快照。
- `RangeScan` 已完成页级查询路径：
  - 每个 partition 优先走句柄 root 元数据直达的磁盘页级扫描，按 `start` 定位首 leaf，再沿 leaf 链扫描。
  - legacy 快照（无 root 元数据）回退一次 manifest 读取。
  - 仅在页解码失败时回退旧整树加载路径。
- manifest 已完成 root 原子提交协议：
  - L1 hybrid snapshot 区改为双槽（slot0/slot1）。
  - 每次提交写入 inactive slot，成功后只更新 super 中 active slot 元数据。
- SSD allocator 建数路径已新增位图批量持久化：
  - `SegmentAllocator` 新增 `FLOWKV_BITMAP_PERSIST_EVERY`（默认 `1` 保持原语义）。
  - `AllocLogSegment/AllocSortedSegment/FreeSegment` 从“每次操作立即持久化”改为“按阈值批量持久化”。
  - 退出与析构时强制 `PersistBitmapsNow()`，保证最终落盘。
- point benchmark 已新增 benchmark-only 快速建库：
  - `build_mode=fast_bulk_l1` 跳过在线 `Put -> Flush -> Compaction`。
  - 直接批量构建最终 L1 PST，并一次性重建 L1 hybrid 索引。
  - 目标是将实验时间集中在读路径测量，而不是在线建库过程。

## 10. 最新收敛（2026-03-08，PST 4KB 与点查简化）

- `PDataBlock` 默认粒度已切换为 `4KB`（`FLOWKV_KEY16` 下每块 `128` 条 KV）。
- PST 分段采样键（`key_256/key_512/key_768`）已从元数据与查询路径移除。
- 点查路径已收敛为单块二分：
  - 使用 `datablock_ptr_` 直接读取一个 `4KB` KV block。
  - 在块内按 `entry_num_` 上界执行二分查找。
- `DataBlockReader` 点查缓冲改为 `sizeof(PDataBlock)`，与块大小一致。
- 当前口径下，“不读额外分段元数据”已满足；点查仅依赖：
  - 路由选中的 `PSTMeta.datablock_ptr_`
  - `entry_num_`（用于最后未填满块的二分上界）
  - （可选）`min/max` 作为早筛条件

## 11. 最新收敛（2026-03-08，Leaf Value 位段化）

- L1 子树叶记录新增 `leaf_value`（64-bit）位段编码，布局为：
  - 高 `44` 位：`kv_block_ptr`（4KB block id）
  - 中 `12` 位：`offset`（该 prefix 在 block 内的起始 entry）
  - 低 `8` 位：`count`（该 prefix 在 block 内的有效 entry 数）
- `SubtreeRecord` 已提供统一 `Pack/Unpack` 与 `SetLeafWindow` 接口。
- `Rebuilder` 在构建 partition 时会扫描对应 KV block，按 prefix 回填精确 `offset/count`。
- subtree page 编码版本已提升到 `v2`（避免旧版本页按新记录布局误解析）。
- `Version::Get` 在 L1 路径上优先使用 `leaf_value` 窗口做局部点查，miss 时回退整块点查。
- 当前兼容策略：
  - `table_idx` 仍保留（用于现有 compaction/调试/一致性路径）。
  - `leaf_value` 作为新的查询优化字段并已持久化到 subtree page 编码中。

## 12. 最新收敛（2026-03-09，内存压缩）

- 路由分区结构已压缩：
  - 移除冗余 `route_key`（可由 `prefix` 推导）
  - `record_count` 收紧为 `uint32_t`
  - `subtree_pages_cache` 从内嵌对象改为按需指针（仅 debug fallback 分配）
- 路由容器内存已收紧：
  - `BulkLoad/BulkLoadFromTables/Rebuild/Import/Clear` 后对 `partitions_` 执行 `shrink_to_fit`
- subtree 页引用已压缩：
  - `SubtreeStoredPageRef` 从 16B 降为 8B（`segment_id` 使用 `uint32_t`）
  - 快照导入增加 `segment_id` 越界检查（超出 `uint32` 直接拒绝导入）
- 结构尺寸变化（本地编译测得）：
  - `sizeof(RoutePartition): 144 -> 88`
  - `sizeof(SubtreeStoredPageRef): 16 -> 8`
- 快速 sanity（`1M keys / 100k prefixes`）：
  - `l1_index_bytes_estimated=15,552,480`（此前同量级约 `26,726,656`）
