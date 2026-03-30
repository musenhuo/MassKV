# L1 Hybrid B+Tree 设计方案 V5（绝对地址直达）

## 1. 版本定位

V5 在 V4 的 SSD 驻留主线上继续收敛，核心目标是去掉 L1 子树读路径中的“页表式中间层（page refs）”，改为**绝对地址直达**。

从本版开始，推荐只维护：

- [l1_hybrid_bptree_design_v5.md](/home/zwt/yjy/FlowKV/l1_hybrid_bptree_design_v5.md)
- `FlowKV_L1_Hybrid_V5_Progress_<timestamp>.md`

## 2. 核心收敛目标

- layer0（Masstree）结构保持不变，读路径 value 收敛为：`prefix -> root_page_ptr`
- layer1（B+tree）查询路径改为：
  - 不经过 `partition_idx`
  - 不做 `partitions` 的 prefix 一致性二次校验
  - subtree 内部比较键收敛为 `suffix`
  - 根页开始逐层页读（`internal -> leaf`）
- 移除读路径中的 `page_refs/page_ref_begin/page_ref_count` 结构依赖
- manifest 不再参与点查主路径

## 3. 数据结构调整

### 3.0 Subtree 页大小约束（V5）

- 当前 V5 主线固定 subtree B+Tree 页大小为 `16KB`（leaf/internal 统一）。
- `prefix -> root_page_ptr` 读路径按 `16KB` 页粒度执行 `root->leaf` 逐页读取。
- 导入旧状态时若出现非 `16KB` subtree 句柄，会拒绝导入并走重建路径。

### 3.0.1 Subtree 键语义约束（V5）

- layer1 subtree 的逻辑输入固定为 `8B suffix`。
- internal page 的分隔键为 `RouteSuffix`，不再使用完整 `16B key`。
- leaf page 的页头 `high_key` 也为 `RouteSuffix`。
- 叶内记录仍保留 `SubtreeRecord`，用于表达 suffix 范围与叶子 payload。

### 3.1 路由发布结构（PublishedRoutePartition）

路由 value 改为最小必要字段：

- `page_size`
- `page_count`
- `root_page_ptr`（绝对地址）
- `manifest_page_ptr`（恢复/重建辅助）
- `record_count / flags`

不再保存 `page_ref` 列表。

### 3.1.1 叶子 payload 收敛（V5）

- layer1 叶子记录的 value 语义固定为“直接物理块引用”：
  - `kv_block_ptr`
  - `offset`
  - `count`
- 其中：
  - `kv_block_ptr` 指向最终 `4KB KV block` 的物理地址（块号编码）
  - `offset/count` 描述该 prefix 在块内的有效窗口
- 点查主路径不再通过 `table_idx -> level1_tables_ -> PSTMeta` 回查块地址或条目数。
- 只要 leaf record 命中，便直接以 `kv_block_ptr + offset + count` 对 PST block 做窗口二分。

### 3.2 Subtree 页内指针编码

subtree page 编码调整为：

- internal child 指针：`u64 absolute page ptr`
- leaf prev/next 指针：`u64 absolute page ptr`

仍保留逻辑 `page_id` 头字段，用于导入/校验/CoW 匹配。

### 3.3 SubtreePageStoreHandle

句柄收敛为标量元信息：

- `page_size`
- `page_count`
- `record_count`
- `root_page_ptr`
- `manifest_page_ptr`
- `flags`

不再内含 `vector<SubtreeStoredPageRef>`。

## 4. 读路径（点查）V5

1. layer0 Masstree 根据 key 前缀定位 route。  
2. route 直接得到 `root_page_ptr`（无 `partition_idx` 中间跳转）。  
3. 从 key 中提取 `suffix`，逐层读取 subtree 页并按 `suffix` 下钻。  
4. 到 leaf 后按 `suffix` 定位 record，利用 `leaf_value` 指向 KV block 做最终点查。  

当前主路径收敛为：

`prefix -> root_page_ptr -> subtree(root->leaf by suffix) -> (kv_block_ptr, offset, count) -> PST block window search`

主路径不读取 subtree manifest，不做 page_ref 映射。

## 5. 写入/发布与回收

- Persist：先分配页，再把逻辑 child/leaf 链接重写为绝对地址后落盘。
- PersistCow：按逻辑 `page_id` + page bytes 判断可复用页，复用旧绝对地址，变化页重新分配并重写指针。
- Destroy / DestroyUnshared：从 `root_page_ptr` 递归遍历收集可达页，做整树回收或差集回收（不依赖页表）。

## 6. 恢复协议（Version Snapshot）

L1 hybrid snapshot 协议升级到 `format=3`，每个 partition 持久化：

- `page_size`
- `page_count`
- `flags`
- `root_page_ptr`
- `manifest_page_ptr`
- `record_count`

旧格式不再导入（recover 失败后走重建路径）。

## 7. V5 直接收益

- 去掉 `page_refs` 大数组常驻内存
- 读路径减少一次间接映射
- 结构更接近论文语义：`prefix -> root_page_ptr`

## 8. 后续重点

1. 基于 V5 重新评估 10M 点查的内存拆分（重点看 `route_partition_bytes / subtree_bytes`）。  
2. 跟踪 `p99` 与 `avg_io_l1_pages_per_query` 的对应关系。  
3. 如果需要进一步降内存，再讨论 route 描述符压缩（不破坏 Masstree 前缀语义）。

## 9. 当前实现对齐检查（路径 + 尺寸 + 结构）

### 9.1 理想路径（目标）

`16B key -> (8B prefix, 8B suffix) -> layer0(prefix->root_page_ptr) -> layer1(root->leaf by suffix) -> leaf(value直达KV block物理地址) -> PST block window point query`

### 9.2 当前代码对齐状态

- 已对齐：
  - key 语义为 `prefix(8B) + suffix(8B)`（`FLOWKV_KEY16`）
  - layer0 路由键已收敛为 `8B prefix`（不再使用完整 `Key16` 作为 route key）
  - layer0 value 为 `root_page_ptr`（绝对地址）
  - layer1 下钻输入为 `8B suffix`
  - layer1 leaf payload 为 `kv_block_ptr + offset + count`
  - 点查主路径使用 `PointQueryWindow` 直接命中 KV block，不经过 manifest
  - subtree page size 固定 `16KB`（构造时强约束）
  - KV block 粒度固定 `4KB`（`kKvBlockShift=12`，与 `PDataBlock` 对齐）

- 控制面保留但不属于点查主路径：
  - `RoutePartition`
  - `PublishedSnapshot`
  - `manifest_page_ptr`
  - 这些结构用于恢复/发布/校验/调试，不属于核心点查数据流

### 9.3 内存口径对齐

- `L1 Index Memory (bytes)` 主口径固定为 route Masstree：
  - `l1_index_bytes_estimated = l1_route_index_estimated_bytes`
- route 索引估算按论文近似模型：
  - `32B / prefix`（`8B key + 8B value` 叠加节点负载与内部层摊销）
- `route_partition/subtree/subtree_cache` 仅作为拆分诊断列展示，不并入主索引口径
