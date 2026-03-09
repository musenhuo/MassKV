# FlowKV 一次读流程详解（2026-03-08，当前实现口径）

## 1. 适用范围与前提

- 本文描述的是当前代码中的 `Get` 路径（`FLOWKV_KEY16`，`key=16B`，`value=16B`）。
- 当前 `PDataBlock` 粒度为 `4KB`（每块最多 `128` 条 KV）。
- L1 双层索引已采用「root 直达」：
  - `RouteLayer`（Masstree）在内存；
  - `Subtree B+Tree` 在磁盘，查询时按页读取；
  - 优先使用 `SubtreePageStoreHandle` 内的 root 元数据，不再固定读取 subtree manifest 页。

---

## 2. 参与组件与核心数据结构

### 2.1 调用链组件

1. `MYDBClient::Get`：统一读入口（先 memtable，后 Version）。
2. `Version::Get`：LSM 读主流程（L0 -> L1）。
3. `L1HybridIndex`：L1 路由+子树检索。
4. `PSTReader` / `DataBlockReader`：最终 KV block 读取与块内二分。

### 2.2 关键数据结构

1. `mem_index_[MAX_MEMTABLE_NUM]`：内存 memtable 索引。
2. `level0_trees_ + level0_table_lists_`：L0 内存索引 + 表元数据列表。
3. `level1_tree_ (L1HybridIndex)`：L1 双层索引入口。
4. `RoutePartition`：
   - `prefix / route_key / generation / record_count`
   - `subtree_store`（磁盘句柄，含 `root_page_id/page_count/record_count`）
5. `SubtreeRecord`：
   - `table_idx`（指向 `level1_tables_`）
   - `leaf_value`（44/12/8：block ptr / offset / count）
6. `TaggedPstMeta / PSTMeta`：
   - `datablock_ptr_`（4KB KV block 物理地址）
   - `entry_num_`（块内有效条目数）

---

## 3. 一次点查询完整步骤（命中场景）

以下以 `MYDBClient::Get(key)` 为起点：

### Step 0: 入口与统计

1. `MYDBClient::Get` 增加读计数。
2. 调用 `GetFromMemtable`。

### Step 1: MemTable 查询（纯内存）

1. 按从新到旧遍历 memtable 槽位。
2. 在 `mem_index_[id]->Get(int_key)` 查 key。
3. 若命中：
   - 若 tombstone -> 直接返回 not found；
   - 否则从 log buffer/log segment 读 value 到输出缓冲（这里不是 PST 路径）。
4. 若全部 miss -> 进入 `Version::Get`。

> 该阶段不走 L1 双层结构。

### Step 2: L0 查询（内存索引 + PST 块读）

1. `Version::Get` 遍历活跃 L0 tree。
2. 对每棵树执行 `FindTableByIndex`（内存索引 scan）。
3. 若拿到候选 `TaggedPstMeta`，做最小 key 边界过滤。
4. 调 `pst_reader->PointQuery(datablock_ptr_, key, ..., entry_num_)`：
   - `DataBlockReader::ReadBuf` 读取 `4KB` KV block；
   - 块内二分查找 key。
5. 找到且非 tombstone -> 返回成功；否则继续 L0 / L1。

### Step 3: L1 路由层定位（内存 Masstree）

1. `Version::Get` 调 `FindTableBySubtreeIndex` -> `L1HybridIndex::LookupCandidate`。
2. `FixedRouteLayout::FindPartitionByKey`：
   - 从 key 提取 prefix；
   - 在 route Masstree 查 `prefix -> partition_idx`。
3. 得到目标 `RoutePartition`。

### Step 4: L1 子树页级检索（磁盘 B+Tree）

1. `LookupCandidatesFromDisk(partition_idx, key, limit=1)`。
2. 解析查询元数据：
   - 若 `subtree_store.HasQueryMeta()`：直接拿 `root_page_id/page_count/record_count`；
   - 否则（legacy 快照）回退读一次 subtree manifest 页。
3. 从 `root_page_id` 开始逐层读取页面：
   - 每层 `LoadPageById` 一次；
   - internal 页：按 `child_high_keys` 选择下层 child；
   - leaf 页：`lower_bound` 后顺序检查 `SubtreeRecord`。
4. 找到命中 `SubtreeRecord`（含 `table_idx`、`leaf_value`）后返回。

### Step 5: 由 SubtreeRecord 落到 PST 数据块

1. `table_idx -> level1_tables_[idx]` 取 `PSTMeta`。
2. 先做 table 有效性与 key range 检查。
3. 若 `leaf_value` 可用，尝试“窗口化读取”：
   - 由 `kv_block_ptr/offset/count` 计算 hinted 位置；
   - `pst_reader->PointQuery(hinted_off, ..., hinted_entries)`。
4. 若窗口化 miss，则回退整块读：
   - `pst_reader->PointQuery(datablock_ptr_, ..., entry_num_)`。
5. 命中后若值为 tombstone -> 返回 not found；否则返回 found。

---

## 4. 读路径中的 I/O 位置与次数

## 4.1 可能发生 I/O 的位置

1. L0/L1 的 `PSTReader::PointQuery`（读 4KB KV block）。
2. L1 子树页读取 `SubtreePageStore::LoadPageById`（读 subtree page，通常 16KB）。
3. legacy 回退时的 `LoadManifestPage`（读取 subtree manifest 页）。

## 4.2 单次查询 I/O 计数模型

记：

- `R_l0`：L0 阶段实际触发的 PST 块读次数。
- `R_l1_idx`：L1 子树索引页读次数（root/internal/leaf/next-leaf）。
- `R_l1_pst`：L1 最终 PST 块读次数（通常 1；窗口化失败可能先尝试一次再回退一次）。

则一次查询总 I/O 近似：

`R_total = R_l0 + R_l1_idx + R_l1_pst`

### 常见量级（当前实现）

1. L0 未命中、L1 子树高度为 1（root 即 leaf）：
   - `R_l1_idx ≈ 1`
   - `R_l1_pst ≈ 1`
   - 总计约 `2` 次读 I/O。
2. L0 未命中、L1 子树高度为 2（root internal + leaf）：
   - `R_l1_idx ≈ 2`
   - `R_l1_pst ≈ 1`
   - 总计约 `3` 次读 I/O。
3. legacy 快照（无 root 元数据）：
   - 额外 `+1` 次 manifest 页读取。

---

## 5. “root 直达”改造后相对旧路径的变化

旧路径（每次固定）：

1. 读 subtree manifest 页；
2. 再 root->leaf。

新路径（当前）：

1. 直接用 `subtree_store` 内 root 元数据；
2. 直接 root->leaf；
3. 仅 legacy 快照回退读 manifest。

效果：点查路径中 L1 索引页 I/O 理论减少 `1` 次（新快照数据）。

---

## 6. 当前实现中的关键边界情况

1. `leaf_value` 窗口提示路径在 `use_direct_io=1` 下，如果 `hinted_off` 不是 4KB 对齐，`pread(O_DIRECT)` 可能失败，随后会走整块回退读取。
2. 因此在某些 workload 下，`R_l1_pst` 可能出现“先失败一次再成功一次”的额外成本。
3. 查询正确性仍依赖回退路径保障（窗口化 miss/失败不影响最终正确性）。

---

## 7. 参与者总结（一次读）

1. API 层：`MYDBClient`
2. 版本与层级决策：`Version`
3. L0 内存索引：`level0_trees_`
4. L1 路由层（内存）：`FixedRouteLayout.route_index_`（Masstree）
5. L1 子树层（磁盘页）：`SubtreePageStore + SubtreePageCodec`
6. KV 块读取：`PSTReader + DataBlockReader`
7. 元数据映射：`SubtreeRecord -> table_idx -> level1_tables_[idx] -> PSTMeta`

---

## 8. 一句话总结

当前一次 `Get` 的主路径是：

`MemTable(内存) -> L0(内存索引 + 4KB块读) -> L1 Route(Masstree内存) -> L1 Subtree(root直达页读) -> PST 4KB块读 -> 块内二分返回值`。
