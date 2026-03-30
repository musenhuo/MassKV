# Route Swap V2：Masstree Leaf-Level Cold Spill

最后更新：2026-03-21

## 一、目标

控制 route layer（Masstree）内存，在 prefix 数量极大时不无限增长。
核心约束：L1 index 内存开销必须显著小于 RocksDB（bloom filter + index block）。

## 二、旧方案问题

旧方案（codex 版本）在超阈值时全量 spill 所有 route entry 到 SSD leaf pages：

| 问题 | 影响 |
|------|------|
| 全量 spill，无粒度控制 | 阈值一触发，所有 prefix 都付出 SSD 读代价 |
| descriptor 也跟着下沉 | TinyDirect 失去零额外 SSD 读优势 |
| 读路径强制多一次 SSD 读 | 点查延迟增加 |
| 估算公式偏保守（P×64B） | 提前触发 swap |
| 无迟滞机制 | 阈值附近反复 spill/unspill |

## 三、新方案设计

### 核心思路

不再 spill 整棵 Masstree，而是在 Masstree leaf 级别做 cold swap：
- `route_descriptor_index_` 始终保留在内存（TinyDirect/NormalPack 路径不受影响）
- 只对 Masstree 的叶子节点做 cold 替换：用 24B 的 `ColdLeafStub` 替换原始 leaf node
- 读路径遇到 cold leaf 时走 SSD fallback

### 内存模型

```
热模式：route_descriptor_index_ = P × ~33B（一棵 Masstree）
冷模式：cold leaf stubs = N_cold_leaves × 24B（远小于原始 leaf nodes）
```

### 触发策略

```
route_hot_leaf_budget_bytes：Masstree leaf 层的内存预算
超预算时，按 LRU/访问频率选择 leaf 节点做 cold swap
```

### 读路径

```
热 leaf：prefix → descriptor（纯内存，不变）
冷 leaf：find_unlocked() 检测 cold bit → 返回 cold_leaf 指针
       → 从 ColdLeafStub.ssd_page_ptr 读 4KB SSD page
       → 二分查找 prefix → descriptor
```

TinyDirect 路径在 leaf 未被 spill 时完全不受影响。

## 四、实施步骤

### Step 1: nodeversion.hh — cold bit 支持

文件：`lib/masstree/nodeversion.hh`

在 Masstree 的 `nodeversion` 中新增 `cold_bit`（利用 version 字段的空闲 bit）：
- `cold()` — 检测当前节点是否为 cold stub
- `mark_cold()` — 标记节点为 cold

### Step 2: masstree_get.hh — cold 检测

文件：`lib/masstree/masstree_get.hh`

修改 `find_unlocked()` 函数，在 reach_leaf 后检测 cold bit：
- 如果 leaf 是 cold stub，提前返回并携带 cold_leaf 指针
- 调用方可以据此走 SSD fallback 路径

### Step 3: route_cold_leaf.h — ColdLeafStub 定义

新建文件：`lib/hybrid_l1/route_cold_leaf.h`

```cpp
template <typename P>
struct ColdLeafStub : public Masstree::node_base<P> {
    SubtreePagePtr ssd_page_ptr;   // 8B: SSD 上的 leaf data 位置
    uint16_t entry_count;          // 2B: SSD page 中的 entry 数量
    uint16_t padding_;             // 2B
    uint32_t magic;                // 4B: kColdLeafMagic 校验
    node_base<P>* parent_ptr;     // 8B: 父 internode
};
```

同时包含：
- `IsColdLeafStub()` — 通过 version cold bit 检测
- `AsColdStub()` — 安全类型转换
- SSD page 序列化/反序列化（`SerializeLeafToPage` / `DeserializeLeafPage` / `ScanLeafPageRange`）
- SSD page 格式：16B header + N × 16B entries（prefix 8B + descriptor 8B）

### Step 4: masstree_wrapper.h — cold-aware 查找

文件：`lib/masstree/masstree_wrapper.h`

新增方法：
- `search_cold_aware(key, value, cold_leaf)` — 点查时如果遇到 cold leaf，返回 cold_leaf 指针而非失败
- `ForEachLeaf(fn)` — 遍历所有 leaf 节点（用于 swap 时扫描哪些 leaf 需要 spill）
- `EstimateMemoryUsageBytes()` 中跳过 cold stub 节点（不计入热内存）

### Step 5: route_layout.h — 重构 swap 成员

文件：`lib/hybrid_l1/route_layout.h`

移除旧的 swap 相关字段：
- 删除 `route_leaf_directory_index_`、`route_leaf_page_metas_`、`route_leaf_page_ptrs_`
- 删除 `route_leafs_spilled_to_ssd_`

新增：
- `cold_stubs_` — `vector<node_base*>`，持有所有 ColdLeafStub 的所有权
- `cold_ssd_pages_` — `vector<SubtreePagePtr>`，持有 SSD page 的分配记录
- `route_hot_leaf_budget_bytes` — leaf 层内存预算（替代旧的 `route_hot_index_max_bytes`）

新增方法：
- `SwapColdLeaves()` — 执行 leaf-level cold swap
- `ReleaseColdResources()` — 释放 cold stubs 和 SSD pages
- `ColdStubCount()` — 返回当前 cold stub 数量
- `EstimateColdSsdBytes()` — 估算 SSD 占用

### Step 6: route_layout.cc — 核心 swap 逻辑

文件：`lib/hybrid_l1/route_layout.cc`

`SwapColdLeaves()` 实现：
1. 调用 `route_descriptor_index_->ForEachLeaf()` 遍历所有 Masstree leaf
2. 收集每个 leaf 的 entries（prefix → descriptor）
3. 将 entries 序列化为 4KB SSD page，写入 segment allocator
4. 创建 `ColdLeafStub`，设置 ssd_page_ptr 和 entry_count
5. 通过父 internode 的 `set_child()` 将原始 leaf 替换为 cold stub
6. 释放原始 leaf 节点

在 `RefreshPartitions()` 末尾，如果 `route_hot_leaf_budget_bytes > 0` 且实测内存超预算，调用 `SwapColdLeaves()`。

`FindDescriptorByKey()` 修改：
- 调用 `search_cold_aware()` 替代普通 `search()`
- 如果返回 cold_leaf，从 SSD page 读取并二分查找 descriptor

### Step 7: version.cpp — 环境变量映射

文件：`db/compaction/version.cpp`

新增环境变量 `FLOWKV_ROUTE_HOT_LEAF_BUDGET_MB`：
- 映射到 `BuildOptions.route_hot_leaf_budget_bytes`
- 默认值 0（不启用 swap）

移除旧的环境变量：
- `FLOWKV_ROUTE_HOT_INDEX_MAX_BYTES`
- `FLOWKV_ROUTE_SWAP_ALL_ON_OVERFLOW`

### Step 8: 测试与清理

文件：`tests/hybrid_l1_test.cpp`

- 更新 `TestHybridIndexRouteSwapColdStubFallback` 测试用例
- 验证 cold swap 后点查和范围扫描仍然正确
- 验证 swap 后 descriptor index 内存不增长

清理：
- 从 CMakeLists.txt 移除 hmasstree 编译和链接
- 清理 `db.cpp`、`version.cpp`、`config.h` 中的 `#ifdef USE_HMASSTREE` 条件编译
- 统一 `Masstree::relax_fence_function()` → `relax_fence_function()`
- 重命名旧 API：`route_cold_snapshot_*` → `route_cold_stub_count`

## 五、文件变更清单

| 文件 | 变更类型 |
|------|----------|
| `lib/masstree/nodeversion.hh` | 修改：新增 cold_bit / cold() / mark_cold() |
| `lib/masstree/masstree_get.hh` | 修改：find_unlocked cold 检测 |
| `lib/masstree/masstree_scan.hh` | 修改：find_next cold 检测 |
| `lib/masstree/masstree_wrapper.h` | 修改：search_cold_aware / ForEachLeaf / EstimateMemoryUsageBytes |
| `lib/hybrid_l1/route_cold_leaf.h` | 新建：ColdLeafStub + SSD codec |
| `lib/hybrid_l1/route_layout.h` | 修改：swap 成员重构 |
| `lib/hybrid_l1/route_layout.cc` | 修改：SwapColdLeaves / FindDescriptorByKey cold fallback |
| `lib/hybrid_l1/l1_hybrid_index.h` | 修改：MemoryUsageStats 字段更新 |
| `lib/hybrid_l1/l1_hybrid_index.cc` | 修改：EstimateMemoryUsage 适配新 API |
| `db/compaction/version.cpp` | 修改：环境变量映射 + 移除 hmasstree |
| `db/db.cpp` | 修改：移除 hmasstree 条件编译 |
| `include/config.h` | 修改：移除 hmasstree 文档 |
| `CMakeLists.txt` | 修改：移除 hmasstree 编译目标 |
| `tests/hybrid_l1_test.cpp` | 修改：适配新 API + 回归测试 |
| `experiments/.../point_lookup_benchmark.cpp` | 修改：metrics 字段适配 |

## 六、测试状态

`hybrid_l1_test` — 全部通过（2026-03-21）
