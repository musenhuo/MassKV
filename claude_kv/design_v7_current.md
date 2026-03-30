# MassKV 设计方向 V7（当前版本）

最后更新：2026-03-21

对应设计文档：`l1_hybrid_bptree_design_v7.md`

---

## 核心架构：双层 L1 索引

```
写路径：Put → Memtable → Flush → L0 PST → Compaction → L1 PST + L1DeltaBatch → L1HybridIndex::ApplyDeltaBatch
读路径：Get → Memtable → L0 → RouteLayer(Masstree) → SubtreeLayer(B+Tree, SSD) → PST block window → value
```

### Layer 0（路由层，内存）
- 数据结构：Masstree (`FixedRouteLayout`)
- 存储内容：`RoutePrefix → descriptor`（64位编码，三种模式）
- 内存占用：仅路由 Masstree 本身（~几十MB量级）

### Layer 1（子树层，SSD）
- 数据结构：只读 B+Tree (`L1SubtreeBPTree`)，每个 prefix 一棵
- 存储内容：`SubtreeRecord`（suffix range → kv_block_ptr + offset + count）
- 页大小：16KB，通过 `SubtreePageStore` 持久化到 SSD（复用 SegmentAllocator）

---

## Descriptor 三种模式（V7 新增）

| 模式 | 触发条件 | 存储方式 | 读路径 |
|---|---|---|---|
| `kTinyDirect` | 单窗口 prefix | inline 在 descriptor 64位中 | 无 I/O，直接解码 |
| `kNormalSubtree` | 普通 prefix | 独立 B+Tree 页集合 | 读 root→leaf，2-3次 I/O |
| `kNormalPack` | 多个小 prefix 聚合 | 共享 pack page（16KB） | 读 1 个 pack page |

迟滞阈值防止模式抖动：需连续 2 次 compaction 才能降级 TinyDirect→NormalPack。

---

## 增量更新路径（V6 引入，V7 收紧）

1. CompactionJob 输出 `L1DeltaBatch`（每个 prefix 的 `L1DeltaOp` 列表）
2. `L1HybridRebuilder::ApplyDeltaBatch` 对每个 prefix 决策：
   - **CoW patch**：变化记录少（< `cow_max_changed_records`）且叶跨度小 → 共享未变叶节点
   - **Bulk rebuild**：变化大或首次构建 → 全量重建该 prefix 的 B+Tree
3. 热 prefix（record_count > 512）使用宽松阈值（`hot_prefix_relaxed_*`）

---

## 持久化协议

- `SubtreePageStore::PersistCow`：CoW 写，复用未变页，仅写 dirty 页
- `Manifest::PersistL1HybridState`：将 hybrid index 的 page layout 序列化到 Manifest 的 64MB 区域
- `Version::ExportL1HybridState` / `ImportL1HybridState`：崩溃恢复时重建内存结构

---

## 关键参数（当前实验基准）

| 参数 | 值 |
|---|---|
| subtree page size | 16KB |
| subtree cache capacity | 256 pages |
| subtree cache max bytes | 256MB |
| leaf capacity | 128 records/leaf |
| internal fanout | 1000 |
| cow_max_changed_records | 64 |
| cow_max_leaf_spans | 4 |
| hot_prefix_record_threshold | 512 |
| force_cow_record_threshold | 1024 |

---

## 下一步方向（V8 候选）

- Route swap：layer0 叶节点在内存压力下溢出到 SSD（已实现框架，待实验归因）
- 多线程点查性能优化
- 200M+ 规模稳定性验证
- RocksDB 写性能对比
