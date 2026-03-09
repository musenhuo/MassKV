# Layer 2: Structure Consistency

该层负责验证 L1 双层索引自身的结构不变式。

建议覆盖：

- `L1HybridIndex::Validate()`
- `ExportAll()` 顺序合法性
- partition 路由范围正确性
- 相同 `max_key` 记录不跨 partition 撕裂
- `GetLevel1Iter()` 结果合法性
- `PickOverlappedL1Tables()` 结果合法性
- subtree page set 的结构与恢复一致性

建议后续放入的实验类型：

- flush 后结构检查
- compaction 后结构检查
- 局部重建后结构检查
- recovery 后结构检查
- 边界 key 与重叠记录专项检查

当前已落地实验入口：

- `l1_structure_consistency_regression`

当前已实现覆盖：

- `L1HybridIndex::Validate()`
- local fragment 导出顺序检查
- local fragment 与真实 `TaggedPstMeta` 的裁剪一致性
- table-level `ExportAll()` 去重与排序检查
- `PickOverlappedL1Tables()` 对导出记录的可达性检查
- delete 后多轮 flush/compaction 下的结构一致性
- recovery 后的结构一致性
