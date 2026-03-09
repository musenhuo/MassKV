# 03 Compaction Update

目标：

- 验证 prefix-local 更新收益
- 验证 BulkLoadRebuild 与 CowPatch 的代价差异

建议对比：

- 原始单层 L1 更新路径
- 仅 BulkLoadRebuild
- rule-based CoW

建议负载：

- small affected prefix set
- medium affected prefix set
- repeated hot-prefix updates
