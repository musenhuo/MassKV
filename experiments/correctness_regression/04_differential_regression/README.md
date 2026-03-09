# Layer 4: Differential Regression

该层负责验证当前双层 L1 与基线实现之间的语义一致性。

建议对比对象：

- 原始单层 L1 路由
- 当前双层 L1 路由
- 需要时可加入消融版本

建议覆盖：

- 点查候选选择一致性
- overlap 选择一致性
- recovery 前后一致性
- 多轮 compaction 后一致性

建议后续放入的实验类型：

- `Version` 层差分回归
- DB 端到端差分回归
- partition 边界差分回归
- recovery 后继续 compaction 的差分回归

当前已落地实验入口：

- `l1_differential_regression`

当前已实现覆盖：

- 当前双层 L1 与原始单层 L1 基线语义的点查候选选择一致性
- 当前双层 L1 与原始单层 L1 基线语义的 overlap 选择一致性
- delete 后多轮 flush/compaction 下的差分一致性
- recovery 后的差分一致性
