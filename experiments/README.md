# Experiments

该目录用于存放 FlowKV 后续论文与系统验证相关的实验组织。

当前目录约定：

- `correctness_regression/`：大规模数据正确性与回归验证
- `performance_evaluation/`：论文主实验，覆盖点查、范围查、更新、空间、恢复与混合负载
- `ablation_studies/`：论文消融实验，验证各创新设计点的必要性
- `common/`：实验公共组件、结果格式、工作负载生成与复用工具

当前原则：

- `tests/` 保留为已有单元测试、功能测试、smoke test
- `experiments/` 保留为阶段化、大规模、可重复的实验组织与脚本/程序入口

当前推荐的论文实验顺序：

- 先完成 `correctness_regression/`
- 再完成 `performance_evaluation/01_point_lookup`
- 再完成 `performance_evaluation/02_range_query`
- 再完成 `performance_evaluation/03_compaction_update`
- 然后补 `04_space_overhead`、`05_recovery_runtime`、`06_long_running_mixed`
- 最后完成 `ablation_studies/`
