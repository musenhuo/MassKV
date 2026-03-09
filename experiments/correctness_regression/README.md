# Correctness And Regression

该目录用于存放 L1 双层结构接入后的系统级正确性与回归实验。

当前组织按四层验证框架拆分：

- `01_end_to_end_semantics/`
- `02_structure_consistency/`
- `03_long_running_cycles/`
- `04_differential_regression/`

每一层的目标不同：

- 端到端语义回归：验证用户可见语义是否正确
- 结构一致性回归：验证 L1 双层索引内部结构不变式
- 长时间运行回归：验证多轮 flush/compaction/recovery 下系统是否漂移
- 差分回归：验证双层 L1 与基线实现是否保持语义一致

后续每一层内部可继续按：

- workload
- 数据规模
- 单线程/多线程
- recovery/非 recovery

等子维度扩展测试程序或脚本。
