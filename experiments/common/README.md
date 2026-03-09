# Common Experiment Assets

该目录用于后续存放公共实验组件，例如：

- workload 生成器
- key/value 分布配置
- 结果输出 schema
- 图表输入格式
- 公共统计与计时工具

当前已落地：

- `fast_bulk_l1_builder.h/.cpp`
  - 统一的 benchmark-only 快速建库接口（自底向上批量构建 L1 PST + 一次性重建 L1 hybrid 索引）
  - 供性能实验复用，避免各 benchmark 内嵌重复建库逻辑
