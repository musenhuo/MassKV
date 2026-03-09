# 04 Space Overhead

目标：

- 量化 RouteLayer、SubtreeLayer、CoW 与治理元数据的空间开销

建议统计：

- RouteLayer DRAM
- SubtreeLayer DRAM
- bytes per record
- page-set size
- CoW 额外版本开销
- bucket 元数据开销
