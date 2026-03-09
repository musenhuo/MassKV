# 02 Range Query

目标：

- 验证 prefix-by-prefix 范围扫描的正确代价
- 验证并行 range scan 的收益

建议对比：

- 原始单层 L1
- 方向 B 串行 range scan
- 方向 B 并行 range scan

建议负载：

- short range
- medium range
- long range
- boundary-crossing range
