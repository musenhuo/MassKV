# Layer 3: Long-Running Cycles

该层负责验证长时间运行与多轮生命周期下的正确性。

目标不是单次操作正确，而是确认系统在重复执行后不漂移。

建议覆盖：

- 多轮写入
- 多轮 flush
- 多轮 compaction
- 周期性 recovery
- 发布替换与延迟回收的长期稳定性

建议后续放入的实验类型：

- fixed-round cycle test
- random operation cycle test
- write-heavy mixed cycle test
- recovery-every-N-rounds test
- long-horizon partition-local rebuild test

当前已落地实验入口：

- `l1_long_running_cycle_regression`

当前已实现覆盖：

- 多轮随机 `Put/Delete/Get`
- 每轮 `BGFlush -> BGCompaction`
- 每轮 shadow model 对拍
- 每轮 L1 结构一致性检查
- 每隔固定 round 的 `recover=true` 重启验证
- 多轮 delete、局部重建、recovery 交替后的长期稳定性
