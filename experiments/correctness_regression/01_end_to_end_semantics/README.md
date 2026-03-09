# Layer 1: End-To-End Semantics

该层负责验证用户可见语义。

建议覆盖：

- `Put/Get/Delete`
- memtable 命中
- flush 后从 L0 读取
- compaction 后从 L1 读取
- recovery 后继续读取
- recovery 后继续写入与 compaction

建议测试方法：

- 使用 shadow model 对拍
- 以 key 为单位验证最终值语义
- 在每轮 flush/compaction/recovery 后抽样或全量校验

建议后续放入的实验类型：

- 基础随机写读
- 覆盖写
- 删除语义
- 顺序写加随机读
- prefix 偏斜负载下的语义正确性

当前已落地实验入口：

- `correctness_e2e_semantics_stress`

当前已实现覆盖：

- 固定随机种子的端到端随机 workload
- shadow model 对拍
- `Put/Get`
- `Delete`
- 覆盖写
- 周期性 `BGFlush`
- 周期性 `BGCompaction`
- 重启 recovery 后继续写入与 compaction

当前稳定实验边界：

- 当前稳定路径已覆盖 `Put/Get/Delete/覆盖写/recovery`
- `Delete` 已纳入第一层随机 stress 入口
