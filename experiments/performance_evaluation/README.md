# Performance Evaluation

该目录用于存放方向 B 与论文主张直接对应的性能实验。

## 当前冻结的实验前提

- key size = `16B`
- value size = `16B`
- KV block size = `4KB`
- subtree leaf page size = `4KB`
- subtree internal node size = `16KB`
- key 语义固定为：
  - 前 `8B` = `prefix`
  - 后 `8B` = `suffix`

## 当前冻结的数据规模

主实验固定覆盖以下三档：

- `10^6`
- `10^7`
- `10^8`

扩展实验预留：

- `10^9`

当前不把 `10^10+` 或更高规模作为首轮硬门槛，而是待主实验程序稳定后，再单独评估可行的展示型超大规模实验。

## 当前冻结的操作数

- 点查询：每组 `1,000,000 ops`
- 范围查询：每个范围档位 `20,000 ops`
- 混合负载：每组总计 `1,000,000 ops`
- recovery：每组 `10 rounds`

## 当前冻结的 prefix 基数档位

对每个总数据规模 `N`，固定使用三档独立 prefix 数 `P`：

- 高基数：`P = 0.1N`
- 中基数：`P = 0.05N`
- 低基数：`P = 0.01N`

对应平均每个 prefix 的 key 数分别约为：

- `10`
- `20`
- `100`

## 当前冻结的数据分布

主实验必须同时覆盖：

- `uniform`
- `prefix-skew`

后续如有余量，再补：

- uniform data + skewed query workload

## 当前冻结的数据目录

实验数据统一放在独立 SSD 挂载点：

- `/mnt/nvme0`

推荐后续实际目录布局：

- `/mnt/nvme0/flowkv_exp/datasets/`
- `/mnt/nvme0/flowkv_exp/dbfiles/`
- `/mnt/nvme0/flowkv_exp/results/`

当前已知 SSD 可用空间约为 `2.2T`。

按 key/value 均为 `16B` 的原始载荷估算：

- `10^8` KV 约 `3.2GB` 原始数据
- `10^9` KV 约 `32GB` 原始数据
- `10^10` KV 约 `320GB` 原始数据
- `10^11` KV 约 `3.2TB` 原始数据

这还没有计入：

- LSM/compaction 写放大
- log / manifest / index / page set
- 多轮实验结果与临时文件

因此，当前 `2.2T` 可用空间：

- 足够覆盖 `10^6 ~ 10^9` 的主实验与扩展实验
- 在严格控盘和清理策略下，可尝试部分 `10^10` 级展示实验
- 不足以支撑 `10^11` 级完整实验，更不用说 `10^12`

当前分组：

- `01_point_lookup/`：点查询吞吐与延迟
- `02_range_query/`：范围查询与并行扫描
- `03_compaction_update/`：compaction、CoW 与更新代价
- `04_space_overhead/`：空间与内存开销
- `05_recovery_runtime/`：恢复时间与恢复后运行代价
- `06_long_running_mixed/`：长时间混合负载

约定：

- 每个子目录后续至少包含一个实验入口和一个 README
- 若需要共享输入生成器或输出 schema，统一放到 `../common/`
- 数据生成统一要求：
  - 默认使用 `experiments/common/fast_bulk_l1_builder.h` 提供的 `BuildFastBulkL1Dataset` 接口
  - 采用自底向上批量建库（benchmark-only），避免在线写入/compaction 路径带来的建数耗时噪声

## 结果文档统一规范（强制）

- 每次实验必须同时产出：
  - `results.csv`（完整字段）
  - `RESULTS.md`（可读分析版）
  - `plots/`（图表）
  - `raw/`（原始 stdout）
- `RESULTS.md` 的格式以 `01_point_lookup/results/20260308_112413/RESULTS.md` 为基准模板。
- `RESULTS.md` 必须包含“结构改动前后对比”段（若本次实验对应代码/结构变更）。
- 结论必须给出定量对比，不接受仅描述性结论。
- 内存开销指标为强制项：
  - `Raw Result Table` 必须包含 `RSS (bytes)` 与 `L1 Index Memory (bytes)`。
  - 必须提供 `Memory Overhead Table`（至少拆分 route/subtree/cache/governance 五类开销）。
