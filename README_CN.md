# MassKV

MassKV 是一个由 FlowKV 演进而来的实验型持久化键值存储系统。当前代码重点面向高速 SSD 或持久化内存设备，研究高吞吐写入、崩溃恢复，以及低内存占用的 L1 索引结构。

代码仍保留 FlowKV 风格的公开接口（`MYDB` / `MYDBClient`），同时加入了 hybrid L1 route/subtree 索引、持久化表元数据恢复、点查询、在线写入、正确性回归和 YCSB 风格实验入口。

## 主要特性

- 日志化写路径：`Put` 先写 WAL，再更新 memtable，后台 flush/compaction 将数据固化为持久化有序表。
- 持久化有序表（PST）：通过 segment allocator 管理固定格式 data block，并由 manifest/version 维护表元数据。
- Hybrid L1 索引：热 route 元数据保留在 DRAM，冷 subtree 数据可持久化并按需读取，降低常驻内存开销。
- 恢复支持：通过 manifest replay 和 WAL recovery 重建内存 version 视图与未固化 memtable 状态。
- 16B key/value 实验路径：`FLOWKV_KEY16` 用于当前 16 字节 key 和 16 字节 value 的实验配置。
- 实验框架：`experiments/` 下包含正确性回归、点查询、在线写入和 YCSB 风格 benchmark。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `include/` | 对外 API、运行时配置、key/value/log 格式定义 |
| `db/` | 核心引擎实现：WAL、PST、manifest/version、flush、compaction、allocator |
| `lib/masstree/` | 修改后的 Masstree，用于 memtable 和索引路径 |
| `lib/hybrid_l1/` | Hybrid L1 route/subtree 索引实现 |
| `lib/hmasstree/` | H-Masstree 相关实验代码 |
| `lib/ThreadPool/` | 后台任务使用的线程池代码 |
| `benchmarks/` | 简单 benchmark 可执行程序 |
| `tests/` | CTest 注册的单元测试和 smoke test |
| `experiments/` | 可复现实验程序、批量运行脚本和报告生成脚本 |
| `docs/` | 模块设计和维护说明 |

## 依赖

- Linux
- CMake 3.14 或更新版本
- 支持 C++17 的编译器
- gflags
- PMDK `libpmem`
- pthread
- 支持 `CMakeLists.txt` 中编译参数的 x86 CPU（`-mavx`、`-mavx2`、`-mbmi`、`-mbmi2`、`-mlzcnt`、`-mssse3`）

Ubuntu 类系统常用依赖安装方式：

```bash
sudo apt-get install cmake g++ libgflags-dev libpmem-dev
```

## 构建

```bash
git clone https://github.com/musenhuo/MassKV.git
cd MassKV
cmake -S . -B build -DKV_SEPARATION=ON -DFLOWKV_KEY16=ON
cmake --build build -j
```

默认会生成 `libflowkv.a`、`build/benchmarks/benchmark`、测试程序和已启用的实验程序。

重要构建选项：

| 选项 | 默认值 | 含义 |
| --- | --- | --- |
| `KV_SEPARATION` | `ON` | 启用 index-log memtable 与 value separation 支持 |
| `FLOWKV_KEY16` | `OFF` | 启用当前实验使用的 16 字节固定 key 路径 |

## 存储配置

`MYDBConfig` 控制持久化 pool 路径、pool 大小、恢复开关、direct I/O 开关和后台线程数。当前默认 pool 路径是 `/dev/nvme1n1`，实际运行前通常需要显式传入自己的路径。

C++ 配置示例：

```cpp
MYDBConfig cfg("/mnt/pmem/masskv.pool");
cfg.pm_pool_size = 40ul << 30;
cfg.recover = false;
cfg.use_direct_io = false;

MYDB db(cfg);
auto client = db.GetClient();
```

如果使用 DAX 持久化内存，需要先完成设备配置和挂载。如果使用 SSD 文件或裸设备做实验，请确认路径可以被覆盖，并且容量满足 `pm_pool_size` 和 benchmark 参数要求。

## 基础 Benchmark

```bash
sudo ./build/benchmarks/benchmark \
  --benchmarks=read \
  --num=100000000 \
  --num_ops=10000000 \
  --threads=16 \
  --pool_path=/mnt/pmem/masskv.pool \
  --pool_size_GB=500
```

常用参数：

| 参数 | 含义 |
| --- | --- |
| `--benchmarks=read|write` | 随机读或随机更新 benchmark |
| `--num` | 加载数据量 |
| `--num_ops` | benchmark 操作次数 |
| `--threads` | 用户线程数 |
| `--pool_path` | 持久化 pool 路径 |
| `--pool_size_GB` | 持久化 pool 大小 |
| `--recover` | 打开已有数据库并执行恢复 |
| `--skip_load` | 跳过加载阶段 |
| `--use_direct_io` | 对支持的 PST 读取使用 `O_DIRECT` |

## 测试与实验

运行已注册测试：

```bash
ctest --test-dir build --output-on-failure
```

代表性实验入口：

```bash
# 点查询 benchmark
./build/experiments/performance_evaluation/01_point_lookup/point_lookup_benchmark

# 在线写入 benchmark
./build/experiments/performance_evaluation/03_compaction_update/write_online_benchmark

# Phase 2 验收测试
./experiments/performance_evaluation/03_compaction_update/run_phase2_acceptance.sh build
```

批量运行脚本和报告生成脚本通常放在对应实验目录下。大规模结果目录、构建目录和临时分析产物已通过 Git 忽略规则排除。

## 术语对应

| 论文术语 | 代码术语 |
| --- | --- |
| Chunk | Segment |
| Logical sorted table (LST) | Persistent sorted table (PST) |
| FastStore | Memtable |
| Manifest | 内存中的 Version 加持久化存储中的 Manifest |
| BufferStore | Level 0 (L0) |
| Buffer-tree | Level 0 tree |
| StableStore | Level 1 (L1) |

## 注意

MassKV 是研究原型。运行前请仔细检查 `MYDBConfig`、benchmark 参数和 pool 路径，因为系统可能会创建或覆盖持久化数据文件或设备。
