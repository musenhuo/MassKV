# FlowKV
A multi-stage key-value store for high performance and memory efficiency on persistent memory

## Directory contents
* `include/db.h`: FlowKV interface
* `include/config.h`: static (macro defination) and dynamic (parameters) configurations.
* `db/`: implementation of FlowKV
* `db/allocator/`: the coarse-grained PM allocator
* `db/blocks/`: structures of index block and data block
* `db/compaction/`: implementation of flush and compaction
* `lib/`: code we modified from external libraries, mainly including [Masstree](https://github.com/kohler/masstree-beta) and [RocksDB](https://github.com/facebook/rocksdb) thread pool.
* `util/`: some utilities

include/db.h: FlowKV 接口定义

include/config.h: 静态（宏定义）和动态（参数）配置

db/: FlowKV 的具体实现代码

db/allocator/: 粗粒度的 PM（持久内存）分配器

db/blocks/: 索引块和数据块的结构定义

db/compaction/: Flush（刷盘）和 Compaction（合并/压实）的实现

lib/: 我们修改自外部库的代码，主要包括 Masstree 和 RocksDB 的线程池

util/: 一些通用工具/组件

## Terminology Correspondence between our paper and codes
| In the paper               | In the code                                |
| -------------------------- | ------------------------------------------ |
| Chunk                      | Segment                                    |
| Logical sorted table (LST) | Persistent sorted table (PST)              |
| FastStore                  | Memtable                                   |
| Manifest                   | Version (volatile) + Manifest (persistent) |
| BufferStore                | Level 0 (L0)                               |
| Buffer-tree                | Level 0 tree                               |
| StableStore                | Level 1 (L1)                               |

论文中的术语,代码中的术语
Chunk（块）,Segment（段）
Logical sorted table (LST),Persistent sorted table (PST)
FastStore,Memtable
Manifest,Version (易失性内存中) + Manifest (持久化存储中)
BufferStore,Level 0 (L0)
Buffer-tree,Level 0 tree
StableStore,Level 1 (L1)

## Building

Dependencies:
- CMake
- [gflags](https://github.com/gflags/gflags) 
- [PMDK](https://github.com/pmem/pmdk) (libpmem) 

FlowKV is based on persistent memory. So a configured PM path is necessary. 
If you have a configured PM path, skip this step.
```shell
# set Optane DCPMM to AppDirect mode
$ sudo ipmctl create -f -goal persistentmemorytype=appdirect

# configure PM device to fsdax mode
$ sudo ndctl create-namespace -m fsdax

# create and mount a file system with DAX
$ sudo mkfs.ext4 -f /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem
```

Build FlowKV with CMake:
```shell
$ cmake -B build -DKV_SEPARATION=ON/OFF # enable/disable key-value separation for supporting variable-sized value
$ cmake --build build -j${n_proc}
```

A static library (./build/libflowkv.a) and a benchmarking tool (./build/benchmarks/benchmark) will be generated.

## Running benchmark

```
sudo ./build/benchmarks/benchmark
```
Parameters:
```
-benchmarks (write: random update, read: random get) type: string
      default: "read"
-num (total number of data) type: uint64 default: 200000000
-num_ops (number of operations for each benchmark) type: uint64
      default: 100000000
-pool_path (directory of target pmem) type: string
      default: "/mnt/pmem/flowkv"
-pool_size_GB (total size of pmem pool) type: uint64 default: 40
-recover (recover an existing db instead of recreating a new one)
      type: bool default: false
-skip_load (skip the load data step) type: bool default: false
-threads (number of user threads during loading and benchmarking)
      type: uint64 default: 1
-value_size (value size, only available with KV separation enabled) 
      type: uint64 default: 8
```

-benchmarks (类型: string, 默认: "read")
      指定测试类型 (write: 随机更新, read: 随机读取)
-num (类型: uint64, 默认: 200000000)
      数据总量
-num_ops (类型: uint64, 默认: 100000000)
      每个基准测试的操作次数
-pool_path (类型: string, 默认: "/mnt/pmem/flowkv")
      目标 pmem 的目录路径
-pool_size_GB (类型: uint64, 默认: 40)
      pmem 内存池的总大小 (GB)
-recover (类型: bool, 默认: false)
      恢复现有的数据库，而不是重新创建一个新的
-skip_load (类型: bool, 默认: false)
      跳过数据加载 (Load) 步骤
-threads (类型: uint64, 默认: 1)
      加载和基准测试期间的用户线程数
-value_size (类型: uint64, 默认: 8)
      Value 的大小 (仅在启用 KV 分离时有效)

## For comparisons with baselines
We did macro-benchmarks and comparison experiments with [PKBench](https://github.com/luziyi23/PKBench) which is our modified version of [PiBench](https://github.com/sfu-dis/pibench) for PM-based key-value stores. Please see PKBench repository for more in-depth benchmarking.
