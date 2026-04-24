# MassKV

MassKV is an experimental persistent key-value store derived from FlowKV. The
current codebase focuses on high-throughput writes, persistent recovery, and a
memory-efficient L1 index for read-heavy workloads on fast SSD or persistent
memory devices.

The engine keeps the public FlowKV-style API (`MYDB` and `MYDBClient`) while
adding a hybrid L1 route/subtree index, persistent table metadata recovery, and
benchmark suites for point lookup, online write performance, correctness
regression, and YCSB-style workloads.

## Features

- Log-structured write path: `Put` writes WAL records, updates memtables, and
  lets background flush/compaction move data into persistent sorted tables.
- Persistent sorted tables (PST): fixed-format sorted data blocks are managed
  through a segment allocator and tracked by manifest/version metadata.
- Hybrid L1 index: hot route metadata stays in DRAM while colder subtree data
  can be persisted and loaded on demand, reducing resident memory pressure.
- Recovery support: manifest replay and WAL recovery rebuild the in-memory
  version view and pending memtable state.
- 16-byte key/value experiment path: `FLOWKV_KEY16` enables the current
  16B-key and 16B-value benchmark configuration used by recent experiments.
- Experiment harnesses: correctness regression, point lookup, online write, and
  YCSB-style benchmarks live under `experiments/`.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `include/` | Public API, runtime config, key/value/log format definitions |
| `db/` | Core engine implementation: WAL, PST, manifest/version, flush, compaction, allocator |
| `lib/masstree/` | Modified Masstree implementation used by memtable and index paths |
| `lib/hybrid_l1/` | Hybrid L1 route/subtree index implementation |
| `lib/hmasstree/` | H-Masstree related experimental code |
| `lib/ThreadPool/` | Thread pool code used by background workers |
| `benchmarks/` | Simple benchmark executable |
| `tests/` | Unit and smoke tests registered with CTest |
| `experiments/` | Reproducible experiment programs, batch runners, and report scripts |
| `docs/` | Design and maintenance notes for internal modules |

## Dependencies

- Linux
- CMake 3.14 or newer
- C++17 compiler
- gflags
- PMDK `libpmem`
- pthread
- x86 CPU support for the flags used in `CMakeLists.txt` (`-mavx`, `-mavx2`,
  `-mbmi`, `-mbmi2`, `-mlzcnt`, `-mssse3`)

On Ubuntu-like systems, the main packages are typically:

```bash
sudo apt-get install cmake g++ libgflags-dev libpmem-dev
```

## Build

```bash
git clone https://github.com/musenhuo/MassKV.git
cd MassKV
cmake -S . -B build -DKV_SEPARATION=ON -DFLOWKV_KEY16=ON
cmake --build build -j
```

The default build creates `libflowkv.a`, `build/benchmarks/benchmark`, test
binaries, and enabled experiment binaries.

Important build options:

| Option | Default | Meaning |
| --- | --- | --- |
| `KV_SEPARATION` | `ON` | Enables index-log memtable and value separation support |
| `FLOWKV_KEY16` | `OFF` | Enables the 16-byte fixed key path used by current experiments |

## Storage Configuration

`MYDBConfig` controls the persistent pool path, pool size, recovery mode, direct
I/O mode, and background thread counts. The current default pool path is
`/dev/nvme1n1`, so most users should pass an explicit path before running
benchmarks or tests.

Example configuration in C++:

```cpp
MYDBConfig cfg("/mnt/pmem/masskv.pool");
cfg.pm_pool_size = 40ul << 30;
cfg.recover = false;
cfg.use_direct_io = false;

MYDB db(cfg);
auto client = db.GetClient();
```

For DAX persistent memory deployments, prepare and mount the device before
running MassKV. For SSD-backed experiments, choose a device/file path that is
safe to overwrite and has enough capacity for the configured pool size.

## Basic Benchmark

```bash
sudo ./build/benchmarks/benchmark \
  --benchmarks=read \
  --num=100000000 \
  --num_ops=10000000 \
  --threads=16 \
  --pool_path=/mnt/pmem/masskv.pool \
  --pool_size_GB=500
```

Useful flags include:

| Flag | Meaning |
| --- | --- |
| `--benchmarks=read|write` | Random read or random update benchmark |
| `--num` | Number of loaded records |
| `--num_ops` | Number of benchmark operations |
| `--threads` | User thread count |
| `--pool_path` | Persistent pool path |
| `--pool_size_GB` | Persistent pool size |
| `--recover` | Open an existing database and run recovery |
| `--skip_load` | Skip the loading phase |
| `--use_direct_io` | Use `O_DIRECT` for supported PST reads |

## Tests and Experiments

Run registered tests:

```bash
ctest --test-dir build --output-on-failure
```

Representative experiment entry points:

```bash
# Point lookup benchmark
./build/experiments/performance_evaluation/01_point_lookup/point_lookup_benchmark

# Online write benchmark
./build/experiments/performance_evaluation/03_compaction_update/write_online_benchmark

# Phase 2 acceptance tests
./experiments/performance_evaluation/03_compaction_update/run_phase2_acceptance.sh build
```

Batch runners and report generators are stored beside each experiment program.
Large result directories, build directories, and scratch analysis artifacts are
intentionally ignored by Git.

## Terminology

| Paper term | Code term |
| --- | --- |
| Chunk | Segment |
| Logical sorted table (LST) | Persistent sorted table (PST) |
| FastStore | Memtable |
| Manifest | Version in memory plus Manifest on persistent storage |
| BufferStore | Level 0 (L0) |
| Buffer-tree | Level 0 tree |
| StableStore | Level 1 (L1) |

## Notes

MassKV is a research prototype. Before running on a real device, review
`MYDBConfig`, benchmark flags, and the selected pool path carefully because the
engine may create or overwrite persistent data files/devices.
