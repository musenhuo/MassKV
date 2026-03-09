# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlowKV is a multi-stage key-value store designed for high performance and memory efficiency on persistent memory (Intel Optane DCPMM in AppDirect mode). It uses an LSM-tree-like multi-level architecture with persistent memory as the primary storage medium.

**Paper-to-code terminology mapping:**

| Paper Term | Code Term |
|---|---|
| Chunk | Segment |
| Logical Sorted Table (LST) | Persistent Sorted Table (PST) |
| FastStore | Memtable |
| Manifest | Version (volatile) + Manifest (persistent) |
| BufferStore | Level0 (L0) |
| Buffer-tree | Level 0 tree |
| StableStore | Level 1 (L1) |

## Build Commands

```bash
# Standard build (with KV separation)
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DKV_SEPARATION=ON
make -j

# Build without KV separation
cmake .. -DCMAKE_BUILD_TYPE=Release -DKV_SEPARATION=OFF

# Build with 16-byte keys (default is 8-byte)
cmake .. -DFLOWKV_KEY16=ON

# Build with H-Masstree index
cmake .. -DUSE_HMASSTREE=ON

# Build with H-Masstree external storage
cmake .. -DUSE_HMASSTREE=ON -DHMASSTREE_EXTERNAL_STORAGE=ON
```

Build produces: `libflowkv.a` (static library) and `benchmark` (executable).

Dependencies: `libpmem` (PMDK), `pthread`, `gflags`, `lib/ThreadPool/libthreadpool.so` (prebuilt).

## Running Benchmarks

```bash
# Basic benchmark (workload A = 50% put / 50% get)
./benchmark --pool_path=/mnt/pmem0/flowkv --pool_size_GB=16 --num=10000000 --threads=16 --workload=a

# Key flags: --num, --num_ops, --threads, --pool_path, --pool_size_GB
# --recover (recovery mode), --skip_load, --use_direct_io, --duration (for timed workloads)
# Workloads: a(mixed), b(read-heavy), c(seq-get), d(extra-put+read), e(scan), f(RMW), g(random-get), h(timed-read)
```

## Architecture

### Core Engine (`db/`)

**MYDB** (`db/db.cpp`, `include/db.h`) — singleton engine holding global resources:
- `SegmentAllocator` for PM space management
- `Version` / `Manifest` for metadata (volatile + persistent)
- Multiple memtable indexes with state machine (ACTIVE/FREEZE/EMPTY)
- Thread pools for flush and compaction
- Range partitioning (`RANGE_PARTITION_NUM=12`) for parallel flush/compaction

**MYDBClient** (`db/db_client.cpp`) — per-thread read/write handle providing Put/Get/Delete/Scan. Each client owns its own `LogWriter`, `LogReader`, `PSTReader`.

### Compile-Time Modes

The `KV_SEPARATION` CMake option fundamentally changes the data path:
- **ON**: defines `INDEX_LOG_MEMTABLE` + `KV_SEPARATE` — memtable stores log pointers, values stored separately
- **OFF**: defines `BUFFER_WAL_MEMTABLE` — values stored inline in WAL entries

### Key Types (`include/db_common.h`, `db/table.h`)

- `KeyType` = `uint64_t` (8B default) or `Key16` (16B when `FLOWKV_KEY16` defined)
- `ValuePtr` — bit-packed: valid(1) + ptr(33-34 bits, 64B-aligned) + lsn(29-30 bits)
- `Index` — abstract interface with `Get/Put/Delete/Scan` virtuals; scan must use lower_bound semantics

### Storage Layer

**PST (Persistent Sorted Table)** — minimum write granularity for sorted data:
- `PSTBuilder` (`db/pst_builder.h`) — builds PSTs from sorted entries via `DataBlockWriter`
- `PSTReader` (`db/pst_reader.h`) — point queries (binary search) and iteration over PSTs
- `PSTMeta` (`db/table.h`) — metadata with key range, sampled keys at 1/4/1/2/3/4 positions
- Default data block: 16KB (`PDataBlock16KForFixed16B`), 1024 entries for 8B keys

**WAL** — per-client sequential log:
- `LogWriter` (`db/log_writer.h`) — appends `LogEntry32`/`LogEntry64` to log segments
- `LogReader` (`db/log_reader.h`) — reads values by `ValuePtr`, bulk reads for recovery
- Log segments are 4MB (`SEGMENT_SIZE`)

### Compaction System (`db/compaction/`)

- **Version** (`version.h`) — L0 as ring buffer of trees (`MAX_L0_TREE_NUM=32`), L1 as sorted vector of `TaggedPstMeta`
- **Manifest** (`manifest.h`) — persistent metadata with crash-consistent flush log
- **FlushJob** (`flush.h`) — converts frozen memtable → L0 PSTs, supports partitioned parallel flush
- **CompactionJob** (`compaction.h`) — merges L0 trees + overlapping L1 tables, supports partitioned parallel compaction

### Allocator (`db/allocator/`)

- **SegmentAllocator** (`segment_allocator.h`) — manages PM pool file, dual bitmaps (all segments + log segments)
- **LogSegment** / **SortedSegment** (`segment.h`) — 4MB segments for WAL and data blocks respectively
- **BitMap** (`bitmap.h`) — allocation tracking with differential persistence

### Index Libraries (`lib/`)

Active:
- **Masstree** (`lib/masstree/`, `lib/index_masstree.h`) — default memtable and L1 index
- **H-Masstree** (`lib/hmasstree/`, `lib/index_hmasstree.h`) — extended Masstree with external storage, node caching, serialization

Deprecated (commented out in CMake): BwTree, HOT, TLBtree

### Key Constants

| Constant | Value |
|---|---|
| `SEGMENT_SIZE` | 4MB |
| `MAX_MEMTABLE_NUM` | 4 |
| `MAX_MEMTABLE_ENTRIES` | 500,000 |
| `MAX_L0_TREE_NUM` | 32 |
| `MAX_USER_THREAD_NUM` | 64 |
| `RANGE_PARTITION_NUM` | 12 |

## Known Issues (from TODO)

1. Compaction may error when key=0
2. Manifest takes up unexpectedly large PM space
3. Variable-length KV not supported without KV separation enabled
