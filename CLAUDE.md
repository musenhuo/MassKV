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

Note: `hybrid_l1` is always compiled and linked into `flowkv` unconditionally (no CMake option gates it).

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
- `PSTReader` (`db/pst_reader.h`) — point queries (`PointQuery`), window queries (`PointQueryWindow`), and iteration; `RowIterator` is a multi-PST iterator used during compaction/merging
- `PSTMeta` (`db/table.h`) — metadata with key range, sampled keys at 1/4/1/2/3/4 positions
- Default data block: 16KB (`PDataBlock16KForFixed16B`), 1024 entries for 8B keys

**WAL** — per-client sequential log:
- `LogWriter` (`db/log_writer.h`) — appends `LogEntry32`/`LogEntry64` to log segments
- `LogReader` (`db/log_reader.h`) — reads values by `ValuePtr`, bulk reads for recovery
- Log segments are 4MB (`SEGMENT_SIZE`)

### Compaction System (`db/compaction/`)

- **Version** (`version.h`) — volatile metadata layer:
  - L0: ring buffer of up to 32 trees (`MAX_L0_TREE_NUM=32`) with head/tail pointers
  - L1: sorted vector of `TaggedPstMeta` + `L1HybridIndex` for hybrid indexing
  - `ExportL1HybridState` / `ImportL1HybridState` — serializes hybrid index state for persistence
  - `ApplyDeltaBatch` — applies compaction-produced `L1DeltaBatch` to the hybrid index
  - `PickLevel0Trees` / `PickOverlappedL1Tables` — compaction input selection
- **Manifest** (`manifest.h`) — crash-consistent persistent metadata:
  - File regions: L0 metadata (204.8MB), L1 metadata (512MB), flush log, L1 hybrid state (64MB), transaction log (256MB)
  - `PersistL1HybridState` / `LoadL1HybridState` — persists hybrid index page layout to disk
  - `BeginBatchUpdate` / `CommitBatchUpdate` / `AbortBatchUpdate` — atomic durable commit protocol
  - `ManifestSuperMeta` — 4KB header with sequence numbers, tail pointers, hybrid state metadata
- **FlushJob** (`flush.h`) — converts frozen memtable → L0 PSTs, supports partitioned parallel flush
- **CompactionJob** (`compaction.h`) — merges L0 trees + overlapping L1 tables; produces `L1DeltaBatch` encoding touched prefix windows for incremental hybrid index update; supports partitioned parallel compaction

### Allocator (`db/allocator/`)

- **SegmentAllocator** (`segment_allocator.h`) — manages PM pool file, dual bitmaps (all segments + log segments)
- **LogSegment** / **SortedSegment** (`segment.h`) — 4MB segments for WAL and data blocks respectively
- **BitMap** (`bitmap.h`) — allocation tracking with differential persistence

### Index Libraries (`lib/`)

Active:
- **Masstree** (`lib/masstree/`, `lib/index_masstree.h`) — default memtable and L1 index
- **H-Masstree** (`lib/hmasstree/`, `lib/index_hmasstree.h`) — extended Masstree with external storage, node caching, serialization
- **Hybrid L1** (`lib/hybrid_l1/`) — two-layer L1 index: route layer (Masstree, memory) + per-partition B+Tree subtrees persisted to SSD

Deprecated (commented out in CMake): BwTree, HOT, TLBtree

### Hybrid L1 Index (`lib/hybrid_l1/`)

A two-layer L1 index designed for SSD-resident sorted data with low memory overhead. The route layer stays in memory (Masstree); per-partition B+Trees live on SSD and are loaded on demand.

**Key splitting** (`prefix_suffix.h`):
- 8B key mode: `RoutePrefix` always 0 (single routing domain), `RouteSuffix = bswap64(key)` for lexicographic order
- 16B key mode (`FLOWKV_KEY16`): `RoutePrefix = key.hi`, `RouteSuffix = key.lo`

**Core data structures:**
- `SubtreeRecord` (`subtree_record.h`) — leaf record in the B+Tree; stores `min_key`/`max_key`, `route_prefix`/`route_min_suffix`/`route_max_suffix`, and a packed 64-bit `leaf_value`: `[44-bit kv_block_ptr | 12-bit offset | 8-bit count]` pointing directly into a PST data block window
- `RoutePartition` (`route_partition.h`) — per-partition state: generation, record count, descriptor mode, `SubtreePageStoreHandle`, `BucketGovernanceState`
- `FixedRouteLayout` (`route_layout.h`) — top-level Masstree mapping `RoutePrefix` → partition root page pointer; also maintains a descriptor index
- `L1SubtreeBPTree` (`subtree_bptree.h`) — read-only bulk-loaded B+Tree per partition; leaf capacity=128, internal fanout=1000; supports `LowerBound`, `LookupCandidate`, `RangeScan`; COW update via `BulkLoadCow` (shares unchanged leaf nodes)
- `SubtreePageStore` (`subtree_page_store.h`) — persists/loads B+Tree page sets to/from SSD via `SegmentAllocator`; `PersistCow` reuses unchanged pages; page magic=`0x5042544C` ("LTBP")

**Descriptor modes** (`route_descriptor.h`, `RouteDescriptorMode`):
- `kTinyDirect` (type=0) — single-window partition encoded inline in the 64-bit descriptor; no page I/O on lookup
- `kNormalSubtree` (type=1) — standard per-partition B+Tree stored on SSD; descriptor encodes root page pointer
- `kNormalPack` (type=2) — multiple small partitions packed into one shared 16KB page (`NormalPackPage` in `normal_pack.h`); descriptor encodes pack page pointer + slot id; reduces write amplification for high-cardinality prefix workloads

**Update path** (`l1_hybrid_rebuilder.h`, `L1HybridRebuilder`):
- `BulkLoad` — full rebuild from sorted `SubtreeRecord` list (used after full compaction or recovery)
- `ApplyDeltaBatch` — incremental update from `L1DeltaBatch`; per-partition decision: COW patch (small change) or bulk rebuild (large change), based on `PartitionUpdatePolicy` thresholds
- `PartitionUpdatePolicy` — controls COW eligibility: `cow_max_changed_records=64`, `cow_max_leaf_spans=4`, `cow_max_change_percent=20`; relaxed thresholds for hot prefixes
- `PrefixGovernancePolicy` — tracks hot prefixes (`hot_prefix_record_threshold=512`), forces COW for large partitions (`force_cow_record_threshold=1024`), enables parallel scan above threshold

**Delta batch** (`l1_delta_batch.h`):
- `L1DeltaBatch` — batch of per-prefix deltas (`L1PrefixDelta`) each containing `L1DeltaOp` (Add/Delete/Replace) with `suffix_begin`/`suffix_end`, `kv_block_ptr`, `offset`, `count`
- Produced by `CompactionJob` encoding exactly the touched PST block windows; avoids full L1 scan

**Top-level interface** (`l1_hybrid_index.h`, `L1HybridIndex`):
- `Get` / `Scan` — point lookup and range scan; checks LRU subtree cache first, falls back to SSD load via `SubtreePageStore`
- `ApplyDeltaBatch` — applies compaction output as incremental delta
- `BulkLoad` — full rebuild
- `PublishedSnapshot` — 32B-aligned array of `PublishedRoutePartition` for lock-free read path
- `MemoryUsageStats` — tracks route index bytes (estimated + measured), subtree cache bytes, COW stats (persist calls, reused/written pages and bytes)

**Read path summary** (Get):
1. `ExtractPrefix(key)` → look up `FixedRouteLayout` → get descriptor
2. Descriptor mode dispatch: TinyDirect (inline decode) / NormalPack (load pack page, binary search slot) / NormalSubtree (load subtree from cache or SSD)
3. `L1SubtreeBPTree::LookupCandidate` → `SubtreeRecord` with `leaf_value`
4. Decode `leaf_value` → `(kv_block_ptr, offset, count)` → `PSTReader::PointQueryWindow`

### Key Constants

| Constant | Value |
|---|---|
| `SEGMENT_SIZE` | 4MB |
| `MAX_MEMTABLE_NUM` | 4 |
| `MAX_MEMTABLE_ENTRIES` | 500,000 |
| `MAX_L0_TREE_NUM` | 32 |
| `MAX_USER_THREAD_NUM` | 64 |
| `RANGE_PARTITION_NUM` | 12 |
| Subtree page size | 16KB (fixed) |
| KV block size | 4KB |
| `SubtreeRecord::leaf_value` layout | 44-bit block ptr \| 12-bit offset \| 8-bit count |

## Experiments

All experiments live under `experiments/`. Execution order: correctness_regression → performance_evaluation → ablation_studies.

### Frozen experimental premises (`experiments/performance_evaluation/README.md`)

- Key/value: 16B each; KV block: 4KB; subtree page: 16KB
- Key semantics: 8B prefix + 8B suffix (requires `FLOWKV_KEY16=ON`)
- L1 index: SSD-resident main path (hybrid L1)
- Data scales: 10^6, 10^7, 10^8 (10^9 reserved)
- Prefix ratios: 0.1N, 0.05N, 0.01N (avg keys/prefix: 10, 20, 100)
- Distributions: uniform, prefix-skew
- Result format: `results.csv` + `RESULTS.md` + `plots/` + `raw/`
- Memory overhead: mandatory RSS + L1 Index Memory breakdown (route/subtree/cache/governance)

### 01_point_lookup (`experiments/performance_evaluation/01_point_lookup/`)

**Binary:** `point_lookup_benchmark` — standalone benchmark, does not use the main `benchmark` binary.

**Key flags:**
- `--variant` — query path variant (default: `direction_b_full`)
- `--build_mode` — `fast_bulk_l1` (bypass online Put/Flush/Compaction) or `online`
- `--key_count`, `--prefix_count`, `--query_count` (default: 1M queries)
- `--hit_percent` (default: 80%)
- `--use_direct_io` (default: true), `--warmup_queries` (default: 0)
- `--enable_subtree_cache`, `--subtree_cache_capacity=256`, `--subtree_cache_max_bytes=256MB`

**Metrics:** avg/p99 latency (ns), throughput (ops/s), `l1_page_reads` per query, `pst_reads` per query, L1 index memory (estimated route Masstree bytes + measured RAM layer0 bytes).

**Scripts:**
- `run_point_lookup_batch.py` — batch runner; sweeps key_count × prefix_ratio × distribution; writes `results.csv`
- `generate_point_lookup_report.py` — generates `RESULTS.md` from `results.csv`
- `plot_point_lookup_results.py` — generates plots

**Latest results** (`results/20260321_100m_swap64m_rebuilt/`): 100M keys, 1M queries, 80% hit ratio:
- 0.10N prefix: avg=98.8µs, p99=284µs, 10,092 ops/s, l1_pages=0.063
- 0.05N prefix: avg=80.9µs, p99=520µs, 12,333 ops/s, l1_pages=0.750
- 0.01N prefix: similar trend; route index RAM = 320B–28.8MB (scales with prefix cardinality)

### 03_compaction_update (`experiments/performance_evaluation/03_compaction_update/`)

**Binary:** `write_online_benchmark` — measures online write + compaction performance.

**Key flags:**
- `--write_ops` (default: 1M), `--prefix_count`, `--distribution`
- `--maintenance_mode` — `background` (default) or `manual`
- `--flush_batch`, `--l0_compaction_trigger`, `--l0_write_stall_trigger`

**Metrics:** avg/p99 put latency, `put_path_throughput` (foreground only), `ingest_throughput`, compaction time ratio; V7-specific: `delta_prefix_count`, `effective_delta_prefix_count`, `index_update_total_ms` (CoW/bulk split), `tiny_descriptor_count`, `normal_pack_count`, `dirty_pack_pages`, `pack_write_bytes`.

**Scripts:**
- `run_write_online_batch.py` — batch runner
- `generate_write_online_report.py` — generates `RESULTS.md`
- `plot_write_online_results.py` — generates plots

**Key results (10M keys, 1 thread):**
- Uniform 0.1N prefix: ingest=18,764 ops/s, compaction_time_ratio=97.7%
- Prefix-skew 0.01N: ingest=146,937 ops/s, compaction_time_ratio=77.8%

### Common utilities (`experiments/common/`)

- `FastBulkL1Builder` (`fast_bulk_l1_builder.h/.cpp`) — builds a complete L1 dataset without going through online Put→Flush→Compaction; uses bottom-up batch PST construction + one-shot `L1HybridIndex::BulkLoad`; used by `point_lookup_benchmark` in `fast_bulk_l1` build mode
- `DeterministicValue16` — generates deterministic 16B values from prefix/suffix for reproducible benchmarks
- `KeyForLogicalIndex` / `UsedCountForPrefix` — key generation helpers for uniform and prefix-skew distributions

## Design Evolution (V4→V7)

| Version | Key change |
|---|---|
| V4 | SSD-resident main path; RouteLayer (Masstree) + SubtreeLayer (B+Tree); snapshot publishing with 32B-aligned array |
| V5 | Absolute address protocol; `leaf_value` encodes direct `(kv_block_ptr, offset, count)`; format=3 recovery |
| V6 | Incremental patch-driven updates via `L1DeltaBatch`; density-driven CoW/bulk-load decision; leaf-stream bulk-load |
| V7 | Patch granularity tightening; TinyDirect + NormalPack descriptor dual-mode; hysteresis thresholds; route swap (memory-controlled SSD spill) |

## Known Issues (from TODO)

1. Compaction may error when key=0
2. Manifest takes up unexpectedly large PM space
3. Variable-length KV not supported without KV separation enabled
