#include "db.h"
#include "db/compaction/version.h"
#include "lib/hybrid_l1/l1_hybrid_rebuilder.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>
#include <numeric>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace {

using flowkv::hybrid_l1::ComposeKey;
using flowkv::hybrid_l1::ExtractPrefix;
using flowkv::hybrid_l1::RoutePrefix;
using flowkv::hybrid_l1::RouteSuffix;

enum class Distribution {
    kUniform,
    kPrefixSkew,
    kPrefixSkewExtreme,
};

enum class MaintenanceMode {
    kManual,
    kBackground,
};

struct Config {
    std::string variant = "direction_b_full";
    std::string build_mode = "online";
    std::string maintenance_mode = "manual";
    std::string db_dir = "/tmp";
    std::string distribution = "uniform";
    size_t write_ops = 1000000;
    size_t prefix_count = 100000;
    size_t flush_batch = 250000;
    size_t l0_compaction_trigger = 4;
    size_t l0_write_stall_threshold = MAX_L0_TREE_NUM - 1;
    size_t pool_size_bytes = 64ULL << 30;
    size_t threads = 1;
    size_t flush_threads = 0;      // 0 means keep MYDBConfig default
    size_t compaction_threads = 0; // 0 means auto-tune by writer threads
    bool use_direct_io = true;
    bool keep_db_files = false;
    bool trace_manual_phases = false;
    bool trace_l1_resident = false;
    bool trace_allocator = false;
    bool dump_malloc_info = false;
    std::string allocator_dump_dir;
    bool rss_trim_probe = false;
    bool unbuffered_stdout = false;
    bool probe_release_components = false;
    bool collect_put_latencies = true;
    bool trace_batch_events = true;
};

struct WriteMetrics {
    double avg_put_latency_ns = 0.0;
    uint64_t p99_put_latency_ns = 0;
    // Foreground-only wall-clock throughput (pure Put phase).
    double put_path_throughput_ops = 0.0;
    double foreground_put_throughput_ops = 0.0;
    uint64_t foreground_put_phase_time_ms = 0;
    double drain_wait_time_ms = 0.0;
    double ingest_throughput_ops = 0.0;
    double end_to_end_throughput_ops = 0.0;
    double flush_total_time_ms = 0.0;
    double flush_avg_time_ms = 0.0;
    double flush_max_time_ms = 0.0;
    double compaction_total_time_ms = 0.0;
    double compaction_avg_time_ms = 0.0;
    double compaction_max_time_ms = 0.0;
    double compaction_time_ratio = 0.0;
    uint64_t total_ingest_time_ms = 0;
    uint64_t flush_count = 0;
    uint64_t compaction_count = 0;
    uint64_t flush_threads_effective = 0;
    uint64_t compaction_threads_effective = 0;
    uint64_t write_ops = 0;
    uint64_t rss_bytes = 0;
    uint64_t rss_before_drain_wait_bytes = 0;
    uint64_t rss_after_drain_wait_bytes = 0;
    int64_t rss_drain_wait_delta_bytes = 0;
    uint64_t rss_after_malloc_trim_bytes = 0;
    int64_t rss_malloc_trim_delta_bytes = 0;
    uint64_t alloc_arena_bytes = 0;
    uint64_t alloc_hblkhd_bytes = 0;
    uint64_t alloc_uordblks_bytes = 0;
    uint64_t alloc_fordblks_bytes = 0;
    uint64_t alloc_keepcost_bytes = 0;
    uint64_t alloc_total_system_bytes = 0;
    uint64_t alloc_total_inuse_estimated_bytes = 0;
    uint64_t alloc_total_free_estimated_bytes = 0;
    uint64_t alloc_supported = 0;
    uint64_t alloc_backend = 0;
    uint64_t alloc_jemalloc_stats_valid = 0;
    uint64_t alloc_jemalloc_allocated_bytes = 0;
    uint64_t alloc_jemalloc_active_bytes = 0;
    uint64_t alloc_jemalloc_resident_bytes = 0;
    uint64_t alloc_jemalloc_mapped_bytes = 0;
    uint64_t alloc_jemalloc_retained_bytes = 0;

    size_t l1_route_index_estimated_bytes = 0;
    size_t l1_route_index_measured_bytes = 0;
    size_t l1_route_partition_bytes = 0;
    size_t l1_subtree_published_bytes = 0;
    size_t l1_pending_changed_route_keys_bytes = 0;
    size_t l1_pending_delta_estimated_bytes = 0;
    uint64_t l0_tree_index_count = 0;
    uint64_t l0_tree_index_tree_bytes = 0;
    uint64_t l0_tree_index_pool_bytes = 0;
    uint64_t l0_tree_index_total_bytes = 0;
    uint64_t l1_resident_total_estimated_bytes = 0;
    uint64_t seg_bitmap_bytes = 0;
    uint64_t seg_bitmap_history_bytes = 0;
    uint64_t seg_bitmap_freed_bits_capacity_bytes = 0;
    uint64_t seg_log_bitmap_bytes = 0;
    uint64_t seg_log_bitmap_history_bytes = 0;
    uint64_t seg_log_bitmap_freed_bits_capacity_bytes = 0;
    uint64_t seg_cache_count = 0;
    uint64_t seg_cache_queue_estimated_bytes = 0;
    uint64_t seg_cache_segment_object_bytes = 0;
    uint64_t seg_cache_segment_buffer_bytes = 0;
    uint64_t seg_cache_segment_bitmap_bytes = 0;
    uint64_t seg_log_group_slot_bytes = 0;
    uint64_t seg_total_estimated_bytes = 0;
    uint64_t manifest_super_buffer_bytes = 0;
    uint64_t manifest_super_meta_bytes = 0;
    uint64_t manifest_batch_super_meta_bytes = 0;
    uint64_t manifest_l0_freelist_estimated_bytes = 0;
    uint64_t manifest_batch_pages_data_bytes = 0;
    uint64_t manifest_batch_pages_map_node_estimated_bytes = 0;
    uint64_t manifest_batch_pages_map_bucket_bytes = 0;
    uint64_t manifest_total_estimated_bytes = 0;
    uint64_t db_core_fixed_estimated_bytes = 0;
    uint64_t total_known_resident_estimated_bytes = 0;

    uint64_t l1_active_pst_count = 0;
    uint64_t l0_tree_count_after_drain = 0;
    size_t l1_subtree_cache_bytes = 0;
    uint64_t l1_subtree_cache_requests = 0;
    uint64_t l1_subtree_cache_hits = 0;
    uint64_t l1_subtree_cache_misses = 0;
    double l1_subtree_cache_hit_rate = 0.0;
    uint64_t l1_cow_persist_calls = 0;
    uint64_t l1_cow_reused_pages = 0;
    uint64_t l1_cow_written_pages = 0;
    uint64_t l1_cow_reused_bytes = 0;
    uint64_t l1_cow_written_bytes = 0;
    double l1_cow_page_reuse_ratio = 0.0;

    uint64_t delta_prefix_count = 0;
    uint64_t delta_ops_count = 0;
    uint64_t effective_delta_prefix_count = 0;
    uint64_t effective_delta_ops_count = 0;
    double index_update_total_ms = 0.0;
    double index_update_cow_ms = 0.0;
    double index_update_bulk_ms = 0.0;
    uint64_t cow_prefix_count = 0;
    uint64_t bulk_prefix_count = 0;
    double leaf_stream_merge_ms = 0.0;
    uint64_t rebuild_fallback_count = 0;
    uint64_t tiny_descriptor_count = 0;
    uint64_t normal_pack_count = 0;
    double tiny_hit_ratio = 0.0;
    uint64_t dirty_pack_pages = 0;
    uint64_t pack_write_bytes = 0;
};

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[write_online_benchmark] " << message << std::endl;
    std::exit(1);
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Fail(message);
    }
}

Distribution ParseDistribution(const std::string& text) {
    if (text == "uniform") {
        return Distribution::kUniform;
    }
    if (text == "prefix-skew") {
        return Distribution::kPrefixSkew;
    }
    if (text == "prefix-skew-extreme") {
        return Distribution::kPrefixSkewExtreme;
    }
    Fail("unsupported distribution: " + text);
}

MaintenanceMode ParseMaintenanceMode(const std::string& text) {
    if (text == "manual") {
        return MaintenanceMode::kManual;
    }
    if (text == "background") {
        return MaintenanceMode::kBackground;
    }
    if (text == "overlap") {
        std::cerr << "[write_online_benchmark] maintenance_mode=overlap is deprecated, "
                     "fallback to background mode"
                  << std::endl;
        return MaintenanceMode::kBackground;
    }
    Fail("unsupported maintenance_mode: " + text + ", expected manual|background");
}

size_t ParseSize(const std::string& text) {
    return static_cast<size_t>(std::stoull(text));
}

bool ParseBoolFlag(const std::string& text) {
    if (text == "1" || text == "true" || text == "TRUE") {
        return true;
    }
    if (text == "0" || text == "false" || text == "FALSE") {
        return false;
    }
    Fail("invalid bool flag value: " + text + ", expected 0/1/true/false");
}

extern "C" int mallctl(const char* name,
                       void* oldp,
                       size_t* oldlenp,
                       void* newp,
                       size_t newlen) __attribute__((weak));

enum class AllocatorBackend : uint64_t {
    kUnknown = 0,
    kGlibcMallinfo2 = 1,
    kJemallocMallctl = 2,
};

const char* AllocatorBackendToken(AllocatorBackend backend) {
    switch (backend) {
        case AllocatorBackend::kGlibcMallinfo2:
            return "glibc_mallinfo2";
        case AllocatorBackend::kJemallocMallctl:
            return "jemalloc_mallctl";
        case AllocatorBackend::kUnknown:
        default:
            return "unknown";
    }
}

struct AllocatorSnapshot {
    bool supported = false;
    AllocatorBackend backend = AllocatorBackend::kUnknown;
    uint64_t arena_bytes = 0;
    uint64_t hblkhd_bytes = 0;
    uint64_t uordblks_bytes = 0;
    uint64_t fordblks_bytes = 0;
    uint64_t keepcost_bytes = 0;
    uint64_t total_system_bytes = 0;
    uint64_t total_inuse_estimated_bytes = 0;
    uint64_t total_free_estimated_bytes = 0;

    bool jemalloc_stats_valid = false;
    uint64_t jemalloc_allocated_bytes = 0;
    uint64_t jemalloc_active_bytes = 0;
    uint64_t jemalloc_resident_bytes = 0;
    uint64_t jemalloc_mapped_bytes = 0;
    uint64_t jemalloc_retained_bytes = 0;
};

bool CaptureJemallocSnapshot(AllocatorSnapshot* snap) {
    if (snap == nullptr || mallctl == nullptr) {
        return false;
    }

    uint64_t epoch = 1;
    size_t epoch_sz = sizeof(epoch);
    if (mallctl("epoch", &epoch, &epoch_sz, &epoch, epoch_sz) != 0) {
        return false;
    }

    auto read_stat = [](const char* key, uint64_t* out) -> bool {
        size_t value = 0;
        size_t sz = sizeof(value);
        if (mallctl(key, &value, &sz, nullptr, 0) != 0) {
            return false;
        }
        if (out != nullptr) {
            *out = static_cast<uint64_t>(value);
        }
        return true;
    };

    uint64_t allocated = 0;
    uint64_t active = 0;
    uint64_t resident = 0;
    uint64_t mapped = 0;
    if (!read_stat("stats.allocated", &allocated) ||
        !read_stat("stats.active", &active) ||
        !read_stat("stats.resident", &resident) ||
        !read_stat("stats.mapped", &mapped)) {
        return false;
    }
    uint64_t retained = 0;
    (void)read_stat("stats.retained", &retained);

    snap->supported = true;
    snap->backend = AllocatorBackend::kJemallocMallctl;
    snap->jemalloc_stats_valid = true;
    snap->jemalloc_allocated_bytes = allocated;
    snap->jemalloc_active_bytes = active;
    snap->jemalloc_resident_bytes = resident;
    snap->jemalloc_mapped_bytes = mapped;
    snap->jemalloc_retained_bytes = retained;

    // Compatibility mapping for existing residual calculations:
    // in-use ~= allocated, free-retained ~= active - allocated.
    snap->uordblks_bytes = allocated;
    snap->hblkhd_bytes = 0;
    snap->arena_bytes = active;
    snap->fordblks_bytes = active > allocated ? (active - allocated) : 0;
    snap->keepcost_bytes = 0;
    snap->total_inuse_estimated_bytes = allocated;
    snap->total_free_estimated_bytes = snap->fordblks_bytes;
    snap->total_system_bytes = mapped;
    return true;
}

AllocatorSnapshot CaptureAllocatorSnapshot() {
    AllocatorSnapshot snap;
    if (CaptureJemallocSnapshot(&snap)) {
        return snap;
    }
#if defined(__GLIBC__)
    const auto mi = mallinfo2();
    snap.supported = true;
    snap.backend = AllocatorBackend::kGlibcMallinfo2;
    snap.arena_bytes = static_cast<uint64_t>(mi.arena);
    snap.hblkhd_bytes = static_cast<uint64_t>(mi.hblkhd);
    snap.uordblks_bytes = static_cast<uint64_t>(mi.uordblks);
    snap.fordblks_bytes = static_cast<uint64_t>(mi.fordblks);
    snap.keepcost_bytes = static_cast<uint64_t>(mi.keepcost);
    snap.total_system_bytes = snap.arena_bytes + snap.hblkhd_bytes;
    snap.total_inuse_estimated_bytes = snap.uordblks_bytes + snap.hblkhd_bytes;
    snap.total_free_estimated_bytes = snap.fordblks_bytes;
#endif
    return snap;
}

int DumpMallocInfoSnapshot(const std::filesystem::path& path) {
#if defined(__GLIBC__)
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    FILE* fp = std::fopen(path.c_str(), "w");
    if (fp == nullptr) {
        return -2;
    }
    const int ret = malloc_info(0, fp);
    std::fclose(fp);
    return ret;
#else
    (void)path;
    return -3;
#endif
}

size_t GetProcessRSSBytes();

void EmitProcessOnlyResidentSnapshot(const Config& cfg,
                                     const std::string& event,
                                     const std::chrono::steady_clock::time_point& total_begin,
                                     size_t batch_idx,
                                     uint64_t* malloc_info_snapshot_seq) {
    if (!cfg.trace_manual_phases ||
        (!cfg.trace_l1_resident && !cfg.trace_allocator && !cfg.dump_malloc_info)) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    const double elapsed_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(now - total_begin).count()) /
        1000.0;
    const uint64_t rss_bytes = static_cast<uint64_t>(GetProcessRSSBytes());
    const auto alloc_before = CaptureAllocatorSnapshot();
    auto alloc_after = alloc_before;
    uint64_t rss_after_trim = rss_bytes;
    int64_t rss_trim_delta = 0;
    int trim_result = -1;
    std::string malloc_info_file = "-";
    int malloc_info_result = -1;
#if defined(__GLIBC__)
    if (cfg.rss_trim_probe) {
        trim_result = malloc_trim(0);
        rss_after_trim = static_cast<uint64_t>(GetProcessRSSBytes());
        rss_trim_delta = static_cast<int64_t>(rss_after_trim) - static_cast<int64_t>(rss_bytes);
        alloc_after = CaptureAllocatorSnapshot();
    }
#endif
    if (cfg.dump_malloc_info) {
        const auto file_name = "malloc_info_" + std::to_string(++(*malloc_info_snapshot_seq)) +
                               "_" + event + ".xml";
        const auto dump_path = std::filesystem::path(cfg.allocator_dump_dir) / file_name;
        malloc_info_result = DumpMallocInfoSnapshot(dump_path);
        malloc_info_file = dump_path.string();
    }

    std::cout << "[MANUAL_TRACE]"
              << " t_ms=" << elapsed_ms
              << " event=" << event
              << " batch_idx=" << batch_idx
              << " logical_begin=" << cfg.write_ops
              << " logical_end=" << cfg.write_ops
              << " phase_time_ms=0"
              << " compacted=1"
              << " rss_bytes=" << rss_bytes
              << " l0_tree_num=0"
              << " l1_active_pst_count=0"
              << "\n";

    std::cout << "[MANUAL_L1_RESIDENT]"
              << " event=" << event
              << " rss_bytes=" << rss_bytes
              << " l1_route_index_estimated_bytes=0"
              << " l1_route_index_measured_bytes=0"
              << " l1_route_index_pool_bytes=0"
              << " l1_route_partition_bytes=0"
              << " l1_subtree_published_bytes=0"
              << " l1_subtree_cache_bytes=0"
              << " l1_pending_changed_route_keys_bytes=0"
              << " l1_pending_delta_estimated_bytes=0"
              << " l0_table_lists_total_size=0"
              << " l0_table_lists_total_capacity=0"
              << " l0_table_lists_total_capacity_bytes=0"
              << " l0_tree_index_count=0"
              << " l0_tree_index_tree_bytes=0"
              << " l0_tree_index_pool_bytes=0"
              << " l0_tree_index_total_bytes=0"
              << " l1_resident_total_estimated_bytes=0"
              << " seg_bitmap_bytes=0"
              << " seg_bitmap_history_bytes=0"
              << " seg_bitmap_freed_bits_capacity_bytes=0"
              << " seg_log_bitmap_bytes=0"
              << " seg_log_bitmap_history_bytes=0"
              << " seg_log_bitmap_freed_bits_capacity_bytes=0"
              << " seg_cache_count=0"
              << " seg_cache_queue_estimated_bytes=0"
              << " seg_cache_segment_object_bytes=0"
              << " seg_cache_segment_buffer_bytes=0"
              << " seg_cache_segment_bitmap_bytes=0"
              << " seg_cache_segment_bitmap_freed_bits_capacity_bytes=0"
              << " seg_log_group_slot_bytes=0"
              << " seg_total_estimated_bytes=0"
              << " manifest_super_buffer_bytes=0"
              << " manifest_super_meta_bytes=0"
              << " manifest_batch_super_meta_bytes=0"
              << " manifest_l0_freelist_estimated_bytes=0"
              << " manifest_batch_pages_data_bytes=0"
              << " manifest_batch_pages_map_node_estimated_bytes=0"
              << " manifest_batch_pages_map_bucket_bytes=0"
              << " manifest_total_estimated_bytes=0"
              << " memtable_masstree_active_count=0"
              << " memtable_masstree_tree_bytes=0"
              << " memtable_masstree_pool_bytes=0"
              << " memtable_masstree_total_bytes=0"
              << " db_core_fixed_estimated_bytes=0"
              << " total_known_resident_estimated_bytes=0"
              << " alloc_supported=" << (alloc_before.supported ? 1 : 0)
              << " alloc_before_backend=" << AllocatorBackendToken(alloc_before.backend)
              << " alloc_before_backend_code="
              << static_cast<uint64_t>(alloc_before.backend)
              << " alloc_before_arena_bytes=" << alloc_before.arena_bytes
              << " alloc_before_hblkhd_bytes=" << alloc_before.hblkhd_bytes
              << " alloc_before_uordblks_bytes=" << alloc_before.uordblks_bytes
              << " alloc_before_fordblks_bytes=" << alloc_before.fordblks_bytes
              << " alloc_before_keepcost_bytes=" << alloc_before.keepcost_bytes
              << " alloc_before_total_system_bytes=" << alloc_before.total_system_bytes
              << " alloc_before_total_inuse_estimated_bytes="
              << alloc_before.total_inuse_estimated_bytes
              << " alloc_before_total_free_estimated_bytes="
              << alloc_before.total_free_estimated_bytes
              << " alloc_before_jemalloc_stats_valid="
              << (alloc_before.jemalloc_stats_valid ? 1 : 0)
              << " alloc_before_jemalloc_allocated_bytes="
              << alloc_before.jemalloc_allocated_bytes
              << " alloc_before_jemalloc_active_bytes="
              << alloc_before.jemalloc_active_bytes
              << " alloc_before_jemalloc_resident_bytes="
              << alloc_before.jemalloc_resident_bytes
              << " alloc_before_jemalloc_mapped_bytes="
              << alloc_before.jemalloc_mapped_bytes
              << " alloc_before_jemalloc_retained_bytes="
              << alloc_before.jemalloc_retained_bytes
              << " alloc_after_backend=" << AllocatorBackendToken(alloc_after.backend)
              << " alloc_after_backend_code="
              << static_cast<uint64_t>(alloc_after.backend)
              << " alloc_after_arena_bytes=" << alloc_after.arena_bytes
              << " alloc_after_hblkhd_bytes=" << alloc_after.hblkhd_bytes
              << " alloc_after_uordblks_bytes=" << alloc_after.uordblks_bytes
              << " alloc_after_fordblks_bytes=" << alloc_after.fordblks_bytes
              << " alloc_after_keepcost_bytes=" << alloc_after.keepcost_bytes
              << " alloc_after_total_system_bytes=" << alloc_after.total_system_bytes
              << " alloc_after_total_inuse_estimated_bytes="
              << alloc_after.total_inuse_estimated_bytes
              << " alloc_after_total_free_estimated_bytes="
              << alloc_after.total_free_estimated_bytes
              << " alloc_after_jemalloc_stats_valid="
              << (alloc_after.jemalloc_stats_valid ? 1 : 0)
              << " alloc_after_jemalloc_allocated_bytes="
              << alloc_after.jemalloc_allocated_bytes
              << " alloc_after_jemalloc_active_bytes="
              << alloc_after.jemalloc_active_bytes
              << " alloc_after_jemalloc_resident_bytes="
              << alloc_after.jemalloc_resident_bytes
              << " alloc_after_jemalloc_mapped_bytes="
              << alloc_after.jemalloc_mapped_bytes
              << " alloc_after_jemalloc_retained_bytes="
              << alloc_after.jemalloc_retained_bytes
              << " has_pending_delta_batch=0"
              << " pending_delta_prefix_count=0"
              << " pending_delta_ops_count=0"
              << " dump_malloc_info_called=" << (cfg.dump_malloc_info ? 1 : 0)
              << " malloc_info_result=" << malloc_info_result
              << " malloc_info_file=" << malloc_info_file
              << " rss_after_malloc_trim_bytes=" << rss_after_trim
              << " rss_malloc_trim_delta_bytes=" << rss_trim_delta
              << " malloc_trim_called=" << (cfg.rss_trim_probe ? 1 : 0)
              << " malloc_trim_result=" << trim_result
              << "\n";
}

Config ParseArgs(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        const auto split = arg.find('=');
        if (split == std::string::npos) {
            Fail("invalid argument: " + arg);
        }
        const std::string key = arg.substr(0, split);
        const std::string value = arg.substr(split + 1);
        if (key == "--variant") {
            cfg.variant = value;
        } else if (key == "--build-mode") {
            cfg.build_mode = value;
        } else if (key == "--maintenance-mode") {
            cfg.maintenance_mode = value;
        } else if (key == "--db-dir") {
            cfg.db_dir = value;
        } else if (key == "--distribution") {
            cfg.distribution = value;
        } else if (key == "--write-ops") {
            cfg.write_ops = ParseSize(value);
        } else if (key == "--prefix-count") {
            cfg.prefix_count = ParseSize(value);
        } else if (key == "--flush-batch") {
            cfg.flush_batch = ParseSize(value);
        } else if (key == "--l0-compaction-trigger") {
            cfg.l0_compaction_trigger = ParseSize(value);
        } else if (key == "--l0-write-stall-threshold") {
            cfg.l0_write_stall_threshold = ParseSize(value);
        } else if (key == "--pool-size-bytes") {
            cfg.pool_size_bytes = ParseSize(value);
        } else if (key == "--threads") {
            cfg.threads = ParseSize(value);
        } else if (key == "--flush-threads") {
            cfg.flush_threads = ParseSize(value);
        } else if (key == "--compaction-threads") {
            cfg.compaction_threads = ParseSize(value);
        } else if (key == "--use-direct-io") {
            cfg.use_direct_io = ParseBoolFlag(value);
        } else if (key == "--keep-db-files") {
            cfg.keep_db_files = ParseBoolFlag(value);
        } else if (key == "--trace-manual-phases") {
            cfg.trace_manual_phases = ParseBoolFlag(value);
        } else if (key == "--trace-l1-resident") {
            cfg.trace_l1_resident = ParseBoolFlag(value);
        } else if (key == "--trace-allocator") {
            cfg.trace_allocator = ParseBoolFlag(value);
        } else if (key == "--dump-malloc-info") {
            cfg.dump_malloc_info = ParseBoolFlag(value);
        } else if (key == "--allocator-dump-dir") {
            cfg.allocator_dump_dir = value;
        } else if (key == "--rss-trim-probe") {
            cfg.rss_trim_probe = ParseBoolFlag(value);
        } else if (key == "--unbuffered-stdout") {
            cfg.unbuffered_stdout = ParseBoolFlag(value);
        } else if (key == "--probe-release-components") {
            cfg.probe_release_components = ParseBoolFlag(value);
        } else if (key == "--collect-put-latencies") {
            cfg.collect_put_latencies = ParseBoolFlag(value);
        } else if (key == "--trace-batch-events") {
            cfg.trace_batch_events = ParseBoolFlag(value);
        } else {
            Fail("unknown argument: " + key);
        }
    }

    if (cfg.variant != "direction_b_full") {
        Fail("currently only --variant=direction_b_full is implemented");
    }
    if (cfg.build_mode != "online") {
        Fail("write_online_benchmark requires --build-mode=online");
    }
    if (cfg.write_ops == 0 || cfg.prefix_count == 0 || cfg.flush_batch == 0 ||
        cfg.threads == 0 ||
        cfg.l0_compaction_trigger == 0) {
        Fail("write_ops, prefix_count, flush_batch, threads and l0_compaction_trigger must be > 0");
    }
    if (cfg.l0_write_stall_threshold == 0 ||
        cfg.l0_write_stall_threshold > (MAX_L0_TREE_NUM - 1)) {
        Fail("l0_write_stall_threshold must be in [1, MAX_L0_TREE_NUM-1]");
    }
    if (cfg.prefix_count > cfg.write_ops) {
        Fail("prefix_count must be <= write_ops");
    }
    if (cfg.dump_malloc_info && cfg.allocator_dump_dir.empty()) {
        cfg.allocator_dump_dir = "allocator_snapshots";
    }
    return cfg;
}

struct KeySlice {
    std::array<uint8_t, 16> bytes{};
    Slice slice{reinterpret_cast<const char*>(bytes.data()), bytes.size()};

    explicit KeySlice(const KeyType& key) {
#if defined(FLOWKV_KEY16)
        key.ToBigEndianBytes(bytes.data());
#else
        std::memcpy(bytes.data(), &key, sizeof(key));
#endif
    }
};

struct ValueSlice {
    FixedValue16 value{};
    Slice slice{reinterpret_cast<const char*>(&value), sizeof(value)};

    explicit ValueSlice(FixedValue16 v) : value(v) {}
};

FixedValue16 DeterministicValue16(RoutePrefix prefix, RouteSuffix suffix) {
    return FixedValue16{
        prefix * 1469598103934665603ULL ^ (suffix + 0x9e3779b97f4a7c15ULL),
        (suffix << 1) ^ (prefix + 0x517cc1b727220a95ULL),
    };
}

size_t UsedCountForPrefix(const Config& cfg, Distribution dist, RoutePrefix prefix) {
    if (dist == Distribution::kUniform) {
        const size_t base = cfg.write_ops / cfg.prefix_count;
        const size_t rem = cfg.write_ops % cfg.prefix_count;
        return prefix < rem ? base + 1 : base;
    }

    size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    size_t hot_keys = cfg.write_ops * 80 / 100;
    if (dist == Distribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 100);
        hot_keys = cfg.write_ops * 99 / 100;
    }
    const size_t cold_keys = cfg.write_ops - hot_keys;
    if (prefix < hot_prefixes) {
        const size_t base = hot_keys / hot_prefixes;
        const size_t rem = hot_keys % hot_prefixes;
        return prefix < rem ? base + 1 : base;
    }

    const size_t cold_prefixes = cfg.prefix_count - hot_prefixes;
    if (cold_prefixes == 0) {
        return 0;
    }
    const size_t cold_idx = prefix - hot_prefixes;
    const size_t base = cold_keys / cold_prefixes;
    const size_t rem = cold_keys % cold_prefixes;
    return cold_idx < rem ? base + 1 : base;
}

KeyType KeyForLogicalIndex(const Config& cfg, Distribution dist, size_t logical_idx) {
    if (dist == Distribution::kUniform) {
        const RoutePrefix prefix = static_cast<RoutePrefix>(logical_idx % cfg.prefix_count);
        const RouteSuffix suffix = static_cast<RouteSuffix>(logical_idx / cfg.prefix_count);
        return ComposeKey(prefix, suffix);
    }

    size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    size_t hot_keys = cfg.write_ops * 80 / 100;
    if (dist == Distribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 100);
        hot_keys = cfg.write_ops * 99 / 100;
    }
    if (logical_idx < hot_keys) {
        const RoutePrefix prefix = static_cast<RoutePrefix>(logical_idx % hot_prefixes);
        const RouteSuffix suffix = static_cast<RouteSuffix>(logical_idx / hot_prefixes);
        return ComposeKey(prefix, suffix);
    }

    const size_t cold_keys_idx = logical_idx - hot_keys;
    const size_t cold_prefixes = cfg.prefix_count - hot_prefixes;
    if (cold_prefixes == 0) {
        return ComposeKey(0, static_cast<RouteSuffix>(logical_idx));
    }
    const RoutePrefix prefix =
        static_cast<RoutePrefix>(hot_prefixes + (cold_keys_idx % cold_prefixes));
    const RouteSuffix suffix = static_cast<RouteSuffix>(cold_keys_idx / cold_prefixes);
    return ComposeKey(prefix, suffix);
}

size_t GetProcessRSSBytes() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::string number;
            for (char c : line) {
                if (std::isdigit(static_cast<unsigned char>(c))) {
                    number.push_back(c);
                } else if (!number.empty()) {
                    break;
                }
            }
            if (!number.empty()) {
                return static_cast<size_t>(std::stoull(number)) * 1024ULL;
            }
        }
    }
    return 0;
}

std::string MakePoolPath(const Config& cfg) {
    const auto dir = std::filesystem::path(cfg.db_dir);
    const std::string name =
        "flowkv_write_online_" + std::to_string(getpid()) + "_" +
        std::to_string(cfg.write_ops) + "_" + cfg.distribution + ".pool";
    return (dir / name).string();
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

void InsertBatchParallel(const Config& cfg,
                         MYDB* db,
                         Distribution dist,
                         size_t logical_begin,
                         size_t logical_end,
                         std::vector<uint64_t>* put_latencies_ns) {
    Check(logical_end >= logical_begin, "invalid batch bounds");
    const size_t op_count = logical_end - logical_begin;
    if (op_count == 0) {
        return;
    }
    const size_t worker_count = cfg.threads;
    Check(worker_count > 0, "worker_count must be > 0");

    std::vector<std::vector<uint64_t>> local_latencies(worker_count);
    std::vector<std::thread> workers;
    workers.reserve(worker_count);

    for (size_t tid = 0; tid < worker_count; ++tid) {
        workers.emplace_back([&, tid]() {
            auto client = db->GetClient(static_cast<int>(tid + 1));
            auto& lat = local_latencies[tid];
            if (cfg.collect_put_latencies) {
                lat.reserve(op_count / worker_count + 1);
            }
            for (size_t i = logical_begin + tid; i < logical_end; i += worker_count) {
                const KeyType key = KeyForLogicalIndex(cfg, dist, i);
                const RoutePrefix prefix = ExtractPrefix(key);
                const RouteSuffix suffix = flowkv::hybrid_l1::ExtractSuffix(key);
                KeySlice key_slice(key);
                ValueSlice value_slice(DeterministicValue16(prefix, suffix));

                if (cfg.collect_put_latencies) {
                    const auto put_begin = std::chrono::steady_clock::now();
                    Check(client->Put(key_slice.slice, value_slice.slice), "Put should succeed");
                    const auto put_end = std::chrono::steady_clock::now();
                    lat.push_back(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(put_end - put_begin)
                            .count()));
                } else {
                    Check(client->Put(key_slice.slice, value_slice.slice), "Put should succeed");
                }
            }
        });
    }

    for (auto& t : workers) {
        t.join();
    }
    if (cfg.collect_put_latencies) {
        for (auto& lat : local_latencies) {
            put_latencies_ns->insert(
                put_latencies_ns->end(),
                std::make_move_iterator(lat.begin()),
                std::make_move_iterator(lat.end()));
        }
    }
}

void InsertRangeParallelWithPersistentClients(
    const Config& cfg,
    MYDB* db,
    Distribution dist,
    size_t logical_begin,
    size_t logical_end,
    std::vector<uint64_t>* put_latencies_ns,
    std::vector<std::unique_ptr<MYDBClient>>& persistent_clients) {
    Check(logical_end >= logical_begin, "invalid range bounds");
    const size_t op_count = logical_end - logical_begin;
    if (op_count == 0) {
        return;
    }
    const size_t worker_count = cfg.threads;
    Check(worker_count > 0, "worker_count must be > 0");
    Check(persistent_clients.size() == worker_count,
          "persistent client count must match worker_count");

    std::vector<std::vector<uint64_t>> local_latencies(worker_count);
    std::vector<std::thread> workers;
    workers.reserve(worker_count);

    for (size_t tid = 0; tid < worker_count; ++tid) {
        workers.emplace_back([&, tid]() {
            // Important: create client in the same worker thread that executes Put,
            // so Masstree thread-local initialization is correct.
            persistent_clients[tid] = db->GetClient();
            MYDBClient* client = persistent_clients[tid].get();
            Check(client != nullptr, "persistent client should not be null");

            auto& lat = local_latencies[tid];
            if (cfg.collect_put_latencies) {
                lat.reserve(op_count / worker_count + 1);
            }
            for (size_t i = logical_begin + tid; i < logical_end; i += worker_count) {
                const KeyType key = KeyForLogicalIndex(cfg, dist, i);
                const RoutePrefix prefix = ExtractPrefix(key);
                const RouteSuffix suffix = flowkv::hybrid_l1::ExtractSuffix(key);
                KeySlice key_slice(key);
                ValueSlice value_slice(DeterministicValue16(prefix, suffix));

                if (cfg.collect_put_latencies) {
                    const auto put_begin = std::chrono::steady_clock::now();
                    const bool ok = client->Put(key_slice.slice, value_slice.slice);
                    Check(ok, "Put should succeed");
                    const auto put_end = std::chrono::steady_clock::now();
                    lat.push_back(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(put_end - put_begin)
                            .count()));
                } else {
                    const bool ok = client->Put(key_slice.slice, value_slice.slice);
                    Check(ok, "Put should succeed");
                }
            }
        });
    }

    for (auto& t : workers) {
        t.join();
    }
    if (cfg.collect_put_latencies) {
        for (auto& lat : local_latencies) {
            put_latencies_ns->insert(
                put_latencies_ns->end(),
                std::make_move_iterator(lat.begin()),
                std::make_move_iterator(lat.end()));
        }
    }
}

WriteMetrics RunWriteWorkload(const Config& cfg, Distribution dist, const std::string& pool_path) {
    flowkv::hybrid_l1::L1HybridRebuilder::ResetIndexUpdateStats();
    const MaintenanceMode maintenance_mode = ParseMaintenanceMode(cfg.maintenance_mode);

    MYDBConfig db_cfg(pool_path);
    db_cfg.pm_pool_size = cfg.pool_size_bytes;
    db_cfg.recover = false;
    db_cfg.use_direct_io = cfg.use_direct_io;
    const size_t effective_flush_threads =
        cfg.flush_threads > 0 ? cfg.flush_threads : db_cfg.flush_threads;
    db_cfg.flush_threads = effective_flush_threads;
    const size_t effective_compaction_threads =
        cfg.compaction_threads > 0 ? cfg.compaction_threads : std::max<size_t>(4, cfg.threads);
    db_cfg.compaction_threads = effective_compaction_threads;

    WriteMetrics metrics;
    auto db_holder = std::make_unique<MYDB>(db_cfg);
    MYDB& db = *db_holder;
    if (maintenance_mode == MaintenanceMode::kManual) {
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
    } else {
        db.SetCompactionEnabled(true);
    }
    db.SetL0CompactionTreeNum(cfg.l0_compaction_trigger);
    db.SetL0WriteStallTreeNum(cfg.l0_write_stall_threshold);

    std::vector<uint64_t> put_latencies_ns;
    if (cfg.collect_put_latencies) {
        put_latencies_ns.reserve(cfg.write_ops);
    }

    uint64_t flush_count = 0;
    uint64_t compaction_count = 0;
    double flush_total_time_ms = 0.0;
    double flush_max_time_ms = 0.0;
    double compaction_total_time_ms = 0.0;
    double compaction_max_time_ms = 0.0;

    auto record_flush_metrics = [&](const std::chrono::steady_clock::time_point& begin,
                                    const std::chrono::steady_clock::time_point& end) {
        const double flush_ms =
            static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()) /
            1000.0;
        ++flush_count;
        flush_total_time_ms += flush_ms;
        flush_max_time_ms = std::max(flush_max_time_ms, flush_ms);
    };

    auto record_compaction_metrics = [&](const std::chrono::steady_clock::time_point& begin,
                                         const std::chrono::steady_clock::time_point& end) {
        const double comp_ms =
            static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()) /
            1000.0;
        ++compaction_count;
        compaction_total_time_ms += comp_ms;
        compaction_max_time_ms = std::max(compaction_max_time_ms, comp_ms);
    };

    const auto total_begin = std::chrono::steady_clock::now();
    uint64_t foreground_elapsed_ns = 0;
    double drain_wait_ms = 0.0;
    size_t rss_before_drain_wait_bytes = 0;
    size_t rss_after_drain_wait_bytes = 0;
    uint64_t rss_after_malloc_trim_bytes = 0;
    int64_t rss_malloc_trim_delta_bytes = 0;
    uint64_t malloc_info_snapshot_seq = 0;

    const auto emit_l1_resident_snapshot =
        [&](const std::string& event, bool enable_trim_probe) {
            if (!cfg.trace_manual_phases ||
                (!cfg.trace_l1_resident && !cfg.trace_allocator && !cfg.dump_malloc_info)) {
                return;
            }
            const uint64_t rss_bytes = static_cast<uint64_t>(GetProcessRSSBytes());
            const auto engine_stats = db.DebugEstimateEngineResidentMemory();
            const auto resident_stats = db.current_version_->DebugEstimateLevel1ResidentMemory();
            const auto alloc_before = CaptureAllocatorSnapshot();

            uint64_t rss_after_trim = rss_bytes;
            int64_t rss_trim_delta = 0;
            int trim_result = -1;
            auto alloc_after = alloc_before;
            std::string malloc_info_file = "-";
            int malloc_info_result = -1;
#if defined(__GLIBC__)
            if (enable_trim_probe && cfg.rss_trim_probe) {
                trim_result = malloc_trim(0);
                rss_after_trim = static_cast<uint64_t>(GetProcessRSSBytes());
                rss_trim_delta = static_cast<int64_t>(rss_after_trim) -
                                 static_cast<int64_t>(rss_bytes);
                rss_after_malloc_trim_bytes = rss_after_trim;
                rss_malloc_trim_delta_bytes = rss_trim_delta;
                alloc_after = CaptureAllocatorSnapshot();
            }
#endif
            if (cfg.dump_malloc_info) {
                const auto file_name =
                    "malloc_info_" + std::to_string(++malloc_info_snapshot_seq) + "_" + event +
                    ".xml";
                const auto dump_path = std::filesystem::path(cfg.allocator_dump_dir) / file_name;
                malloc_info_result = DumpMallocInfoSnapshot(dump_path);
                malloc_info_file = dump_path.string();
            }

            std::cout << "[MANUAL_L1_RESIDENT]"
                      << " event=" << event
                      << " rss_bytes=" << rss_bytes
                      << " l1_route_index_estimated_bytes="
                      << engine_stats.l1_route_index_estimated_bytes
                      << " l1_route_index_measured_bytes="
                      << engine_stats.l1_route_index_measured_bytes
                      << " l1_route_index_pool_bytes="
                      << engine_stats.l1_route_index_pool_bytes
                      << " l1_route_partition_bytes="
                      << engine_stats.l1_route_partition_bytes
                      << " l1_subtree_published_bytes="
                      << engine_stats.l1_subtree_published_bytes
                      << " l1_subtree_cache_bytes="
                      << engine_stats.l1_subtree_cache_bytes
                      << " l1_pending_changed_route_keys_bytes="
                      << engine_stats.l1_pending_changed_route_keys_bytes
                      << " l1_pending_delta_estimated_bytes="
                      << engine_stats.l1_pending_delta_estimated_bytes
                      << " l0_table_lists_total_size="
                      << engine_stats.l0_table_lists_total_size
                      << " l0_table_lists_total_capacity="
                      << engine_stats.l0_table_lists_total_capacity
                      << " l0_table_lists_total_capacity_bytes="
                      << engine_stats.l0_table_lists_total_capacity_bytes
                      << " l0_tree_index_count="
                      << engine_stats.l0_tree_index_count
                      << " l0_tree_index_tree_bytes="
                      << engine_stats.l0_tree_index_tree_bytes
                      << " l0_tree_index_pool_bytes="
                      << engine_stats.l0_tree_index_pool_bytes
                      << " l0_tree_index_total_bytes="
                      << engine_stats.l0_tree_index_total_bytes
                      << " l1_resident_total_estimated_bytes="
                      << engine_stats.l1_total_estimated_bytes
                      << " seg_bitmap_bytes=" << engine_stats.seg_bitmap_bytes
                      << " seg_bitmap_history_bytes="
                      << engine_stats.seg_bitmap_history_bytes
                      << " seg_bitmap_freed_bits_capacity_bytes="
                      << engine_stats.seg_bitmap_freed_bits_capacity_bytes
                      << " seg_log_bitmap_bytes=" << engine_stats.seg_log_bitmap_bytes
                      << " seg_log_bitmap_history_bytes="
                      << engine_stats.seg_log_bitmap_history_bytes
                      << " seg_log_bitmap_freed_bits_capacity_bytes="
                      << engine_stats.seg_log_bitmap_freed_bits_capacity_bytes
                      << " seg_cache_count=" << engine_stats.seg_cache_count
                      << " seg_cache_queue_estimated_bytes="
                      << engine_stats.seg_cache_queue_estimated_bytes
                      << " seg_cache_segment_object_bytes="
                      << engine_stats.seg_cache_segment_object_bytes
                      << " seg_cache_segment_buffer_bytes="
                      << engine_stats.seg_cache_segment_buffer_bytes
                      << " seg_cache_segment_bitmap_bytes="
                      << engine_stats.seg_cache_segment_bitmap_bytes
                      << " seg_cache_segment_bitmap_freed_bits_capacity_bytes="
                      << engine_stats.seg_cache_segment_bitmap_freed_bits_capacity_bytes
                      << " seg_log_group_slot_bytes="
                      << engine_stats.seg_log_group_slot_bytes
                      << " seg_total_estimated_bytes="
                      << engine_stats.seg_total_estimated_bytes
                      << " manifest_super_buffer_bytes="
                      << engine_stats.manifest_super_buffer_bytes
                      << " manifest_super_meta_bytes="
                      << engine_stats.manifest_super_meta_bytes
                      << " manifest_batch_super_meta_bytes="
                      << engine_stats.manifest_batch_super_meta_bytes
                      << " manifest_l0_freelist_estimated_bytes="
                      << engine_stats.manifest_l0_freelist_estimated_bytes
                      << " manifest_batch_pages_data_bytes="
                      << engine_stats.manifest_batch_pages_data_bytes
                      << " manifest_batch_pages_map_node_estimated_bytes="
                      << engine_stats.manifest_batch_pages_map_node_estimated_bytes
                      << " manifest_batch_pages_map_bucket_bytes="
                      << engine_stats.manifest_batch_pages_map_bucket_bytes
                      << " manifest_total_estimated_bytes="
                      << engine_stats.manifest_total_estimated_bytes
                      << " memtable_masstree_active_count="
                      << engine_stats.memtable_masstree_active_count
                      << " memtable_masstree_tree_bytes="
                      << engine_stats.memtable_masstree_tree_bytes
                      << " memtable_masstree_pool_bytes="
                      << engine_stats.memtable_masstree_pool_bytes
                      << " memtable_masstree_total_bytes="
                      << engine_stats.memtable_masstree_total_bytes
                      << " db_core_fixed_estimated_bytes="
                      << engine_stats.db_core_fixed_estimated_bytes
                      << " total_known_resident_estimated_bytes="
                      << engine_stats.total_known_resident_estimated_bytes
                      << " alloc_supported=" << (alloc_before.supported ? 1 : 0)
                      << " alloc_before_backend=" << AllocatorBackendToken(alloc_before.backend)
                      << " alloc_before_backend_code="
                      << static_cast<uint64_t>(alloc_before.backend)
                      << " alloc_before_arena_bytes=" << alloc_before.arena_bytes
                      << " alloc_before_hblkhd_bytes=" << alloc_before.hblkhd_bytes
                      << " alloc_before_uordblks_bytes=" << alloc_before.uordblks_bytes
                      << " alloc_before_fordblks_bytes=" << alloc_before.fordblks_bytes
                      << " alloc_before_keepcost_bytes=" << alloc_before.keepcost_bytes
                      << " alloc_before_total_system_bytes="
                      << alloc_before.total_system_bytes
                      << " alloc_before_total_inuse_estimated_bytes="
                      << alloc_before.total_inuse_estimated_bytes
                      << " alloc_before_total_free_estimated_bytes="
                      << alloc_before.total_free_estimated_bytes
                      << " alloc_before_jemalloc_stats_valid="
                      << (alloc_before.jemalloc_stats_valid ? 1 : 0)
                      << " alloc_before_jemalloc_allocated_bytes="
                      << alloc_before.jemalloc_allocated_bytes
                      << " alloc_before_jemalloc_active_bytes="
                      << alloc_before.jemalloc_active_bytes
                      << " alloc_before_jemalloc_resident_bytes="
                      << alloc_before.jemalloc_resident_bytes
                      << " alloc_before_jemalloc_mapped_bytes="
                      << alloc_before.jemalloc_mapped_bytes
                      << " alloc_before_jemalloc_retained_bytes="
                      << alloc_before.jemalloc_retained_bytes
                      << " alloc_after_backend=" << AllocatorBackendToken(alloc_after.backend)
                      << " alloc_after_backend_code="
                      << static_cast<uint64_t>(alloc_after.backend)
                      << " alloc_after_arena_bytes=" << alloc_after.arena_bytes
                      << " alloc_after_hblkhd_bytes=" << alloc_after.hblkhd_bytes
                      << " alloc_after_uordblks_bytes=" << alloc_after.uordblks_bytes
                      << " alloc_after_fordblks_bytes=" << alloc_after.fordblks_bytes
                      << " alloc_after_keepcost_bytes=" << alloc_after.keepcost_bytes
                      << " alloc_after_total_system_bytes="
                      << alloc_after.total_system_bytes
                      << " alloc_after_total_inuse_estimated_bytes="
                      << alloc_after.total_inuse_estimated_bytes
                      << " alloc_after_total_free_estimated_bytes="
                      << alloc_after.total_free_estimated_bytes
                      << " alloc_after_jemalloc_stats_valid="
                      << (alloc_after.jemalloc_stats_valid ? 1 : 0)
                      << " alloc_after_jemalloc_allocated_bytes="
                      << alloc_after.jemalloc_allocated_bytes
                      << " alloc_after_jemalloc_active_bytes="
                      << alloc_after.jemalloc_active_bytes
                      << " alloc_after_jemalloc_resident_bytes="
                      << alloc_after.jemalloc_resident_bytes
                      << " alloc_after_jemalloc_mapped_bytes="
                      << alloc_after.jemalloc_mapped_bytes
                      << " alloc_after_jemalloc_retained_bytes="
                      << alloc_after.jemalloc_retained_bytes
                      << " has_pending_delta_batch="
                      << (resident_stats.has_pending_delta_batch ? 1 : 0)
                      << " pending_delta_prefix_count="
                      << resident_stats.pending_delta_prefix_count
                      << " pending_delta_ops_count="
                      << resident_stats.pending_delta_ops_count
                      << " dump_malloc_info_called=" << (cfg.dump_malloc_info ? 1 : 0)
                      << " malloc_info_result=" << malloc_info_result
                      << " malloc_info_file=" << malloc_info_file
                      << " rss_after_malloc_trim_bytes=" << rss_after_trim
                      << " rss_malloc_trim_delta_bytes=" << rss_trim_delta
                      << " malloc_trim_called="
                      << ((enable_trim_probe && cfg.rss_trim_probe) ? 1 : 0)
                      << " malloc_trim_result=" << trim_result
                      << "\n";
        };

    auto trace_manual_event = [&](const std::string& event,
                                  size_t batch_idx,
                                  size_t logical_begin,
                                  size_t logical_end,
                                  double phase_time_ms,
                                  bool compacted,
                                  const flowkv::hybrid_l1::L1HybridRebuilder::IndexUpdateStats*
                                      stats_snapshot) {
        if (!cfg.trace_manual_phases) {
            return;
        }
        const bool is_probe_event = event.rfind("probe_after_", 0) == 0;
        const bool should_print_trace =
            cfg.trace_batch_events || event == "run_start" || event == "post_wait" ||
            is_probe_event;
        const auto now = std::chrono::steady_clock::now();
        const double elapsed_ms =
            static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(now - total_begin).count()) /
            1000.0;
        const uint64_t rss_bytes = static_cast<uint64_t>(GetProcessRSSBytes());
        if (should_print_trace) {
            std::cout << "[MANUAL_TRACE]"
                      << " t_ms=" << elapsed_ms
                      << " event=" << event
                      << " batch_idx=" << batch_idx
                      << " logical_begin=" << logical_begin
                      << " logical_end=" << logical_end
                      << " phase_time_ms=" << phase_time_ms
                      << " compacted=" << (compacted ? 1 : 0)
                      << " rss_bytes=" << rss_bytes
                      << " l0_tree_num=" << db.current_version_->GetLevel0TreeNum()
                      << " l1_active_pst_count=" << db.current_version_->GetLevelSize(1);
            if (stats_snapshot != nullptr) {
                std::cout << " delta_prefix_count=" << stats_snapshot->delta_prefix_count
                          << " delta_ops_count=" << stats_snapshot->delta_ops_count
                          << " effective_delta_prefix_count="
                          << stats_snapshot->effective_delta_prefix_count
                          << " effective_delta_ops_count="
                          << stats_snapshot->effective_delta_ops_count
                          << " index_update_total_ms=" << stats_snapshot->index_update_total_ms
                          << " index_update_cow_ms=" << stats_snapshot->index_update_cow_ms
                          << " index_update_bulk_ms=" << stats_snapshot->index_update_bulk_ms
                          << " cow_prefix_count=" << stats_snapshot->cow_prefix_count
                          << " bulk_prefix_count=" << stats_snapshot->bulk_prefix_count;
            }
            std::cout << "\n";
        }

        if (event == "batch_compaction_end" ||
            event == "drain_compaction_end") {
            emit_l1_resident_snapshot(event, false);
        } else if (event == "post_wait") {
            emit_l1_resident_snapshot(event, true);
        } else if (event.rfind("probe_after_", 0) == 0) {
            emit_l1_resident_snapshot(event, false);
        }
    };

    trace_manual_event("run_start", 0, 0, 0, 0.0, false, nullptr);

    if (maintenance_mode == MaintenanceMode::kManual) {
        for (size_t begin = 0; begin < cfg.write_ops; begin += cfg.flush_batch) {
            const size_t end = std::min(cfg.write_ops, begin + cfg.flush_batch);
            const size_t batch_idx = begin / cfg.flush_batch;
            trace_manual_event("batch_put_start", batch_idx, begin, end, 0.0, false, nullptr);
            const auto fg_begin = std::chrono::steady_clock::now();
            InsertBatchParallel(cfg, &db, dist, begin, end, &put_latencies_ns);
            const auto fg_end = std::chrono::steady_clock::now();
            const double put_ms =
                static_cast<double>(
                    std::chrono::duration_cast<std::chrono::microseconds>(fg_end - fg_begin).count()) /
                1000.0;
            foreground_elapsed_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(fg_end - fg_begin).count());
            trace_manual_event("batch_put_end", batch_idx, begin, end, put_ms, false, nullptr);

            trace_manual_event("batch_flush_start", batch_idx, begin, end, 0.0, false, nullptr);
            const auto flush_begin = std::chrono::steady_clock::now();
            Check(db.BGFlush(), "BGFlush should succeed during ingest");
            const auto flush_end = std::chrono::steady_clock::now();
            const double flush_ms =
                static_cast<double>(
                    std::chrono::duration_cast<std::chrono::microseconds>(flush_end - flush_begin)
                        .count()) /
                1000.0;
            record_flush_metrics(flush_begin, flush_end);
            trace_manual_event("batch_flush_end", batch_idx, begin, end, flush_ms, true, nullptr);

            while (db.current_version_->GetLevel0TreeNum() >=
                   static_cast<int>(cfg.l0_compaction_trigger)) {
                trace_manual_event(
                    "batch_compaction_start", batch_idx, begin, end, 0.0, false, nullptr);
                const auto comp_begin = std::chrono::steady_clock::now();
                const bool compacted = db.BGCompaction();
                const auto comp_end = std::chrono::steady_clock::now();
                const double comp_ms =
                    static_cast<double>(
                        std::chrono::duration_cast<std::chrono::microseconds>(comp_end - comp_begin)
                            .count()) /
                    1000.0;
                if (!compacted) {
                    trace_manual_event(
                        "batch_compaction_skip", batch_idx, begin, end, comp_ms, false, nullptr);
                    break;
                }
                record_compaction_metrics(comp_begin, comp_end);
                const auto stats_snapshot =
                    flowkv::hybrid_l1::L1HybridRebuilder::GetIndexUpdateStats();
                trace_manual_event(
                    "batch_compaction_end",
                    batch_idx,
                    begin,
                    end,
                    comp_ms,
                    true,
                    &stats_snapshot);
            }
        }

        const size_t drain_batch_idx = cfg.write_ops / cfg.flush_batch;
        while (db.current_version_->GetLevel0TreeNum() > 0) {
            trace_manual_event(
                "drain_compaction_start",
                drain_batch_idx,
                cfg.write_ops,
                cfg.write_ops,
                0.0,
                false,
                nullptr);
            const auto comp_begin = std::chrono::steady_clock::now();
            const bool compacted = db.BGCompaction();
            const auto comp_end = std::chrono::steady_clock::now();
            const double comp_ms =
                static_cast<double>(
                    std::chrono::duration_cast<std::chrono::microseconds>(comp_end - comp_begin)
                        .count()) /
                1000.0;
            if (!compacted) {
                trace_manual_event(
                    "drain_compaction_skip",
                    drain_batch_idx,
                    cfg.write_ops,
                    cfg.write_ops,
                    comp_ms,
                    false,
                    nullptr);
                db.WaitForFlushAndCompaction();
                if (db.current_version_->GetLevel0TreeNum() > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                continue;
            }
            record_compaction_metrics(comp_begin, comp_end);
            const auto stats_snapshot =
                flowkv::hybrid_l1::L1HybridRebuilder::GetIndexUpdateStats();
            trace_manual_event(
                "drain_compaction_end",
                drain_batch_idx,
                cfg.write_ops,
                cfg.write_ops,
                comp_ms,
                true,
                &stats_snapshot);
        }
    } else {
        // Align with FlowKV native benchmark: foreground put threads run continuously
        // while background trigger handles flush/compaction; end with one global barrier.
        std::vector<std::unique_ptr<MYDBClient>> persistent_clients;
        persistent_clients.resize(cfg.threads);
        const auto fg_begin = std::chrono::steady_clock::now();
        InsertRangeParallelWithPersistentClients(
            cfg, &db, dist, 0, cfg.write_ops, &put_latencies_ns, persistent_clients);
        const auto fg_end = std::chrono::steady_clock::now();
        foreground_elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(fg_end - fg_begin).count());
        persistent_clients.clear();
    }

    // Capture an instantaneous RSS snapshot right after foreground ingest finishes,
    // then capture another snapshot right after all flush/compaction drains complete.
    rss_before_drain_wait_bytes = GetProcessRSSBytes();
    const auto wait_begin = std::chrono::steady_clock::now();
    db.WaitForFlushAndCompaction();
    const auto wait_end = std::chrono::steady_clock::now();
    drain_wait_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(wait_end - wait_begin).count()) /
        1000.0;
    rss_after_drain_wait_bytes = GetProcessRSSBytes();
    trace_manual_event(
        "post_wait",
        cfg.write_ops / cfg.flush_batch,
        cfg.write_ops,
        cfg.write_ops,
        drain_wait_ms,
        true,
        nullptr);

    if (cfg.probe_release_components && cfg.trace_manual_phases) {
        const uint64_t released_l0_lists_est =
            static_cast<uint64_t>(db.DebugReleaseLevel0TableListCapacityForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_l0_table_lists"
                  << " released_estimated_bytes=" << released_l0_lists_est << "\n";
        trace_manual_event(
            "probe_after_release_l0_table_lists",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);

        const uint64_t released_seg_cache_est =
            static_cast<uint64_t>(db.DebugReleaseSegmentCacheForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_segment_cache"
                  << " released_estimated_bytes=" << released_seg_cache_est << "\n";
        trace_manual_event(
            "probe_after_release_segment_cache",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);

        const uint64_t released_l1_all_est =
            static_cast<uint64_t>(db.DebugReleaseAllLevel1ForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_l1_all"
                  << " released_estimated_bytes=" << released_l1_all_est << "\n";
        trace_manual_event(
            "probe_after_release_l1_all",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);

        const uint64_t released_manifest_est =
            static_cast<uint64_t>(db.DebugReleaseManifestStateForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_manifest_state"
                  << " released_estimated_bytes=" << released_manifest_est << "\n";
        trace_manual_event(
            "probe_after_release_manifest_state",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);

        const uint64_t released_memtable_est =
            static_cast<uint64_t>(db.DebugReleaseActiveMemtableForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_active_memtable"
                  << " released_estimated_bytes=" << released_memtable_est << "\n";
        trace_manual_event(
            "probe_after_release_active_memtable",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);

        const uint64_t released_thread_pools_est =
            static_cast<uint64_t>(db.DebugReleaseThreadPoolsForProbe());
        std::cout << "[MANUAL_PROBE]"
                  << " event=release_thread_pools"
                  << " released_estimated_bytes=" << released_thread_pools_est << "\n";
        trace_manual_event(
            "probe_after_release_thread_pools",
            cfg.write_ops / cfg.flush_batch,
            cfg.write_ops,
            cfg.write_ops,
            0.0,
            true,
            nullptr);
    }

    if (maintenance_mode == MaintenanceMode::kBackground) {
        // In background mode, wait time includes mixed drain cost (flush + compaction + scheduler jitter).
        // Keep compaction metrics as 0 to avoid mislabeling this mixed wait time.
        compaction_total_time_ms = 0.0;
        compaction_count = 0;
        compaction_max_time_ms = 0.0;
    }

    const auto total_end = std::chrono::steady_clock::now();
    const uint64_t total_elapsed_ns =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  total_end - total_begin)
                                  .count());

    uint64_t p99_put_latency_ns = 0;
    double avg_put_latency_ns = 0.0;
    if (cfg.collect_put_latencies) {
        Check(!put_latencies_ns.empty(), "no put latencies recorded");
        std::sort(put_latencies_ns.begin(), put_latencies_ns.end());
        const size_t p99_idx = std::min<size_t>(
            put_latencies_ns.size() - 1, static_cast<size_t>(put_latencies_ns.size() * 0.99));
        const uint64_t sum_put_ns = std::accumulate(
            put_latencies_ns.begin(), put_latencies_ns.end(), static_cast<uint64_t>(0));
        avg_put_latency_ns =
            static_cast<double>(sum_put_ns) / static_cast<double>(put_latencies_ns.size());
        p99_put_latency_ns = put_latencies_ns[p99_idx];
    }

    metrics.write_ops = cfg.write_ops;
    metrics.flush_threads_effective = effective_flush_threads;
    metrics.compaction_threads_effective = effective_compaction_threads;
    metrics.avg_put_latency_ns = avg_put_latency_ns;
    metrics.p99_put_latency_ns = p99_put_latency_ns;
    metrics.total_ingest_time_ms = total_elapsed_ns / 1000000ULL;
    metrics.foreground_put_phase_time_ms = foreground_elapsed_ns / 1000000ULL;
    metrics.drain_wait_time_ms = drain_wait_ms;
    metrics.put_path_throughput_ops =
        foreground_elapsed_ns == 0
            ? 0.0
            : static_cast<double>(cfg.write_ops) /
                  (static_cast<double>(foreground_elapsed_ns) / 1000000000.0);
    metrics.foreground_put_throughput_ops = metrics.put_path_throughput_ops;
    metrics.ingest_throughput_ops =
        total_elapsed_ns == 0
            ? 0.0
            : static_cast<double>(cfg.write_ops) /
                  (static_cast<double>(total_elapsed_ns) / 1000000000.0);
    metrics.end_to_end_throughput_ops = metrics.ingest_throughput_ops;
    metrics.flush_count = flush_count;
    metrics.flush_total_time_ms = flush_total_time_ms;
    metrics.flush_avg_time_ms = flush_count == 0 ? 0.0 : flush_total_time_ms / flush_count;
    metrics.flush_max_time_ms = flush_max_time_ms;
    metrics.compaction_count = compaction_count;
    metrics.compaction_total_time_ms = compaction_total_time_ms;
    metrics.compaction_avg_time_ms =
        compaction_count == 0 ? 0.0 : compaction_total_time_ms / compaction_count;
    metrics.compaction_max_time_ms = compaction_max_time_ms;
    metrics.compaction_time_ratio =
        total_elapsed_ns == 0
            ? 0.0
            : (compaction_total_time_ms / (static_cast<double>(total_elapsed_ns) / 1000000.0));
    metrics.rss_before_drain_wait_bytes =
        static_cast<uint64_t>(rss_before_drain_wait_bytes);
    metrics.rss_after_drain_wait_bytes =
        static_cast<uint64_t>(rss_after_drain_wait_bytes);
    metrics.rss_drain_wait_delta_bytes =
        static_cast<int64_t>(rss_after_drain_wait_bytes) -
        static_cast<int64_t>(rss_before_drain_wait_bytes);
    metrics.rss_after_malloc_trim_bytes =
        rss_after_malloc_trim_bytes == 0 ? metrics.rss_after_drain_wait_bytes
                                         : rss_after_malloc_trim_bytes;
    metrics.rss_malloc_trim_delta_bytes =
        rss_after_malloc_trim_bytes == 0 ? 0 : rss_malloc_trim_delta_bytes;
    const auto allocator_snapshot = CaptureAllocatorSnapshot();
    metrics.alloc_supported = allocator_snapshot.supported ? 1 : 0;
    metrics.alloc_backend = static_cast<uint64_t>(allocator_snapshot.backend);
    metrics.alloc_arena_bytes = allocator_snapshot.arena_bytes;
    metrics.alloc_hblkhd_bytes = allocator_snapshot.hblkhd_bytes;
    metrics.alloc_uordblks_bytes = allocator_snapshot.uordblks_bytes;
    metrics.alloc_fordblks_bytes = allocator_snapshot.fordblks_bytes;
    metrics.alloc_keepcost_bytes = allocator_snapshot.keepcost_bytes;
    metrics.alloc_total_system_bytes = allocator_snapshot.total_system_bytes;
    metrics.alloc_total_inuse_estimated_bytes =
        allocator_snapshot.total_inuse_estimated_bytes;
    metrics.alloc_total_free_estimated_bytes =
        allocator_snapshot.total_free_estimated_bytes;
    metrics.alloc_jemalloc_stats_valid = allocator_snapshot.jemalloc_stats_valid ? 1 : 0;
    metrics.alloc_jemalloc_allocated_bytes = allocator_snapshot.jemalloc_allocated_bytes;
    metrics.alloc_jemalloc_active_bytes = allocator_snapshot.jemalloc_active_bytes;
    metrics.alloc_jemalloc_resident_bytes = allocator_snapshot.jemalloc_resident_bytes;
    metrics.alloc_jemalloc_mapped_bytes = allocator_snapshot.jemalloc_mapped_bytes;
    metrics.alloc_jemalloc_retained_bytes = allocator_snapshot.jemalloc_retained_bytes;
    // Backward-compatibility: rss_bytes keeps the "after drain" meaning.
    metrics.rss_bytes = metrics.rss_after_drain_wait_bytes;

    const auto l1_memory = db.current_version_->DebugEstimateLevel1MemoryUsage();
    const auto resident_stats = db.current_version_->DebugEstimateLevel1ResidentMemory();
    const auto engine_stats = db.DebugEstimateEngineResidentMemory();
    metrics.l1_route_index_estimated_bytes = engine_stats.l1_route_index_estimated_bytes;
    metrics.l1_route_index_measured_bytes = engine_stats.l1_route_index_measured_bytes;
    metrics.l1_route_partition_bytes = engine_stats.l1_route_partition_bytes;
    metrics.l1_subtree_published_bytes = engine_stats.l1_subtree_published_bytes;
    metrics.l1_pending_changed_route_keys_bytes =
        engine_stats.l1_pending_changed_route_keys_bytes;
    metrics.l1_pending_delta_estimated_bytes = engine_stats.l1_pending_delta_estimated_bytes;
    metrics.l0_tree_index_count = engine_stats.l0_tree_index_count;
    metrics.l0_tree_index_tree_bytes = engine_stats.l0_tree_index_tree_bytes;
    metrics.l0_tree_index_pool_bytes = engine_stats.l0_tree_index_pool_bytes;
    metrics.l0_tree_index_total_bytes = engine_stats.l0_tree_index_total_bytes;
    metrics.l1_resident_total_estimated_bytes = engine_stats.l1_total_estimated_bytes;
    metrics.seg_bitmap_bytes = engine_stats.seg_bitmap_bytes;
    metrics.seg_bitmap_history_bytes = engine_stats.seg_bitmap_history_bytes;
    metrics.seg_bitmap_freed_bits_capacity_bytes =
        engine_stats.seg_bitmap_freed_bits_capacity_bytes;
    metrics.seg_log_bitmap_bytes = engine_stats.seg_log_bitmap_bytes;
    metrics.seg_log_bitmap_history_bytes = engine_stats.seg_log_bitmap_history_bytes;
    metrics.seg_log_bitmap_freed_bits_capacity_bytes =
        engine_stats.seg_log_bitmap_freed_bits_capacity_bytes;
    metrics.seg_cache_count = engine_stats.seg_cache_count;
    metrics.seg_cache_queue_estimated_bytes = engine_stats.seg_cache_queue_estimated_bytes;
    metrics.seg_cache_segment_object_bytes =
        engine_stats.seg_cache_segment_object_bytes;
    metrics.seg_cache_segment_buffer_bytes =
        engine_stats.seg_cache_segment_buffer_bytes;
    metrics.seg_cache_segment_bitmap_bytes =
        engine_stats.seg_cache_segment_bitmap_bytes;
    metrics.seg_log_group_slot_bytes = engine_stats.seg_log_group_slot_bytes;
    metrics.seg_total_estimated_bytes = engine_stats.seg_total_estimated_bytes;
    metrics.manifest_super_buffer_bytes = engine_stats.manifest_super_buffer_bytes;
    metrics.manifest_super_meta_bytes = engine_stats.manifest_super_meta_bytes;
    metrics.manifest_batch_super_meta_bytes = engine_stats.manifest_batch_super_meta_bytes;
    metrics.manifest_l0_freelist_estimated_bytes =
        engine_stats.manifest_l0_freelist_estimated_bytes;
    metrics.manifest_batch_pages_data_bytes = engine_stats.manifest_batch_pages_data_bytes;
    metrics.manifest_batch_pages_map_node_estimated_bytes =
        engine_stats.manifest_batch_pages_map_node_estimated_bytes;
    metrics.manifest_batch_pages_map_bucket_bytes =
        engine_stats.manifest_batch_pages_map_bucket_bytes;
    metrics.manifest_total_estimated_bytes = engine_stats.manifest_total_estimated_bytes;
    metrics.db_core_fixed_estimated_bytes = engine_stats.db_core_fixed_estimated_bytes;
    metrics.total_known_resident_estimated_bytes =
        engine_stats.total_known_resident_estimated_bytes;
    metrics.l1_active_pst_count = static_cast<uint64_t>(db.current_version_->GetLevelSize(1));
    metrics.l0_tree_count_after_drain = static_cast<uint64_t>(db.current_version_->GetLevel0TreeNum());
    metrics.l1_subtree_cache_bytes = l1_memory.subtree_cache_bytes;
    metrics.l1_subtree_cache_requests = l1_memory.subtree_cache_requests;
    metrics.l1_subtree_cache_hits = l1_memory.subtree_cache_hits;
    metrics.l1_subtree_cache_misses = l1_memory.subtree_cache_misses;
    metrics.l1_subtree_cache_hit_rate =
        metrics.l1_subtree_cache_requests == 0
            ? 0.0
            : static_cast<double>(metrics.l1_subtree_cache_hits) /
                  static_cast<double>(metrics.l1_subtree_cache_requests);
    metrics.l1_cow_persist_calls = l1_memory.cow_persist_calls;
    metrics.l1_cow_reused_pages = l1_memory.cow_reused_pages;
    metrics.l1_cow_written_pages = l1_memory.cow_written_pages;
    metrics.l1_cow_reused_bytes = l1_memory.cow_reused_bytes;
    metrics.l1_cow_written_bytes = l1_memory.cow_written_bytes;
    const uint64_t cow_total_pages =
        metrics.l1_cow_reused_pages + metrics.l1_cow_written_pages;
    metrics.l1_cow_page_reuse_ratio =
        cow_total_pages == 0
            ? 0.0
            : static_cast<double>(metrics.l1_cow_reused_pages) /
                  static_cast<double>(cow_total_pages);

    const auto index_update_stats = flowkv::hybrid_l1::L1HybridRebuilder::GetIndexUpdateStats();
    metrics.delta_prefix_count = index_update_stats.delta_prefix_count;
    metrics.delta_ops_count = index_update_stats.delta_ops_count;
    metrics.effective_delta_prefix_count = index_update_stats.effective_delta_prefix_count;
    metrics.effective_delta_ops_count = index_update_stats.effective_delta_ops_count;
    metrics.index_update_total_ms = index_update_stats.index_update_total_ms;
    metrics.index_update_cow_ms = index_update_stats.index_update_cow_ms;
    metrics.index_update_bulk_ms = index_update_stats.index_update_bulk_ms;
    metrics.cow_prefix_count = index_update_stats.cow_prefix_count;
    metrics.bulk_prefix_count = index_update_stats.bulk_prefix_count;
    metrics.leaf_stream_merge_ms = index_update_stats.leaf_stream_merge_ms;
    metrics.rebuild_fallback_count = index_update_stats.rebuild_fallback_count;
    metrics.tiny_descriptor_count = index_update_stats.tiny_descriptor_count;
    metrics.normal_pack_count = index_update_stats.normal_pack_count;
    metrics.tiny_hit_ratio = index_update_stats.tiny_hit_ratio;
    metrics.dirty_pack_pages = index_update_stats.dirty_pack_pages;
    metrics.pack_write_bytes = index_update_stats.pack_write_bytes;

    if (cfg.probe_release_components && cfg.trace_manual_phases) {
        db.WaitForFlushAndCompaction();
        db_holder.reset();
        EmitProcessOnlyResidentSnapshot(
            cfg,
            "probe_after_db_destroy",
            total_begin,
            cfg.write_ops / cfg.flush_batch,
            &malloc_info_snapshot_seq);
    }

    return metrics;
}

void PrintMetrics(const Config& cfg, const WriteMetrics& metrics) {
    std::cout << "variant=" << cfg.variant << "\n";
    std::cout << "build_mode=" << cfg.build_mode << "\n";
    std::cout << "maintenance_mode=" << cfg.maintenance_mode << "\n";
    std::cout << "distribution=" << cfg.distribution << "\n";
    std::cout << "write_ops=" << metrics.write_ops << "\n";
    std::cout << "threads=" << cfg.threads << "\n";
    std::cout << "flush_threads_effective=" << metrics.flush_threads_effective << "\n";
    std::cout << "compaction_threads_effective=" << metrics.compaction_threads_effective << "\n";
    std::cout << "prefix_count=" << cfg.prefix_count << "\n";
    std::cout << "flush_batch=" << cfg.flush_batch << "\n";
    std::cout << "l0_compaction_trigger=" << cfg.l0_compaction_trigger << "\n";
    std::cout << "l0_write_stall_threshold=" << cfg.l0_write_stall_threshold << "\n";
    std::cout << "use_direct_io=" << (cfg.use_direct_io ? 1 : 0) << "\n";
    std::cout << "trace_manual_phases=" << (cfg.trace_manual_phases ? 1 : 0) << "\n";
    std::cout << "trace_l1_resident=" << (cfg.trace_l1_resident ? 1 : 0) << "\n";
    std::cout << "trace_allocator=" << (cfg.trace_allocator ? 1 : 0) << "\n";
    std::cout << "dump_malloc_info=" << (cfg.dump_malloc_info ? 1 : 0) << "\n";
    std::cout << "allocator_dump_dir=" << cfg.allocator_dump_dir << "\n";
    std::cout << "rss_trim_probe=" << (cfg.rss_trim_probe ? 1 : 0) << "\n";
    std::cout << "unbuffered_stdout=" << (cfg.unbuffered_stdout ? 1 : 0) << "\n";
    std::cout << "probe_release_components=" << (cfg.probe_release_components ? 1 : 0) << "\n";
    std::cout << "collect_put_latencies=" << (cfg.collect_put_latencies ? 1 : 0) << "\n";
    std::cout << "trace_batch_events=" << (cfg.trace_batch_events ? 1 : 0) << "\n";
    std::cout << "avg_put_latency_ns=" << metrics.avg_put_latency_ns << "\n";
    std::cout << "p99_put_latency_ns=" << metrics.p99_put_latency_ns << "\n";
    std::cout << "put_path_throughput_ops=" << metrics.put_path_throughput_ops << "\n";
    std::cout << "foreground_put_throughput_ops=" << metrics.foreground_put_throughput_ops << "\n";
    std::cout << "foreground_put_phase_time_ms=" << metrics.foreground_put_phase_time_ms << "\n";
    std::cout << "drain_wait_time_ms=" << metrics.drain_wait_time_ms << "\n";
    std::cout << "ingest_throughput_ops=" << metrics.ingest_throughput_ops << "\n";
    std::cout << "end_to_end_throughput_ops=" << metrics.end_to_end_throughput_ops << "\n";
    std::cout << "total_ingest_time_ms=" << metrics.total_ingest_time_ms << "\n";
    std::cout << "flush_count=" << metrics.flush_count << "\n";
    std::cout << "flush_total_time_ms=" << metrics.flush_total_time_ms << "\n";
    std::cout << "flush_avg_time_ms=" << metrics.flush_avg_time_ms << "\n";
    std::cout << "flush_max_time_ms=" << metrics.flush_max_time_ms << "\n";
    std::cout << "compaction_count=" << metrics.compaction_count << "\n";
    std::cout << "compaction_total_time_ms=" << metrics.compaction_total_time_ms << "\n";
    std::cout << "compaction_avg_time_ms=" << metrics.compaction_avg_time_ms << "\n";
    std::cout << "compaction_max_time_ms=" << metrics.compaction_max_time_ms << "\n";
    std::cout << "compaction_time_ratio=" << metrics.compaction_time_ratio << "\n";
    std::cout << "rss_bytes=" << metrics.rss_bytes << "\n";
    std::cout << "rss_before_drain_wait_bytes=" << metrics.rss_before_drain_wait_bytes << "\n";
    std::cout << "rss_after_drain_wait_bytes=" << metrics.rss_after_drain_wait_bytes << "\n";
    std::cout << "rss_drain_wait_delta_bytes=" << metrics.rss_drain_wait_delta_bytes << "\n";
    std::cout << "rss_after_malloc_trim_bytes=" << metrics.rss_after_malloc_trim_bytes << "\n";
    std::cout << "rss_malloc_trim_delta_bytes=" << metrics.rss_malloc_trim_delta_bytes << "\n";
    std::cout << "alloc_supported=" << metrics.alloc_supported << "\n";
    std::cout << "alloc_backend_code=" << metrics.alloc_backend << "\n";
    std::cout << "alloc_backend="
              << AllocatorBackendToken(static_cast<AllocatorBackend>(metrics.alloc_backend))
              << "\n";
    std::cout << "alloc_arena_bytes=" << metrics.alloc_arena_bytes << "\n";
    std::cout << "alloc_hblkhd_bytes=" << metrics.alloc_hblkhd_bytes << "\n";
    std::cout << "alloc_uordblks_bytes=" << metrics.alloc_uordblks_bytes << "\n";
    std::cout << "alloc_fordblks_bytes=" << metrics.alloc_fordblks_bytes << "\n";
    std::cout << "alloc_keepcost_bytes=" << metrics.alloc_keepcost_bytes << "\n";
    std::cout << "alloc_total_system_bytes=" << metrics.alloc_total_system_bytes << "\n";
    std::cout << "alloc_total_inuse_estimated_bytes="
              << metrics.alloc_total_inuse_estimated_bytes << "\n";
    std::cout << "alloc_total_free_estimated_bytes="
              << metrics.alloc_total_free_estimated_bytes << "\n";
    std::cout << "alloc_jemalloc_stats_valid=" << metrics.alloc_jemalloc_stats_valid << "\n";
    std::cout << "alloc_jemalloc_allocated_bytes="
              << metrics.alloc_jemalloc_allocated_bytes << "\n";
    std::cout << "alloc_jemalloc_active_bytes="
              << metrics.alloc_jemalloc_active_bytes << "\n";
    std::cout << "alloc_jemalloc_resident_bytes="
              << metrics.alloc_jemalloc_resident_bytes << "\n";
    std::cout << "alloc_jemalloc_mapped_bytes="
              << metrics.alloc_jemalloc_mapped_bytes << "\n";
    std::cout << "alloc_jemalloc_retained_bytes="
              << metrics.alloc_jemalloc_retained_bytes << "\n";
    std::cout << "l1_route_index_estimated_bytes="
              << metrics.l1_route_index_estimated_bytes << "\n";
    std::cout << "l1_route_index_measured_bytes=" << metrics.l1_route_index_measured_bytes << "\n";
    std::cout << "l1_route_partition_bytes=" << metrics.l1_route_partition_bytes << "\n";
    std::cout << "l1_subtree_published_bytes=" << metrics.l1_subtree_published_bytes << "\n";
    std::cout << "l1_pending_changed_route_keys_bytes="
              << metrics.l1_pending_changed_route_keys_bytes << "\n";
    std::cout << "l1_pending_delta_estimated_bytes="
              << metrics.l1_pending_delta_estimated_bytes << "\n";
    std::cout << "l0_tree_index_count=" << metrics.l0_tree_index_count << "\n";
    std::cout << "l0_tree_index_tree_bytes=" << metrics.l0_tree_index_tree_bytes << "\n";
    std::cout << "l0_tree_index_pool_bytes=" << metrics.l0_tree_index_pool_bytes << "\n";
    std::cout << "l0_tree_index_total_bytes=" << metrics.l0_tree_index_total_bytes << "\n";
    std::cout << "l1_resident_total_estimated_bytes="
              << metrics.l1_resident_total_estimated_bytes << "\n";
    std::cout << "seg_bitmap_bytes=" << metrics.seg_bitmap_bytes << "\n";
    std::cout << "seg_bitmap_history_bytes=" << metrics.seg_bitmap_history_bytes << "\n";
    std::cout << "seg_bitmap_freed_bits_capacity_bytes="
              << metrics.seg_bitmap_freed_bits_capacity_bytes << "\n";
    std::cout << "seg_log_bitmap_bytes=" << metrics.seg_log_bitmap_bytes << "\n";
    std::cout << "seg_log_bitmap_history_bytes="
              << metrics.seg_log_bitmap_history_bytes << "\n";
    std::cout << "seg_log_bitmap_freed_bits_capacity_bytes="
              << metrics.seg_log_bitmap_freed_bits_capacity_bytes << "\n";
    std::cout << "seg_cache_count=" << metrics.seg_cache_count << "\n";
    std::cout << "seg_cache_queue_estimated_bytes="
              << metrics.seg_cache_queue_estimated_bytes << "\n";
    std::cout << "seg_cache_segment_object_bytes="
              << metrics.seg_cache_segment_object_bytes << "\n";
    std::cout << "seg_cache_segment_buffer_bytes="
              << metrics.seg_cache_segment_buffer_bytes << "\n";
    std::cout << "seg_cache_segment_bitmap_bytes="
              << metrics.seg_cache_segment_bitmap_bytes << "\n";
    std::cout << "seg_log_group_slot_bytes=" << metrics.seg_log_group_slot_bytes << "\n";
    std::cout << "seg_total_estimated_bytes=" << metrics.seg_total_estimated_bytes << "\n";
    std::cout << "manifest_super_buffer_bytes="
              << metrics.manifest_super_buffer_bytes << "\n";
    std::cout << "manifest_super_meta_bytes="
              << metrics.manifest_super_meta_bytes << "\n";
    std::cout << "manifest_batch_super_meta_bytes="
              << metrics.manifest_batch_super_meta_bytes << "\n";
    std::cout << "manifest_l0_freelist_estimated_bytes="
              << metrics.manifest_l0_freelist_estimated_bytes << "\n";
    std::cout << "manifest_batch_pages_data_bytes="
              << metrics.manifest_batch_pages_data_bytes << "\n";
    std::cout << "manifest_batch_pages_map_node_estimated_bytes="
              << metrics.manifest_batch_pages_map_node_estimated_bytes << "\n";
    std::cout << "manifest_batch_pages_map_bucket_bytes="
              << metrics.manifest_batch_pages_map_bucket_bytes << "\n";
    std::cout << "manifest_total_estimated_bytes="
              << metrics.manifest_total_estimated_bytes << "\n";
    std::cout << "db_core_fixed_estimated_bytes="
              << metrics.db_core_fixed_estimated_bytes << "\n";
    std::cout << "total_known_resident_estimated_bytes="
              << metrics.total_known_resident_estimated_bytes << "\n";
    std::cout << "l1_active_pst_count=" << metrics.l1_active_pst_count << "\n";
    std::cout << "l0_tree_count_after_drain=" << metrics.l0_tree_count_after_drain << "\n";
    std::cout << "l1_subtree_cache_bytes=" << metrics.l1_subtree_cache_bytes << "\n";
    std::cout << "l1_subtree_cache_requests=" << metrics.l1_subtree_cache_requests << "\n";
    std::cout << "l1_subtree_cache_hits=" << metrics.l1_subtree_cache_hits << "\n";
    std::cout << "l1_subtree_cache_misses=" << metrics.l1_subtree_cache_misses << "\n";
    std::cout << "l1_subtree_cache_hit_rate=" << metrics.l1_subtree_cache_hit_rate << "\n";
    std::cout << "l1_cow_persist_calls=" << metrics.l1_cow_persist_calls << "\n";
    std::cout << "l1_cow_reused_pages=" << metrics.l1_cow_reused_pages << "\n";
    std::cout << "l1_cow_written_pages=" << metrics.l1_cow_written_pages << "\n";
    std::cout << "l1_cow_reused_bytes=" << metrics.l1_cow_reused_bytes << "\n";
    std::cout << "l1_cow_written_bytes=" << metrics.l1_cow_written_bytes << "\n";
    std::cout << "l1_cow_page_reuse_ratio=" << metrics.l1_cow_page_reuse_ratio << "\n";
    std::cout << "delta_prefix_count=" << metrics.delta_prefix_count << "\n";
    std::cout << "delta_ops_count=" << metrics.delta_ops_count << "\n";
    std::cout << "effective_delta_prefix_count=" << metrics.effective_delta_prefix_count << "\n";
    std::cout << "effective_delta_ops_count=" << metrics.effective_delta_ops_count << "\n";
    std::cout << "index_update_total_ms=" << metrics.index_update_total_ms << "\n";
    std::cout << "index_update_cow_ms=" << metrics.index_update_cow_ms << "\n";
    std::cout << "index_update_bulk_ms=" << metrics.index_update_bulk_ms << "\n";
    std::cout << "cow_prefix_count=" << metrics.cow_prefix_count << "\n";
    std::cout << "bulk_prefix_count=" << metrics.bulk_prefix_count << "\n";
    std::cout << "leaf_stream_merge_ms=" << metrics.leaf_stream_merge_ms << "\n";
    std::cout << "rebuild_fallback_count=" << metrics.rebuild_fallback_count << "\n";
    std::cout << "tiny_descriptor_count=" << metrics.tiny_descriptor_count << "\n";
    std::cout << "normal_pack_count=" << metrics.normal_pack_count << "\n";
    std::cout << "tiny_hit_ratio=" << metrics.tiny_hit_ratio << "\n";
    std::cout << "dirty_pack_pages=" << metrics.dirty_pack_pages << "\n";
    std::cout << "pack_write_bytes=" << metrics.pack_write_bytes << "\n";
}

}  // namespace

int main(int argc, char** argv) {
#if !defined(FLOWKV_KEY16)
    Fail("write_online_benchmark requires FLOWKV_KEY16=ON");
#endif

    const Config cfg = ParseArgs(argc, argv);
    if (cfg.unbuffered_stdout) {
        std::cout.setf(std::ios::unitbuf);
        std::cerr.setf(std::ios::unitbuf);
    }
    const Distribution dist = ParseDistribution(cfg.distribution);

    std::filesystem::create_directories(cfg.db_dir);
    const std::string pool_path = MakePoolPath(cfg);
    if (!cfg.keep_db_files) {
        RemovePoolFiles(pool_path);
    }

    const WriteMetrics metrics = RunWriteWorkload(cfg, dist, pool_path);
    PrintMetrics(cfg, metrics);

    if (!cfg.keep_db_files) {
        RemovePoolFiles(pool_path);
    }
    return 0;
}
