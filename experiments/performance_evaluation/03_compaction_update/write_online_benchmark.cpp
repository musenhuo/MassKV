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
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

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

    size_t l1_route_index_measured_bytes = 0;
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
            lat.reserve(op_count / worker_count + 1);
            for (size_t i = logical_begin + tid; i < logical_end; i += worker_count) {
                const KeyType key = KeyForLogicalIndex(cfg, dist, i);
                const RoutePrefix prefix = ExtractPrefix(key);
                const RouteSuffix suffix = flowkv::hybrid_l1::ExtractSuffix(key);
                KeySlice key_slice(key);
                ValueSlice value_slice(DeterministicValue16(prefix, suffix));

                const auto put_begin = std::chrono::steady_clock::now();
                Check(client->Put(key_slice.slice, value_slice.slice), "Put should succeed");
                const auto put_end = std::chrono::steady_clock::now();
                lat.push_back(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(put_end - put_begin)
                        .count()));
            }
        });
    }

    for (auto& t : workers) {
        t.join();
    }
    for (auto& lat : local_latencies) {
        put_latencies_ns->insert(
            put_latencies_ns->end(),
            std::make_move_iterator(lat.begin()),
            std::make_move_iterator(lat.end()));
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
            lat.reserve(op_count / worker_count + 1);
            for (size_t i = logical_begin + tid; i < logical_end; i += worker_count) {
                const KeyType key = KeyForLogicalIndex(cfg, dist, i);
                const RoutePrefix prefix = ExtractPrefix(key);
                const RouteSuffix suffix = flowkv::hybrid_l1::ExtractSuffix(key);
                KeySlice key_slice(key);
                ValueSlice value_slice(DeterministicValue16(prefix, suffix));

                const auto put_begin = std::chrono::steady_clock::now();
                const bool ok = client->Put(key_slice.slice, value_slice.slice);
                Check(ok, "Put should succeed");
                const auto put_end = std::chrono::steady_clock::now();
                lat.push_back(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(put_end - put_begin)
                        .count()));
            }
        });
    }

    for (auto& t : workers) {
        t.join();
    }
    for (auto& lat : local_latencies) {
        put_latencies_ns->insert(
            put_latencies_ns->end(),
            std::make_move_iterator(lat.begin()),
            std::make_move_iterator(lat.end()));
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

    MYDB db(db_cfg);
    if (maintenance_mode == MaintenanceMode::kManual) {
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
    } else {
        db.SetCompactionEnabled(true);
    }
    db.SetL0CompactionTreeNum(cfg.l0_compaction_trigger);
    db.SetL0WriteStallTreeNum(cfg.l0_write_stall_threshold);

    std::vector<uint64_t> put_latencies_ns;
    put_latencies_ns.reserve(cfg.write_ops);

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

    if (maintenance_mode == MaintenanceMode::kManual) {
        for (size_t begin = 0; begin < cfg.write_ops; begin += cfg.flush_batch) {
            const size_t end = std::min(cfg.write_ops, begin + cfg.flush_batch);
            const auto fg_begin = std::chrono::steady_clock::now();
            InsertBatchParallel(cfg, &db, dist, begin, end, &put_latencies_ns);
            const auto fg_end = std::chrono::steady_clock::now();
            foreground_elapsed_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(fg_end - fg_begin).count());
            const auto flush_begin = std::chrono::steady_clock::now();
            Check(db.BGFlush(), "BGFlush should succeed during ingest");
            const auto flush_end = std::chrono::steady_clock::now();
            record_flush_metrics(flush_begin, flush_end);

            while (db.current_version_->GetLevel0TreeNum() >=
                   static_cast<int>(cfg.l0_compaction_trigger)) {
                const auto comp_begin = std::chrono::steady_clock::now();
                const bool compacted = db.BGCompaction();
                const auto comp_end = std::chrono::steady_clock::now();
                if (!compacted) {
                    break;
                }
                record_compaction_metrics(comp_begin, comp_end);
            }
        }

        while (db.current_version_->GetLevel0TreeNum() > 0) {
            const auto comp_begin = std::chrono::steady_clock::now();
            const bool compacted = db.BGCompaction();
            const auto comp_end = std::chrono::steady_clock::now();
            if (!compacted) {
                db.WaitForFlushAndCompaction();
                if (db.current_version_->GetLevel0TreeNum() > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                continue;
            }
            record_compaction_metrics(comp_begin, comp_end);
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

    Check(!put_latencies_ns.empty(), "no put latencies recorded");
    std::sort(put_latencies_ns.begin(), put_latencies_ns.end());
    const size_t p99_idx = std::min<size_t>(
        put_latencies_ns.size() - 1, static_cast<size_t>(put_latencies_ns.size() * 0.99));
    const uint64_t sum_put_ns = std::accumulate(
        put_latencies_ns.begin(), put_latencies_ns.end(), static_cast<uint64_t>(0));

    WriteMetrics metrics;
    metrics.write_ops = cfg.write_ops;
    metrics.flush_threads_effective = effective_flush_threads;
    metrics.compaction_threads_effective = effective_compaction_threads;
    metrics.avg_put_latency_ns =
        static_cast<double>(sum_put_ns) / static_cast<double>(put_latencies_ns.size());
    metrics.p99_put_latency_ns = put_latencies_ns[p99_idx];
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
    // Backward-compatibility: rss_bytes keeps the "after drain" meaning.
    metrics.rss_bytes = metrics.rss_after_drain_wait_bytes;

    const auto l1_memory = db.current_version_->DebugEstimateLevel1MemoryUsage();
    metrics.l1_route_index_measured_bytes = l1_memory.route_index_measured_bytes;
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
    std::cout << "l1_route_index_measured_bytes=" << metrics.l1_route_index_measured_bytes << "\n";
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
