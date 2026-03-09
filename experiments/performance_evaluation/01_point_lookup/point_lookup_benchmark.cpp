#include "db.h"
#include "db/compaction/version.h"
#include "db/datablock_reader.h"
#include "experiments/common/fast_bulk_l1_builder.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_page_store.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
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
};

struct Config {
    std::string variant = "direction_b_full";
    std::string build_mode = "fast_bulk_l1";
    std::string db_dir = "/tmp";
    std::string distribution = "uniform";
    size_t key_count = 1000000;
    size_t prefix_count = 100000;
    size_t query_count = 1000000;
    uint32_t hit_percent = 80;
    size_t threads = 1;
    size_t flush_batch = 250000;
    size_t l0_compaction_trigger = 4;
    size_t pool_size_bytes = 8ULL << 30;
    bool use_direct_io = true;
    size_t warmup_queries = 0;
    bool keep_db_files = false;
    bool enable_subtree_cache = true;
    size_t subtree_cache_capacity = 256;
    size_t subtree_cache_max_bytes = 256ULL << 20;
    size_t bitmap_persist_every = 1024;
    bool pst_nowait_poll = false;
};

struct QuerySpec {
    KeyType key{};
    FixedValue16 expected_value{};
    bool expect_hit = false;
};

struct QueryTrace {
    uint64_t latency_ns = 0;
    uint64_t l1_page_reads = 0;
    uint64_t pst_reads = 0;
};

struct Metrics {
    double avg_latency_ns = 0.0;
    uint64_t p99_latency_ns = 0;
    double throughput_ops = 0.0;
    double avg_io_total_per_query = 0.0;
    double avg_io_total_top1pct_latency = 0.0;
    double avg_io_l1_pages_per_query = 0.0;
    double avg_io_l1_pages_top1pct_latency = 0.0;
    double avg_io_pst_reads_per_query = 0.0;
    double avg_io_pst_reads_top1pct_latency = 0.0;
    size_t rss_bytes = 0;
    size_t l1_index_bytes_estimated = 0;
    size_t l1_route_partition_bytes = 0;
    size_t l1_route_index_estimated_bytes = 0;
    size_t l1_subtree_bytes = 0;
    size_t l1_subtree_cache_bytes = 0;
    size_t l1_governance_bytes = 0;
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
    uint64_t pst_direct_unaligned_fallbacks = 0;
    uint64_t pst_short_reads = 0;
    uint64_t pst_nowait_eagain_retries = 0;
    uint64_t pst_nowait_unsupported_fallbacks = 0;
};

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[point_lookup_benchmark] " << message << std::endl;
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
    Fail("unsupported distribution: " + text);
}

flowkv::experiments::PrefixDistribution ToPrefixDistribution(Distribution dist) {
    if (dist == Distribution::kUniform) {
        return flowkv::experiments::PrefixDistribution::kUniform;
    }
    return flowkv::experiments::PrefixDistribution::kPrefixSkew;
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
        auto split = arg.find('=');
        if (split == std::string::npos) {
            Fail("invalid argument: " + arg);
        }
        const std::string key = arg.substr(0, split);
        const std::string value = arg.substr(split + 1);
        if (key == "--variant") {
            cfg.variant = value;
        } else if (key == "--build-mode") {
            cfg.build_mode = value;
        } else if (key == "--db-dir") {
            cfg.db_dir = value;
        } else if (key == "--distribution") {
            cfg.distribution = value;
        } else if (key == "--key-count") {
            cfg.key_count = ParseSize(value);
        } else if (key == "--prefix-count") {
            cfg.prefix_count = ParseSize(value);
        } else if (key == "--query-count") {
            cfg.query_count = ParseSize(value);
        } else if (key == "--hit-percent") {
            cfg.hit_percent = static_cast<uint32_t>(ParseSize(value));
        } else if (key == "--threads") {
            cfg.threads = ParseSize(value);
        } else if (key == "--flush-batch") {
            cfg.flush_batch = ParseSize(value);
        } else if (key == "--l0-compaction-trigger") {
            cfg.l0_compaction_trigger = ParseSize(value);
        } else if (key == "--pool-size-bytes") {
            cfg.pool_size_bytes = ParseSize(value);
        } else if (key == "--use-direct-io") {
            cfg.use_direct_io = ParseBoolFlag(value);
        } else if (key == "--warmup-queries") {
            cfg.warmup_queries = ParseSize(value);
        } else if (key == "--keep-db-files") {
            cfg.keep_db_files = ParseBoolFlag(value);
        } else if (key == "--enable-subtree-cache") {
            cfg.enable_subtree_cache = ParseBoolFlag(value);
        } else if (key == "--subtree-cache-capacity") {
            cfg.subtree_cache_capacity = ParseSize(value);
        } else if (key == "--subtree-cache-max-bytes") {
            cfg.subtree_cache_max_bytes = ParseSize(value);
        } else if (key == "--bitmap-persist-every") {
            cfg.bitmap_persist_every = ParseSize(value);
        } else if (key == "--pst-nowait-poll") {
            cfg.pst_nowait_poll = ParseBoolFlag(value);
        } else {
            Fail("unknown argument: " + key);
        }
    }
    if (cfg.variant != "direction_b_full") {
        Fail("currently only --variant=direction_b_full is implemented");
    }
    if (cfg.build_mode != "online" && cfg.build_mode != "fast_bulk_l1") {
        Fail("unsupported --build-mode, expected online|fast_bulk_l1");
    }
    if (cfg.key_count == 0 || cfg.prefix_count == 0 || cfg.query_count == 0 || cfg.threads == 0) {
        Fail("key_count, prefix_count, query_count and threads must be > 0");
    }
    if (cfg.hit_percent > 100) {
        Fail("hit_percent must be in [0, 100]");
    }
    if (cfg.prefix_count > cfg.key_count) {
        // still valid, but not useful for this experiment
        Fail("prefix_count must be <= key_count");
    }
    if (!cfg.enable_subtree_cache) {
        cfg.subtree_cache_capacity = 0;
        cfg.subtree_cache_max_bytes = 0;
    }
    return cfg;
}

void ApplyL1CacheEnv(const Config& cfg) {
    std::string enable = cfg.enable_subtree_cache ? "1" : "0";
    std::string capacity = std::to_string(cfg.subtree_cache_capacity);
    std::string max_bytes = std::to_string(cfg.subtree_cache_max_bytes);
    std::string bitmap_persist_every = std::to_string(cfg.bitmap_persist_every);
    std::string pst_nowait_poll = cfg.pst_nowait_poll ? "1" : "0";
    setenv("FLOWKV_L1_ENABLE_SUBTREE_CACHE", enable.c_str(), 1);
    setenv("FLOWKV_L1_SUBTREE_CACHE_CAPACITY", capacity.c_str(), 1);
    setenv("FLOWKV_L1_SUBTREE_CACHE_MAX_BYTES", max_bytes.c_str(), 1);
    setenv("FLOWKV_BITMAP_PERSIST_EVERY", bitmap_persist_every.c_str(), 1);
    setenv("FLOWKV_PST_NOWAIT_POLL", pst_nowait_poll.c_str(), 1);
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

std::string MakePoolPath(const Config& cfg) {
    const auto dir = std::filesystem::path(cfg.db_dir);
    const std::string name =
        "flowkv_point_lookup_" + std::to_string(getpid()) + "_" +
        std::to_string(cfg.key_count) + "_" + cfg.distribution + ".pool";
    return (dir / name).string();
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

size_t UsedCountForPrefix(const Config& cfg, Distribution dist, RoutePrefix prefix) {
    if (dist == Distribution::kUniform) {
        const size_t base = cfg.key_count / cfg.prefix_count;
        const size_t rem = cfg.key_count % cfg.prefix_count;
        return prefix < rem ? base + 1 : base;
    }

    const size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    const size_t hot_keys = cfg.key_count * 80 / 100;
    const size_t cold_keys = cfg.key_count - hot_keys;
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

    const size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    const size_t hot_keys = cfg.key_count * 80 / 100;
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

std::vector<QuerySpec> BuildQueries(const Config& cfg, Distribution dist) {
    std::mt19937_64 rng(20260304ULL);
    std::uniform_int_distribution<size_t> logical_dist(0, cfg.key_count - 1);
    std::vector<QuerySpec> queries;
    queries.reserve(cfg.query_count);

    const size_t hit_queries = cfg.query_count * cfg.hit_percent / 100;
    for (size_t i = 0; i < hit_queries; ++i) {
        const size_t logical_idx = logical_dist(rng);
        const KeyType key = KeyForLogicalIndex(cfg, dist, logical_idx);
        queries.push_back(QuerySpec{
            key,
            flowkv::experiments::DeterministicValue16(
                ExtractPrefix(key), flowkv::hybrid_l1::ExtractSuffix(key)),
            true,
        });
    }

    for (size_t i = hit_queries; i < cfg.query_count; ++i) {
        const size_t logical_idx = logical_dist(rng);
        const KeyType base_key = KeyForLogicalIndex(cfg, dist, logical_idx);
        const RoutePrefix prefix = ExtractPrefix(base_key);
        const RouteSuffix miss_suffix =
            static_cast<RouteSuffix>(UsedCountForPrefix(cfg, dist, prefix) + 1 + (i - hit_queries));
        queries.push_back(QuerySpec{
            ComposeKey(prefix, miss_suffix),
            0,
            false,
        });
    }

    std::shuffle(queries.begin(), queries.end(), rng);
    return queries;
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

void BuildDatabase(const Config& cfg, Distribution dist, const std::string& pool_path) {
    auto build_online = [&]() {
        MYDBConfig db_cfg(pool_path);
        db_cfg.pm_pool_size = cfg.pool_size_bytes;
        db_cfg.recover = false;
        db_cfg.use_direct_io = cfg.use_direct_io;

        MYDB db(db_cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
        db.SetL0CompactionTreeNum(cfg.l0_compaction_trigger);

        auto client = db.GetClient(1);
        size_t inserted_since_flush = 0;
        for (size_t i = 0; i < cfg.key_count; ++i) {
            const KeyType key = KeyForLogicalIndex(cfg, dist, i);
            KeySlice key_slice(key);
            ValueSlice value_slice(flowkv::experiments::DeterministicValue16(
                ExtractPrefix(key), flowkv::hybrid_l1::ExtractSuffix(key)));
            Check(client->Put(key_slice.slice, value_slice.slice), "Put should succeed");
            ++inserted_since_flush;

            if (inserted_since_flush >= cfg.flush_batch) {
                Check(db.BGFlush(), "BGFlush should succeed during build");
                inserted_since_flush = 0;
                if (db.current_version_->GetLevel0TreeNum() >= static_cast<int>(cfg.l0_compaction_trigger)) {
                    Check(db.BGCompaction(), "BGCompaction should succeed during build");
                }
            }
        }
        if (inserted_since_flush > 0) {
            Check(db.BGFlush(), "final BGFlush should succeed");
        }
        while (db.current_version_->GetLevel0TreeNum() > 0) {
            Check(db.BGCompaction(), "final BGCompaction should succeed");
            if (db.current_version_->GetLevel0TreeNum() == 0) {
                break;
            }
        }
    };

    auto build_fast_bulk_l1 = [&]() {
        flowkv::experiments::FastBulkL1BuildOptions options;
        options.pool_path = pool_path;
        options.pool_size_bytes = cfg.pool_size_bytes;
        options.use_direct_io = cfg.use_direct_io;
        options.key_count = cfg.key_count;
        options.prefix_count = cfg.prefix_count;
        options.distribution = ToPrefixDistribution(dist);

        std::string error_message;
        const bool built = flowkv::experiments::BuildFastBulkL1Dataset(options, &error_message);
        Check(built, "BuildFastBulkL1Dataset failed: " + error_message);
    };

    if (cfg.build_mode == "fast_bulk_l1") {
        build_fast_bulk_l1();
    } else {
        build_online();
    }
}

Metrics RunQueries(const Config& cfg,
                   const std::string& pool_path,
                   const std::vector<QuerySpec>& queries) {
    MYDBConfig db_cfg(pool_path);
    db_cfg.pm_pool_size = cfg.pool_size_bytes;
    db_cfg.recover = true;
    db_cfg.use_direct_io = cfg.use_direct_io;

    MYDB db(db_cfg);
    db.StopBackgroundTriggerForTesting();
    db.SetCompactionEnabled(false);

    const size_t rss_before = GetProcessRSSBytes();

    {
        auto warmup_client = db.GetClient(0);
        std::array<char, 16> value_buf{};
        Slice out(value_buf.data(), value_buf.size());
        const size_t warmup_count = std::min(cfg.warmup_queries, queries.size());
        for (size_t i = 0; i < warmup_count; ++i) {
            KeySlice key_slice(queries[i].key);
            warmup_client->Get(key_slice.slice, out);
        }
    }

    DataBlockReader::ResetGlobalCacheStats();

    std::vector<QueryTrace> traces(queries.size());
    const auto bench_begin = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(cfg.threads);

    for (size_t tid = 0; tid < cfg.threads; ++tid) {
        workers.emplace_back([&, tid]() {
            DataBlockReader::ResetThreadLocalCacheStats();
            flowkv::hybrid_l1::SubtreePageStore::ResetThreadLocalReadPages();
            auto client = db.GetClient(static_cast<int>(tid + 1));
            const size_t begin = tid * queries.size() / cfg.threads;
            const size_t end = (tid + 1) * queries.size() / cfg.threads;
            std::array<char, 16> value_buf{};
            Slice out(value_buf.data(), value_buf.size());
            for (size_t i = begin; i < end; ++i) {
                KeySlice key_slice(queries[i].key);
                const uint64_t l1_read_before =
                    flowkv::hybrid_l1::SubtreePageStore::GetThreadLocalReadPages();
                const uint64_t pst_read_before = DataBlockReader::GetThreadLocalCacheMisses();
                const auto start = std::chrono::steady_clock::now();
                const bool found = client->Get(key_slice.slice, out);
                const auto finish = std::chrono::steady_clock::now();
                const uint64_t l1_read_after =
                    flowkv::hybrid_l1::SubtreePageStore::GetThreadLocalReadPages();
                const uint64_t pst_read_after = DataBlockReader::GetThreadLocalCacheMisses();
                traces[i].latency_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count());
                traces[i].l1_page_reads = l1_read_after - l1_read_before;
                traces[i].pst_reads = pst_read_after - pst_read_before;
                Check(found == queries[i].expect_hit, "point query hit/miss mismatch");
                if (found) {
                    FixedValue16 actual_value{};
                    std::memcpy(&actual_value, value_buf.data(), sizeof(actual_value));
                    if (!(actual_value.lo == queries[i].expected_value.lo &&
                          actual_value.hi == queries[i].expected_value.hi)) {
                        std::cerr << "value mismatch for prefix=" << ExtractPrefix(queries[i].key)
                                  << " expected_lo=" << queries[i].expected_value.lo
                                  << " expected_hi=" << queries[i].expected_value.hi
                                  << " actual_lo=" << actual_value.lo
                                  << " actual_hi=" << actual_value.hi << std::endl;
                        Fail("point query value mismatch");
                    }
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto bench_end = std::chrono::steady_clock::now();

    std::vector<QueryTrace> sorted_traces = traces;
    std::sort(sorted_traces.begin(), sorted_traces.end(),
              [](const QueryTrace& lhs, const QueryTrace& rhs) {
                  return lhs.latency_ns < rhs.latency_ns;
              });
    std::vector<uint64_t> latencies_ns(sorted_traces.size(), 0);
    for (size_t i = 0; i < sorted_traces.size(); ++i) {
        latencies_ns[i] = sorted_traces[i].latency_ns;
    }
    const uint64_t total_latency =
        std::accumulate(latencies_ns.begin(), latencies_ns.end(), uint64_t{0});
    const double avg_latency =
        static_cast<double>(total_latency) / static_cast<double>(latencies_ns.size());
    const size_t p99_idx = std::min(latencies_ns.size() - 1,
                                    (latencies_ns.size() * 99) / 100);
    const size_t top1_begin = p99_idx;
    const double elapsed_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(bench_end - bench_begin).count();
    const auto l1_memory = db.current_version_->DebugEstimateLevel1MemoryUsage();

    uint64_t total_l1_reads = 0;
    uint64_t total_pst_reads = 0;
    for (const auto& trace : traces) {
        total_l1_reads += trace.l1_page_reads;
        total_pst_reads += trace.pst_reads;
    }
    const uint64_t total_io_reads = total_l1_reads + total_pst_reads;
    const double avg_l1_reads =
        static_cast<double>(total_l1_reads) / static_cast<double>(traces.size());
    const double avg_pst_reads =
        static_cast<double>(total_pst_reads) / static_cast<double>(traces.size());
    const double avg_total_reads =
        static_cast<double>(total_io_reads) / static_cast<double>(traces.size());

    uint64_t top1_l1_reads = 0;
    uint64_t top1_pst_reads = 0;
    for (size_t i = top1_begin; i < sorted_traces.size(); ++i) {
        top1_l1_reads += sorted_traces[i].l1_page_reads;
        top1_pst_reads += sorted_traces[i].pst_reads;
    }
    const size_t top1_count = sorted_traces.size() - top1_begin;
    const double top1_avg_l1_reads =
        static_cast<double>(top1_l1_reads) / static_cast<double>(top1_count);
    const double top1_avg_pst_reads =
        static_cast<double>(top1_pst_reads) / static_cast<double>(top1_count);
    const double top1_avg_total_reads =
        static_cast<double>(top1_l1_reads + top1_pst_reads) / static_cast<double>(top1_count);

    Metrics metrics;
    metrics.avg_latency_ns = avg_latency;
    metrics.p99_latency_ns = latencies_ns[p99_idx];
    metrics.throughput_ops = static_cast<double>(queries.size()) / elapsed_sec;
    metrics.avg_io_total_per_query = avg_total_reads;
    metrics.avg_io_total_top1pct_latency = top1_avg_total_reads;
    metrics.avg_io_l1_pages_per_query = avg_l1_reads;
    metrics.avg_io_l1_pages_top1pct_latency = top1_avg_l1_reads;
    metrics.avg_io_pst_reads_per_query = avg_pst_reads;
    metrics.avg_io_pst_reads_top1pct_latency = top1_avg_pst_reads;
    metrics.rss_bytes = rss_before;
    metrics.l1_index_bytes_estimated = l1_memory.TotalBytes();
    metrics.l1_route_partition_bytes = l1_memory.route_partition_bytes;
    metrics.l1_route_index_estimated_bytes = l1_memory.route_index_estimated_bytes;
    metrics.l1_subtree_bytes = l1_memory.subtree_bytes;
    metrics.l1_subtree_cache_bytes = l1_memory.subtree_cache_bytes;
    metrics.l1_governance_bytes = l1_memory.governance_bytes;
    metrics.l1_subtree_cache_requests = l1_memory.subtree_cache_requests;
    metrics.l1_subtree_cache_hits = l1_memory.subtree_cache_hits;
    metrics.l1_subtree_cache_misses = l1_memory.subtree_cache_misses;
    metrics.l1_cow_persist_calls = l1_memory.cow_persist_calls;
    metrics.l1_cow_reused_pages = l1_memory.cow_reused_pages;
    metrics.l1_cow_written_pages = l1_memory.cow_written_pages;
    metrics.l1_cow_reused_bytes = l1_memory.cow_reused_bytes;
    metrics.l1_cow_written_bytes = l1_memory.cow_written_bytes;
    if (metrics.l1_subtree_cache_requests == 0) {
        metrics.l1_subtree_cache_hit_rate = 0.0;
    } else {
        metrics.l1_subtree_cache_hit_rate =
            static_cast<double>(metrics.l1_subtree_cache_hits) /
            static_cast<double>(metrics.l1_subtree_cache_requests);
    }
    const uint64_t total_cow_pages = metrics.l1_cow_reused_pages + metrics.l1_cow_written_pages;
    if (total_cow_pages == 0) {
        metrics.l1_cow_page_reuse_ratio = 0.0;
    } else {
        metrics.l1_cow_page_reuse_ratio =
            static_cast<double>(metrics.l1_cow_reused_pages) / static_cast<double>(total_cow_pages);
    }
    metrics.pst_direct_unaligned_fallbacks = DataBlockReader::GetGlobalDirectUnalignedFallbacks();
    metrics.pst_short_reads = DataBlockReader::GetGlobalShortReads();
    metrics.pst_nowait_eagain_retries = DataBlockReader::GetGlobalNowaitEagainRetries();
    metrics.pst_nowait_unsupported_fallbacks = DataBlockReader::GetGlobalNowaitUnsupportedFallbacks();
    return metrics;
}

void PrintMetrics(const Config& cfg, const Metrics& metrics) {
    std::cout << "variant=" << cfg.variant << "\n";
    std::cout << "build_mode=" << cfg.build_mode << "\n";
    std::cout << "distribution=" << cfg.distribution << "\n";
    std::cout << "key_count=" << cfg.key_count << "\n";
    std::cout << "prefix_count=" << cfg.prefix_count << "\n";
    std::cout << "query_count=" << cfg.query_count << "\n";
    std::cout << "hit_percent=" << cfg.hit_percent << "\n";
    std::cout << "threads=" << cfg.threads << "\n";
    std::cout << "use_direct_io=" << (cfg.use_direct_io ? 1 : 0) << "\n";
    std::cout << "warmup_queries=" << cfg.warmup_queries << "\n";
    std::cout << "enable_subtree_cache=" << (cfg.enable_subtree_cache ? 1 : 0) << "\n";
    std::cout << "subtree_cache_capacity=" << cfg.subtree_cache_capacity << "\n";
    std::cout << "subtree_cache_max_bytes=" << cfg.subtree_cache_max_bytes << "\n";
    std::cout << "bitmap_persist_every=" << cfg.bitmap_persist_every << "\n";
    std::cout << "pst_nowait_poll=" << (cfg.pst_nowait_poll ? 1 : 0) << "\n";
    std::cout << "avg_latency_ns=" << metrics.avg_latency_ns << "\n";
    std::cout << "p99_latency_ns=" << metrics.p99_latency_ns << "\n";
    std::cout << "throughput_ops=" << metrics.throughput_ops << "\n";
    std::cout << "avg_io_total_per_query=" << metrics.avg_io_total_per_query << "\n";
    std::cout << "avg_io_total_top1pct_latency=" << metrics.avg_io_total_top1pct_latency << "\n";
    std::cout << "avg_io_l1_pages_per_query=" << metrics.avg_io_l1_pages_per_query << "\n";
    std::cout << "avg_io_l1_pages_top1pct_latency=" << metrics.avg_io_l1_pages_top1pct_latency << "\n";
    std::cout << "avg_io_pst_reads_per_query=" << metrics.avg_io_pst_reads_per_query << "\n";
    std::cout << "avg_io_pst_reads_top1pct_latency=" << metrics.avg_io_pst_reads_top1pct_latency << "\n";
    std::cout << "rss_bytes=" << metrics.rss_bytes << "\n";
    std::cout << "l1_index_bytes_estimated=" << metrics.l1_index_bytes_estimated << "\n";
    std::cout << "l1_route_partition_bytes=" << metrics.l1_route_partition_bytes << "\n";
    std::cout << "l1_route_index_estimated_bytes=" << metrics.l1_route_index_estimated_bytes << "\n";
    std::cout << "l1_subtree_bytes=" << metrics.l1_subtree_bytes << "\n";
    std::cout << "l1_subtree_cache_bytes=" << metrics.l1_subtree_cache_bytes << "\n";
    std::cout << "l1_governance_bytes=" << metrics.l1_governance_bytes << "\n";
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
    std::cout << "pst_direct_unaligned_fallbacks=" << metrics.pst_direct_unaligned_fallbacks << "\n";
    std::cout << "pst_short_reads=" << metrics.pst_short_reads << "\n";
    std::cout << "pst_nowait_eagain_retries=" << metrics.pst_nowait_eagain_retries << "\n";
    std::cout << "pst_nowait_unsupported_fallbacks=" << metrics.pst_nowait_unsupported_fallbacks << "\n";
}

}  // namespace

int main(int argc, char** argv) {
#if !defined(FLOWKV_KEY16)
    Fail("point_lookup_benchmark requires FLOWKV_KEY16=ON");
#endif

    const Config cfg = ParseArgs(argc, argv);
    const Distribution dist = ParseDistribution(cfg.distribution);
    ApplyL1CacheEnv(cfg);

    std::filesystem::create_directories(cfg.db_dir);
    const std::string pool_path = MakePoolPath(cfg);
    if (!cfg.keep_db_files) {
        RemovePoolFiles(pool_path);
    }

    BuildDatabase(cfg, dist, pool_path);
    const auto queries = BuildQueries(cfg, dist);
    const Metrics metrics = RunQueries(cfg, pool_path, queries);
    PrintMetrics(cfg, metrics);

    if (!cfg.keep_db_files) {
        RemovePoolFiles(pool_path);
    }
    return 0;
}
