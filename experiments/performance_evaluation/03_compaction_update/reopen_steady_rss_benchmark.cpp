#include "db.h"
#include "db/compaction/version.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

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

struct Config {
    std::string pool_path;
    size_t pool_size_bytes = 256ULL << 30;
    bool use_direct_io = true;
    std::string distribution = "uniform";
    size_t key_count = 0;
    size_t prefix_count = 0;
    size_t validate_queries = 1000;
    size_t settle_ms = 3000;
};

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[reopen_steady_rss_benchmark] " << message << std::endl;
    std::exit(1);
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Fail(message);
    }
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
    Fail("invalid bool value: " + text);
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
        if (key == "--pool-path") {
            cfg.pool_path = value;
        } else if (key == "--pool-size-bytes") {
            cfg.pool_size_bytes = ParseSize(value);
        } else if (key == "--use-direct-io") {
            cfg.use_direct_io = ParseBoolFlag(value);
        } else if (key == "--distribution") {
            cfg.distribution = value;
        } else if (key == "--key-count") {
            cfg.key_count = ParseSize(value);
        } else if (key == "--prefix-count") {
            cfg.prefix_count = ParseSize(value);
        } else if (key == "--validate-queries") {
            cfg.validate_queries = ParseSize(value);
        } else if (key == "--settle-ms") {
            cfg.settle_ms = ParseSize(value);
        } else {
            Fail("unknown argument: " + key);
        }
    }
    if (cfg.pool_path.empty()) {
        Fail("--pool-path is required");
    }
    if (cfg.key_count == 0 || cfg.prefix_count == 0) {
        Fail("--key-count and --prefix-count must be > 0");
    }
    if (cfg.prefix_count > cfg.key_count) {
        Fail("--prefix-count must be <= --key-count");
    }
    return cfg;
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

FixedValue16 DeterministicValue16(RoutePrefix prefix, RouteSuffix suffix) {
    return FixedValue16{
        prefix * 1469598103934665603ULL ^ (suffix + 0x9e3779b97f4a7c15ULL),
        (suffix << 1) ^ (prefix + 0x517cc1b727220a95ULL),
    };
}

size_t UsedCountForPrefix(const Config& cfg, Distribution dist, RoutePrefix prefix) {
    if (dist == Distribution::kUniform) {
        const size_t base = cfg.key_count / cfg.prefix_count;
        const size_t rem = cfg.key_count % cfg.prefix_count;
        return prefix < rem ? base + 1 : base;
    }

    size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    size_t hot_keys = cfg.key_count * 80 / 100;
    if (dist == Distribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 100);
        hot_keys = cfg.key_count * 99 / 100;
    }
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

    size_t hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 5);
    size_t hot_keys = cfg.key_count * 80 / 100;
    if (dist == Distribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, cfg.prefix_count / 100);
        hot_keys = cfg.key_count * 99 / 100;
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

struct ValidateResult {
    size_t checked = 0;
    size_t hit = 0;
};

ValidateResult ValidateData(MYDB& db, const Config& cfg, Distribution dist) {
    ValidateResult res{};
    if (cfg.validate_queries == 0) {
        return res;
    }
    auto client = db.GetClient(1);
    std::array<char, 16> value_buf{};
    Slice out(value_buf.data(), value_buf.size());
    std::mt19937_64 rng(20260329ULL);
    std::uniform_int_distribution<size_t> pick(0, cfg.key_count - 1);
    for (size_t i = 0; i < cfg.validate_queries; ++i) {
        const size_t logical_idx = pick(rng);
        const KeyType key = KeyForLogicalIndex(cfg, dist, logical_idx);
        const bool ok = client->Get(KeySlice(key).slice, out);
        Check(ok, "validation Get miss for logical_idx=" + std::to_string(logical_idx));
        FixedValue16 actual{};
        std::memcpy(&actual, value_buf.data(), sizeof(actual));
        const FixedValue16 expected =
            DeterministicValue16(ExtractPrefix(key), flowkv::hybrid_l1::ExtractSuffix(key));
        Check(actual.lo == expected.lo && actual.hi == expected.hi,
              "validation value mismatch for logical_idx=" + std::to_string(logical_idx));
        ++res.checked;
        ++res.hit;
    }
    return res;
}

}  // namespace

int main(int argc, char** argv) {
#if !defined(FLOWKV_KEY16)
    Fail("reopen_steady_rss_benchmark requires FLOWKV_KEY16=ON");
#endif

    const Config cfg = ParseArgs(argc, argv);
    const Distribution dist = ParseDistribution(cfg.distribution);
    Check(std::filesystem::exists(cfg.pool_path), "pool file not found: " + cfg.pool_path);
    Check(std::filesystem::exists(cfg.pool_path + ".manifest"),
          "manifest file not found: " + cfg.pool_path + ".manifest");

    MYDBConfig db_cfg(cfg.pool_path);
    db_cfg.pm_pool_size = cfg.pool_size_bytes;
    db_cfg.recover = true;
    db_cfg.use_direct_io = cfg.use_direct_io;
    MYDB db(db_cfg);
    db.StopBackgroundTriggerForTesting();
    db.SetCompactionEnabled(false);
    db.EnableReadOnlyMode();
    db.WaitForFlushAndCompaction();

    if (cfg.settle_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.settle_ms));
    }
    const size_t rss_bytes = GetProcessRSSBytes();
    const auto l1_memory = db.current_version_->DebugEstimateLevel1MemoryUsage();
    const int l1_table_count = db.current_version_->GetLevelSize(1);
    const ValidateResult validate = ValidateData(db, cfg, dist);

    std::cout << "pool_path=" << cfg.pool_path << "\n";
    std::cout << "recover_only=1\n";
    std::cout << "distribution=" << cfg.distribution << "\n";
    std::cout << "key_count=" << cfg.key_count << "\n";
    std::cout << "prefix_count=" << cfg.prefix_count << "\n";
    std::cout << "validate_queries=" << cfg.validate_queries << "\n";
    std::cout << "validate_checked=" << validate.checked << "\n";
    std::cout << "validate_hit=" << validate.hit << "\n";
    std::cout << "l1_table_count=" << l1_table_count << "\n";
    std::cout << "rss_bytes=" << rss_bytes << "\n";
    std::cout << "l1_index_bytes_estimated=" << l1_memory.ReadPathBytes() << "\n";
    std::cout << "l1_index_bytes_measured=" << l1_memory.ReadPathMeasuredBytes() << "\n";
    std::cout << "l1_route_partition_bytes=" << l1_memory.route_partition_bytes << "\n";
    std::cout << "l1_route_index_estimated_bytes=" << l1_memory.route_index_estimated_bytes << "\n";
    std::cout << "l1_route_index_measured_bytes=" << l1_memory.route_index_measured_bytes << "\n";
    std::cout << "l1_route_hot_root_index_measured_bytes="
              << l1_memory.route_hot_root_index_measured_bytes << "\n";
    std::cout << "l1_route_hot_descriptor_index_measured_bytes="
              << l1_memory.route_hot_descriptor_index_measured_bytes << "\n";
    std::cout << "l1_route_cold_stub_count=" << l1_memory.route_cold_stub_count << "\n";
    std::cout << "l1_route_cold_ssd_bytes=" << l1_memory.route_cold_ssd_bytes << "\n";
    std::cout << "l1_subtree_bytes=" << l1_memory.subtree_bytes << "\n";
    std::cout << "l1_subtree_cache_bytes=" << l1_memory.subtree_cache_bytes << "\n";
    std::cout << "l1_governance_bytes=" << l1_memory.governance_bytes << "\n";
    return 0;
}

