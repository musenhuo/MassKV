#include "experiments/common/fast_bulk_l1_builder.h"

#include "db/compaction/manifest.h"
#include "db/compaction/version.h"
#include "db/pst_builder.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
#include <string>
#include <vector>

namespace flowkv::experiments {
namespace {

constexpr size_t kInvalidManifestPosition = std::numeric_limits<size_t>::max();

using hybrid_l1::ComposeKey;
using hybrid_l1::ExtractPrefix;
using hybrid_l1::ExtractSuffix;
using hybrid_l1::RoutePrefix;
using hybrid_l1::RouteSuffix;

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

bool SetError(std::string* error_message, const std::string& message) {
    if (error_message != nullptr) {
        *error_message = message;
    }
    return false;
}

}  // namespace

FixedValue16 DeterministicValue16(RoutePrefix prefix, RouteSuffix suffix) {
    return FixedValue16{
        prefix * 1469598103934665603ULL ^ (suffix + 0x9e3779b97f4a7c15ULL),
        (suffix << 1) ^ (prefix + 0x517cc1b727220a95ULL),
    };
}

size_t UsedCountForPrefix(const FastBulkL1BuildOptions& options, RoutePrefix prefix) {
    if (options.distribution == PrefixDistribution::kUniform) {
        const size_t base = options.key_count / options.prefix_count;
        const size_t rem = options.key_count % options.prefix_count;
        return static_cast<size_t>(prefix) < rem ? base + 1 : base;
    }

    size_t hot_prefixes = std::max<size_t>(1, options.prefix_count / 5);
    size_t hot_keys = options.key_count * 80 / 100;
    if (options.distribution == PrefixDistribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, options.prefix_count / 100);
        hot_keys = options.key_count * 99 / 100;
    }
    const size_t cold_keys = options.key_count - hot_keys;
    if (static_cast<size_t>(prefix) < hot_prefixes) {
        const size_t base = hot_keys / hot_prefixes;
        const size_t rem = hot_keys % hot_prefixes;
        return static_cast<size_t>(prefix) < rem ? base + 1 : base;
    }

    const size_t cold_prefixes = options.prefix_count - hot_prefixes;
    if (cold_prefixes == 0) {
        return 0;
    }
    const size_t cold_idx = static_cast<size_t>(prefix) - hot_prefixes;
    const size_t base = cold_keys / cold_prefixes;
    const size_t rem = cold_keys % cold_prefixes;
    return cold_idx < rem ? base + 1 : base;
}

KeyType KeyForLogicalIndex(const FastBulkL1BuildOptions& options, size_t logical_idx) {
    if (options.distribution == PrefixDistribution::kUniform) {
        const RoutePrefix prefix = static_cast<RoutePrefix>(logical_idx % options.prefix_count);
        const RouteSuffix suffix = static_cast<RouteSuffix>(logical_idx / options.prefix_count);
        return ComposeKey(prefix, suffix);
    }

    size_t hot_prefixes = std::max<size_t>(1, options.prefix_count / 5);
    size_t hot_keys = options.key_count * 80 / 100;
    if (options.distribution == PrefixDistribution::kPrefixSkewExtreme) {
        hot_prefixes = std::max<size_t>(1, options.prefix_count / 100);
        hot_keys = options.key_count * 99 / 100;
    }
    if (logical_idx < hot_keys) {
        const RoutePrefix prefix = static_cast<RoutePrefix>(logical_idx % hot_prefixes);
        const RouteSuffix suffix = static_cast<RouteSuffix>(logical_idx / hot_prefixes);
        return ComposeKey(prefix, suffix);
    }

    const size_t cold_keys_idx = logical_idx - hot_keys;
    const size_t cold_prefixes = options.prefix_count - hot_prefixes;
    if (cold_prefixes == 0) {
        return ComposeKey(0, static_cast<RouteSuffix>(logical_idx));
    }
    const RoutePrefix prefix =
        static_cast<RoutePrefix>(hot_prefixes + (cold_keys_idx % cold_prefixes));
    const RouteSuffix suffix = static_cast<RouteSuffix>(cold_keys_idx / cold_prefixes);
    return ComposeKey(prefix, suffix);
}

bool BuildFastBulkL1Dataset(const FastBulkL1BuildOptions& options, std::string* error_message) {
    if (options.pool_path.empty()) {
        return SetError(error_message, "pool_path is empty");
    }
    if (options.pool_size_bytes == 0) {
        return SetError(error_message, "pool_size_bytes must be > 0");
    }
    if (options.key_count == 0 || options.prefix_count == 0) {
        return SetError(error_message, "key_count and prefix_count must be > 0");
    }
    if (options.prefix_count > options.key_count) {
        return SetError(error_message, "prefix_count must be <= key_count");
    }

    SegmentAllocator allocator(options.pool_path, options.pool_size_bytes, false, options.use_direct_io);
    const std::string manifest_path = options.pool_path + ".manifest";
    const int manifest_fd = open(manifest_path.c_str(), O_DIRECT | O_RDWR | O_CREAT, 0666);
    if (manifest_fd < 0) {
        return SetError(error_message, "open manifest file failed in fast_bulk_l1 mode");
    }

    bool success = false;
    std::string stage = "init";
    try {
        stage = "manifest_ctor";
        Manifest manifest(manifest_fd, false);
        stage = "version_ctor";
        Version version(&allocator);
        stage = "builder_ctor";
        PSTBuilder builder(&allocator);

        const uint32_t l1_seq_no = version.GenerateL1Seq();
        std::vector<TaggedPstMeta> level1_tables;
        level1_tables.reserve((options.key_count + 1023) / 1024);

        auto flush_one_pst = [&]() {
            PSTMeta meta = builder.Flush();
            if (!meta.Valid()) {
                return;
            }
            meta.seq_no_ = l1_seq_no;
            TaggedPstMeta tagged{};
            tagged.meta = meta;
            tagged.level = 1;
            tagged.manifest_position = kInvalidManifestPosition;
            level1_tables.push_back(tagged);
        };

        size_t generated = 0;
        stage = "build_entries";
        for (RoutePrefix prefix = 0;
             static_cast<size_t>(prefix) < options.prefix_count;
             ++prefix) {
            const size_t count = UsedCountForPrefix(options, prefix);
            for (RouteSuffix suffix = 0; static_cast<size_t>(suffix) < count; ++suffix) {
                const KeyType key = ComposeKey(prefix, suffix);
                KeySlice key_slice(key);
                ValueSlice value_slice(DeterministicValue16(prefix, suffix));
                if (!builder.AddEntry(key_slice.slice, value_slice.slice)) {
                    flush_one_pst();
                    if (!builder.AddEntry(key_slice.slice, value_slice.slice)) {
                        throw std::runtime_error("PSTBuilder AddEntry failed after flush");
                    }
                }
                ++generated;
            }
        }
        if (generated != options.key_count) {
            throw std::runtime_error("generated key count mismatch in fast_bulk_l1 mode");
        }

        stage = "flush_checkpoint";
        flush_one_pst();
        builder.PersistCheckpoint();

        stage = "recover_l1_tables";
        version.RecoverLevel1Tables(std::move(level1_tables), l1_seq_no + 1);
        stage = "manifest_update_l1_version";
        manifest.UpdateL1Version(l1_seq_no);

        std::vector<uint8_t> l1_hybrid_state_bytes;
        uint32_t current_l1_seq_no = 0;
        stage = "export_hybrid_state";
        if (!version.ExportL1HybridState(l1_hybrid_state_bytes, current_l1_seq_no)) {
            throw std::runtime_error("ExportL1HybridState failed in fast_bulk_l1 mode");
        }
        stage = "persist_hybrid_state";
        if (!manifest.PersistL1HybridState(l1_hybrid_state_bytes, current_l1_seq_no)) {
            throw std::runtime_error("PersistL1HybridState failed in fast_bulk_l1 mode");
        }

        stage = "success";
        success = true;
    } catch (const std::exception& e) {
        SetError(error_message, std::string("fast_bulk_l1 build failed: ") + e.what());
    } catch (...) {
        SetError(error_message, "fast_bulk_l1 build failed: unknown exception");
    }

    close(manifest_fd);
    if (!success && error_message != nullptr && error_message->empty()) {
        *error_message = "fast_bulk_l1 build failed at stage: " + stage;
    }
    return success;
}

}  // namespace flowkv::experiments
