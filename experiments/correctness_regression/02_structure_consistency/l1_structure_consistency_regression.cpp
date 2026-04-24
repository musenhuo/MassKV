#include "db.h"
#include "db/compaction/version.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_record.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include <unistd.h>

namespace {

using flowkv::hybrid_l1::RecordMaxKeyLess;
using flowkv::hybrid_l1::RecordRouteKeyLess;
using flowkv::hybrid_l1::SubtreeRecord;
using flowkv::hybrid_l1::ExtractPrefix;

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[l1_structure_consistency_regression] " << message << std::endl;
    std::exit(1);
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Fail(message);
    }
}

KeyType MakeKey(uint64_t value) {
#if defined(FLOWKV_KEY16)
    constexpr uint64_t kPrefixSpan = 64;
    return flowkv::hybrid_l1::ComposeKey(value / kPrefixSpan, value % kPrefixSpan);
#else
    return __builtin_bswap_64(value);
#endif
}

struct KeySlice {
#if defined(FLOWKV_KEY16)
    std::array<uint8_t, 16> bytes{};
#else
    uint64_t bytes = 0;
#endif
    Slice slice;

    explicit KeySlice(uint64_t value)
#if defined(FLOWKV_KEY16)
        : slice(reinterpret_cast<const char*>(bytes.data()), bytes.size())
#else
        : slice(reinterpret_cast<const char*>(&bytes), sizeof(bytes))
#endif
    {
#if defined(FLOWKV_KEY16)
        MakeKey(value).ToBigEndianBytes(bytes.data());
#else
        bytes = MakeKey(value);
#endif
    }
};

struct ValueSlice {
    uint64_t value = 0;
    Slice slice;

    explicit ValueSlice(uint64_t v) : value(v), slice(reinterpret_cast<const char*>(&value), sizeof(value)) {}
};

struct WorkloadConfig {
    uint64_t seed = 20260303;
    uint64_t key_space = 512;
    uint64_t first_phase_ops = 384;
    uint64_t second_phase_ops = 256;
    uint64_t cycle_interval = 64;
    size_t pool_size = 128 * SEGMENT_SIZE;
    int put_ratio = 60;
    int delete_ratio = 20;
};

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_l1_structure_") + std::to_string(getpid()) + ".pool";
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

void PutValue(MYDBClient& client, uint64_t key_value, uint64_t value) {
    KeySlice key(key_value);
    ValueSlice val(value);
    Check(client.Put(key.slice, val.slice), "Put should succeed");
}

void DeleteValue(MYDBClient& client, uint64_t key_value) {
    KeySlice key(key_value);
    Check(client.Delete(key.slice), "Delete should succeed");
}

void ValidateL1Structure(MYDB& db, const std::string& stage) {
    Version* version = db.current_version_;
    Check(version->DebugValidateLevel1Structure(), stage + ": L1HybridIndex::Validate should succeed");

    std::vector<SubtreeRecord> local_fragments;
    version->DebugExportLevel1LocalFragments(local_fragments);

    RecordRouteKeyLess route_less;
    for (size_t i = 1; i < local_fragments.size(); ++i) {
        Check(!route_less(local_fragments[i], local_fragments[i - 1]),
              stage + ": exported local fragments must stay sorted by RecordRouteKeyLess");
    }

    std::unordered_set<uint64_t> seen_prefixes;
    for (const auto& record : local_fragments) {
        Check(record.Valid(), stage + ": exported record must be valid");
        Check(KeyTypeLessEq(record.min_key, record.max_key),
              stage + ": exported record min/max order is invalid");
        Check(record.MatchesLocalFragment(record.route_prefix),
              stage + ": local fragment should match clipped prefix-local bounds");

        TaggedPstMeta table;
        Check(version->DebugResolveLevel1Record(record, table),
              stage + ": exported record must resolve to a valid L1 table");
        Check(table.Valid(), stage + ": resolved L1 table must stay valid");
        Check(CompareKeyType(record.min_key, table.meta.MinKey()) == 0,
              stage + ": record min_key must match PST meta");
        Check(CompareKeyType(record.max_key, table.meta.MaxKey()) == 0,
              stage + ": record max_key must match PST meta");
        Check(record.seq_no == table.meta.seq_no_,
              stage + ": record seq_no must match PST meta");
        Check(record.TouchesPrefix(record.route_prefix),
              stage + ": exported fragment must touch its route prefix");
        seen_prefixes.insert(record.route_prefix);

        std::vector<SubtreeRecord> overlap_records;
        std::vector<uint64_t> overlap_blocks;
        std::vector<TaggedPstMeta> overlaps;
        Check(version->PickOverlappedL1Records(record.min_key, record.max_key, overlap_records, overlap_blocks),
              stage + ": PickOverlappedL1Records should succeed");
        if (!overlap_blocks.empty()) {
            version->ResolveL1BlocksToTables(overlap_blocks, overlaps);
        } else {
            version->ResolveL1RecordsToTables(overlap_records, overlaps);
        }

        bool found = false;
        for (const auto& overlap : overlaps) {
            if (overlap.meta.datablock_ptr_ == table.meta.datablock_ptr_ &&
                overlap.meta.seq_no_ == table.meta.seq_no_) {
                found = true;
                break;
            }
        }
        Check(found, stage + ": exported record must be reachable through overlap routing");
    }

    if (!local_fragments.empty()) {
        Check(seen_prefixes.size() >= 2, stage + ": prefix-routing workload should materialize multiple prefixes");
    }

    std::vector<SubtreeRecord> table_records;
    version->DebugExportLevel1Records(table_records);
    RecordMaxKeyLess table_less;
    std::unordered_set<uint64_t> seen_table_idx;
    for (size_t i = 1; i < table_records.size(); ++i) {
        Check(!table_less(table_records[i], table_records[i - 1]),
              stage + ": exported table records must stay sorted by RecordMaxKeyLess");
    }
    for (const auto& record : table_records) {
        Check(seen_table_idx.insert(record.table_idx).second,
              stage + ": exported table view should not duplicate table_idx");
    }
}

void FlushCompactAndValidate(MYDB& db, const std::string& stage_prefix) {
    Check(db.BGFlush(), stage_prefix + ": BGFlush should succeed");
    Check(db.BGCompaction(), stage_prefix + ": BGCompaction should succeed");
    Check(db.current_version_->GetLevelSize(1) > 0, stage_prefix + ": L1 should contain tables");
    ValidateL1Structure(db, stage_prefix + ": structure");
}

void RunPhase(MYDB& db,
              MYDBClient& client,
              std::mt19937_64& rng,
              const WorkloadConfig& cfg,
              uint64_t op_count,
              uint64_t value_base,
              const std::string& phase_name) {
    std::uniform_int_distribution<uint64_t> key_dist(0, cfg.key_space - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);

    bool dirty = false;
    for (uint64_t i = 0; i < op_count; ++i) {
        const uint64_t key = key_dist(rng);
        const int op = op_dist(rng);

        if (op < cfg.put_ratio) {
            PutValue(client, key, value_base + i * 19 + key);
            dirty = true;
        } else if (op < cfg.put_ratio + cfg.delete_ratio) {
            DeleteValue(client, key);
            dirty = true;
        }

        if ((i + 1) % cfg.cycle_interval == 0 && dirty) {
            FlushCompactAndValidate(db, phase_name + ": cycle " + std::to_string(i + 1));
            dirty = false;
        }
    }

    if (dirty) {
        FlushCompactAndValidate(db, phase_name + ": final cycle");
    }
}

void RunExperiment() {
    const WorkloadConfig cfg;
    const std::string pool_path = MakeTempPoolPath();
    RemovePoolFiles(pool_path);

    std::mt19937_64 rng(cfg.seed);

    {
        MYDBConfig db_cfg(pool_path);
        db_cfg.pm_pool_size = cfg.pool_size;
        db_cfg.recover = false;
        db_cfg.use_direct_io = false;

        MYDB db(db_cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
        auto client = db.GetClient(1);

        RunPhase(db, *client, rng, cfg, cfg.first_phase_ops, 300000, "phase1");
        ValidateL1Structure(db, "before recovery");
    }

    {
        MYDBConfig db_cfg(pool_path);
        db_cfg.pm_pool_size = cfg.pool_size;
        db_cfg.recover = true;
        db_cfg.use_direct_io = false;

        MYDB db(db_cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
        auto client = db.GetClient(1);

        ValidateL1Structure(db, "after recovery");
        RunPhase(db, *client, rng, cfg, cfg.second_phase_ops, 400000, "phase2");
        ValidateL1Structure(db, "final");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    RunExperiment();
    std::cout << "[l1_structure_consistency_regression] all checks passed" << std::endl;
    return 0;
}
