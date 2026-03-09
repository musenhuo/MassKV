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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <unistd.h>

namespace {

using flowkv::hybrid_l1::RecordMaxKeyLess;
using flowkv::hybrid_l1::RecordRouteKeyLess;
using flowkv::hybrid_l1::SubtreeRecord;
using flowkv::hybrid_l1::ExtractPrefix;
using ShadowModel = std::unordered_map<uint64_t, uint64_t>;

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[l1_long_running_cycle_regression] " << message << std::endl;
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
    uint64_t seed = 20260304;
    uint64_t key_space = 384;
    uint64_t rounds = 8;
    uint64_t ops_per_round = 160;
    uint64_t recovery_every = 2;
    size_t pool_size = 128 * SEGMENT_SIZE;
    int put_ratio = 55;
    int delete_ratio = 25;
};

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_l1_long_cycles_") + std::to_string(getpid()) + ".pool";
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

bool GetValue(MYDBClient& client, uint64_t key_value, uint64_t& value_out) {
    KeySlice key(key_value);
    std::array<char, 32> value_buf{};
    Slice out(value_buf.data(), value_buf.size());
    if (!client.Get(key.slice, out)) {
        return false;
    }
    std::memcpy(&value_out, value_buf.data(), sizeof(value_out));
    return true;
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

void ValidateShadow(MYDB& db,
                    MYDBClient& client,
                    const ShadowModel& shadow,
                    uint64_t key_space,
                    const std::string& stage) {
    for (uint64_t key = 0; key < key_space; ++key) {
        uint64_t actual = 0;
        const bool found = GetValue(client, key, actual);
        const auto it = shadow.find(key);
        const bool expected_found = it != shadow.end();
        if (found != expected_found) {
            std::string detail = stage + ": presence mismatch at key=" + std::to_string(key) +
                                 ", found=" + (found ? "true" : "false") +
                                 ", expected_found=" + (expected_found ? "true" : "false");
            Fail(detail);
        }
        if (found && actual != it->second) {
            Fail(stage + ": value mismatch at key=" + std::to_string(key));
        }
    }

    Check(db.current_version_->GetLevel0TreeNum() >= 0, stage + ": invalid L0 tree count");
    Check(db.current_version_->GetLevelSize(1) >= 0, stage + ": invalid L1 size");
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
        Check(table.Valid(), stage + ": resolved table must stay valid");
        Check(CompareKeyType(record.min_key, table.meta.MinKey()) == 0,
              stage + ": record min_key must match PST meta");
        Check(CompareKeyType(record.max_key, table.meta.MaxKey()) == 0,
              stage + ": record max_key must match PST meta");
        Check(record.seq_no == table.meta.seq_no_,
              stage + ": record seq_no must match PST meta");
        seen_prefixes.insert(record.route_prefix);
    }

    if (!local_fragments.empty()) {
        Check(seen_prefixes.size() >= 2, stage + ": long-running workload should keep multiple prefixes active");
    }

    std::vector<SubtreeRecord> records;
    version->DebugExportLevel1Records(records);
    RecordMaxKeyLess less;
    for (size_t i = 1; i < records.size(); ++i) {
        Check(!less(records[i], records[i - 1]),
              stage + ": exported table records must stay sorted by RecordMaxKeyLess");
    }
    std::unordered_set<uint64_t> seen_table_idx;
    for (const auto& record : records) {
        Check(seen_table_idx.insert(record.table_idx).second,
              stage + ": exported table_idx duplicated in L1 route");
    }
}

void ValidateDatabaseState(MYDB& db,
                           MYDBClient& client,
                           const ShadowModel& shadow,
                           uint64_t key_space,
                           const std::string& stage) {
    ValidateShadow(db, client, shadow, key_space, stage + ": shadow");
    ValidateL1Structure(db, stage + ": structure");
}

void RunRound(MYDB& db,
              MYDBClient& client,
              ShadowModel& shadow,
              std::mt19937_64& rng,
              const WorkloadConfig& cfg,
              uint64_t round_idx) {
    std::uniform_int_distribution<uint64_t> key_dist(0, cfg.key_space - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);

    for (uint64_t i = 0; i < cfg.ops_per_round; ++i) {
        const uint64_t key = key_dist(rng);
        const int op = op_dist(rng);

        if (op < cfg.put_ratio) {
            const uint64_t value = 500000 + round_idx * 1000 + i * 13 + key;
            PutValue(client, key, value);
            shadow[key] = value;
        } else if (op < cfg.put_ratio + cfg.delete_ratio) {
            DeleteValue(client, key);
            shadow.erase(key);
        } else {
            uint64_t actual = 0;
            const bool found = GetValue(client, key, actual);
            const auto it = shadow.find(key);
            const bool expected_found = it != shadow.end();
            if (found != expected_found) {
                Fail("round " + std::to_string(round_idx) + ": read presence mismatch at op=" +
                     std::to_string(i));
            }
            if (found && actual != it->second) {
                Fail("round " + std::to_string(round_idx) + ": read value mismatch at op=" +
                     std::to_string(i));
            }
        }
    }

    Check(db.BGFlush(), "round " + std::to_string(round_idx) + ": BGFlush should succeed");
    Check(db.BGCompaction(), "round " + std::to_string(round_idx) + ": BGCompaction should succeed");
    ValidateDatabaseState(db, client, shadow, cfg.key_space,
                          "round " + std::to_string(round_idx) + ": after compaction");
}

void RunExperiment() {
    const WorkloadConfig cfg;
    const std::string pool_path = MakeTempPoolPath();
    RemovePoolFiles(pool_path);

    ShadowModel shadow;
    std::mt19937_64 rng(cfg.seed);

    bool recover = false;
    for (uint64_t round = 0; round < cfg.rounds; ++round) {
        MYDBConfig db_cfg(pool_path);
        db_cfg.pm_pool_size = cfg.pool_size;
        db_cfg.recover = recover;
        db_cfg.use_direct_io = false;

        {
            MYDB db(db_cfg);
            db.StopBackgroundTriggerForTesting();
            db.SetCompactionEnabled(false);
            auto client = db.GetClient(1);

            ValidateDatabaseState(db, *client, shadow, cfg.key_space,
                                  "round " + std::to_string(round) + ": start");
            RunRound(db, *client, shadow, rng, cfg, round);
        }

        if ((round + 1) % cfg.recovery_every == 0) {
            MYDBConfig recover_cfg(pool_path);
            recover_cfg.pm_pool_size = cfg.pool_size;
            recover_cfg.recover = true;
            recover_cfg.use_direct_io = false;

            MYDB recovered_db(recover_cfg);
            recovered_db.StopBackgroundTriggerForTesting();
            recovered_db.SetCompactionEnabled(false);
            auto recovered_client = recovered_db.GetClient(1);
            ValidateDatabaseState(recovered_db, *recovered_client, shadow, cfg.key_space,
                                  "round " + std::to_string(round) + ": after recovery");
        }

        recover = true;
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    RunExperiment();
    std::cout << "[l1_long_running_cycle_regression] all checks passed" << std::endl;
    return 0;
}
