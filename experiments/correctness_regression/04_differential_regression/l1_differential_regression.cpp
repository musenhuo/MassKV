#include "db.h"
#include "db/compaction/version.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[l1_differential_regression] " << message << std::endl;
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
    return __builtin_bswap64(value);
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
    uint64_t seed = 20260305;
    uint64_t key_space = 320;
    uint64_t first_phase_ops = 256;
    uint64_t second_phase_ops = 192;
    uint64_t validate_interval = 64;
    size_t pool_size = 128 * SEGMENT_SIZE;
    int put_ratio = 60;
    int delete_ratio = 20;
};

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_l1_diff_") + std::to_string(getpid()) + ".pool";
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

bool SameTable(const TaggedPstMeta& lhs, const TaggedPstMeta& rhs) {
    return lhs.meta.datablock_ptr_ == rhs.meta.datablock_ptr_ &&
           CompareKeyType(lhs.meta.MinKey(), rhs.meta.MinKey()) == 0 &&
           CompareKeyType(lhs.meta.MaxKey(), rhs.meta.MaxKey()) == 0 &&
           lhs.meta.seq_no_ == rhs.meta.seq_no_;
}

bool BaselineLess(const TaggedPstMeta& lhs, const TaggedPstMeta& rhs) {
    const int cmp = CompareKeyType(lhs.meta.MaxKey(), rhs.meta.MaxKey());
    if (cmp != 0) {
        return cmp < 0;
    }
    if (lhs.meta.seq_no_ != rhs.meta.seq_no_) {
        return lhs.meta.seq_no_ > rhs.meta.seq_no_;
    }
    return lhs.meta.datablock_ptr_ < rhs.meta.datablock_ptr_;
}

std::vector<TaggedPstMeta> SortedActiveTables(const std::vector<TaggedPstMeta>& tables) {
    std::vector<TaggedPstMeta> active = tables;
    std::sort(active.begin(), active.end(), BaselineLess);
    return active;
}

std::vector<TaggedPstMeta> BaselineLookupCandidates(const std::vector<TaggedPstMeta>& tables,
                                                    const KeyType& key,
                                                    size_t limit) {
    std::vector<TaggedPstMeta> sorted = SortedActiveTables(tables);
    std::vector<TaggedPstMeta> out;
    auto it = std::lower_bound(
        sorted.begin(), sorted.end(), key,
        [](const TaggedPstMeta& table, const KeyType& rhs) {
            return CompareKeyType(table.meta.MaxKey(), rhs) < 0;
        });

    while (it != sorted.end() && out.size() < limit) {
        if (KeyTypeLessEq(it->meta.MinKey(), key) && KeyTypeLessEq(key, it->meta.MaxKey())) {
            out.push_back(*it);
        } else if (CompareKeyType(it->meta.MinKey(), key) > 0) {
            break;
        }
        ++it;
    }
    return out;
}

std::vector<TaggedPstMeta> BaselineRangeScan(const std::vector<TaggedPstMeta>& tables,
                                             const KeyType& min,
                                             const KeyType& max) {
    std::vector<TaggedPstMeta> sorted = SortedActiveTables(tables);
    std::vector<TaggedPstMeta> out;
    auto it = std::lower_bound(
        sorted.begin(), sorted.end(), min,
        [](const TaggedPstMeta& table, const KeyType& rhs) {
            return CompareKeyType(table.meta.MaxKey(), rhs) < 0;
        });

    while (it != sorted.end()) {
        if (CompareKeyType(it->meta.MinKey(), max) > 0 &&
            CompareKeyType(it->meta.MaxKey(), max) > 0) {
            break;
        }
        if (KeyTypeLessEq(it->meta.MinKey(), max) && KeyTypeLessEq(min, it->meta.MaxKey())) {
            out.push_back(*it);
        }
        ++it;
    }
    return out;
}

void CheckSameSequence(const std::vector<TaggedPstMeta>& actual,
                       const std::vector<TaggedPstMeta>& expected,
                       const std::string& context) {
    Check(actual.size() == expected.size(), context + ": size mismatch");
    for (size_t i = 0; i < expected.size(); ++i) {
        Check(SameTable(actual[i], expected[i]),
              context + ": table mismatch at index " + std::to_string(i));
    }
}

void ValidateDifferential(Version& version, uint64_t key_space, const std::string& stage) {
    std::vector<TaggedPstMeta> active_tables;
    version.DebugExportActiveLevel1Tables(active_tables);

    for (uint64_t key = 0; key < key_space; ++key) {
        std::vector<TaggedPstMeta> actual;
        version.DebugCollectLevel1Candidates(MakeKey(key), actual);
        const auto expected = BaselineLookupCandidates(active_tables, MakeKey(key), 2);
        CheckSameSequence(actual, expected, stage + ": lookup key=" + std::to_string(key));
    }

    for (uint64_t start = 0; start < key_space; start += 17) {
        const uint64_t end = std::min<uint64_t>(key_space - 1, start + 31);
        std::vector<TaggedPstMeta> actual;
        version.DebugCollectLevel1Overlaps(MakeKey(start), MakeKey(end), actual);
        const auto expected = BaselineRangeScan(active_tables, MakeKey(start), MakeKey(end));
        CheckSameSequence(actual, expected,
                          stage + ": range [" + std::to_string(start) + "," + std::to_string(end) + "]");
    }
}

void FlushCompactAndDiff(MYDB& db, uint64_t key_space, const std::string& stage_prefix) {
    Check(db.BGFlush(), stage_prefix + ": BGFlush should succeed");
    Check(db.BGCompaction(), stage_prefix + ": BGCompaction should succeed");
    ValidateDifferential(*db.current_version_, key_space, stage_prefix + ": differential");
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
            PutValue(client, key, value_base + i * 11 + key);
            dirty = true;
        } else if (op < cfg.put_ratio + cfg.delete_ratio) {
            DeleteValue(client, key);
            dirty = true;
        }

        if ((i + 1) % cfg.validate_interval == 0 && dirty) {
            FlushCompactAndDiff(db, cfg.key_space, phase_name + ": cycle " + std::to_string(i + 1));
            dirty = false;
        }
    }

    if (dirty) {
        FlushCompactAndDiff(db, cfg.key_space, phase_name + ": final cycle");
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

        RunPhase(db, *client, rng, cfg, cfg.first_phase_ops, 700000, "phase1");
        ValidateDifferential(*db.current_version_, cfg.key_space, "before recovery");
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

        ValidateDifferential(*db.current_version_, cfg.key_space, "after recovery");
        RunPhase(db, *client, rng, cfg, cfg.second_phase_ops, 800000, "phase2");
        ValidateDifferential(*db.current_version_, cfg.key_space, "final");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    RunExperiment();
    std::cout << "[l1_differential_regression] all checks passed" << std::endl;
    return 0;
}
