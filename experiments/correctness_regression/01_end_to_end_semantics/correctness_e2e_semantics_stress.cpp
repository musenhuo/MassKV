#include "db.h"
#include "db/compaction/version.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[correctness_e2e_semantics_stress] " << message << std::endl;
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
    uint64_t seed = 20260302;
    uint64_t key_space = 256;
    uint64_t first_phase_ops = 320;
    uint64_t second_phase_ops = 192;
    uint64_t validate_interval = 64;
    size_t pool_size = 128 * SEGMENT_SIZE;
    int put_ratio = 55;
    int delete_ratio = 20;
};

using ShadowModel = std::unordered_map<uint64_t, uint64_t>;

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_correctness_e2e_") + std::to_string(getpid()) + ".pool";
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
            if (found) {
                detail += ", actual=" + std::to_string(actual);
            }
            if (expected_found) {
                detail += ", expected=" + std::to_string(it->second);
            }
            Fail(detail);
        }
        if (found && actual != it->second) {
            Fail(stage + ": value mismatch at key=" + std::to_string(key));
        }
    }

    Check(db.current_version_->GetLevel0TreeNum() >= 0, stage + ": invalid L0 tree count");
    Check(db.current_version_->GetLevelSize(1) >= 0, stage + ": invalid L1 size");
}

void FlushCompactAndValidate(MYDB& db,
                             MYDBClient& client,
                             const ShadowModel& shadow,
                             uint64_t key_space,
                             const std::string& stage_prefix) {
    Check(db.BGFlush(), stage_prefix + ": BGFlush should succeed");
    ValidateShadow(db, client, shadow, key_space, stage_prefix + ": after flush");

    Check(db.BGCompaction(), stage_prefix + ": BGCompaction should succeed");
    ValidateShadow(db, client, shadow, key_space, stage_prefix + ": after compaction");
}

void RunPhase(MYDB& db,
              MYDBClient& client,
              ShadowModel& shadow,
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
            const uint64_t value = value_base + i * 17 + key;
            PutValue(client, key, value);
            shadow[key] = value;
            dirty = true;
        } else if (op < cfg.put_ratio + cfg.delete_ratio) {
            DeleteValue(client, key);
            shadow.erase(key);
            dirty = true;
        } else {
            uint64_t actual = 0;
            const bool found = GetValue(client, key, actual);
            const auto it = shadow.find(key);
            const bool expected_found = it != shadow.end();
            if (found != expected_found) {
                Fail(phase_name + ": read presence mismatch at op=" + std::to_string(i));
            }
            if (found && actual != it->second) {
                Fail(phase_name + ": read value mismatch at op=" + std::to_string(i));
            }
        }

        if ((i + 1) % cfg.validate_interval == 0) {
            ValidateShadow(db, client, shadow, cfg.key_space,
                           phase_name + ": validate interval " + std::to_string(i + 1));
            if (dirty) {
                FlushCompactAndValidate(db, client, shadow, cfg.key_space,
                                        phase_name + ": cycle " + std::to_string(i + 1));
                dirty = false;
            }
        }
    }

    ValidateShadow(db, client, shadow, cfg.key_space, phase_name + ": phase end");
    if (dirty) {
        FlushCompactAndValidate(db, client, shadow, cfg.key_space, phase_name + ": final cycle");
    }
}

void RunExperiment() {
    const WorkloadConfig cfg;
    const std::string pool_path = MakeTempPoolPath();
    RemovePoolFiles(pool_path);

    ShadowModel shadow;
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

        RunPhase(db, *client, shadow, rng, cfg, cfg.first_phase_ops, 100000, "phase1");
        ValidateShadow(db, *client, shadow, cfg.key_space, "before recovery");
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

        ValidateShadow(db, *client, shadow, cfg.key_space, "after recovery");
        RunPhase(db, *client, shadow, rng, cfg, cfg.second_phase_ops, 200000, "phase2");
        ValidateShadow(db, *client, shadow, cfg.key_space, "final");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    RunExperiment();
    std::cout << "[correctness_e2e_semantics_stress] all checks passed" << std::endl;
    return 0;
}
