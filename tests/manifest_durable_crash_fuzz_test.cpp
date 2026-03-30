#include "db.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[manifest_durable_crash_fuzz_test] " << message << std::endl;
    std::exit(1);
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Fail(message);
    }
}

KeyType MakeKey(uint64_t value) {
#if defined(FLOWKV_KEY16)
    return Key16{0, value};
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

struct KVPair {
    uint64_t key = 0;
    uint64_t value = 0;
};

std::string MakePoolPath(size_t round_id) {
    return std::string("/tmp/flowkv_manifest_durable_fuzz_") +
           std::to_string(getpid()) + "_" + std::to_string(round_id) + ".pool";
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

void ExpectFound(MYDBClient& client, uint64_t key, uint64_t expected_value, const std::string& stage) {
    uint64_t actual = 0;
    if (!GetValue(client, key, actual)) {
        Fail(stage + ": missing key=" + std::to_string(key));
    }
    if (actual != expected_value) {
        Fail(stage + ": value mismatch for key=" + std::to_string(key));
    }
}

void RunCrashChild(const std::string& pool_path,
                   const char* failpoint,
                   const std::vector<KVPair>& kvs) {
    Check(setenv("FLOWKV_MANIFEST_TXN_FAILPOINT", failpoint, 1) == 0,
          "setenv failpoint failed");

    MYDBConfig cfg(pool_path);
    cfg.pm_pool_size = 64 * SEGMENT_SIZE;
    cfg.recover = false;
    cfg.use_direct_io = false;

    MYDB db(cfg);
    db.StopBackgroundTriggerForTesting();
    db.SetCompactionEnabled(false);
    auto client = db.GetClient(1);

    for (const auto& kv : kvs) {
        PutValue(*client, kv.key, kv.value);
    }
    Check(db.BGFlush(), "BGFlush should succeed before crash");

    // Expected to crash in manifest durable failpoint.
    (void)db.BGCompaction();
    _exit(0);
}

void ValidateRecovery(const std::string& pool_path,
                      const std::vector<KVPair>& kvs,
                      const std::string& stage_prefix) {
    MYDBConfig cfg(pool_path);
    cfg.pm_pool_size = 64 * SEGMENT_SIZE;
    cfg.recover = true;
    cfg.use_direct_io = false;

    MYDB db(cfg);
    db.StopBackgroundTriggerForTesting();
    db.SetCompactionEnabled(false);
    auto client = db.GetClient(1);

    for (const auto& kv : kvs) {
        ExpectFound(*client, kv.key, kv.value, stage_prefix);
    }
}

void RunOneRound(size_t round_id, const char* failpoint) {
    const std::string pool_path = MakePoolPath(round_id);
    RemovePoolFiles(pool_path);

    std::vector<KVPair> kvs;
    kvs.reserve(24);
    const uint64_t base = static_cast<uint64_t>(round_id + 1) * 10000ULL;
    for (uint64_t i = 0; i < 24; ++i) {
        kvs.push_back(KVPair{
            .key = base + i * 2ULL + 1ULL,
            .value = base * 3ULL + i * 7ULL,
        });
    }

    const pid_t pid = fork();
    Check(pid >= 0, "fork failed");
    if (pid == 0) {
        RunCrashChild(pool_path, failpoint, kvs);
        _exit(0);
    }

    int status = 0;
    Check(waitpid(pid, &status, 0) == pid, "waitpid failed");
    Check(WIFEXITED(status), "child must exit");
    Check(WEXITSTATUS(status) == 99, "child must exit via failpoint");

    ValidateRecovery(pool_path, kvs, "first_recover_round_" + std::to_string(round_id));
    ValidateRecovery(pool_path, kvs, "second_recover_round_" + std::to_string(round_id));

    RemovePoolFiles(pool_path);
}

void RunCrashFuzz() {
    constexpr size_t kRounds = 10;
    constexpr const char* kFailpoints[2] = {
        "after_prepare_sync",
        "after_apply_before_clear",
    };

    std::mt19937_64 rng(20260313ULL);
    std::uniform_int_distribution<int> picker(0, 1);

    for (size_t round = 0; round < kRounds; ++round) {
        const char* failpoint = kFailpoints[picker(rng)];
        RunOneRound(round, failpoint);
    }
}

}  // namespace

int main() {
    RunCrashFuzz();
    std::cout << "[manifest_durable_crash_fuzz_test] all tests passed" << std::endl;
    return 0;
}
