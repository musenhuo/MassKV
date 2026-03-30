#include "db.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[manifest_durable_crash_recovery_smoke_test] " << message << std::endl;
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

std::string MakeTempPoolPath(const std::string& tag) {
    return std::string("/tmp/flowkv_manifest_durable_crash_smoke_") +
           std::to_string(getpid()) + "_" + tag + ".pool";
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

void ExpectFound(MYDBClient& client, uint64_t key_value, uint64_t expected_value, const std::string& stage) {
    uint64_t actual = 0;
    if (!GetValue(client, key_value, actual)) {
        Fail(stage + ": key missing");
    }
    if (actual != expected_value) {
        Fail(stage + ": value mismatch");
    }
}

void RunCrashChild(const std::string& pool_path, const char* failpoint) {
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

    PutValue(*client, 10, 1010);
    PutValue(*client, 20, 1020);
    PutValue(*client, 30, 1030);
    PutValue(*client, 40, 1040);

    Check(db.BGFlush(), "BGFlush should succeed before compaction crash");
    // Expected to crash in manifest commit path.
    (void)db.BGCompaction();

    // If we arrive here, failpoint did not trigger.
    _exit(0);
}

void TestCrashAndRecoverManifestBatchTxnAt(const char* failpoint, const std::string& case_tag) {
    const std::string pool_path = MakeTempPoolPath(case_tag);
    RemovePoolFiles(pool_path);

    const pid_t pid = fork();
    Check(pid >= 0, "fork failed");
    if (pid == 0) {
        RunCrashChild(pool_path, failpoint);
        _exit(0);
    }

    int status = 0;
    Check(waitpid(pid, &status, 0) == pid, "waitpid failed");
    Check(WIFEXITED(status), "child should exit by failpoint");
    Check(WEXITSTATUS(status) == 99, "child should exit with failpoint code 99");

    {
        MYDBConfig cfg(pool_path);
        cfg.pm_pool_size = 64 * SEGMENT_SIZE;
        cfg.recover = true;
        cfg.use_direct_io = false;

        MYDB db(cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
        auto client = db.GetClient(1);

        ExpectFound(*client, 10, 1010, "post-recover key10");
        ExpectFound(*client, 20, 1020, "post-recover key20");
        ExpectFound(*client, 30, 1030, "post-recover key30");
        ExpectFound(*client, 40, 1040, "post-recover key40");
    }

    // Reopen one more time to ensure replay has cleared pending txn state.
    {
        MYDBConfig cfg(pool_path);
        cfg.pm_pool_size = 64 * SEGMENT_SIZE;
        cfg.recover = true;
        cfg.use_direct_io = false;

        MYDB db(cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);
        auto client = db.GetClient(1);

        ExpectFound(*client, 10, 1010, "second recover key10");
        ExpectFound(*client, 20, 1020, "second recover key20");
        ExpectFound(*client, 30, 1030, "second recover key30");
        ExpectFound(*client, 40, 1040, "second recover key40");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    const std::vector<std::pair<const char*, std::string>> scenarios = {
        {"after_prepare_sync", "after_prepare_sync"},
        {"after_apply_before_clear", "after_apply_before_clear"},
    };
    for (const auto& [failpoint, case_tag] : scenarios) {
        TestCrashAndRecoverManifestBatchTxnAt(failpoint, case_tag);
    }
    std::cout << "[manifest_durable_crash_recovery_smoke_test] all tests passed" << std::endl;
    return 0;
}
