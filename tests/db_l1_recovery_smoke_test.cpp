#include "db.h"
#include "db/compaction/version.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[db_l1_recovery_smoke_test] " << message << std::endl;
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

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_db_l1_recovery_smoke_") + std::to_string(getpid()) + ".pool";
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

uint64_t GetValue(MYDBClient& client, uint64_t key_value) {
    KeySlice key(key_value);
    std::array<char, 32> value_buf{};
    Slice out(value_buf.data(), value_buf.size());
    Check(client.Get(key.slice, out), "Get should succeed");
    uint64_t actual = 0;
    std::memcpy(&actual, value_buf.data(), sizeof(actual));
    return actual;
}

void PutValue(MYDBClient& client, uint64_t key_value, uint64_t value) {
    KeySlice key(key_value);
    ValueSlice val(value);
    Check(client.Put(key.slice, val.slice), "Put should succeed");
}

void TestRecoverAndContinueCompaction() {
    const std::string pool_path = MakeTempPoolPath();
    RemovePoolFiles(pool_path);

    {
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

        Check(db.BGFlush(), "initial BGFlush should succeed");
        Check(db.BGCompaction(), "initial BGCompaction should succeed");
        Check(db.current_version_->GetLevelSize(1) > 0, "initial compaction should populate L1");

        Check(GetValue(*client, 10) == 1010, "initial key 10 value mismatch");
        Check(GetValue(*client, 20) == 1020, "initial key 20 value mismatch");
        Check(GetValue(*client, 30) == 1030, "initial key 30 value mismatch");
        Check(GetValue(*client, 40) == 1040, "initial key 40 value mismatch");
    }

    {
        MYDBConfig cfg(pool_path);
        cfg.pm_pool_size = 64 * SEGMENT_SIZE;
        cfg.recover = true;
        cfg.use_direct_io = false;

        MYDB db(cfg);
        db.StopBackgroundTriggerForTesting();
        db.SetCompactionEnabled(false);

        auto client = db.GetClient(1);
        Check(db.current_version_->GetLevelSize(1) > 0, "recovered DB should rebuild L1");
        Check(GetValue(*client, 10) == 1010, "recovered key 10 value mismatch");
        Check(GetValue(*client, 20) == 1020, "recovered key 20 value mismatch");
        Check(GetValue(*client, 30) == 1030, "recovered key 30 value mismatch");
        Check(GetValue(*client, 40) == 1040, "recovered key 40 value mismatch");

        PutValue(*client, 20, 2020);
        PutValue(*client, 50, 2050);

        Check(db.BGFlush(), "post-recovery BGFlush should succeed");
        Check(db.BGCompaction(), "post-recovery BGCompaction should succeed");

        Check(GetValue(*client, 10) == 1010, "post-recovery key 10 value mismatch");
        Check(GetValue(*client, 20) == 2020, "post-recovery updated key 20 value mismatch");
        Check(GetValue(*client, 30) == 1030, "post-recovery key 30 value mismatch");
        Check(GetValue(*client, 40) == 1040, "post-recovery key 40 value mismatch");
        Check(GetValue(*client, 50) == 2050, "post-recovery new key 50 value mismatch");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    TestRecoverAndContinueCompaction();
    std::cout << "[db_l1_recovery_smoke_test] all tests passed" << std::endl;
    return 0;
}
