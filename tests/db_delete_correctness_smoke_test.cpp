#include "db.h"
#include "db/compaction/version.h"
#include "db/log_reader.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[db_delete_correctness_smoke_test] " << message << std::endl;
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
    return std::string("/tmp/flowkv_delete_smoke_") + std::to_string(getpid()) + ".pool";
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

void ExpectFound(MYDBClient& client, uint64_t key_value, uint64_t expected_value, const std::string& stage) {
    uint64_t actual = 0;
    if (!GetValue(client, key_value, actual)) {
        Fail(stage + ": expected key to exist");
    }
    if (actual != expected_value) {
        Fail(stage + ": unexpected value, actual=" + std::to_string(actual) +
             ", expected=" + std::to_string(expected_value));
    }
}

void ExpectMissing(MYDBClient& client, uint64_t key_value, const std::string& stage) {
    uint64_t actual = 0;
    if (GetValue(client, key_value, actual)) {
        Fail(stage + ": expected key to be deleted");
    }
}

void TestDeleteAcrossFlushCompactionAndRecovery() {
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
        ExpectFound(*client, 20, 1020, "before delete");

        DeleteValue(*client, 20);
        ExpectMissing(*client, 20, "after memtable delete");
        ExpectFound(*client, 10, 1010, "neighbor key after memtable delete");

        Check(db.BGFlush(), "BGFlush after delete should succeed");
        ExpectMissing(*client, 20, "after flush delete");
        ExpectFound(*client, 30, 1030, "other key after flush delete");

        Check(db.BGCompaction(), "BGCompaction after delete should succeed");
        Check(db.current_version_->GetLevelSize(1) > 0, "compaction should populate L1");
        ExpectMissing(*client, 20, "after compaction delete");
        ExpectFound(*client, 10, 1010, "neighbor key after compaction delete");

        PutValue(*client, 20, 2020);
        ExpectFound(*client, 20, 2020, "after reinsertion");

        Check(db.BGFlush(), "BGFlush after reinsertion should succeed");
        Check(db.BGCompaction(), "BGCompaction after reinsertion should succeed");
        ExpectFound(*client, 20, 2020, "after reinsertion compaction");

        DeleteValue(*client, 20);
        ExpectMissing(*client, 20, "after second memtable delete");

        Check(db.BGFlush(), "BGFlush after second delete should succeed");
        Check(db.BGCompaction(), "BGCompaction after second delete should succeed");
        ExpectMissing(*client, 20, "after second delete compaction");
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
        ExpectMissing(*client, 20, "after recovery delete");
        ExpectFound(*client, 10, 1010, "neighbor key after recovery");

        PutValue(*client, 20, 3030);
        ExpectFound(*client, 20, 3030, "after recovery reinsertion");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    TestDeleteAcrossFlushCompactionAndRecovery();
    std::cout << "[db_delete_correctness_smoke_test] all tests passed" << std::endl;
    return 0;
}
