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
    std::cerr << "[db_l1_route_smoke_test] " << message << std::endl;
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

std::string MakeTempPoolPath() {
    return std::string("/tmp/flowkv_db_l1_route_smoke_") + std::to_string(getpid()) + ".pool";
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

void TestPutFlushCompactGet() {
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
        const std::vector<uint64_t> keys = {10, 20, 30, 40, 50, 60};
        std::vector<uint64_t> expected_values;
        expected_values.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            const uint64_t value = 1000 + i;
            expected_values.push_back(value);
            KeySlice key(keys[i]);
            ValueSlice val(value);
            Check(client->Put(key.slice, val.slice), "Put should succeed");
        }

        Check(db.BGFlush(), "BGFlush should succeed");
        Check(db.current_version_->GetLevel0TreeNum() > 0, "flush should produce at least one L0 tree");

        Check(db.BGCompaction(), "BGCompaction should succeed");
        Check(db.current_version_->GetLevel0TreeNum() == 0, "compaction should consume all L0 trees");
        Check(db.current_version_->GetLevelSize(1) > 0, "compaction should populate L1");

        for (size_t i = 0; i < keys.size(); ++i) {
            KeySlice key(keys[i]);
            std::array<char, 32> value_buf{};
            Slice out(value_buf.data(), value_buf.size());
            Check(client->Get(key.slice, out), "Get after compaction should succeed");

            uint64_t actual = 0;
            std::memcpy(&actual, value_buf.data(), sizeof(actual));
            Check(actual == expected_values[i], "Get returned unexpected value after compaction");
        }

        KeySlice missing_key(999);
        std::array<char, 32> missing_buf{};
        Slice missing_out(missing_buf.data(), missing_buf.size());
        Check(!client->Get(missing_key.slice, missing_out), "missing key should not be found");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    TestPutFlushCompactGet();
    std::cout << "[db_l1_route_smoke_test] all tests passed" << std::endl;
    return 0;
}
