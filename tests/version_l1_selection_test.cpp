#include "db/compaction/version.h"
#include "db/allocator/segment_allocator.h"
#include "db/pst_builder.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[version_l1_selection_test] " << message << std::endl;
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

KeyType MakeKey(uint64_t prefix, uint64_t suffix) {
#if defined(FLOWKV_KEY16)
    return flowkv::hybrid_l1::ComposeKey(prefix, suffix);
#else
    (void)prefix;
    return __builtin_bswap64(suffix);
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

TaggedPstMeta MakeTable(uint64_t min_key, uint64_t max_key, uint64_t data_ptr, uint32_t seq_no) {
    TaggedPstMeta table;
    table.level = 1;
    table.manifest_position = data_ptr;
    table.meta.datablock_ptr_ = data_ptr;
#if defined(FLOWKV_KEY16)
    table.meta.min_key_hi = 0;
    table.meta.min_key_lo = min_key;
    table.meta.max_key_hi = 0;
    table.meta.max_key_lo = max_key;
#else
    table.meta.min_key_ = MakeKey(min_key);
    table.meta.max_key_ = MakeKey(max_key);
#endif
    table.meta.seq_no_ = seq_no;
    table.meta.entry_num_ = 1;
    return table;
}

TaggedPstMeta MakeTable(uint64_t min_prefix,
                        uint64_t min_suffix,
                        uint64_t max_prefix,
                        uint64_t max_suffix,
                        uint64_t data_ptr,
                        uint32_t seq_no) {
    TaggedPstMeta table;
    table.level = 1;
    table.manifest_position = data_ptr;
    table.meta.datablock_ptr_ = data_ptr;
#if defined(FLOWKV_KEY16)
    table.meta.min_key_hi = min_prefix;
    table.meta.min_key_lo = min_suffix;
    table.meta.max_key_hi = max_prefix;
    table.meta.max_key_lo = max_suffix;
#else
    table.meta.min_key_ = MakeKey(min_suffix);
    table.meta.max_key_ = MakeKey(max_suffix);
#endif
    table.meta.seq_no_ = seq_no;
    table.meta.entry_num_ = 1;
    return table;
}

bool AddEntryToBuilder(PSTBuilder& builder, const KeyType& key, const FixedValue16& value) {
#if defined(FLOWKV_KEY16)
    uint8_t key_bytes[16];
    key.ToBigEndianBytes(key_bytes);
    return builder.AddEntry(
        Slice(reinterpret_cast<const char*>(key_bytes), sizeof(key_bytes)),
        Slice(reinterpret_cast<const char*>(&value), sizeof(value)));
#else
    return builder.AddEntry(
        Slice(&key),
        Slice(reinterpret_cast<const char*>(&value), sizeof(value)));
#endif
}

TaggedPstMeta BuildTableOnDisk(SegmentAllocator& allocator,
                               uint64_t min_prefix,
                               uint64_t min_suffix,
                               uint64_t max_prefix,
                               uint64_t max_suffix,
                               uint32_t seq_no) {
    PSTBuilder builder(&allocator);
    const KeyType min_key = MakeKey(min_prefix, min_suffix);
    const KeyType max_key = MakeKey(max_prefix, max_suffix);
    Check(AddEntryToBuilder(builder, min_key, FixedValue16{seq_no, 0}),
          "failed to add min key to PSTBuilder");
    if (CompareKeyType(min_key, max_key) != 0) {
        Check(AddEntryToBuilder(builder, max_key, FixedValue16{seq_no + 1, 0}),
              "failed to add max key to PSTBuilder");
    }
    PSTMeta meta = builder.Flush();
    Check(meta.Valid(), "failed to flush PSTBuilder");
    meta.seq_no_ = seq_no;
    TaggedPstMeta table;
    table.level = 1;
    table.manifest_position = meta.datablock_ptr_;
    table.meta = meta;
    return table;
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
    const int min_cmp = CompareKeyType(lhs.meta.MinKey(), rhs.meta.MinKey());
    if (min_cmp != 0) {
        return min_cmp < 0;
    }
    return lhs.meta.datablock_ptr_ < rhs.meta.datablock_ptr_;
}

std::vector<TaggedPstMeta> SortedActiveTables(const std::vector<TaggedPstMeta>& tables) {
    std::vector<TaggedPstMeta> active;
    for (const auto& table : tables) {
        if (table.Valid()) {
            active.push_back(table);
        }
    }
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
                                             const KeyType& min_key,
                                             const KeyType& max_key) {
    std::vector<TaggedPstMeta> sorted = SortedActiveTables(tables);
    std::vector<TaggedPstMeta> out;
    auto it = std::lower_bound(
        sorted.begin(), sorted.end(), min_key,
        [](const TaggedPstMeta& table, const KeyType& rhs) {
            return CompareKeyType(table.meta.MaxKey(), rhs) < 0;
        });

    while (it != sorted.end()) {
        if (CompareKeyType(it->meta.MinKey(), max_key) > 0 &&
            CompareKeyType(it->meta.MaxKey(), max_key) > 0) {
            break;
        }
        if (KeyTypeLessEq(it->meta.MinKey(), max_key) && KeyTypeLessEq(min_key, it->meta.MaxKey())) {
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
        Check(SameTable(actual[i], expected[i]), context + ": table mismatch at index " + std::to_string(i));
    }
}

void CheckCoversExpected(const std::vector<TaggedPstMeta>& actual,
                         const std::vector<TaggedPstMeta>& expected,
                         const std::string& context) {
    for (size_t i = 0; i < expected.size(); ++i) {
        const bool found = std::any_of(
            actual.begin(), actual.end(),
            [&](const TaggedPstMeta& table) { return SameTable(table, expected[i]); });
        Check(found, context + ": missing expected table at index " + std::to_string(i));
    }
}

std::string MakeTempPoolPath(const char* test_name) {
    return std::string("/tmp/") + test_name + "_" + std::to_string(getpid()) + ".pool";
}

void RemovePoolFiles(const std::string& pool_path) {
    std::error_code ec;
    std::filesystem::remove(pool_path, ec);
    std::filesystem::remove(pool_path + ".manifest", ec);
}

void RunLookupComparison(Version& version,
                         SegmentAllocator& allocator,
                         const std::vector<TaggedPstMeta>& baseline_tables,
                         const KeyType& key,
                         const std::string& label) {
    PSTReader reader(&allocator);
    std::vector<TaggedPstMeta> actual;
    KeySlice key_slice(0);
#if defined(FLOWKV_KEY16)
    key.ToBigEndianBytes(key_slice.bytes.data());
#else
    key_slice.bytes = key;
#endif
    RowIterator* iter = version.GetLevel1Iter(key_slice.slice, &reader, actual);
    delete iter;

    const auto expected = BaselineLookupCandidates(baseline_tables, key, 2);
    CheckSameSequence(actual, expected, "lookup " + label);
}

void RunRangeComparison(Version& version,
                        const std::vector<TaggedPstMeta>& baseline_tables,
                        const KeyType& min_key,
                        const KeyType& max_key,
                        const std::string& label) {
    std::vector<TaggedPstMeta> actual;
    Check(version.PickOverlappedL1Tables(min_key, max_key, actual),
          "PickOverlappedL1Tables should succeed");
    const auto expected = BaselineRangeScan(baseline_tables, min_key, max_key);
    CheckCoversExpected(actual, expected, "range " + label);
}

void RunRecordRangeComparison(Version& version,
                              const std::vector<TaggedPstMeta>& baseline_tables,
                              const KeyType& min_key,
                              const KeyType& max_key,
                              const std::string& label) {
    std::vector<flowkv::hybrid_l1::SubtreeRecord> records;
    std::vector<uint64_t> actual_blocks;
    Check(version.PickOverlappedL1Records(min_key, max_key, records, actual_blocks),
          "PickOverlappedL1Records should succeed");

    const auto expected_tables = BaselineRangeScan(baseline_tables, min_key, max_key);
    std::set<uint64_t> expected_block_set;
    for (const auto& table : expected_tables) {
        if (!table.Valid()) {
            continue;
        }
        expected_block_set.insert(
            flowkv::hybrid_l1::SubtreeRecord::EncodeKvBlockPtr(table.meta.datablock_ptr_));
    }
    std::sort(actual_blocks.begin(), actual_blocks.end());
    for (const uint64_t expected_block : expected_block_set) {
        const bool found = std::binary_search(actual_blocks.begin(), actual_blocks.end(), expected_block);
        Check(found, "record range " + label + ": missing expected unique block");
    }

    if (!expected_block_set.empty()) {
        bool has_readable_window = false;
        for (const auto& record : records) {
            const auto window = record.LeafWindow();
            if (window.count != 0) {
                has_readable_window = true;
                break;
            }
        }
        Check(has_readable_window, "record range " + label + ": expect readable window fragments");
    }
}

void TestVersionL1SelectionMatchesBaseline() {
    const std::string pool_path = MakeTempPoolPath("flowkv_version_l1_selection_test");
    RemovePoolFiles(pool_path);

    {
        SegmentAllocator allocator(pool_path, 16 * SEGMENT_SIZE, false, false);
        Version version(&allocator);
        std::vector<TaggedPstMeta> baseline_tables;

        const std::vector<TaggedPstMeta> initial_tables = {
            BuildTableOnDisk(allocator, 0x10, 10, 0x10, 19, 1),
            BuildTableOnDisk(allocator, 0x10, 20, 0x10, 35, 1),
            BuildTableOnDisk(allocator, 0x10, 25, 0x10, 35, 3),
            BuildTableOnDisk(allocator, 0x10, 36, 0x10, 50, 2),
            BuildTableOnDisk(allocator, 0x11, 0, 0x11, 10, 1),
            BuildTableOnDisk(allocator, 0x11, 20, 0x11, 40, 1),
            BuildTableOnDisk(allocator, 0x12, 5, 0x12, 20, 1),
            BuildTableOnDisk(allocator, 0x13, 0, 0x13, 16, 1)
        };

        for (const auto& table : initial_tables) {
            version.InsertTableToL1(table);
            baseline_tables.push_back(table);
        }

        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x10, 10), "p10-s10");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x10, 28), "p10-s28");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x10, 36), "p10-s36");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x11, 5), "p11-s5");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x11, 18), "p11-s18");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x12, 8), "p12-s8");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x13, 8), "p13-s8");

        RunRangeComparison(version, baseline_tables, MakeKey(0x10, 18), MakeKey(0x10, 37), "single-prefix");
        RunRangeComparison(version, baseline_tables, MakeKey(0x10, 45), MakeKey(0x11, 25), "cross-p10-p11");
        RunRangeComparison(version, baseline_tables, MakeKey(0x11, 18), MakeKey(0x13, 8), "cross-multi-prefix");
        RunRangeComparison(version, baseline_tables, MakeKey(0x13, 32), MakeKey(0x13, 48), "miss");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x10, 18), MakeKey(0x10, 37),
                                 "single-prefix");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x10, 45), MakeKey(0x11, 25),
                                 "cross-p10-p11");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x11, 18), MakeKey(0x13, 8),
                                 "cross-multi-prefix");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x13, 32), MakeKey(0x13, 48), "miss");

        Check(version.DeleteTableInL1(initial_tables[1].meta), "deleting an existing L1 table should succeed");
        baseline_tables[1].meta = PSTMeta::InvalidTable();

        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x10, 28), "p10-s28-after-delete");
        RunRangeComparison(version, baseline_tables, MakeKey(0x10, 18), MakeKey(0x10, 37),
                           "single-prefix-after-delete");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x10, 18), MakeKey(0x10, 37),
                                 "single-prefix-after-delete");

        Check(version.DeleteTableInL1(initial_tables[6].meta),
              "deleting an existing cross-partition L1 table should succeed");
        baseline_tables[6].meta = PSTMeta::InvalidTable();

        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x12, 8), "p12-s8-after-delete");
        RunLookupComparison(version, allocator, baseline_tables, MakeKey(0x13, 8), "p13-s8-after-delete");
        RunRecordRangeComparison(version, baseline_tables, MakeKey(0x12, 0), MakeKey(0x13, 16),
                                 "cross-after-delete");
    }

    RemovePoolFiles(pool_path);
}

}  // namespace

int main() {
    TestVersionL1SelectionMatchesBaseline();
    std::cout << "[version_l1_selection_test] all tests passed" << std::endl;
    return 0;
}
