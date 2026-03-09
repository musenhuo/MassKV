#include "lib/hybrid_l1/subtree_bptree.h"
#include "lib/hybrid_l1/l1_hybrid_index.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_page.h"
#include "lib/hybrid_l1/subtree_page_store.h"

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <unistd.h>

using flowkv::hybrid_l1::L1SubtreeBPTree;
using flowkv::hybrid_l1::L1HybridIndex;
using flowkv::hybrid_l1::SubtreeRecord;
using flowkv::hybrid_l1::SubtreePageCodec;
using flowkv::hybrid_l1::SubtreePageStore;
using flowkv::hybrid_l1::ExtractPrefix;
using flowkv::hybrid_l1::ExtractSuffix;
using flowkv::hybrid_l1::ComposeKey;

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[hybrid_l1_test] " << message << std::endl;
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
    return ComposeKey(value / kPrefixSpan, value % kPrefixSpan);
#else
    return value;
#endif
}

KeyType MakeKey(uint64_t prefix, uint64_t suffix) {
#if defined(FLOWKV_KEY16)
    return ComposeKey(prefix, suffix);
#else
    (void)prefix;
    return suffix;
#endif
}

SubtreeRecord MakeRecord(uint64_t min_key, uint64_t max_key, uint64_t table_idx, uint32_t seq_no = 1) {
    SubtreeRecord record;
    record.min_key = MakeKey(min_key);
    record.max_key = MakeKey(max_key);
    record.route_prefix = ExtractPrefix(record.max_key);
    record.route_min_suffix =
        ExtractPrefix(record.min_key) == record.route_prefix ? ExtractSuffix(record.min_key) : 0;
    record.route_max_suffix =
        ExtractPrefix(record.max_key) == record.route_prefix ? ExtractSuffix(record.max_key) : UINT64_MAX;
    record.table_idx = table_idx;
    record.seq_no = seq_no;
    return record;
}

SubtreeRecord MakeRecordWithPrefix(uint64_t prefix,
                                   uint64_t min_suffix,
                                   uint64_t max_suffix,
                                   uint64_t table_idx,
                                   uint32_t seq_no = 1) {
    SubtreeRecord record;
    record.min_key = MakeKey(prefix, min_suffix);
    record.max_key = MakeKey(prefix, max_suffix);
    record.route_prefix = prefix;
    record.route_min_suffix = min_suffix;
    record.route_max_suffix = max_suffix;
    record.table_idx = table_idx;
    record.seq_no = seq_no;
    return record;
}

void TestBulkLoadAndExport() {
    L1SubtreeBPTree tree({2, 2});
    std::vector<SubtreeRecord> records = {
        MakeRecord(10, 19, 100),
        MakeRecord(20, 29, 101),
        MakeRecord(30, 39, 102),
        MakeRecord(40, 49, 103),
        MakeRecord(50, 59, 104)
    };

    tree.BulkLoad(records);

    Check(!tree.Empty(), "tree should not be empty after bulk load");
    Check(tree.Size() == records.size(), "tree size mismatch after bulk load");
    Check(tree.Validate(), "tree validation failed after bulk load");

    std::vector<SubtreeRecord> exported;
    tree.ExportAll(exported);
    Check(exported.size() == records.size(), "exported record count mismatch");
    for (size_t i = 0; i < records.size(); ++i) {
        Check(exported[i].table_idx == records[i].table_idx, "exported table_idx order mismatch");
        Check(CompareKeyType(exported[i].max_key, records[i].max_key) == 0, "exported max_key mismatch");
    }
}

void TestPrefixSuffixHelpers() {
#if defined(FLOWKV_KEY16)
    const KeyType key = ComposeKey(0x1122334455667788ULL, 0x8877665544332211ULL);
    Check(ExtractPrefix(key) == 0x1122334455667788ULL, "prefix helper should extract high 64 bits");
    Check(ExtractSuffix(key) == 0x8877665544332211ULL, "suffix helper should extract low 64 bits");

    const SubtreeRecord record = MakeRecord(10, 19, 100);
    Check(record.route_prefix == 0, "record route prefix should default to max_key prefix");
    Check(record.route_min_suffix == 10, "record route min suffix should follow local fragment bound");
    Check(record.route_max_suffix == 19, "record route max suffix should follow local fragment bound");
#endif
}

void TestPrefixLocalFragments() {
#if defined(FLOWKV_KEY16)
    TaggedPstMeta table;
    table.meta.datablock_ptr_ = 1234;
    table.meta.min_key_hi = 0x60;
    table.meta.min_key_lo = 0x50;
    table.meta.max_key_hi = 0x62;
    table.meta.max_key_lo = 0x20;
    table.meta.seq_no_ = 7;

    const auto left = SubtreeRecord::FromTaggedPstMetaForPrefix(table, 9, 0x60);
    const auto middle = SubtreeRecord::FromTaggedPstMetaForPrefix(table, 9, 0x61);
    const auto right = SubtreeRecord::FromTaggedPstMetaForPrefix(table, 9, 0x62);

    Check(left.MatchesLocalFragment(0x60), "left clipped fragment should match local bounds");
    Check(left.route_min_suffix == 0x50 && left.route_max_suffix == UINT64_MAX,
          "left clipped fragment bounds mismatch");

    Check(middle.MatchesLocalFragment(0x61), "middle clipped fragment should match local bounds");
    Check(middle.route_min_suffix == 0 && middle.route_max_suffix == UINT64_MAX,
          "middle clipped fragment bounds mismatch");

    Check(right.MatchesLocalFragment(0x62), "right clipped fragment should match local bounds");
    Check(right.route_min_suffix == 0 && right.route_max_suffix == 0x20,
          "right clipped fragment bounds mismatch");
#endif
}

void TestLeafValuePacking() {
    SubtreeRecord record;
    const uint64_t kv_block_ptr = 0xABCDEULL;
    const uint16_t offset = 77;
    const uint8_t count = 21;
    record.SetLeafWindowByBlockPtr(kv_block_ptr, offset, count);
    Check(record.HasLeafWindow(), "leaf value window should be marked valid");

    const auto parts = record.LeafWindow();
    Check(parts.kv_block_ptr == kv_block_ptr, "leaf value block pointer mismatch");
    Check(parts.offset == offset, "leaf value offset mismatch");
    Check(parts.count == count, "leaf value count mismatch");

    TaggedPstMeta table;
    table.meta.datablock_ptr_ = (kv_block_ptr << SubtreeRecord::kKvBlockShift);
    table.meta.entry_num_ = 128;
#if defined(FLOWKV_KEY16)
    table.meta.min_key_hi = 0x20;
    table.meta.min_key_lo = 0x10;
    table.meta.max_key_hi = 0x20;
    table.meta.max_key_lo = 0x20;
#else
    table.meta.min_key_ = MakeKey(0x10);
    table.meta.max_key_ = MakeKey(0x20);
#endif
    const auto from_table = SubtreeRecord::FromTaggedPstMeta(table, 9);
    const auto from_table_parts = from_table.LeafWindow();
    Check(from_table_parts.kv_block_ptr == kv_block_ptr,
          "record from table should preserve kv block pointer");
    Check(from_table_parts.offset == 0, "record from table default offset should be zero");
    Check(from_table_parts.count == 128,
          "record from table default count should follow entry_num");
}

void TestLookupCandidate() {
    L1SubtreeBPTree tree({2, 2});
    tree.BulkLoad({
        MakeRecord(10, 19, 100),
        MakeRecord(25, 35, 101),
        MakeRecord(36, 50, 102)
    });

    SubtreeRecord out;
    Check(tree.LookupCandidate(MakeKey(10), out), "lookup should find left boundary");
    Check(out.table_idx == 100, "lookup returned wrong record at left boundary");

    Check(tree.LookupCandidate(MakeKey(30), out), "lookup should find middle range");
    Check(out.table_idx == 101, "lookup returned wrong middle record");

    Check(tree.LookupCandidate(MakeKey(50), out), "lookup should find right boundary");
    Check(out.table_idx == 102, "lookup returned wrong right boundary record");

    Check(!tree.LookupCandidate(MakeKey(24), out), "lookup should miss gap before middle range");
    Check(!tree.LookupCandidate(MakeKey(60), out), "lookup should miss key larger than all ranges");
    Check(!tree.LookupCandidate(MakeKey(1), out), "lookup should miss key smaller than all ranges");
}

void TestLookupCandidatesWithOverlap() {
    L1SubtreeBPTree tree({2, 2});
    tree.BulkLoad({
        MakeRecord(10, 25, 100),
        MakeRecord(15, 25, 101),
        MakeRecord(18, 30, 102),
        MakeRecord(40, 50, 103)
    });

    std::vector<SubtreeRecord> out;
    tree.LookupCandidates(MakeKey(20), 8, out);

    Check(out.size() == 3, "overlap lookup should return three candidates");
    Check(out[0].table_idx == 100, "first overlap candidate mismatch");
    Check(out[1].table_idx == 101, "second overlap candidate mismatch");
    Check(out[2].table_idx == 102, "third overlap candidate mismatch");

    tree.LookupCandidates(MakeKey(20), 2, out);
    Check(out.size() == 2, "limit should cap overlap lookup results");
    Check(out[0].table_idx == 100 && out[1].table_idx == 101, "limit should preserve order");
}

void TestSameMaxKeyPrefersNewerSeqNo() {
    L1SubtreeBPTree tree({2, 2});
    tree.BulkLoad({
        MakeRecord(10, 25, 101, 3),
        MakeRecord(10, 25, 102, 2),
        MakeRecord(10, 25, 100, 1)
    });

    std::vector<SubtreeRecord> out;
    tree.LookupCandidates(MakeKey(20), 8, out);

    Check(out.size() == 3, "same-max lookup should return all matching candidates");
    Check(out[0].table_idx == 101, "newest seq_no should come first for same max_key");
    Check(out[1].table_idx == 102, "second highest seq_no should come second");
    Check(out[2].table_idx == 100, "oldest seq_no should come last");
}

void TestRangeScan() {
    L1SubtreeBPTree tree({2, 2});
    tree.BulkLoad({
        MakeRecord(0, 9, 10),
        MakeRecord(10, 19, 11),
        MakeRecord(15, 25, 12),
        MakeRecord(30, 39, 13),
        MakeRecord(40, 49, 14)
    });

    std::vector<SubtreeRecord> out;
    tree.RangeScan(MakeKey(12), MakeKey(32), out);

    Check(out.size() == 3, "range scan should return three overlapping records");
    Check(out[0].table_idx == 11, "range scan first record mismatch");
    Check(out[1].table_idx == 12, "range scan second record mismatch");
    Check(out[2].table_idx == 13, "range scan third record mismatch");

    tree.RangeScan(MakeKey(60), MakeKey(70), out);
    Check(out.empty(), "range scan should be empty outside all ranges");

    tree.RangeScan(MakeKey(32), MakeKey(12), out);
    Check(out.empty(), "range scan with invalid interval should be empty");
}

void TestBulkLoadRejectsUnsortedInput() {
    L1SubtreeBPTree tree({2, 2});
    bool threw = false;
    try {
        tree.BulkLoad({
            MakeRecord(20, 29, 101),
            MakeRecord(10, 19, 100)
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    Check(threw, "bulk load should reject unsorted records");

    threw = false;
    try {
        tree.BulkLoad({
            MakeRecord(10, 25, 100, 1),
            MakeRecord(10, 25, 101, 3)
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    Check(threw, "bulk load should reject wrong seq_no ordering for same max_key");
}

void TestBulkLoadFromTablesSkipsInvalidEntries() {
    L1SubtreeBPTree tree({2, 2});

    std::vector<TaggedPstMeta> tables(4);
    tables[0].meta.datablock_ptr_ = 1000;
#if defined(FLOWKV_KEY16)
    tables[0].meta.min_key_hi = 0;
    tables[0].meta.min_key_lo = 10;
    tables[0].meta.max_key_hi = 0;
    tables[0].meta.max_key_lo = 19;
#else
    tables[0].meta.min_key_ = MakeKey(10);
    tables[0].meta.max_key_ = MakeKey(19);
#endif
    tables[0].meta.seq_no_ = 1;

    tables[1].meta = PSTMeta::InvalidTable();

    tables[2].meta.datablock_ptr_ = 2000;
#if defined(FLOWKV_KEY16)
    tables[2].meta.min_key_hi = 0;
    tables[2].meta.min_key_lo = 20;
    tables[2].meta.max_key_hi = 0;
    tables[2].meta.max_key_lo = 29;
#else
    tables[2].meta.min_key_ = MakeKey(20);
    tables[2].meta.max_key_ = MakeKey(29);
#endif
    tables[2].meta.seq_no_ = 2;

    tables[3].meta.datablock_ptr_ = 3000;
#if defined(FLOWKV_KEY16)
    tables[3].meta.min_key_hi = 0;
    tables[3].meta.min_key_lo = 30;
    tables[3].meta.max_key_hi = 0;
    tables[3].meta.max_key_lo = 39;
#else
    tables[3].meta.min_key_ = MakeKey(30);
    tables[3].meta.max_key_ = MakeKey(39);
#endif
    tables[3].meta.seq_no_ = 3;

    tree.BulkLoadFromTables(tables);

    Check(tree.Size() == 3, "bulk load from tables should skip invalid pst meta");

    std::vector<SubtreeRecord> out;
    tree.ExportAll(out);
    Check(out.size() == 3, "exported record count mismatch after BulkLoadFromTables");
    Check(out[0].table_idx == 0, "first table_idx should match original valid slot");
    Check(out[1].table_idx == 2, "second table_idx should keep original vector index");
    Check(out[2].table_idx == 3, "third table_idx should keep original vector index");
}

void TestSubtreePageRoundTrip() {
    L1SubtreeBPTree tree({2, 2});
    std::vector<SubtreeRecord> records = {
        MakeRecord(10, 19, 100),
        MakeRecord(20, 29, 101),
        MakeRecord(30, 39, 102),
        MakeRecord(40, 49, 103),
        MakeRecord(50, 59, 104)
    };
    tree.BulkLoad(records);

    auto page_set = tree.ExportPageSet(512);
    Check(SubtreePageCodec::Validate(page_set), "subtree page set should validate after export");

    const auto manifest = SubtreePageCodec::DecodeManifest(page_set.manifest);
    Check(manifest.page_size == 512, "subtree page manifest should preserve page size");
    Check(manifest.record_count == records.size(), "subtree page manifest record count mismatch");
    Check(manifest.leaf_page_count == 3, "subtree page manifest leaf page count mismatch");
    Check(manifest.internal_page_count == 3, "subtree page manifest internal page count mismatch");

    L1SubtreeBPTree restored;
    restored.ImportPageSet(page_set);
    Check(restored.Validate(), "restored subtree should validate after page import");

    std::vector<SubtreeRecord> restored_records;
    restored.ExportAll(restored_records);
    Check(restored_records.size() == records.size(), "restored subtree record count mismatch");
    for (size_t i = 0; i < records.size(); ++i) {
        Check(restored_records[i].table_idx == records[i].table_idx, "restored subtree table_idx mismatch");
        Check(CompareKeyType(restored_records[i].min_key, records[i].min_key) == 0,
              "restored subtree min_key mismatch");
        Check(CompareKeyType(restored_records[i].max_key, records[i].max_key) == 0,
              "restored subtree max_key mismatch");
    }

    SubtreeRecord candidate;
    Check(restored.LookupCandidate(MakeKey(45), candidate), "restored subtree lookup should succeed");
    Check(candidate.table_idx == 103, "restored subtree lookup candidate mismatch");

    auto corrupted = page_set;
    corrupted.pages[0].bytes[0] ^= 0xFFu;
    Check(!SubtreePageCodec::Validate(corrupted), "corrupted subtree page set should fail validation");
}

void TestSubtreePagePersistenceRoundTrip() {
    const std::filesystem::path pool_path =
        std::filesystem::temp_directory_path() /
        ("flowkv_hybrid_l1_subtree_store_" + std::to_string(getpid()) + ".pool");
    std::filesystem::remove(pool_path);

    L1SubtreeBPTree source_tree({2, 2});
    source_tree.BulkLoad({
        MakeRecord(10, 19, 100),
        MakeRecord(20, 29, 101),
        MakeRecord(30, 39, 102),
        MakeRecord(40, 49, 103),
    });

    auto page_set = source_tree.ExportPageSet();
    Check(SubtreePageCodec::Validate(page_set), "persisted source page set should validate");

    flowkv::hybrid_l1::SubtreePageStoreHandle handle;
    {
        SegmentAllocator allocator(pool_path.string(), 16 * SEGMENT_SIZE, false, false);
        handle = SubtreePageStore::Persist(allocator, page_set);
        Check(handle.Valid(), "subtree page store handle should be valid after persist");
    }

    {
        SegmentAllocator allocator(pool_path.string(), 16 * SEGMENT_SIZE, true, false);
        auto loaded_page_set = SubtreePageStore::Load(allocator, handle);
        Check(SubtreePageCodec::Validate(loaded_page_set),
              "loaded subtree page set should validate after reopen");

        L1SubtreeBPTree restored_tree;
        restored_tree.ImportPageSet(loaded_page_set);
        Check(restored_tree.Validate(), "restored subtree from persistent pages should validate");

        std::vector<SubtreeRecord> out;
        restored_tree.ExportAll(out);
        Check(out.size() == 4, "persistent restored subtree record count mismatch");
        Check(out[0].table_idx == 100, "persistent restored subtree first record mismatch");
        Check(out[3].table_idx == 103, "persistent restored subtree last record mismatch");

        SubtreeRecord candidate;
        Check(restored_tree.LookupCandidate(MakeKey(35), candidate),
              "persistent restored subtree lookup should succeed");
        Check(candidate.table_idx == 102, "persistent restored subtree lookup mismatch");

        SubtreePageStore::Destroy(allocator, handle);
    }

    std::filesystem::remove(pool_path);
}

void TestSubtreePageCowPersistenceRoundTrip() {
    const std::filesystem::path pool_path =
        std::filesystem::temp_directory_path() /
        ("flowkv_hybrid_l1_subtree_store_cow_" + std::to_string(getpid()) + ".pool");
    std::filesystem::remove(pool_path);

    L1SubtreeBPTree base_tree({2, 2});
    base_tree.BulkLoad({
        MakeRecord(10, 19, 100),
        MakeRecord(20, 29, 101),
        MakeRecord(30, 39, 102),
        MakeRecord(40, 49, 103),
        MakeRecord(50, 59, 104),
    });
    auto base_page_set = base_tree.ExportPageSet();

    L1SubtreeBPTree target_tree({2, 2});
    target_tree.BulkLoad({
        MakeRecord(10, 19, 100),
        MakeRecord(20, 29, 201),
        MakeRecord(30, 39, 202),
        MakeRecord(40, 49, 103),
        MakeRecord(50, 59, 104),
    });
    auto target_page_set = target_tree.ExportPageSet();

    {
        SegmentAllocator allocator(pool_path.string(), 16 * SEGMENT_SIZE, false, false);
        const auto base_handle = SubtreePageStore::Persist(allocator, base_page_set);
        const auto cow_handle =
            SubtreePageStore::PersistCow(allocator, base_handle, base_page_set, target_page_set);
        Check(cow_handle.Valid(), "cow handle should be valid");
        Check(cow_handle.pages.size() == target_page_set.pages.size() + 1,
              "cow handle page count mismatch");

        std::unordered_set<uint64_t> base_page_refs;
        for (const auto& ref : base_handle.pages) {
            base_page_refs.insert((ref.segment_id << 32) ^ ref.page_id);
        }
        size_t shared_pages = 0;
        for (const auto& ref : cow_handle.pages) {
            if (base_page_refs.find((ref.segment_id << 32) ^ ref.page_id) != base_page_refs.end()) {
                ++shared_pages;
            }
        }
        Check(shared_pages > 0, "cow handle should reuse at least one page");
        Check(shared_pages < cow_handle.pages.size(), "cow handle should rewrite at least one page");

        auto loaded = SubtreePageStore::Load(allocator, cow_handle);
        Check(SubtreePageCodec::Validate(loaded), "cow loaded page set should validate");
        L1SubtreeBPTree restored;
        restored.ImportPageSet(loaded);
        Check(restored.Validate(), "cow restored tree should validate");

        SubtreePageStore::DestroyUnshared(allocator, base_handle, cow_handle);
        auto reloaded = SubtreePageStore::Load(allocator, cow_handle);
        Check(SubtreePageCodec::Validate(reloaded),
              "cow handle should stay valid after destroying base-unshared pages");

        SubtreePageStore::Destroy(allocator, cow_handle);
    }

    std::filesystem::remove(pool_path);
}

void TestCowReusesInteriorLeavesAndInternalNodes() {
    L1SubtreeBPTree base({2, 2});
    std::vector<SubtreeRecord> base_records;
    for (uint64_t i = 0; i < 12; ++i) {
        base_records.push_back(MakeRecordWithPrefix(0x50, i * 10, i * 10 + 5, 100 + i));
    }
    base.BulkLoad(base_records);
    Check(base.Validate(), "base subtree should validate before cow test");

    std::vector<SubtreeRecord> target_records = base_records;
    target_records[2] = MakeRecordWithPrefix(0x50, 21, 26, 1002);
    target_records[3] = MakeRecordWithPrefix(0x50, 31, 36, 1003);
    target_records[8] = MakeRecordWithPrefix(0x50, 81, 86, 1008);
    target_records[9] = MakeRecordWithPrefix(0x50, 91, 96, 1009);

    L1SubtreeBPTree cow({2, 2});
    cow.BulkLoadCow(base, target_records);
    Check(cow.Validate(), "cow subtree should validate");

    std::vector<SubtreeRecord> exported;
    cow.ExportAll(exported);
    Check(exported.size() == target_records.size(), "cow subtree exported size mismatch");
    for (size_t i = 0; i < target_records.size(); ++i) {
        Check(exported[i].table_idx == target_records[i].table_idx, "cow subtree exported table order mismatch");
    }

    Check(cow.DebugCountSharedLeaves(base) == 4, "cow subtree should reuse prefix, suffix, and interior leaves");
    Check(cow.DebugCountSharedInternalNodes(base) >= 1, "cow subtree should reuse at least one internal node");
}

void TestHybridIndexPrefixLookup() {
    L1HybridIndex index({2, {2, 2}});
    index.BulkLoad({
        MakeRecordWithPrefix(0x10, 0x10, 0x20, 100),
        MakeRecordWithPrefix(0x10, 0x18, 0x28, 101),
        MakeRecordWithPrefix(0x10, 0x20, 0x30, 102),
        MakeRecordWithPrefix(0x11, 0x00, 0x10, 103)
    });

    Check(index.PartitionCount() == 2, "hybrid index should build one partition per active prefix");
    Check(index.Validate(), "hybrid index should validate after bulk load");

    std::vector<SubtreeRecord> out;
    index.LookupCandidates(MakeKey(0x10, 0x20), 8, out);
    Check(out.size() == 3, "same-prefix lookup should return three matching candidates");
    Check(out[0].table_idx == 100, "same-prefix first candidate mismatch");
    Check(out[1].table_idx == 101, "same-prefix second candidate mismatch");
    Check(out[2].table_idx == 102, "same-prefix third candidate mismatch");

    index.LookupCandidates(MakeKey(0x11, 0x08), 8, out);
    Check(out.size() == 1, "lookup should stay within a single prefix subtree");
    Check(out[0].table_idx == 103, "single-prefix routed lookup should hit only matching prefix");
}

void TestHybridIndexPrefixRangeScan() {
    L1HybridIndex index({2, {2, 2}});
    index.BulkLoad({
        MakeRecordWithPrefix(0x10, 0x10, 0x1f, 100),
        MakeRecordWithPrefix(0x10, 0x18, 0x28, 101),
        MakeRecordWithPrefix(0x11, 0x00, 0x10, 102),
        MakeRecordWithPrefix(0x12, 0x00, 0x10, 103)
    });

    std::vector<SubtreeRecord> out;
    index.RangeScan(MakeKey(0x10, 0x18), MakeKey(0x11, 0x08), out);
    Check(out.size() == 3, "cross-prefix range scan should return three records");
    Check(out[0].table_idx == 100, "range scan first candidate mismatch");
    Check(out[1].table_idx == 101, "range scan second candidate mismatch");
    Check(out[2].table_idx == 102, "range scan third candidate mismatch");
}

void TestHybridIndexDiskPointLookupUsesPagePath() {
    const std::filesystem::path pool_path =
        std::filesystem::temp_directory_path() /
        ("flowkv_hybrid_l1_disk_lookup_" + std::to_string(getpid()) + ".pool");
    std::filesystem::remove(pool_path);

    {
        SegmentAllocator allocator(pool_path.string(), 32 * SEGMENT_SIZE, false, false);
        L1HybridIndex::BuildOptions options;
        options.subtree_options = {2, 2};
        options.segment_allocator = &allocator;
        options.enable_subtree_cache = true;
        options.subtree_cache_capacity = 32;
        options.subtree_cache_max_bytes = 32ULL << 20;
        L1HybridIndex index(options);

        index.BulkLoad({
            MakeRecordWithPrefix(0x55, 0x00, 0x10, 100),
            MakeRecordWithPrefix(0x55, 0x08, 0x18, 101),
            MakeRecordWithPrefix(0x56, 0x00, 0x10, 102),
        });
        Check(index.Validate(), "disk lookup test index should validate");
        const auto stats_before = index.EstimateMemoryUsage();

        std::vector<SubtreeRecord> out;
        index.LookupCandidates(MakeKey(0x55, 0x09), 8, out);
        Check(out.size() == 2, "disk lookup path should return overlapping candidates");
        Check(out[0].table_idx == 100, "disk lookup first candidate mismatch");
        Check(out[1].table_idx == 101, "disk lookup second candidate mismatch");

        SubtreeRecord single;
        Check(index.LookupCandidate(MakeKey(0x56, 0x05), single),
              "disk lookup single candidate should succeed");
        Check(single.table_idx == 102, "disk lookup single candidate mismatch");

        out.clear();
        index.LookupCandidates(MakeKey(0x57, 0x01), 4, out);
        Check(out.empty(), "disk lookup miss should return empty");

        const auto stats_after = index.EstimateMemoryUsage();
        Check(stats_after.subtree_cache_requests == stats_before.subtree_cache_requests,
              "disk page-path lookups should not increase subtree cache request counter");
        Check(stats_after.subtree_cache_hits == stats_before.subtree_cache_hits,
              "disk page-path lookups should not increase cache hits");
        Check(stats_after.subtree_cache_misses == stats_before.subtree_cache_misses,
              "disk page-path lookups should not increase cache misses");
    }

    std::filesystem::remove(pool_path);
}

void TestHybridIndexDiskRangeScanUsesPagePath() {
    const std::filesystem::path pool_path =
        std::filesystem::temp_directory_path() /
        ("flowkv_hybrid_l1_disk_range_" + std::to_string(getpid()) + ".pool");
    std::filesystem::remove(pool_path);

    {
        SegmentAllocator allocator(pool_path.string(), 32 * SEGMENT_SIZE, false, false);
        L1HybridIndex::BuildOptions options;
        options.subtree_options = {2, 2};
        options.segment_allocator = &allocator;
        options.enable_parallel_range_scan = false;
        options.enable_subtree_cache = true;
        options.subtree_cache_capacity = 32;
        options.subtree_cache_max_bytes = 32ULL << 20;
        L1HybridIndex index(options);

        index.BulkLoad({
            MakeRecordWithPrefix(0x60, 0x00, 0x10, 100),
            MakeRecordWithPrefix(0x60, 0x08, 0x18, 101),
            MakeRecordWithPrefix(0x61, 0x00, 0x05, 102),
            MakeRecordWithPrefix(0x61, 0x04, 0x20, 103),
        });
        Check(index.Validate(), "disk range-scan test index should validate");
        const auto stats_before = index.EstimateMemoryUsage();

        std::vector<SubtreeRecord> out;
        index.RangeScan(MakeKey(0x60, 0x09), MakeKey(0x61, 0x06), out);
        Check(out.size() == 4, "disk range page path should return expected overlap candidates");
        Check(out[0].table_idx == 100, "disk range first candidate mismatch");
        Check(out[1].table_idx == 101, "disk range second candidate mismatch");
        Check(out[2].table_idx == 102, "disk range third candidate mismatch");
        Check(out[3].table_idx == 103, "disk range fourth candidate mismatch");

        const auto stats_after = index.EstimateMemoryUsage();
        Check(stats_after.subtree_cache_requests == stats_before.subtree_cache_requests,
              "disk page-path range scan should not increase subtree cache request counter");
        Check(stats_after.subtree_cache_hits == stats_before.subtree_cache_hits,
              "disk page-path range scan should not increase cache hits");
        Check(stats_after.subtree_cache_misses == stats_before.subtree_cache_misses,
              "disk page-path range scan should not increase cache misses");
    }

    std::filesystem::remove(pool_path);
}

void TestHybridIndexParallelRangeScanMatchesSerial() {
    L1HybridIndex serial_index({2, {2, 2}, {}, false, 3, 4});
    L1HybridIndex parallel_index({2, {2, 2}, {}, true, 2, 3});

    std::vector<SubtreeRecord> records = {
        MakeRecordWithPrefix(0x70, 0x10, 0x30, 100),
        MakeRecordWithPrefix(0x70, 0x20, 0x40, 101),
        MakeRecordWithPrefix(0x71, 0x00, 0x18, 102),
        MakeRecordWithPrefix(0x72, 0x08, 0x28, 103),
        MakeRecordWithPrefix(0x73, 0x04, 0x14, 104),
        MakeRecordWithPrefix(0x74, 0x01, 0x10, 105)
    };

    serial_index.BulkLoad(records);
    parallel_index.BulkLoad(records);

    std::vector<SubtreeRecord> serial_out;
    std::vector<SubtreeRecord> parallel_out;
    serial_index.RangeScan(MakeKey(0x70, 0x18), MakeKey(0x73, 0x10), serial_out);
    parallel_index.RangeScan(MakeKey(0x70, 0x18), MakeKey(0x73, 0x10), parallel_out);

    Check(serial_out.size() == parallel_out.size(),
          "parallel range scan should return same number of records as serial");
    for (size_t i = 0; i < serial_out.size(); ++i) {
        Check(serial_out[i].table_idx == parallel_out[i].table_idx,
              "parallel range scan should preserve serial ordering");
    }
}

void TestHybridIndexLightweightGovernance() {
    L1HybridIndex::BuildOptions options;
    options.subtree_options = {2, 2};
    options.enable_parallel_range_scan = true;
    options.parallel_scan_min_partitions = 4;
    options.parallel_scan_max_tasks = 3;
    options.governance_policy.hot_prefix_record_threshold = 3;
    options.governance_policy.force_cow_record_threshold = 4;
    options.governance_policy.parallel_scan_record_threshold = 3;

    L1HybridIndex index(options);
    index.BulkLoad({
        MakeRecordWithPrefix(0x90, 0x00, 0x10, 100),
        MakeRecordWithPrefix(0x90, 0x11, 0x20, 101),
        MakeRecordWithPrefix(0x90, 0x21, 0x30, 102),
        MakeRecordWithPrefix(0x90, 0x31, 0x40, 103),
        MakeRecordWithPrefix(0x91, 0x00, 0x08, 104),
        MakeRecordWithPrefix(0x92, 0x00, 0x08, 105)
    });

    flowkv::hybrid_l1::RoutePartition hot_partition;
    Check(index.DebugGetPartitionForPrefix(0x90, hot_partition),
          "should export governance metadata for hot prefix");
    Check(hot_partition.governance.hot_prefix, "hot prefix should be marked hot");
    Check(hot_partition.governance.prefer_cow, "hot prefix should prefer cow");
    Check(hot_partition.governance.prefer_parallel_scan, "hot prefix should prefer parallel scan");

    flowkv::hybrid_l1::RoutePartition cold_partition;
    Check(index.DebugGetPartitionForPrefix(0x91, cold_partition),
          "should export governance metadata for cold prefix");
    Check(!cold_partition.governance.hot_prefix, "cold prefix should not be marked hot");
    Check(!cold_partition.governance.prefer_cow, "cold prefix should not prefer cow");
    Check(!cold_partition.governance.prefer_parallel_scan,
          "cold prefix should not prefer parallel scan");
}

void TestHybridIndexKeepsSameMaxKeyTogether() {
    L1HybridIndex index({2, {2, 2}});
    index.BulkLoad({
        MakeRecordWithPrefix(0x20, 0x00, 0x10, 101, 3),
        MakeRecordWithPrefix(0x20, 0x00, 0x10, 102, 2),
        MakeRecordWithPrefix(0x20, 0x00, 0x10, 100, 1),
        MakeRecordWithPrefix(0x21, 0x00, 0x10, 103, 1)
    });

    Check(index.PartitionCount() == 2, "prefix partition count mismatch");

    std::vector<SubtreeRecord> out;
    index.LookupCandidates(MakeKey(0x20, 0x08), 8, out);
    Check(out.size() == 3, "same max_key group should remain fully visible to lookup");
    Check(out[0].table_idx == 101, "same max_key ordering should be preserved");
    Check(out[1].table_idx == 102, "same max_key second ordering should be preserved");
    Check(out[2].table_idx == 100, "same max_key third ordering should be preserved");
}

void TestHybridIndexPartialPartitionRebuild() {
    L1HybridIndex index({2, {2, 2}});
    std::vector<TaggedPstMeta> tables(4);

    tables[0].meta.datablock_ptr_ = 1000;
    tables[0].meta.min_key_hi = 0x30;
    tables[0].meta.min_key_lo = 0x00;
    tables[0].meta.max_key_hi = 0x30;
    tables[0].meta.max_key_lo = 0x10;
    tables[0].meta.seq_no_ = 1;

    tables[1].meta.datablock_ptr_ = 2000;
    tables[1].meta.min_key_hi = 0x31;
    tables[1].meta.min_key_lo = 0x08;
    tables[1].meta.max_key_hi = 0x31;
    tables[1].meta.max_key_lo = 0x18;
    tables[1].meta.seq_no_ = 1;

    tables[2].meta.datablock_ptr_ = 3000;
    tables[2].meta.min_key_hi = 0x32;
    tables[2].meta.min_key_lo = 0x00;
    tables[2].meta.max_key_hi = 0x32;
    tables[2].meta.max_key_lo = 0x10;
    tables[2].meta.seq_no_ = 1;

    tables[3].meta = PSTMeta::InvalidTable();

    index.BulkLoadFromTables(tables);
    Check(index.Validate(), "hybrid index should validate after initial table bulk load");

    tables[1].meta = PSTMeta::InvalidTable();
    index.RebuildPartitionsFromTables(tables, {MakeKey(0x31, 0x18)});
    Check(index.Validate(), "hybrid index should validate after partial partition rebuild");

    std::vector<SubtreeRecord> out;
    index.LookupCandidates(MakeKey(0x30, 0x08), 4, out);
    Check(out.size() == 1 && out[0].table_idx == 0, "unaffected partition should remain readable");

    index.LookupCandidates(MakeKey(0x32, 0x08), 4, out);
    Check(out.size() == 1 && out[0].table_idx == 2, "later partition should remain readable after local rebuild");

    index.LookupCandidates(MakeKey(0x31, 0x10), 4, out);
    Check(out.empty(), "rebuilt partition should reflect removed table");
}

void TestHybridIndexDelayedReclamation() {
    L1HybridIndex index({2, {2, 2}});
    std::vector<TaggedPstMeta> tables(2);

    tables[0].meta.datablock_ptr_ = 1000;
    tables[0].meta.min_key_hi = 0x40;
    tables[0].meta.min_key_lo = 0x00;
    tables[0].meta.max_key_hi = 0x40;
    tables[0].meta.max_key_lo = 0x10;
    tables[0].meta.seq_no_ = 1;

    tables[1].meta.datablock_ptr_ = 2000;
    tables[1].meta.min_key_hi = 0x41;
    tables[1].meta.min_key_lo = 0x00;
    tables[1].meta.max_key_hi = 0x41;
    tables[1].meta.max_key_lo = 0x10;
    tables[1].meta.seq_no_ = 1;

    index.BulkLoadFromTables(tables);

    const KeyType key = MakeKey(0x40, 0x08);
    auto retained_subtree = index.DebugCaptureSubtreeForKey(MakeKey(0x40, 0x10));
    Check(retained_subtree != nullptr, "should capture old subtree before rebuild");

    tables[0].meta = PSTMeta::InvalidTable();
    index.RebuildPartitionsFromTables(tables, {MakeKey(0x40, 0x10)});

    std::vector<SubtreeRecord> current_out;
    index.LookupCandidates(key, 4, current_out);
    Check(current_out.empty(), "current index should observe rebuilt partition after publish");

    SubtreeRecord old_out;
    Check(retained_subtree->LookupCandidate(key, old_out),
          "retained subtree should remain readable after publish");
    Check(old_out.table_idx == 0, "retained subtree should preserve old record before reclamation");
}

}  // namespace

int main() {
    try {
        TestPrefixSuffixHelpers();
        TestPrefixLocalFragments();
        TestLeafValuePacking();
        TestBulkLoadAndExport();
        TestLookupCandidate();
        TestLookupCandidatesWithOverlap();
        TestSameMaxKeyPrefersNewerSeqNo();
        TestRangeScan();
        TestBulkLoadRejectsUnsortedInput();
        TestBulkLoadFromTablesSkipsInvalidEntries();
        TestSubtreePageRoundTrip();
        TestSubtreePagePersistenceRoundTrip();
        TestSubtreePageCowPersistenceRoundTrip();
        TestCowReusesInteriorLeavesAndInternalNodes();
        TestHybridIndexPrefixLookup();
        TestHybridIndexPrefixRangeScan();
        TestHybridIndexDiskPointLookupUsesPagePath();
        TestHybridIndexDiskRangeScanUsesPagePath();
        TestHybridIndexParallelRangeScanMatchesSerial();
        TestHybridIndexLightweightGovernance();
        TestHybridIndexKeepsSameMaxKeyTogether();
        TestHybridIndexPartialPartitionRebuild();
        TestHybridIndexDelayedReclamation();
    } catch (const std::exception& ex) {
        Fail(std::string("unexpected exception: ") + ex.what());
    }

    std::cout << "[hybrid_l1_test] all tests passed" << std::endl;
    return 0;
}
