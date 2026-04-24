#include "lib/hybrid_l1/l1_hybrid_rebuilder.h"
#include "lib/hybrid_l1/normal_pack.h"
#include "db/blocks/fixed_size_block.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <unistd.h>

namespace flowkv::hybrid_l1 {

namespace {

using Clock = std::chrono::steady_clock;

std::atomic<uint64_t> g_delta_prefix_count{0};
std::atomic<uint64_t> g_delta_ops_count{0};
std::atomic<uint64_t> g_index_update_total_ns{0};
std::atomic<uint64_t> g_index_update_cow_ns{0};
std::atomic<uint64_t> g_index_update_bulk_ns{0};
std::atomic<uint64_t> g_cow_prefix_count{0};
std::atomic<uint64_t> g_bulk_prefix_count{0};
std::atomic<uint64_t> g_leaf_stream_merge_ns{0};
std::atomic<uint64_t> g_rebuild_fallback_count{0};
std::atomic<uint64_t> g_tiny_descriptor_count{0};
std::atomic<uint64_t> g_normal_pack_count{0};
std::atomic<uint64_t> g_dirty_pack_pages{0};
std::atomic<uint64_t> g_pack_write_bytes{0};
std::atomic<uint64_t> g_rebuild_trace_invocation_id{0};

double NsToMs(uint64_t ns) {
    return static_cast<double>(ns) / 1000000.0;
}

bool L1RebuildTraceEnabled() {
    const char* env = std::getenv("FLOWKV_L1_REBUILD_TRACE");
    return env != nullptr && env[0] != '\0' && env[0] != '0';
}

bool SkipUnsharedCleanupEnabled() {
    const char* env = std::getenv("FLOWKV_L1_SKIP_UNSHARED_CLEANUP");
    return env != nullptr && env[0] != '\0' && env[0] != '0';
}

uint64_t ReadProcessRSSBytes() {
    std::ifstream status("/proc/self/status");
    if (!status.is_open()) {
        return 0;
    }
    std::string key;
    while (status >> key) {
        if (key == "VmRSS:") {
            uint64_t kb = 0;
            std::string unit;
            status >> kb >> unit;
            return kb * 1024ULL;
        }
        std::string rest_of_line;
        std::getline(status, rest_of_line);
    }
    return 0;
}

uint64_t CountRecordMapEntries(const std::map<RoutePrefix, std::vector<SubtreeRecord>>& by_prefix) {
    uint64_t total = 0;
    for (const auto& [prefix, records] : by_prefix) {
        (void)prefix;
        total += records.size();
    }
    return total;
}

template <class Fn>
void ForEachTouchedPrefix(const SubtreeRecord& record, Fn&& fn) {
    const RoutePrefix begin = ExtractPrefix(record.min_key);
    const RoutePrefix end = ExtractPrefix(record.max_key);
    RoutePrefix current = begin;
    while (true) {
        fn(current);
        if (current == end) {
            break;
        }
        ++current;
    }
}

size_t CountActiveTables(const std::vector<TaggedPstMeta>& tables) {
    size_t active = 0;
    for (const auto& table : tables) {
        if (table.Valid()) {
            ++active;
        }
    }
    return active;
}

size_t CountPartitionRecords(const std::vector<RoutePartition>& partitions) {
    size_t total = 0;
    for (const auto& partition : partitions) {
        total += static_cast<size_t>(partition.record_count);
    }
    return total;
}

bool SameFragment(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    return CompareKeyType(lhs.min_key, rhs.min_key) == 0 &&
           CompareKeyType(lhs.max_key, rhs.max_key) == 0 &&
           lhs.route_prefix == rhs.route_prefix &&
           lhs.route_min_suffix == rhs.route_min_suffix &&
           lhs.route_max_suffix == rhs.route_max_suffix &&
           lhs.seq_no == rhs.seq_no &&
           lhs.table_idx == rhs.table_idx &&
           lhs.leaf_value == rhs.leaf_value;
}

size_t EstimateChangedRecords(const std::vector<SubtreeRecord>& existing_records,
                              const std::vector<SubtreeRecord>& target_records) {
    size_t changed = 0;
    size_t i = 0;
    size_t j = 0;
    RecordRouteKeyLess less;
    while (i < existing_records.size() && j < target_records.size()) {
        if (SameFragment(existing_records[i], target_records[j])) {
            ++i;
            ++j;
            continue;
        }
        if (less(existing_records[i], target_records[j])) {
            ++changed;
            ++i;
        } else if (less(target_records[j], existing_records[i])) {
            ++changed;
            ++j;
        } else {
            ++changed;
            ++i;
            ++j;
        }
    }
    changed += (existing_records.size() - i);
    changed += (target_records.size() - j);
    return changed;
}

struct PrefixEntryWindow {
    uint16_t offset = 0;
    uint8_t count = 0;
};

using PrefixEntryWindowMap = std::unordered_map<RoutePrefix, PrefixEntryWindow>;

RoutePrefix ExtractPrefixFromEntry(const PDataBlock::Entry& entry) {
#if defined(FLOWKV_KEY16)
    return ExtractPrefix(KeyType{entry.key_hi, entry.key_lo});
#else
    return ExtractPrefix(entry.key);
#endif
}

uint16_t InferValidEntryCount(const PDataBlock& block) {
#if defined(FLOWKV_KEY16)
    Key16 last_key{INVALID_PTR, INVALID_PTR};
    for (uint16_t i = 0; i < static_cast<uint16_t>(PDataBlock::MAX_ENTRIES); ++i) {
        const auto& entry = block.entries[i];
        if (entry.key_hi == INVALID_PTR &&
            entry.key_lo == INVALID_PTR &&
            entry.value_lo == INVALID_PTR &&
            entry.value_hi == INVALID_PTR) {
            return i;
        }
        if (entry.key_hi == last_key.hi && entry.key_lo == last_key.lo) {
            return i;
        }
        last_key = Key16{entry.key_hi, entry.key_lo};
    }
#else
    uint64_t last_key = INVALID_PTR;
    for (uint16_t i = 0; i < static_cast<uint16_t>(PDataBlock::MAX_ENTRIES); ++i) {
        const auto& entry = block.entries[i];
        if (entry.key == INVALID_PTR && entry.value == INVALID_PTR) {
            return i;
        }
        if (entry.key == last_key) {
            return i;
        }
        last_key = entry.key;
    }
#endif
    return static_cast<uint16_t>(PDataBlock::MAX_ENTRIES);
}

bool BuildPrefixWindowMap(const PDataBlock& block,
                          uint16_t entry_num,
                          PrefixEntryWindowMap& windows_out) {
    windows_out.clear();
    if (entry_num == 0 || entry_num > PDataBlock::MAX_ENTRIES) {
        return false;
    }

    uint16_t idx = 0;
    while (idx < entry_num) {
        const RoutePrefix prefix = ExtractPrefixFromEntry(block.entries[idx]);
        const uint16_t start = idx;
        ++idx;
        while (idx < entry_num && ExtractPrefixFromEntry(block.entries[idx]) == prefix) {
            ++idx;
        }
        const uint16_t span = static_cast<uint16_t>(idx - start);
        windows_out[prefix] =
            PrefixEntryWindow{start,
                              static_cast<uint8_t>(std::min<uint16_t>(
                                  span,
                                  static_cast<uint16_t>(std::numeric_limits<uint8_t>::max())))};
    }
    return true;
}

bool LoadPrefixWindowMap(uint64_t kv_block_ptr,
                         SegmentAllocator* segment_allocator,
                         PrefixEntryWindowMap& windows_out) {
    if (segment_allocator == nullptr) {
        return false;
    }
    PDataBlock block{};
    const uint64_t block_off = SubtreeRecord::DecodeKvBlockOffset(kv_block_ptr);
    const ssize_t ret = pread(segment_allocator->Getfd(), &block, sizeof(PDataBlock), block_off);
    if (ret != static_cast<ssize_t>(sizeof(PDataBlock))) {
        return false;
    }
    const uint16_t entry_num = InferValidEntryCount(block);
    if (entry_num == 0) {
        return false;
    }
    return BuildPrefixWindowMap(block, entry_num, windows_out);
}

void RefineRecordLeafWindowForPrefix(
    SubtreeRecord& record,
    RoutePrefix prefix,
    SegmentAllocator* segment_allocator,
    std::unordered_map<uint64_t, PrefixEntryWindowMap>& window_cache) {
    if (!record.HasLeafWindow() || segment_allocator == nullptr) {
        return;
    }
    const auto leaf_window = record.LeafWindow();
    if (leaf_window.count == 0) {
        return;
    }

    const uint64_t cache_key = leaf_window.kv_block_ptr;
    auto cache_it = window_cache.find(cache_key);
    if (cache_it == window_cache.end()) {
        PrefixEntryWindowMap windows;
        if (!LoadPrefixWindowMap(leaf_window.kv_block_ptr, segment_allocator, windows)) {
            return;
        }
        cache_it = window_cache.emplace(cache_key, std::move(windows)).first;
    }
    const auto it = cache_it->second.find(prefix);
    if (it == cache_it->second.end()) {
        record.SetLeafWindowByBlockPtr(leaf_window.kv_block_ptr, 0, 0);
        return;
    }
    record.SetLeafWindowByBlockPtr(leaf_window.kv_block_ptr, it->second.offset, it->second.count);
}

bool MatchesDeleteOp(const SubtreeRecord& record, RoutePrefix prefix, const L1DeltaOp& op) {
    if (record.route_prefix != prefix) {
        return false;
    }
    if (record.route_min_suffix < op.suffix_begin || record.route_max_suffix > op.suffix_end) {
        return false;
    }
    if (op.type == L1DeltaOpType::kReplace) {
        // Replace means "remove the old window in this exact suffix range, then add new payload".
        // The old payload may have a different kv_block_ptr, so do not require ptr equality here.
        return record.route_min_suffix == op.suffix_begin &&
               record.route_max_suffix == op.suffix_end;
    }
    if (op.kv_block_ptr == 0) {
        return true;
    }
    const auto leaf_window = record.LeafWindow();
    if (leaf_window.kv_block_ptr != op.kv_block_ptr) {
        return false;
    }
    // When kv_block_ptr is known, require exact suffix window match to avoid
    // broad range-overlap deletion side effects.
    return record.route_min_suffix == op.suffix_begin &&
           record.route_max_suffix == op.suffix_end;
}

SubtreeRecord MakeRecordFromDeltaOp(RoutePrefix prefix,
                                    const L1DeltaOp& op,
                                    uint64_t synthetic_table_idx,
                                    uint32_t seq_no) {
    SubtreeRecord record;
    record.route_prefix = prefix;
    record.route_min_suffix = op.suffix_begin;
    record.route_max_suffix = op.suffix_end;
    record.min_key = ComposeKey(prefix, op.suffix_begin);
    record.max_key = ComposeKey(prefix, op.suffix_end);
    record.table_idx = synthetic_table_idx;
    record.seq_no = seq_no;
    record.SetLeafWindowByBlockPtr(op.kv_block_ptr, op.offset, op.count);
    return record;
}

struct TinyCandidateStats {
    size_t block_count = 0;
    size_t window_count = 0;
    size_t entry_count = 0;
    bool has_single_window_payload = false;
    uint64_t single_window_leaf_value = 0;
};

TinyCandidateStats CollectTinyCandidateStats(const std::vector<SubtreeRecord>& records) {
    TinyCandidateStats stats;
    stats.window_count = records.size();
    if (records.empty()) {
        return stats;
    }

    std::unordered_set<uint64_t> blocks;
    blocks.reserve(records.size());
    for (const auto& record : records) {
        if (!record.HasLeafWindow()) {
            continue;
        }
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        if (window.offset >= static_cast<uint16_t>(PDataBlock::MAX_ENTRIES)) {
            continue;
        }
        const uint32_t end = static_cast<uint32_t>(window.offset) + static_cast<uint32_t>(window.count);
        if (end > static_cast<uint32_t>(PDataBlock::MAX_ENTRIES)) {
            continue;
        }
        blocks.insert(window.kv_block_ptr);
        stats.entry_count += window.count;
    }
    stats.block_count = blocks.size();

    if (records.size() == 1 && RouteDescriptor::CanEncodeTinyLeafValue(records.front().leaf_value)) {
        stats.has_single_window_payload = true;
        stats.single_window_leaf_value = records.front().leaf_value;
    }
    return stats;
}

RouteDescriptorMode DecideDescriptorMode(const RoutePartition* existing_partition,
                                         const TinyCandidateStats& stats,
                                         uint8_t& tiny_enter_streak_out,
                                         uint64_t& tiny_leaf_value_out) {
    constexpr size_t kTinyEnterMaxEntries = 128;
    constexpr size_t kTinyExitMinEntries = 192;
    tiny_enter_streak_out = 0;
    tiny_leaf_value_out = 0;

    const bool enter_tiny =
        stats.block_count == 1 && stats.window_count == 1 &&
        stats.entry_count <= kTinyEnterMaxEntries && stats.has_single_window_payload;
    const bool exit_tiny =
        stats.block_count > 1 || stats.window_count > 1 || stats.entry_count >= kTinyExitMinEntries ||
        !stats.has_single_window_payload;

    const bool existing_is_tiny =
        existing_partition != nullptr &&
        existing_partition->descriptor_mode == RouteDescriptorMode::kTinyDirect;

    if (existing_is_tiny) {
        if (!exit_tiny) {
            tiny_enter_streak_out =
                std::max<uint8_t>(existing_partition->tiny_enter_streak, static_cast<uint8_t>(2));
            tiny_leaf_value_out = stats.single_window_leaf_value;
            return RouteDescriptorMode::kTinyDirect;
        }
        return RouteDescriptorMode::kNormalSubtree;
    }

    if (enter_tiny) {
        uint8_t streak = 1;
        if (existing_partition != nullptr) {
            streak = static_cast<uint8_t>(std::min<uint16_t>(
                static_cast<uint16_t>(existing_partition->tiny_enter_streak) + 1u, 255u));
        }
        tiny_enter_streak_out = streak;
        if (existing_partition == nullptr || streak >= 2) {
            tiny_leaf_value_out = stats.single_window_leaf_value;
            return RouteDescriptorMode::kTinyDirect;
        }
    }
    return RouteDescriptorMode::kNormalSubtree;
}

struct PackSlotAssignment {
    size_t page_index = 0;
    uint16_t slot_id = 0;
};

size_t ComputeNormalPackMaxEntries(uint32_t page_size, size_t slot_count) {
    if (slot_count == 0 || slot_count > NormalPackCodec::kMaxSlots) {
        return 0;
    }
    const size_t head_bytes =
        NormalPackCodec::kHeaderBytes + slot_count * NormalPackCodec::kSlotBytes;
    if (head_bytes > page_size) {
        return 0;
    }
    return (page_size - head_bytes) / NormalPackCodec::kEntryBytes;
}

bool BuildNormalPackPages(
    const std::map<RoutePrefix, std::vector<SubtreeRecord>>& records_by_prefix,
    uint32_t page_size,
    std::vector<NormalPackPage>& pages_out,
    std::unordered_map<RoutePrefix, PackSlotAssignment>& assignment_out) {
    pages_out.clear();
    assignment_out.clear();
    if (page_size == 0 || page_size % 4096 != 0) {
        return false;
    }

    NormalPackPage current_page;
    auto flush_current = [&]() {
        if (!current_page.slots.empty()) {
            pages_out.push_back(std::move(current_page));
            current_page = NormalPackPage{};
        }
    };

    for (const auto& [prefix, records] : records_by_prefix) {
        if (records.empty()) {
            continue;
        }
        std::vector<NormalPackEntry> entries;
        entries.reserve(records.size());
        for (const auto& record : records) {
            if (record.route_prefix != prefix) {
                return false;
            }
            entries.push_back(
                NormalPackEntry{record.route_min_suffix, record.route_max_suffix, record.leaf_value});
        }
        if (entries.size() > std::numeric_limits<uint16_t>::max()) {
            continue;
        }
        if (entries.size() > ComputeNormalPackMaxEntries(page_size, 1)) {
            continue;
        }

        while (true) {
            const size_t proposed_slots = current_page.slots.size() + 1;
            const size_t proposed_entries = current_page.entries.size() + entries.size();
            const size_t max_entries = ComputeNormalPackMaxEntries(page_size, proposed_slots);
            if (proposed_slots <= NormalPackCodec::kMaxSlots && proposed_entries <= max_entries) {
                break;
            }
            if (current_page.slots.empty()) {
                break;
            }
            flush_current();
        }

        const size_t max_entries_on_empty = ComputeNormalPackMaxEntries(page_size, 1);
        if (entries.size() > max_entries_on_empty) {
            continue;
        }
        const size_t page_index = pages_out.size();
        const uint16_t slot_id = static_cast<uint16_t>(current_page.slots.size());
        const uint16_t entry_begin = static_cast<uint16_t>(current_page.entries.size());
        current_page.slots.push_back(
            NormalPackSlot{prefix, entry_begin, static_cast<uint16_t>(entries.size()), 0});
        current_page.entries.insert(current_page.entries.end(), entries.begin(), entries.end());
        assignment_out.emplace(prefix, PackSlotAssignment{page_index, slot_id});
    }
    flush_current();
    return true;
}

}  // namespace

L1HybridRebuilder::IndexUpdateStats L1HybridRebuilder::GetIndexUpdateStats() {
    IndexUpdateStats stats;
    stats.delta_prefix_count = g_delta_prefix_count.load(std::memory_order_relaxed);
    stats.delta_ops_count = g_delta_ops_count.load(std::memory_order_relaxed);
    stats.effective_delta_prefix_count = stats.delta_prefix_count;
    stats.effective_delta_ops_count = stats.delta_ops_count;
    stats.index_update_total_ms = NsToMs(g_index_update_total_ns.load(std::memory_order_relaxed));
    stats.index_update_cow_ms = NsToMs(g_index_update_cow_ns.load(std::memory_order_relaxed));
    stats.index_update_bulk_ms = NsToMs(g_index_update_bulk_ns.load(std::memory_order_relaxed));
    stats.cow_prefix_count = g_cow_prefix_count.load(std::memory_order_relaxed);
    stats.bulk_prefix_count = g_bulk_prefix_count.load(std::memory_order_relaxed);
    stats.leaf_stream_merge_ms = NsToMs(g_leaf_stream_merge_ns.load(std::memory_order_relaxed));
    stats.rebuild_fallback_count = g_rebuild_fallback_count.load(std::memory_order_relaxed);
    stats.tiny_descriptor_count = g_tiny_descriptor_count.load(std::memory_order_relaxed);
    stats.normal_pack_count = g_normal_pack_count.load(std::memory_order_relaxed);
    const uint64_t descriptor_total = stats.tiny_descriptor_count + stats.normal_pack_count;
    stats.tiny_hit_ratio =
        descriptor_total == 0
            ? 0.0
            : static_cast<double>(stats.tiny_descriptor_count) /
                  static_cast<double>(descriptor_total);
    stats.dirty_pack_pages = g_dirty_pack_pages.load(std::memory_order_relaxed);
    stats.pack_write_bytes = g_pack_write_bytes.load(std::memory_order_relaxed);
    return stats;
}

void L1HybridRebuilder::ResetIndexUpdateStats() {
    g_delta_prefix_count.store(0, std::memory_order_relaxed);
    g_delta_ops_count.store(0, std::memory_order_relaxed);
    g_index_update_total_ns.store(0, std::memory_order_relaxed);
    g_index_update_cow_ns.store(0, std::memory_order_relaxed);
    g_index_update_bulk_ns.store(0, std::memory_order_relaxed);
    g_cow_prefix_count.store(0, std::memory_order_relaxed);
    g_bulk_prefix_count.store(0, std::memory_order_relaxed);
    g_leaf_stream_merge_ns.store(0, std::memory_order_relaxed);
    g_rebuild_fallback_count.store(0, std::memory_order_relaxed);
    g_tiny_descriptor_count.store(0, std::memory_order_relaxed);
    g_normal_pack_count.store(0, std::memory_order_relaxed);
    g_dirty_pack_pages.store(0, std::memory_order_relaxed);
    g_pack_write_bytes.store(0, std::memory_order_relaxed);
}

void L1HybridRebuilder::ResetPartitions(std::vector<RoutePartition>& partitions) {
    ResetPartitions(partitions, nullptr);
}

void L1HybridRebuilder::ResetPartitions(std::vector<RoutePartition>& partitions,
                                        SegmentAllocator* segment_allocator) {
    if (segment_allocator != nullptr) {
        std::map<uint32_t, std::unordered_set<SubtreePagePtr>> pack_ptrs_by_page_size;
        for (const auto& partition : partitions) {
            if (partition.subtree_store.Valid()) {
                SubtreePageStore::Destroy(*segment_allocator, partition.subtree_store);
            }
            if (partition.descriptor_mode == RouteDescriptorMode::kNormalPack &&
                partition.pack_page_ptr != kInvalidSubtreePagePtr) {
                uint32_t pack_page_size = partition.subtree_store.page_size;
                if (pack_page_size == 0) {
                    pack_page_size = NormalPackCodec::kDefaultPageSize;
                }
                pack_ptrs_by_page_size[pack_page_size].insert(partition.pack_page_ptr);
            }
        }
        for (auto& [page_size, ptrs] : pack_ptrs_by_page_size) {
            std::vector<SubtreePagePtr> ptr_vec;
            ptr_vec.reserve(ptrs.size());
            for (const auto ptr : ptrs) {
                ptr_vec.push_back(ptr);
            }
            SubtreePageStore::DestroyOpaquePages(*segment_allocator, page_size, ptr_vec);
        }
    }
    partitions.clear();
}

void L1HybridRebuilder::BulkLoad(const std::vector<SubtreeRecord>& sorted_records,
                                 std::vector<RoutePartition>& partitions,
                                 const FixedRouteLayout& layout,
                                 const L1SubtreeBPTree::BuildOptions& subtree_options,
                                 uint32_t subtree_page_size,
                                 SegmentAllocator* segment_allocator,
                                 const PrefixGovernancePolicy& governance_policy,
                                 size_t& size,
                                 uint64_t& generation) {
    if (segment_allocator == nullptr) {
        throw std::invalid_argument("L1HybridRebuilder::BulkLoad requires segment allocator");
    }
    ResetPartitions(partitions, segment_allocator);
    size = 0;
    if (sorted_records.empty()) {
        layout.RefreshPartitions(partitions);
        return;
    }

    ValidateSortedRecords(sorted_records);

    std::map<RoutePrefix, std::vector<SubtreeRecord>> partition_records;
    for (const auto& record : sorted_records) {
        ForEachTouchedPrefix(record, [&](RoutePrefix prefix) {
            partition_records[prefix].push_back(SubtreeRecord::FromExistingForPrefix(record, prefix));
        });
    }

    const uint64_t build_generation = ++generation;
    for (auto& [prefix, records] : partition_records) {
        std::sort(records.begin(), records.end(), RecordRouteKeyLess{});
        partitions.push_back(BuildPartition(prefix, records, build_generation, subtree_options,
                                            subtree_page_size, segment_allocator, governance_policy));
    }
    layout.RefreshPartitions(partitions);
    size = sorted_records.size();
}

void L1HybridRebuilder::BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables,
                                           std::vector<RoutePartition>& partitions,
                                           const FixedRouteLayout& layout,
                                           const L1SubtreeBPTree::BuildOptions& subtree_options,
                                           uint32_t subtree_page_size,
                                           SegmentAllocator* segment_allocator,
                                           const PrefixGovernancePolicy& governance_policy,
                                           size_t& size,
                                           uint64_t& generation) {
    if (segment_allocator == nullptr) {
        throw std::invalid_argument("L1HybridRebuilder::BulkLoadFromTables requires segment allocator");
    }
    std::vector<SubtreeRecord> records;
    records.reserve(tables.size());
    for (size_t i = 0; i < tables.size(); ++i) {
        if (!tables[i].Valid()) {
            continue;
        }
        records.push_back(SubtreeRecord::FromTaggedPstMeta(tables[i], i));
    }
    std::sort(records.begin(), records.end(), RecordMaxKeyLess{});
    BulkLoad(records, partitions, layout, subtree_options, subtree_page_size, segment_allocator,
             governance_policy, size, generation);
}

void L1HybridRebuilder::RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                                    const std::vector<KeyType>& changed_route_keys,
                                                    std::vector<RoutePartition>& partitions,
                                                    const FixedRouteLayout& layout,
                                                    const L1SubtreeBPTree::BuildOptions& subtree_options,
                                                    uint32_t subtree_page_size,
                                                    SegmentAllocator* segment_allocator,
                                                    const PartitionUpdatePolicy& update_policy,
                                                    const PrefixGovernancePolicy& governance_policy,
                                                    const L1DeltaBatch* delta_batch,
                                                    size_t& size,
                                                    uint64_t& generation) {
    const auto fn_begin = Clock::now();
    uint64_t local_delta_prefix_count = 0;
    uint64_t local_delta_ops_count = 0;
    uint64_t local_index_update_cow_ns = 0;
    uint64_t local_index_update_bulk_ns = 0;
    uint64_t local_cow_prefix_count = 0;
    uint64_t local_bulk_prefix_count = 0;
    uint64_t local_leaf_stream_merge_ns = 0;
    uint64_t local_rebuild_fallback_count = 0;
    uint64_t local_tiny_descriptor_count = 0;
    uint64_t local_normal_pack_count = 0;
    uint64_t local_dirty_pack_pages = 0;
    uint64_t local_pack_write_bytes = 0;
    uint64_t local_cleanup_destroy_unshared_count = 0;
    uint64_t local_cleanup_destroy_full_count = 0;
    uint64_t local_cleanup_reclaim_pack_pages = 0;
    uint64_t local_cleanup_skip_unshared_count = 0;
    const bool trace_enabled = L1RebuildTraceEnabled();
    const bool skip_unshared_cleanup = SkipUnsharedCleanupEnabled();
    const uint64_t trace_invocation_id =
        g_rebuild_trace_invocation_id.fetch_add(1, std::memory_order_relaxed) + 1;
    const uint64_t trace_base_rss_bytes = trace_enabled ? ReadProcessRSSBytes() : 0;
    uint64_t trace_peak_rss_bytes = trace_base_rss_bytes;

    if (segment_allocator == nullptr) {
        throw std::invalid_argument(
            "L1HybridRebuilder::RebuildPartitionsFromTables requires segment allocator");
    }
    if (changed_route_keys.empty()) {
        return;
    }

    std::vector<RoutePrefix> changed_prefixes;
    changed_prefixes.reserve(changed_route_keys.size());
    for (const auto& key : changed_route_keys) {
        changed_prefixes.push_back(ExtractPrefix(key));
    }
    std::sort(changed_prefixes.begin(), changed_prefixes.end());
    changed_prefixes.erase(std::unique(changed_prefixes.begin(), changed_prefixes.end()),
                           changed_prefixes.end());

    std::unordered_map<RoutePrefix, const L1PrefixDelta*> delta_by_prefix;
    if (delta_batch != nullptr) {
        delta_by_prefix.reserve(delta_batch->deltas.size());
        for (const auto& prefix_delta : delta_batch->deltas) {
            delta_by_prefix.emplace(prefix_delta.prefix, &prefix_delta);
        }
    }

    std::map<RoutePrefix, std::vector<SubtreeRecord>> rebuilt_records;
    for (size_t i = 0; i < tables.size(); ++i) {
        if (!tables[i].Valid()) {
            continue;
        }
        const SubtreeRecord record = SubtreeRecord::FromTaggedPstMeta(tables[i], i);
        ForEachTouchedPrefix(record, [&](RoutePrefix prefix) {
            if (std::binary_search(changed_prefixes.begin(), changed_prefixes.end(), prefix)) {
                rebuilt_records[prefix].push_back(
                    SubtreeRecord::FromTaggedPstMetaForPrefix(tables[i], i, prefix));
            }
        });
    }

    const uint64_t build_generation = ++generation;
    std::vector<RoutePartition> next_partitions;
    next_partitions.reserve(partitions.size() + rebuilt_records.size());
    std::map<RoutePrefix, const RoutePartition*> old_partitions_by_prefix;
    std::map<RoutePrefix, SubtreePageStoreHandle> old_handles_for_cleanup;
    std::map<RoutePrefix, SubtreePageStoreHandle> new_handles_after_publish;
    std::map<RoutePrefix, std::vector<SubtreeRecord>> pack_records_by_prefix;
    std::map<uint32_t, std::unordered_set<SubtreePagePtr>> old_pack_ptrs_for_cleanup;
    uint64_t trace_pack_pages_count = 0;
    uint64_t trace_encoded_pages_count = 0;

    const auto trace_rebuild_stage = [&](const char* stage) {
        if (!trace_enabled) {
            return;
        }
        const uint64_t rss_bytes = ReadProcessRSSBytes();
        trace_peak_rss_bytes = std::max(trace_peak_rss_bytes, rss_bytes);
        const uint64_t rebuilt_record_count = CountRecordMapEntries(rebuilt_records);
        const uint64_t pack_record_count = CountRecordMapEntries(pack_records_by_prefix);
        const uint64_t elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - fn_begin).count());
        std::cout << "[L1_REBUILD_TRACE]"
                  << " invocation_id=" << trace_invocation_id
                  << " stage=" << stage
                  << " rss_bytes=" << rss_bytes
                  << " rss_delta_base_bytes="
                  << static_cast<int64_t>(rss_bytes) - static_cast<int64_t>(trace_base_rss_bytes)
                  << " peak_rss_bytes=" << trace_peak_rss_bytes
                  << " elapsed_ms=" << NsToMs(elapsed_ns)
                  << " changed_prefixes=" << changed_prefixes.size()
                  << " delta_prefix_candidates=" << delta_by_prefix.size()
                  << " rebuilt_prefixes=" << rebuilt_records.size()
                  << " rebuilt_records=" << rebuilt_record_count
                  << " rebuilt_est_bytes="
                  << (rebuilt_record_count * static_cast<uint64_t>(sizeof(SubtreeRecord)))
                  << " pack_prefixes=" << pack_records_by_prefix.size()
                  << " pack_records=" << pack_record_count
                  << " pack_est_bytes="
                  << (pack_record_count * static_cast<uint64_t>(sizeof(SubtreeRecord)))
                  << " next_partitions=" << next_partitions.size()
                  << " pack_pages=" << trace_pack_pages_count
                  << " encoded_pages=" << trace_encoded_pages_count
                  << " local_delta_prefixes=" << local_delta_prefix_count
                  << " local_delta_ops=" << local_delta_ops_count
                  << " local_bulk_prefixes=" << local_bulk_prefix_count
                  << " local_cow_prefixes=" << local_cow_prefix_count
                  << " local_fallback_count=" << local_rebuild_fallback_count
                  << " local_dirty_pack_pages=" << local_dirty_pack_pages
                  << " local_pack_write_bytes=" << local_pack_write_bytes
                  << " local_cleanup_destroy_unshared_count="
                  << local_cleanup_destroy_unshared_count
                  << " local_cleanup_destroy_full_count="
                  << local_cleanup_destroy_full_count
                  << " local_cleanup_reclaim_pack_pages="
                  << local_cleanup_reclaim_pack_pages
                  << " local_cleanup_skip_unshared_count="
                  << local_cleanup_skip_unshared_count
                  << " skip_unshared_cleanup="
                  << (skip_unshared_cleanup ? 1 : 0)
                  << " local_index_update_cow_ms=" << NsToMs(local_index_update_cow_ns)
                  << " local_index_update_bulk_ms=" << NsToMs(local_index_update_bulk_ns)
                  << "\n";
    };

    trace_rebuild_stage("records_built");

    const auto collect_pack_records = [&](RoutePrefix prefix,
                                          const std::vector<SubtreeRecord>& records,
                                          const RoutePartition& partition) {
        if (partition.record_count == 0 ||
            partition.descriptor_mode == RouteDescriptorMode::kTinyDirect) {
            return;
        }
        std::vector<SubtreeRecord> refined_records = records;
        std::unordered_map<uint64_t, PrefixEntryWindowMap> window_cache;
        window_cache.reserve(refined_records.size());
        for (auto& record : refined_records) {
            RefineRecordLeafWindowForPrefix(record, prefix, segment_allocator, window_cache);
        }
        std::sort(refined_records.begin(), refined_records.end(), RecordRouteKeyLess{});
        pack_records_by_prefix[prefix] = std::move(refined_records);
    };

    for (const auto& partition : partitions) {
        old_partitions_by_prefix.emplace(partition.prefix, &partition);
        if (!std::binary_search(changed_prefixes.begin(), changed_prefixes.end(), partition.prefix)) {
            next_partitions.push_back(partition);
        } else {
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                old_handles_for_cleanup.emplace(partition.prefix, partition.subtree_store);
            }
            if (partition.descriptor_mode == RouteDescriptorMode::kNormalPack &&
                partition.pack_page_ptr != kInvalidSubtreePagePtr) {
                uint32_t pack_page_size = partition.subtree_store.page_size;
                if (pack_page_size == 0) {
                    pack_page_size = subtree_page_size;
                }
                old_pack_ptrs_for_cleanup[pack_page_size].insert(partition.pack_page_ptr);
            }
        }
    }

    for (const auto& prefix : changed_prefixes) {
        const auto existing_it = old_partitions_by_prefix.find(prefix);
        const RoutePartition* existing_partition =
            existing_it == old_partitions_by_prefix.end() ? nullptr : existing_it->second;
        const L1PrefixDelta* prefix_delta = nullptr;
        if (!delta_by_prefix.empty()) {
            const auto delta_it = delta_by_prefix.find(prefix);
            if (delta_it != delta_by_prefix.end()) {
                prefix_delta = delta_it->second;
                ++local_delta_prefix_count;
                local_delta_ops_count += prefix_delta->ops.size();
            }
        }
        auto it = rebuilt_records.find(prefix);
        if (it == rebuilt_records.end() && prefix_delta == nullptr) {
            continue;
        }
        std::vector<SubtreeRecord> fallback_records;
        const std::vector<SubtreeRecord>* target_records = nullptr;
        if (it != rebuilt_records.end()) {
            std::sort(it->second.begin(), it->second.end(), RecordRouteKeyLess{});
            target_records = &it->second;
        } else {
            const auto merge_begin = Clock::now();
            if (!BuildRecordsFromDelta(prefix, existing_partition, prefix_delta->ops,
                                       subtree_options, segment_allocator, fallback_records)) {
                local_leaf_stream_merge_ns +=
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              Clock::now() - merge_begin)
                                              .count());
                ++local_rebuild_fallback_count;
                continue;
            }
            local_leaf_stream_merge_ns +=
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          Clock::now() - merge_begin)
                                          .count());
            target_records = &fallback_records;
        }

        size_t delta_op_count = 0;
        if (prefix_delta != nullptr) {
            delta_op_count = prefix_delta->ops.size();
        }
        const auto decision =
            ChooseUpdateMode(existing_partition, *target_records, subtree_options, update_policy, delta_op_count);
        if (decision.mode == PartitionUpdateMode::kCowPatch) {
            ++local_cow_prefix_count;
            const auto cow_begin = Clock::now();
            auto partition =
                ApplyCowUpdate(prefix, existing_partition, *target_records, build_generation,
                               subtree_options, subtree_page_size, segment_allocator, governance_policy);
            local_index_update_cow_ns +=
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          Clock::now() - cow_begin)
                                          .count());
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                new_handles_after_publish.emplace(prefix, partition.subtree_store);
            }
            collect_pack_records(prefix, *target_records, partition);
            next_partitions.push_back(std::move(partition));
        } else {
            ++local_bulk_prefix_count;
            std::vector<SubtreeRecord> bulk_records;
            const std::vector<SubtreeRecord>* bulk_source = target_records;
            if (update_policy.enable_leaf_stream_bulk_load && prefix_delta != nullptr &&
                !prefix_delta->ops.empty()) {
                const auto merge_begin = Clock::now();
                if (BuildRecordsFromDelta(prefix, existing_partition, prefix_delta->ops,
                                          subtree_options, segment_allocator, bulk_records)) {
                    bulk_source = &bulk_records;
                } else {
                    ++local_rebuild_fallback_count;
                }
                local_leaf_stream_merge_ns +=
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              Clock::now() - merge_begin)
                                              .count());
            }
            const auto bulk_begin = Clock::now();
            auto partition =
                ApplyBulkLoadUpdate(prefix, existing_partition, *bulk_source, build_generation, subtree_options,
                                    subtree_page_size, segment_allocator, governance_policy);
            local_index_update_bulk_ns +=
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          Clock::now() - bulk_begin)
                                          .count());
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                new_handles_after_publish.emplace(prefix, partition.subtree_store);
            }
            collect_pack_records(prefix, *bulk_source, partition);
            next_partitions.push_back(std::move(partition));
        }
    }
    trace_rebuild_stage("prefix_updates_done");

    std::unordered_map<RoutePrefix, size_t> next_partition_index;
    next_partition_index.reserve(next_partitions.size());
    for (size_t i = 0; i < next_partitions.size(); ++i) {
        next_partition_index.emplace(next_partitions[i].prefix, i);
    }

    std::vector<NormalPackPage> pack_pages;
    std::unordered_map<RoutePrefix, PackSlotAssignment> pack_assignments;
    if (!pack_records_by_prefix.empty()) {
        if (!BuildNormalPackPages(pack_records_by_prefix, subtree_page_size, pack_pages, pack_assignments)) {
            throw std::runtime_error("failed to build normal-pack pages");
        }
    }
    trace_pack_pages_count = static_cast<uint64_t>(pack_pages.size());
    trace_rebuild_stage("pack_pages_built");

    std::vector<SubtreePagePtr> new_pack_page_ptrs;
    if (!pack_pages.empty()) {
        std::vector<std::vector<uint8_t>> encoded_pages;
        encoded_pages.reserve(pack_pages.size());
        for (const auto& page : pack_pages) {
            std::vector<uint8_t> bytes;
            if (!NormalPackCodec::Encode(page, subtree_page_size, bytes)) {
                throw std::runtime_error("failed to encode normal-pack page");
            }
            encoded_pages.push_back(std::move(bytes));
        }
        trace_encoded_pages_count = static_cast<uint64_t>(encoded_pages.size());
        trace_rebuild_stage("pack_pages_encoded");
        new_pack_page_ptrs =
            SubtreePageStore::PersistOpaquePages(*segment_allocator, subtree_page_size, encoded_pages);
        if (new_pack_page_ptrs.size() != pack_pages.size()) {
            throw std::runtime_error("normal-pack page pointer count mismatch");
        }
        local_dirty_pack_pages = static_cast<uint64_t>(new_pack_page_ptrs.size());
        local_pack_write_bytes =
            static_cast<uint64_t>(new_pack_page_ptrs.size()) *
            static_cast<uint64_t>(subtree_page_size);
    }
    trace_rebuild_stage("pack_pages_persisted");

    for (const auto& prefix : changed_prefixes) {
        const auto it_idx = next_partition_index.find(prefix);
        if (it_idx == next_partition_index.end()) {
            continue;
        }
        auto& partition = next_partitions[it_idx->second];
        if (partition.record_count == 0 ||
            partition.descriptor_mode == RouteDescriptorMode::kTinyDirect) {
            if (partition.descriptor_mode == RouteDescriptorMode::kTinyDirect &&
                partition.record_count != 0) {
                ++local_tiny_descriptor_count;
            }
            partition.pack_page_ptr = kInvalidSubtreePagePtr;
            partition.pack_slot_id = 0;
            continue;
        }

        const auto it_assign = pack_assignments.find(prefix);
        if (it_assign == pack_assignments.end()) {
            partition.descriptor_mode = RouteDescriptorMode::kNormalSubtree;
            partition.pack_page_ptr = kInvalidSubtreePagePtr;
            partition.pack_slot_id = 0;
            continue;
        }
        if (it_assign->second.page_index >= new_pack_page_ptrs.size()) {
            throw std::runtime_error("normal-pack assignment page index overflow");
        }
        partition.descriptor_mode = RouteDescriptorMode::kNormalPack;
        partition.pack_page_ptr = new_pack_page_ptrs[it_assign->second.page_index];
        partition.pack_slot_id = static_cast<uint8_t>(it_assign->second.slot_id);
        ++local_normal_pack_count;
    }

    trace_rebuild_stage("before_cleanup");
    if (segment_allocator != nullptr) {
        for (const auto& [prefix, old_handle] : old_handles_for_cleanup) {
            const auto keep_it = new_handles_after_publish.find(prefix);
            if (keep_it != new_handles_after_publish.end()) {
                if (skip_unshared_cleanup) {
                    ++local_cleanup_skip_unshared_count;
                } else {
                    SubtreePageStore::DestroyUnshared(*segment_allocator, old_handle, keep_it->second);
                    ++local_cleanup_destroy_unshared_count;
                }
            } else {
                SubtreePageStore::Destroy(*segment_allocator, old_handle);
                ++local_cleanup_destroy_full_count;
            }
        }
        std::map<uint32_t, std::unordered_set<SubtreePagePtr>> keep_pack_ptrs;
        for (const auto& partition : next_partitions) {
            if (partition.descriptor_mode != RouteDescriptorMode::kNormalPack ||
                partition.pack_page_ptr == kInvalidSubtreePagePtr) {
                continue;
            }
            uint32_t pack_page_size = partition.subtree_store.page_size;
            if (pack_page_size == 0) {
                pack_page_size = subtree_page_size;
            }
            keep_pack_ptrs[pack_page_size].insert(partition.pack_page_ptr);
        }
        for (auto& [page_size, old_ptrs] : old_pack_ptrs_for_cleanup) {
            std::vector<SubtreePagePtr> reclaim;
            reclaim.reserve(old_ptrs.size());
            const auto keep_it = keep_pack_ptrs.find(page_size);
            for (const auto ptr : old_ptrs) {
                if (keep_it == keep_pack_ptrs.end() ||
                    keep_it->second.find(ptr) == keep_it->second.end()) {
                    reclaim.push_back(ptr);
                }
            }
            SubtreePageStore::DestroyOpaquePages(*segment_allocator, page_size, reclaim);
            local_cleanup_reclaim_pack_pages += static_cast<uint64_t>(reclaim.size());
        }
    }
    trace_rebuild_stage("after_cleanup");

    trace_rebuild_stage("before_refresh_layout");
    partitions = std::move(next_partitions);
    layout.RefreshPartitions(partitions);
    if (delta_batch != nullptr) {
        size = CountPartitionRecords(partitions);
    } else {
        size = CountActiveTables(tables);
    }
    trace_rebuild_stage("after_refresh_layout");
    trace_rebuild_stage("publish_done");

    const uint64_t local_index_update_total_ns =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  Clock::now() - fn_begin)
                                  .count());
    g_delta_prefix_count.fetch_add(local_delta_prefix_count, std::memory_order_relaxed);
    g_delta_ops_count.fetch_add(local_delta_ops_count, std::memory_order_relaxed);
    g_index_update_total_ns.fetch_add(local_index_update_total_ns, std::memory_order_relaxed);
    g_index_update_cow_ns.fetch_add(local_index_update_cow_ns, std::memory_order_relaxed);
    g_index_update_bulk_ns.fetch_add(local_index_update_bulk_ns, std::memory_order_relaxed);
    g_cow_prefix_count.fetch_add(local_cow_prefix_count, std::memory_order_relaxed);
    g_bulk_prefix_count.fetch_add(local_bulk_prefix_count, std::memory_order_relaxed);
    g_leaf_stream_merge_ns.fetch_add(local_leaf_stream_merge_ns, std::memory_order_relaxed);
    g_rebuild_fallback_count.fetch_add(local_rebuild_fallback_count, std::memory_order_relaxed);
    g_tiny_descriptor_count.fetch_add(local_tiny_descriptor_count, std::memory_order_relaxed);
    g_normal_pack_count.fetch_add(local_normal_pack_count, std::memory_order_relaxed);
    g_dirty_pack_pages.fetch_add(local_dirty_pack_pages, std::memory_order_relaxed);
    g_pack_write_bytes.fetch_add(local_pack_write_bytes, std::memory_order_relaxed);
    trace_rebuild_stage("function_end");
}

void L1HybridRebuilder::ValidateSortedRecords(const std::vector<SubtreeRecord>& sorted_records) {
    RecordMaxKeyLess less;
    for (size_t i = 1; i < sorted_records.size(); ++i) {
        if (less(sorted_records[i], sorted_records[i - 1])) {
            throw std::invalid_argument("L1HybridIndex::BulkLoad expects records sorted by RecordMaxKeyLess");
        }
    }
}

RoutePartition L1HybridRebuilder::BuildPartition(
    RoutePrefix prefix,
    const std::vector<SubtreeRecord>& records,
    uint64_t generation,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    uint32_t subtree_page_size,
    SegmentAllocator* segment_allocator,
    const PrefixGovernancePolicy& governance_policy,
    const RoutePartition* existing_partition) {
    if (segment_allocator == nullptr) {
        throw std::invalid_argument("L1HybridRebuilder::BuildPartition requires segment allocator");
    }
    RoutePartition partition;
    partition.prefix = prefix;
    if (generation > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("generation overflow");
    }
    partition.generation = static_cast<uint32_t>(generation);
    if (records.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("record_count overflow");
    }
    partition.record_count = static_cast<uint32_t>(records.size());
    partition.pack_slot_id = 0;
    partition.pack_page_ptr = kInvalidSubtreePagePtr;
    if (governance_policy.enable_lightweight_governance) {
        partition.governance.hot_prefix =
            partition.record_count >= governance_policy.hot_prefix_record_threshold;
        partition.governance.prefer_cow =
            partition.record_count >= governance_policy.force_cow_record_threshold;
        partition.governance.prefer_parallel_scan =
            partition.record_count >= governance_policy.parallel_scan_record_threshold;
    }
    if (!records.empty()) {
        std::vector<SubtreeRecord> refined_records = records;
        std::unordered_map<uint64_t, PrefixEntryWindowMap> window_cache;
        window_cache.reserve(refined_records.size());
        for (auto& record : refined_records) {
            RefineRecordLeafWindowForPrefix(record, prefix, segment_allocator, window_cache);
        }

        const auto tiny_stats = CollectTinyCandidateStats(refined_records);
        uint8_t tiny_enter_streak = 0;
        uint64_t tiny_leaf_value = 0;
        partition.descriptor_mode = DecideDescriptorMode(existing_partition, tiny_stats,
                                                         tiny_enter_streak, tiny_leaf_value);
        partition.tiny_enter_streak = tiny_enter_streak;
        partition.tiny_leaf_value = tiny_leaf_value;

        L1SubtreeBPTree subtree(subtree_options);
        subtree.BulkLoad(refined_records);
        auto page_set = subtree.ExportPageSet(subtree_page_size);
        partition.subtree_store = SubtreePageStore::Persist(*segment_allocator, page_set);
    } else {
        partition.descriptor_mode = RouteDescriptorMode::kNormalSubtree;
        partition.tiny_enter_streak = 0;
        partition.tiny_leaf_value = 0;
    }
    return partition;
}

PartitionUpdateDecision L1HybridRebuilder::ChooseUpdateMode(
    const RoutePartition* existing_partition,
    const std::vector<SubtreeRecord>& target_records,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    const PartitionUpdatePolicy& update_policy,
    size_t delta_op_count) {
    PartitionUpdateDecision decision;
    decision.target_record_count = target_records.size();
    if (!update_policy.enable_cow || existing_partition == nullptr ||
        !existing_partition->subtree_store.Valid()) {
        return decision;
    }

    // Disk-resident mode currently does not reconstruct full previous subtree for update diffing.
    // Keep rule-based policy deterministic and conservative: prefer bulk-load rebuild.
    decision.existing_record_count = existing_partition->record_count;
    if (decision.existing_record_count < update_policy.small_tree_record_threshold) {
        return decision;
    }

    if (delta_op_count != 0) {
        decision.changed_record_count = delta_op_count;
    } else {
        decision.changed_record_count = decision.target_record_count;
        if (decision.target_record_count > decision.existing_record_count) {
            decision.changed_record_count = decision.target_record_count - decision.existing_record_count;
        } else if (decision.existing_record_count > decision.target_record_count) {
            decision.changed_record_count = decision.existing_record_count - decision.target_record_count;
        } else {
            decision.changed_record_count = 1;
        }
    }
    const size_t denominator = std::max<size_t>(1, decision.existing_record_count);
    const uint32_t changed_percent =
        static_cast<uint32_t>((decision.changed_record_count * 100) / denominator);
    decision.changed_leaf_span =
        (decision.changed_record_count + subtree_options.leaf_capacity - 1) / subtree_options.leaf_capacity;
    size_t max_changed_records = update_policy.cow_max_changed_records;
    size_t max_leaf_spans = update_policy.cow_max_leaf_spans;
    uint32_t max_change_percent = update_policy.cow_max_change_percent;
    if (existing_partition->governance.prefer_cow) {
        max_changed_records = update_policy.hot_prefix_relaxed_cow_max_changed_records;
        max_leaf_spans = update_policy.hot_prefix_relaxed_cow_max_leaf_spans;
        max_change_percent = update_policy.hot_prefix_relaxed_cow_max_change_percent;
    }
    if (decision.changed_record_count <= max_changed_records &&
        decision.changed_leaf_span <= max_leaf_spans &&
        changed_percent <= max_change_percent) {
        decision.mode = PartitionUpdateMode::kCowPatch;
    }
    return decision;
}

bool L1HybridRebuilder::BuildRecordsFromDelta(
    RoutePrefix prefix,
    const RoutePartition* existing_partition,
    const std::vector<L1DeltaOp>& delta_ops,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    SegmentAllocator* segment_allocator,
    std::vector<SubtreeRecord>& out_records) {
    out_records.clear();
    if (segment_allocator == nullptr) {
        return false;
    }
    if (existing_partition != nullptr && existing_partition->subtree_store.Valid()) {
        const auto page_set = SubtreePageStore::Load(*segment_allocator, existing_partition->subtree_store);
        L1SubtreeBPTree existing_tree(subtree_options);
        existing_tree.ImportPageSet(page_set);
        existing_tree.ExportAll(out_records);
        out_records.erase(std::remove_if(out_records.begin(), out_records.end(),
                                         [prefix](const SubtreeRecord& record) {
                                             return record.route_prefix != prefix;
                                         }),
                          out_records.end());
    }

    uint64_t synthetic_idx = (static_cast<uint64_t>(prefix) << 32);
    constexpr uint32_t kDeltaSyntheticSeqNo = std::numeric_limits<uint32_t>::max();
    for (const auto& op : delta_ops) {
        if (op.type == L1DeltaOpType::kDelete || op.type == L1DeltaOpType::kReplace) {
            out_records.erase(std::remove_if(out_records.begin(), out_records.end(),
                                             [&](const SubtreeRecord& record) {
                                                 return MatchesDeleteOp(record, prefix, op);
                                             }),
                              out_records.end());
        }
        if ((op.type == L1DeltaOpType::kAdd || op.type == L1DeltaOpType::kReplace) && op.count != 0) {
            out_records.push_back(MakeRecordFromDeltaOp(prefix, op, synthetic_idx++, kDeltaSyntheticSeqNo));
        }
    }
    std::sort(out_records.begin(), out_records.end(), RecordRouteKeyLess{});
    return true;
}

RoutePartition L1HybridRebuilder::ApplyBulkLoadUpdate(
    RoutePrefix prefix,
    const RoutePartition* existing_partition,
    const std::vector<SubtreeRecord>& records,
    uint64_t generation,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    uint32_t subtree_page_size,
    SegmentAllocator* segment_allocator,
    const PrefixGovernancePolicy& governance_policy) {
    return BuildPartition(prefix, records, generation, subtree_options, subtree_page_size,
                          segment_allocator, governance_policy, existing_partition);
}

RoutePartition L1HybridRebuilder::ApplyCowUpdate(
    RoutePrefix prefix,
    const RoutePartition* existing_partition,
    const std::vector<SubtreeRecord>& records,
    uint64_t generation,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    uint32_t subtree_page_size,
    SegmentAllocator* segment_allocator,
    const PrefixGovernancePolicy& governance_policy) {
    if (segment_allocator == nullptr) {
        throw std::invalid_argument("L1HybridRebuilder::ApplyCowUpdate requires segment allocator");
    }
    if (existing_partition == nullptr || !existing_partition->subtree_store.Valid()) {
        return BuildPartition(prefix, records, generation, subtree_options, subtree_page_size,
                              segment_allocator, governance_policy, existing_partition);
    }

    RoutePartition partition;
    partition.prefix = prefix;
    if (generation > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("generation overflow");
    }
    partition.generation = static_cast<uint32_t>(generation);
    if (records.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("record_count overflow");
    }
    partition.record_count = static_cast<uint32_t>(records.size());
    partition.pack_slot_id = 0;
    partition.pack_page_ptr = kInvalidSubtreePagePtr;
    if (governance_policy.enable_lightweight_governance) {
        partition.governance.hot_prefix =
            partition.record_count >= governance_policy.hot_prefix_record_threshold;
        partition.governance.prefer_cow =
            partition.record_count >= governance_policy.force_cow_record_threshold;
        partition.governance.prefer_parallel_scan =
            partition.record_count >= governance_policy.parallel_scan_record_threshold;
    }
    const auto tiny_stats = CollectTinyCandidateStats(records);
    uint8_t tiny_enter_streak = 0;
    uint64_t tiny_leaf_value = 0;
    partition.descriptor_mode = DecideDescriptorMode(existing_partition, tiny_stats,
                                                     tiny_enter_streak, tiny_leaf_value);
    partition.tiny_enter_streak = tiny_enter_streak;
    partition.tiny_leaf_value = tiny_leaf_value;

    if (!records.empty()) {
        const auto base_page_set =
            SubtreePageStore::Load(*segment_allocator, existing_partition->subtree_store);
        L1SubtreeBPTree subtree(subtree_options);
        subtree.BulkLoad(records);
        const auto target_page_set = subtree.ExportPageSet(subtree_page_size);
        partition.subtree_store = SubtreePageStore::PersistCow(
            *segment_allocator, existing_partition->subtree_store, base_page_set, target_page_set);
    }
    return partition;
}

void L1HybridRebuilder::RebuildPartitionFromRecords(
    size_t partition_idx,
    const std::vector<SubtreeRecord>& records,
    uint64_t generation,
    std::vector<RoutePartition>& partitions,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    uint32_t subtree_page_size,
    SegmentAllocator* segment_allocator,
    const PrefixGovernancePolicy& governance_policy) {
    auto& partition = partitions[partition_idx];
    partition = BuildPartition(partition.prefix, records, generation, subtree_options,
                               subtree_page_size, segment_allocator, governance_policy, nullptr);
}

}  // namespace flowkv::hybrid_l1
