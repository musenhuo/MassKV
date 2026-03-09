#include "lib/hybrid_l1/l1_hybrid_rebuilder.h"
#include "db/blocks/fixed_size_block.h"

#include <algorithm>
#include <limits>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <unistd.h>

namespace flowkv::hybrid_l1 {

namespace {

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
                         uint16_t entry_num,
                         SegmentAllocator* segment_allocator,
                         PrefixEntryWindowMap& windows_out) {
    if (segment_allocator == nullptr || entry_num == 0) {
        return false;
    }
    PDataBlock block{};
    const uint64_t block_off = SubtreeRecord::DecodeKvBlockOffset(kv_block_ptr);
    const ssize_t ret = pread(segment_allocator->Getfd(), &block, sizeof(PDataBlock), block_off);
    if (ret != static_cast<ssize_t>(sizeof(PDataBlock))) {
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

    const uint64_t cache_key = (leaf_window.kv_block_ptr << 16) | leaf_window.count;
    auto cache_it = window_cache.find(cache_key);
    if (cache_it == window_cache.end()) {
        PrefixEntryWindowMap windows;
        if (!LoadPrefixWindowMap(leaf_window.kv_block_ptr, leaf_window.count,
                                 segment_allocator, windows)) {
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

}  // namespace

void L1HybridRebuilder::ResetPartitions(std::vector<RoutePartition>& partitions) {
    ResetPartitions(partitions, nullptr);
}

void L1HybridRebuilder::ResetPartitions(std::vector<RoutePartition>& partitions,
                                        SegmentAllocator* segment_allocator) {
    if (segment_allocator != nullptr) {
        for (const auto& partition : partitions) {
            if (partition.subtree_store.Valid()) {
                SubtreePageStore::Destroy(*segment_allocator, partition.subtree_store);
            }
        }
    }
    for (auto& partition : partitions) {
        partition.subtree_pages_cache.reset();
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
                                                    size_t& size,
                                                    uint64_t& generation) {
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

    for (const auto& partition : partitions) {
        old_partitions_by_prefix.emplace(partition.prefix, &partition);
        if (!std::binary_search(changed_prefixes.begin(), changed_prefixes.end(), partition.prefix)) {
            next_partitions.push_back(partition);
        } else {
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                old_handles_for_cleanup.emplace(partition.prefix, partition.subtree_store);
            }
        }
    }

    for (const auto& prefix : changed_prefixes) {
        const auto existing_it = old_partitions_by_prefix.find(prefix);
        const RoutePartition* existing_partition =
            existing_it == old_partitions_by_prefix.end() ? nullptr : existing_it->second;
        auto it = rebuilt_records.find(prefix);
        if (it == rebuilt_records.end()) {
            continue;
        }
        std::sort(it->second.begin(), it->second.end(), RecordRouteKeyLess{});
        const auto decision =
            ChooseUpdateMode(existing_partition, it->second, subtree_options, update_policy);
        if (decision.mode == PartitionUpdateMode::kCowPatch) {
            auto partition =
                ApplyCowUpdate(prefix, existing_partition, it->second, build_generation,
                               subtree_options, subtree_page_size, segment_allocator, governance_policy);
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                new_handles_after_publish.emplace(prefix, partition.subtree_store);
            }
            next_partitions.push_back(std::move(partition));
        } else {
            auto partition =
                ApplyBulkLoadUpdate(prefix, it->second, build_generation, subtree_options,
                                    subtree_page_size, segment_allocator, governance_policy);
            if (segment_allocator != nullptr && partition.subtree_store.Valid()) {
                new_handles_after_publish.emplace(prefix, partition.subtree_store);
            }
            next_partitions.push_back(std::move(partition));
        }
    }

    if (segment_allocator != nullptr) {
        for (const auto& [prefix, old_handle] : old_handles_for_cleanup) {
            const auto keep_it = new_handles_after_publish.find(prefix);
            if (keep_it != new_handles_after_publish.end()) {
                SubtreePageStore::DestroyUnshared(*segment_allocator, old_handle, keep_it->second);
            } else {
                SubtreePageStore::Destroy(*segment_allocator, old_handle);
            }
        }
    }

    partitions = std::move(next_partitions);
    layout.RefreshPartitions(partitions);
    size = CountActiveTables(tables);
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
    const PrefixGovernancePolicy& governance_policy) {
    RoutePartition partition;
    partition.prefix = prefix;
    partition.generation = generation;
    if (records.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("record_count overflow");
    }
    partition.record_count = static_cast<uint32_t>(records.size());
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
        if (segment_allocator != nullptr) {
            std::unordered_map<uint64_t, PrefixEntryWindowMap> window_cache;
            window_cache.reserve(refined_records.size());
            for (auto& record : refined_records) {
                RefineRecordLeafWindowForPrefix(record, prefix, segment_allocator, window_cache);
            }
        }

        L1SubtreeBPTree subtree(subtree_options);
        subtree.BulkLoad(refined_records);
        auto page_set = subtree.ExportPageSet(subtree_page_size);
        if (segment_allocator != nullptr) {
            // Disk-resident strict mode: allocator path must persist successfully.
            // Do not silently fall back to in-memory encoded pages.
            partition.subtree_store = SubtreePageStore::Persist(*segment_allocator, page_set);
        } else {
            // Debug/unit-test fallback when allocator is not wired.
            partition.subtree_pages_cache = std::make_shared<SubtreePageSet>(std::move(page_set));
        }
    }
    return partition;
}

PartitionUpdateDecision L1HybridRebuilder::ChooseUpdateMode(
    const RoutePartition* existing_partition,
    const std::vector<SubtreeRecord>& target_records,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    const PartitionUpdatePolicy& update_policy) {
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

    decision.changed_record_count = decision.target_record_count;
    if (decision.target_record_count > decision.existing_record_count) {
        decision.changed_record_count = decision.target_record_count - decision.existing_record_count;
    } else if (decision.existing_record_count > decision.target_record_count) {
        decision.changed_record_count = decision.existing_record_count - decision.target_record_count;
    } else {
        decision.changed_record_count = 1;
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

RoutePartition L1HybridRebuilder::ApplyBulkLoadUpdate(
    RoutePrefix prefix,
    const std::vector<SubtreeRecord>& records,
    uint64_t generation,
    const L1SubtreeBPTree::BuildOptions& subtree_options,
    uint32_t subtree_page_size,
    SegmentAllocator* segment_allocator,
    const PrefixGovernancePolicy& governance_policy) {
    return BuildPartition(prefix, records, generation, subtree_options, subtree_page_size,
                          segment_allocator, governance_policy);
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
    if (existing_partition == nullptr || !existing_partition->subtree_store.Valid() ||
        segment_allocator == nullptr) {
        return BuildPartition(prefix, records, generation, subtree_options, subtree_page_size,
                              segment_allocator, governance_policy);
    }

    RoutePartition partition;
    partition.prefix = prefix;
    partition.generation = generation;
    if (records.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("record_count overflow");
    }
    partition.record_count = static_cast<uint32_t>(records.size());
    if (governance_policy.enable_lightweight_governance) {
        partition.governance.hot_prefix =
            partition.record_count >= governance_policy.hot_prefix_record_threshold;
        partition.governance.prefer_cow =
            partition.record_count >= governance_policy.force_cow_record_threshold;
        partition.governance.prefer_parallel_scan =
            partition.record_count >= governance_policy.parallel_scan_record_threshold;
    }

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
                               subtree_page_size, segment_allocator, governance_policy);
}

}  // namespace flowkv::hybrid_l1
