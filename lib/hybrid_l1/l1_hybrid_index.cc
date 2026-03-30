#include "lib/hybrid_l1/l1_hybrid_index.h"
#include "lib/hybrid_l1/normal_pack.h"

#include <algorithm>
#include <future>
#include <functional>
#include <limits>
#include <queue>
#include <stdexcept>
#include <utility>
#include <unordered_set>

namespace flowkv::hybrid_l1 {

namespace {

constexpr uint8_t kGovernanceHotPrefix = 1u << 0;
constexpr uint8_t kGovernancePreferCow = 1u << 1;
constexpr uint8_t kGovernancePreferParallelScan = 1u << 2;
constexpr uint32_t kFixedSubtreePageSizeBytes = 16 * 1024;

uint8_t EncodeGovernanceFlags(const BucketGovernanceState& governance) {
    uint8_t flags = 0;
    if (governance.hot_prefix) {
        flags |= kGovernanceHotPrefix;
    }
    if (governance.prefer_cow) {
        flags |= kGovernancePreferCow;
    }
    if (governance.prefer_parallel_scan) {
        flags |= kGovernancePreferParallelScan;
    }
    return flags;
}

constexpr size_t kMaxSubtreeTraversalHops = 64;

RouteSuffix RangeStartSuffixForPrefix(RoutePrefix prefix,
                                      RoutePrefix start_prefix,
                                      const KeyType& start) {
    return prefix == start_prefix ? ExtractSuffix(start) : 0;
}

RouteSuffix RangeEndSuffixForPrefix(RoutePrefix prefix,
                                    RoutePrefix end_prefix,
                                    const KeyType& end) {
    return prefix == end_prefix ? ExtractSuffix(end) : std::numeric_limits<RouteSuffix>::max();
}

SubtreeRecord MakeWindowRecord(RoutePrefix prefix,
                               RouteSuffix suffix_min,
                               RouteSuffix suffix_max,
                               uint64_t leaf_value) {
    SubtreeRecord record;
    record.route_prefix = prefix;
    record.route_min_suffix = suffix_min;
    record.route_max_suffix = suffix_max;
    record.min_key = ComposeKey(prefix, suffix_min);
    record.max_key = ComposeKey(prefix, suffix_max);
    record.seq_no = 0;
    record.table_idx = INVALID_PTR;
    record.leaf_value = leaf_value;
    return record;
}

bool WindowDedupLess(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    if (lhs.route_prefix != rhs.route_prefix) {
        return lhs.route_prefix < rhs.route_prefix;
    }
    if (lhs.route_min_suffix != rhs.route_min_suffix) {
        return lhs.route_min_suffix < rhs.route_min_suffix;
    }
    if (lhs.route_max_suffix != rhs.route_max_suffix) {
        return lhs.route_max_suffix < rhs.route_max_suffix;
    }
    if (lhs.leaf_value != rhs.leaf_value) {
        return lhs.leaf_value < rhs.leaf_value;
    }
    if (lhs.seq_no != rhs.seq_no) {
        return lhs.seq_no < rhs.seq_no;
    }
    return lhs.table_idx < rhs.table_idx;
}

bool WindowDedupEqual(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    return lhs.route_prefix == rhs.route_prefix &&
           lhs.route_min_suffix == rhs.route_min_suffix &&
           lhs.route_max_suffix == rhs.route_max_suffix &&
           lhs.leaf_value == rhs.leaf_value;
}

}  // namespace

size_t L1HybridIndex::SubtreeCacheKeyHash::operator()(const SubtreeCacheKey& key) const {
    const size_t h1 = std::hash<RoutePrefix>{}(key.prefix);
    const size_t h2 = std::hash<uint64_t>{}(key.generation);
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
}

L1HybridIndex::L1HybridIndex(BuildOptions options)
    : options_(options),
      layout_(options.route_partition_num,
              RouteSwapOptions{options.route_hot_leaf_budget_bytes,
                               options.segment_allocator,
                               4 * 1024}) {
    if (options_.segment_allocator == nullptr) {
        throw std::invalid_argument("L1HybridIndex requires non-null segment allocator");
    }
    if (options_.subtree_page_size != kFixedSubtreePageSizeBytes) {
        throw std::invalid_argument("L1HybridIndex requires fixed 16KB subtree pages");
    }
    layout_.InitializePartitions(partitions_);
    BuildPublishedSnapshot();
}

L1HybridIndex::~L1HybridIndex() = default;

void L1HybridIndex::Clear() {
    InvalidateSubtreeCache();
    L1HybridRebuilder::ResetPartitions(partitions_, options_.segment_allocator);
    partitions_.shrink_to_fit();
    published_snapshot_.reset();
    size_ = 0;
}

void L1HybridIndex::BulkLoad(const std::vector<SubtreeRecord>& sorted_records) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::BulkLoad(sorted_records, partitions_, layout_, options_.subtree_options,
                                options_.subtree_page_size, options_.segment_allocator,
                                options_.governance_policy, size_, generation_);
    partitions_.shrink_to_fit();
    BuildPublishedSnapshot();
}

void L1HybridIndex::BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::BulkLoadFromTables(tables, partitions_, layout_, options_.subtree_options,
                                          options_.subtree_page_size, options_.segment_allocator,
                                          options_.governance_policy, size_, generation_);
    partitions_.shrink_to_fit();
    BuildPublishedSnapshot();
}

void L1HybridIndex::RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                                const std::vector<KeyType>& changed_route_keys,
                                                const L1DeltaBatch* delta_batch) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::RebuildPartitionsFromTables(tables, changed_route_keys, partitions_, layout_,
                                                   options_.subtree_options, options_.subtree_page_size,
                                                   options_.segment_allocator, options_.update_policy,
                                                   options_.governance_policy, delta_batch,
                                                   size_, generation_);
    partitions_.shrink_to_fit();
    BuildPublishedSnapshot();
}

void L1HybridIndex::RebuildPartitionsFromDelta(
    const std::vector<KeyType>& changed_route_keys,
    const L1DeltaBatch& delta_batch) {
    static const std::vector<TaggedPstMeta> kEmptyTables;
    RebuildPartitionsFromTables(kEmptyTables, changed_route_keys, &delta_batch);
}

bool L1HybridIndex::LookupCandidate(const KeyType& key, SubtreeRecord& out) const {
    if (size_ == 0) {
        return false;
    }

    uint64_t descriptor = 0;
    if (!layout_.FindDescriptorByKey(key, descriptor)) {
        return false;
    }

    if (RouteDescriptor::IsTiny(descriptor)) {
        const auto window = RouteDescriptor::DecodeTinyWindow(descriptor);
        if (window.count == 0) {
            return false;
        }
        const RoutePrefix prefix = ExtractPrefix(key);
        out = SubtreeRecord{};
        out.route_prefix = prefix;
        out.route_min_suffix = 0;
        out.route_max_suffix = std::numeric_limits<RouteSuffix>::max();
        out.min_key = ComposeKey(prefix, out.route_min_suffix);
        out.max_key = ComposeKey(prefix, out.route_max_suffix);
        out.table_idx = INVALID_PTR;
        out.seq_no = 0;
        out.leaf_value = descriptor;
        return true;
    }

    const RoutePrefix prefix = ExtractPrefix(key);
    const auto snapshot = published_snapshot_;
    const PublishedRoutePartition* route = nullptr;
    if (snapshot != nullptr) {
        const auto it =
            std::lower_bound(snapshot->routes.begin(), snapshot->routes.end(), prefix,
                             [](const PublishedRoutePartition& lhs, RoutePrefix rhs) {
                                 return lhs.prefix < rhs;
                             });
        if (it != snapshot->routes.end() && it->prefix == prefix) {
            route = &(*it);
        }
    }

    const auto mode =
        route == nullptr ? RouteDescriptorMode::kNormalSubtree
                         : static_cast<RouteDescriptorMode>(route->descriptor_mode);
    if (mode == RouteDescriptorMode::kNormalPack && RouteDescriptor::IsNormalPack(descriptor)) {
        SubtreePagePtr pack_page_ptr = kInvalidSubtreePagePtr;
        uint8_t pack_slot_id = 0;
        if (!RouteDescriptor::DecodeNormalPack(descriptor, pack_page_ptr, pack_slot_id)) {
            return false;
        }
        try {
            const auto page_bytes = SubtreePageStore::LoadPageByPtr(
                *options_.segment_allocator, options_.subtree_page_size, pack_page_ptr);
            RouteSuffix suffix_min = 0;
            RouteSuffix suffix_max = 0;
            uint64_t leaf_value = 0;
            if (!NormalPackCodec::LookupFromBytes(page_bytes, pack_slot_id, ExtractSuffix(key),
                                                  suffix_min, suffix_max, leaf_value)) {
                return false;
            }
            out = SubtreeRecord{};
            out.route_prefix = prefix;
            out.route_min_suffix = suffix_min;
            out.route_max_suffix = suffix_max;
            out.min_key = ComposeKey(prefix, suffix_min);
            out.max_key = ComposeKey(prefix, suffix_max);
            out.table_idx = INVALID_PTR;
            out.seq_no = 0;
            out.leaf_value = leaf_value;
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    SubtreePagePtr root_page_ptr = RouteDescriptor::DecodeNormalRoot(descriptor);
    if (root_page_ptr == kInvalidSubtreePagePtr &&
        !layout_.FindRootByKey(key, root_page_ptr)) {
        return false;
    }

    std::vector<SubtreeRecord> matches;
    if (!LookupCandidatesFromDisk(root_page_ptr, key, 1, matches)) {
        return false;
    }
    if (matches.empty()) {
        return false;
    }
    out = matches.front();
    return true;
}

void L1HybridIndex::LookupCandidates(const KeyType& key,
                                     size_t limit,
                                     std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (limit == 0 || size_ == 0) {
        return;
    }

    SubtreePagePtr root_page_ptr = kInvalidSubtreePagePtr;
    if (!layout_.FindRootByKey(key, root_page_ptr)) {
        return;
    }

    LookupCandidatesFromDisk(root_page_ptr, key, limit, out);
}

void L1HybridIndex::RangeScan(const KeyType& start,
                              const KeyType& end,
                              std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (CompareKeyType(start, end) > 0 || size_ == 0) {
        return;
    }

    std::vector<RoutedRoot> route_roots;
    layout_.CollectRootsForRange(start, end, route_roots);
    if (route_roots.empty()) {
        return;
    }

    if (options_.enable_parallel_range_scan &&
        route_roots.size() >= options_.parallel_scan_min_partitions &&
        options_.parallel_scan_max_tasks > 1) {
        ParallelRangeScan(route_roots, start, end, out);
    } else {
        SerialRangeScan(route_roots, start, end, out);
    }
}

void L1HybridIndex::RangeScanRecords(const KeyType& start,
                                     const KeyType& end,
                                     const RangeScanRecordOptions& options,
                                     RangeScanRecordResult& out) const {
    out.window_fragments.clear();
    out.unique_kv_block_ptrs.clear();
    if (CompareKeyType(start, end) > 0 || size_ == 0) {
        return;
    }

    std::vector<RouteSnapshotEntry> descriptors;
    layout_.CollectDescriptorsForRange(start, end, descriptors);
    if (descriptors.empty()) {
        return;
    }

    const RoutePrefix start_prefix = ExtractPrefix(start);
    const RoutePrefix end_prefix = ExtractPrefix(end);
    std::unordered_set<uint64_t> unique_blocks;
    if (options.include_unique_blocks) {
        unique_blocks.reserve(descriptors.size() * 2);
    }

    auto emit_record = [&](const SubtreeRecord& record) {
        if (options.include_window_fragments) {
            out.window_fragments.push_back(record);
        }
        if (options.include_unique_blocks) {
            const auto leaf_window = record.LeafWindow();
            if (leaf_window.count != 0) {
                unique_blocks.insert(leaf_window.kv_block_ptr);
            }
        }
    };

    for (const auto& entry : descriptors) {
        const RoutePrefix prefix = entry.prefix;
        const RouteSuffix local_start_suffix =
            RangeStartSuffixForPrefix(prefix, start_prefix, start);
        const RouteSuffix local_end_suffix =
            RangeEndSuffixForPrefix(prefix, end_prefix, end);
        if (local_start_suffix > local_end_suffix) {
            continue;
        }

        if (RouteDescriptor::IsTiny(entry.descriptor)) {
            const auto leaf_window = RouteDescriptor::DecodeTinyWindow(entry.descriptor);
            if (leaf_window.count == 0) {
                continue;
            }
            emit_record(MakeWindowRecord(prefix, local_start_suffix, local_end_suffix,
                                         entry.descriptor));
            continue;
        }

        if (RouteDescriptor::IsNormalPack(entry.descriptor)) {
            SubtreePagePtr pack_page_ptr = kInvalidSubtreePagePtr;
            uint8_t pack_slot_id = 0;
            if (!RouteDescriptor::DecodeNormalPack(entry.descriptor, pack_page_ptr, pack_slot_id)) {
                continue;
            }
            try {
                const auto page_bytes = SubtreePageStore::LoadPageByPtr(
                    *options_.segment_allocator, options_.subtree_page_size, pack_page_ptr);
                NormalPackPage pack_page;
                if (!NormalPackCodec::Decode(page_bytes, pack_page)) {
                    continue;
                }
                if (pack_slot_id >= pack_page.slots.size()) {
                    continue;
                }
                const auto& slot = pack_page.slots[pack_slot_id];
                const size_t begin = static_cast<size_t>(slot.entry_begin);
                const size_t end_idx = begin + static_cast<size_t>(slot.entry_count);
                if (begin > pack_page.entries.size() || end_idx > pack_page.entries.size()) {
                    continue;
                }
                for (size_t i = begin; i < end_idx; ++i) {
                    const auto& packed_entry = pack_page.entries[i];
                    if (packed_entry.suffix_max < local_start_suffix ||
                        packed_entry.suffix_min > local_end_suffix) {
                        continue;
                    }
                    const RouteSuffix clipped_min =
                        std::max(packed_entry.suffix_min, local_start_suffix);
                    const RouteSuffix clipped_max =
                        std::min(packed_entry.suffix_max, local_end_suffix);
                    if (clipped_min > clipped_max) {
                        continue;
                    }
                    emit_record(MakeWindowRecord(prefix, clipped_min, clipped_max,
                                                 packed_entry.leaf_value));
                }
            } catch (const std::exception&) {
                continue;
            }
            continue;
        }

        SubtreePagePtr root_page_ptr = entry.root_page_ptr;
        if (root_page_ptr == kInvalidSubtreePagePtr) {
            root_page_ptr = RouteDescriptor::DecodeNormalRoot(entry.descriptor);
        }
        if (root_page_ptr == kInvalidSubtreePagePtr) {
            continue;
        }
        std::vector<SubtreeRecord> partition_records;
        if (!ScanPartitionRangeFromDisk(prefix,
                                        root_page_ptr,
                                        local_start_suffix,
                                        local_end_suffix,
                                        partition_records)) {
            continue;
        }
        for (const auto& record : partition_records) {
            emit_record(record);
        }
    }

    if (options.include_window_fragments) {
        if (options.dedup_windows) {
            std::sort(out.window_fragments.begin(), out.window_fragments.end(), WindowDedupLess);
            out.window_fragments.erase(
                std::unique(out.window_fragments.begin(),
                            out.window_fragments.end(),
                            WindowDedupEqual),
                out.window_fragments.end());
        }
        std::sort(out.window_fragments.begin(), out.window_fragments.end(), RecordRouteKeyLess{});
    }

    if (options.include_unique_blocks) {
        out.unique_kv_block_ptrs.reserve(unique_blocks.size());
        for (uint64_t block_ptr : unique_blocks) {
            out.unique_kv_block_ptrs.push_back(block_ptr);
        }
        std::sort(out.unique_kv_block_ptrs.begin(), out.unique_kv_block_ptrs.end());
    }
}

void L1HybridIndex::ExportAll(std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (size_ == 0) {
        return;
    }

    std::unordered_set<uint64_t> seen_table_idx;
    out.reserve(size_);
    for (const auto& partition : partitions_) {
        const auto subtree = GetPartitionSubtree(&partition - partitions_.data());
        if (subtree == nullptr) {
            continue;
        }
        std::vector<SubtreeRecord> partition_records;
        subtree->ExportAll(partition_records);
        for (const auto& record : partition_records) {
            if (seen_table_idx.insert(record.table_idx).second) {
                out.push_back(record);
            }
        }
    }
    std::sort(out.begin(), out.end(), RecordMaxKeyLess{});
}

void L1HybridIndex::ExportLocalFragments(std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (size_ == 0) {
        return;
    }

    for (const auto& partition : partitions_) {
        const auto subtree = GetPartitionSubtree(&partition - partitions_.data());
        if (subtree == nullptr) {
            continue;
        }
        std::vector<SubtreeRecord> partition_records;
        subtree->ExportAll(partition_records);
        out.insert(out.end(), partition_records.begin(), partition_records.end());
    }
    std::sort(out.begin(), out.end(), RecordRouteKeyLess{});
}

void L1HybridIndex::ExportPersistedState(std::vector<RoutePartition>& out,
                                         size_t& logical_size,
                                         uint64_t& generation) const {
    out = partitions_;
    logical_size = size_;
    generation = generation_;
}

bool L1HybridIndex::ImportPersistedState(const std::vector<RoutePartition>& partitions,
                                         size_t logical_size,
                                         uint64_t generation) {
    InvalidateSubtreeCache();
    partitions_ = partitions;
    RoutePrefix prev_prefix = 0;
    bool first = true;
    for (const auto& partition : partitions_) {
        if (!first && partition.prefix <= prev_prefix) {
            return false;
        }
        if (partition.descriptor_mode != RouteDescriptorMode::kTinyDirect &&
            partition.descriptor_mode != RouteDescriptorMode::kNormalSubtree &&
            partition.descriptor_mode != RouteDescriptorMode::kNormalPack) {
            return false;
        }
        const bool has_subtree =
            partition.subtree_store.root_page_ptr != kInvalidSubtreePagePtr &&
            partition.subtree_store.page_count != 0;
        if (partition.descriptor_mode == RouteDescriptorMode::kTinyDirect &&
            partition.tiny_leaf_value != 0 &&
            !RouteDescriptor::CanEncodeTinyLeafValue(partition.tiny_leaf_value)) {
            return false;
        }
        if (partition.descriptor_mode == RouteDescriptorMode::kNormalPack &&
            partition.pack_page_ptr == kInvalidSubtreePagePtr) {
            return false;
        }
        if (!has_subtree && partition.record_count != 0) {
            return false;
        }
        if (has_subtree && partition.subtree_store.page_size != options_.subtree_page_size) {
            return false;
        }
        prev_prefix = partition.prefix;
        first = false;
    }

    layout_.RefreshPartitions(partitions_);
    partitions_.shrink_to_fit();
    BuildPublishedSnapshot();
    size_ = logical_size;
    generation_ = generation;
    return true;
}

bool L1HybridIndex::Validate() const {
    std::unordered_set<uint64_t> seen_table_idx;
    RoutePrefix last_prefix = 0;
    bool first = true;
    const auto snapshot = published_snapshot_;
    for (size_t i = 0; i < partitions_.size(); ++i) {
        const auto& partition = partitions_[i];
        if (!first && last_prefix >= partition.prefix) {
            return false;
        }
        const bool has_disk_subtree =
            snapshot != nullptr && i < snapshot->routes.size() &&
            snapshot->routes[i].root_page_ptr != kInvalidSubtreePagePtr;
        if (!has_disk_subtree) {
            if (partition.record_count != 0) {
                return false;
            }
        } else {
            const auto subtree = GetPartitionSubtree(i);
            if (subtree == nullptr) {
                return false;
            }
            if (!subtree->Validate()) {
                return false;
            }
            std::vector<SubtreeRecord> exported;
            subtree->ExportAll(exported);
            if (exported.size() != partition.record_count) {
                return false;
            }
            for (const auto& record : exported) {
                if (record.route_prefix != partition.prefix) {
                    return false;
                }
                if (!record.MatchesLocalFragment(partition.prefix)) {
                    return false;
                }
                seen_table_idx.insert(record.table_idx);
            }
        }
        last_prefix = partition.prefix;
        first = false;
    }
    return seen_table_idx.size() == size_;
}

L1HybridIndex::MemoryUsageStats L1HybridIndex::EstimateMemoryUsage() const {
    MemoryUsageStats stats;
    stats.route_partition_bytes = partitions_.capacity() * sizeof(RoutePartition);
    // Route lookup uses Masstree (prefix -> root_page_ptr).
    // The benchmark memory model follows the paper-side approximation:
    // ~32B per prefix entry (8B key + 8B value with node occupancy/metadata amortized).
    constexpr size_t kRouteMasstreeBytesPerEntry = 32;
    stats.route_index_estimated_bytes = partitions_.size() * kRouteMasstreeBytesPerEntry;
    stats.route_hot_root_index_measured_bytes = 0;  // no separate root index
    stats.route_hot_descriptor_index_measured_bytes =
        layout_.EstimateRouteDescriptorIndexMemoryUsageBytes();
    stats.route_index_measured_bytes = layout_.EstimateRouteIndexMemoryUsageBytes();
    stats.route_cold_stub_count = layout_.ColdStubCount();
    stats.route_cold_ssd_bytes = layout_.EstimateColdSsdBytes();
    // governance fields are embedded in RoutePartition and already counted in route_partition_bytes.
    stats.governance_bytes = 0;
    const auto snapshot = published_snapshot_;
    if (snapshot != nullptr) {
        stats.subtree_bytes += snapshot->routes.capacity() * sizeof(PublishedRoutePartition);
    }
    {
        std::lock_guard<std::mutex> lk(subtree_cache_mutex_);
        stats.subtree_cache_bytes = subtree_cache_bytes_;
    }
    stats.subtree_cache_requests = subtree_cache_requests_.load(std::memory_order_relaxed);
    stats.subtree_cache_hits = subtree_cache_hits_.load(std::memory_order_relaxed);
    stats.subtree_cache_misses = subtree_cache_misses_.load(std::memory_order_relaxed);
    const auto cow_stats = SubtreePageStore::GetCowStats();
    stats.cow_persist_calls = cow_stats.persist_calls;
    stats.cow_reused_pages = cow_stats.reused_pages;
    stats.cow_written_pages = cow_stats.written_pages;
    stats.cow_reused_bytes = cow_stats.reused_bytes;
    stats.cow_written_bytes = cow_stats.written_bytes;
    return stats;
}

std::shared_ptr<const L1SubtreeBPTree> L1HybridIndex::DebugCaptureSubtreeForKey(const KeyType& key) const {
    const RoutePrefix prefix = ExtractPrefix(key);
    const auto it = std::find_if(partitions_.begin(), partitions_.end(),
                                 [prefix](const RoutePartition& partition) {
                                     return partition.prefix == prefix;
                                 });
    if (it == partitions_.end()) {
        return {};
    }
    const size_t partition_idx = static_cast<size_t>(it - partitions_.begin());
    return GetPartitionSubtree(partition_idx);
}

bool L1HybridIndex::DebugGetPartitionForPrefix(RoutePrefix prefix, RoutePartition& out) const {
    const auto it = std::find_if(partitions_.begin(), partitions_.end(),
                                 [prefix](const RoutePartition& partition) {
                                     return partition.prefix == prefix;
                                 });
    if (it == partitions_.end()) {
        return false;
    }
    out = *it;
    const auto snapshot = published_snapshot_;
    if (snapshot == nullptr) {
        return true;
    }
    const size_t idx = static_cast<size_t>(it - partitions_.begin());
    if (idx >= snapshot->routes.size()) {
        return true;
    }
    const auto& route = snapshot->routes[idx];
    out.subtree_store.page_size = route.page_size;
    out.subtree_store.flags = route.subtree_store_flags;
    out.subtree_store.page_count = route.page_count;
    out.subtree_store.record_count = route.record_count;
    out.subtree_store.root_page_ptr = route.root_page_ptr;
    out.subtree_store.manifest_page_ptr = route.manifest_page_ptr;
    out.descriptor_mode = static_cast<RouteDescriptorMode>(route.descriptor_mode);
    out.tiny_enter_streak = route.tiny_enter_streak;
    out.pack_slot_id = route.pack_slot_id;
    out.tiny_leaf_value = route.tiny_leaf_value;
    out.pack_page_ptr = route.pack_page_ptr;
    return true;
}

L1HybridIndex::PartitionScanResult L1HybridIndex::ScanSinglePartition(RoutePrefix partition_prefix,
                                                                      SubtreePagePtr root_page_ptr,
                                                                      const KeyType& start,
                                                                      const KeyType& end) const {
    PartitionScanResult result;
    result.prefix = partition_prefix;

    const RoutePrefix start_prefix = ExtractPrefix(start);
    const RoutePrefix end_prefix = ExtractPrefix(end);
    const RouteSuffix local_start_suffix =
        partition_prefix == start_prefix ? ExtractSuffix(start) : 0;
    const RouteSuffix local_end_suffix =
        partition_prefix == end_prefix ? ExtractSuffix(end) : std::numeric_limits<RouteSuffix>::max();

    ScanPartitionRangeFromDisk(partition_prefix,
                               root_page_ptr,
                               local_start_suffix,
                               local_end_suffix,
                               result.records);
    return result;
}

bool L1HybridIndex::ScanPartitionRangeFromDisk(RoutePrefix prefix,
                                               SubtreePagePtr root_page_ptr,
                                               RouteSuffix local_start_suffix,
                                               RouteSuffix local_end_suffix,
                                               std::vector<SubtreeRecord>& out) const {
    if (root_page_ptr == kInvalidSubtreePagePtr) {
        return false;
    }

    try {
        const RouteSuffix start_suffix = local_start_suffix;
        SubtreePagePtr current_page_ptr = root_page_ptr;
        size_t level_hops = 0;

        while (level_hops++ <= kMaxSubtreeTraversalHops) {
            const auto page_bytes = SubtreePageStore::LoadPageByPtr(
                *options_.segment_allocator, options_.subtree_page_size, current_page_ptr);

            SubtreeDecodedInternalPage internal_page;
            if (SubtreePageCodec::TryDecodeInternalPage(page_bytes, internal_page)) {
                if (internal_page.child_page_ptrs.empty() ||
                    internal_page.child_page_ptrs.size() != internal_page.child_high_keys.size()) {
                    return false;
                }
                size_t child_idx = internal_page.child_page_ptrs.size();
                for (size_t i = 0; i < internal_page.child_high_keys.size(); ++i) {
                    if (start_suffix <= internal_page.child_high_keys[i]) {
                        child_idx = i;
                        break;
                    }
                }
                if (child_idx == internal_page.child_page_ptrs.size()) {
                    return true;
                }
                current_page_ptr = internal_page.child_page_ptrs[child_idx];
                continue;
            }

            SubtreeDecodedLeafPage leaf_page;
            if (!SubtreePageCodec::TryDecodeLeafPage(page_bytes, leaf_page)) {
                return false;
            }

            std::unordered_set<SubtreePagePtr> visited_leaf_pages;
            SubtreePagePtr leaf_page_ptr = current_page_ptr;
            bool first_leaf = true;
            while (true) {
                if (!visited_leaf_pages.insert(leaf_page_ptr).second) {
                    return false;
                }

                size_t start_idx = 0;
                if (first_leaf) {
                    const auto begin_it = std::lower_bound(leaf_page.records.begin(),
                                                           leaf_page.records.end(),
                                                           start_suffix,
                                                           RecordRouteSuffixLess{});
                    start_idx = static_cast<size_t>(begin_it - leaf_page.records.begin());
                }

                for (size_t i = start_idx; i < leaf_page.records.size(); ++i) {
                    const auto& record = leaf_page.records[i];
                    if (record.route_min_suffix > local_end_suffix) {
                        return true;
                    }
                    if (record.OverlapsLocalSuffixRange(local_start_suffix, local_end_suffix)) {
                        out.push_back(record);
                    }
                }

                if (leaf_page.next_page_ptr == kInvalidSubtreePagePtr) {
                    return true;
                }
                leaf_page_ptr = leaf_page.next_page_ptr;
                const auto next_page = SubtreePageStore::LoadPageByPtr(
                    *options_.segment_allocator, options_.subtree_page_size, leaf_page_ptr);
                if (!SubtreePageCodec::TryDecodeLeafPage(next_page, leaf_page)) {
                    return false;
                }
                first_leaf = false;
            }
        }
        return false;
    } catch (const std::exception&) {
        return false;
    }
}

void L1HybridIndex::SerialRangeScan(const std::vector<RoutedRoot>& route_roots,
                                    const KeyType& start,
                                    const KeyType& end,
                                    std::vector<SubtreeRecord>& out) const {
    std::unordered_set<uint64_t> seen_table_idx;
    for (const auto& route : route_roots) {
        auto result = ScanSinglePartition(route.prefix, route.root_page_ptr, start, end);
        for (const auto& record : result.records) {
            if (seen_table_idx.insert(record.table_idx).second) {
                out.push_back(record);
            }
        }
    }
    std::sort(out.begin(), out.end(), RecordRouteKeyLess{});
}

void L1HybridIndex::ParallelRangeScan(const std::vector<RoutedRoot>& route_roots,
                                      const KeyType& start,
                                      const KeyType& end,
                                      std::vector<SubtreeRecord>& out) const {
    const size_t task_count = std::min(route_roots.size(), options_.parallel_scan_max_tasks);
    const size_t chunk_size = (route_roots.size() + task_count - 1) / task_count;

    std::vector<std::future<std::vector<PartitionScanResult>>> futures;
    futures.reserve(task_count);

    for (size_t offset = 0; offset < route_roots.size(); offset += chunk_size) {
        const size_t chunk_end = std::min(offset + chunk_size, route_roots.size());
        futures.emplace_back(std::async(std::launch::async,
                                        [this, &route_roots, offset, chunk_end, start, end]() {
                                            std::vector<PartitionScanResult> chunk_results;
                                            chunk_results.reserve(chunk_end - offset);
                                            for (size_t i = offset; i < chunk_end; ++i) {
                                                chunk_results.push_back(ScanSinglePartition(
                                                    route_roots[i].prefix,
                                                    route_roots[i].root_page_ptr,
                                                    start,
                                                    end));
                                            }
                                            return chunk_results;
                                        }));
    }

    std::vector<PartitionScanResult> results;
    results.reserve(route_roots.size());
    for (auto& future : futures) {
        auto chunk_results = future.get();
        results.insert(results.end(),
                       std::make_move_iterator(chunk_results.begin()),
                       std::make_move_iterator(chunk_results.end()));
    }
    std::sort(results.begin(), results.end(),
              [](const PartitionScanResult& lhs, const PartitionScanResult& rhs) {
                  return lhs.prefix < rhs.prefix;
              });

    struct HeapItem {
        size_t result_idx = 0;
        size_t record_idx = 0;
        SubtreeRecord record{};
    };
    struct HeapLess {
        bool operator()(const HeapItem& lhs, const HeapItem& rhs) const {
            return RecordRouteKeyLess{}(rhs.record, lhs.record);
        }
    };

    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapLess> heap;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].records.empty()) {
            heap.push(HeapItem{i, 0, results[i].records[0]});
        }
    }

    std::unordered_set<uint64_t> seen_table_idx;
    out.clear();
    while (!heap.empty()) {
        HeapItem item = heap.top();
        heap.pop();
        if (seen_table_idx.insert(item.record.table_idx).second) {
            out.push_back(item.record);
        }

        const size_t next_idx = item.record_idx + 1;
        if (next_idx < results[item.result_idx].records.size()) {
            heap.push(HeapItem{item.result_idx, next_idx, results[item.result_idx].records[next_idx]});
        }
    }
}

void L1HybridIndex::InvalidateSubtreeCache() const {
    std::lock_guard<std::mutex> lk(subtree_cache_mutex_);
    subtree_cache_.clear();
    subtree_cache_lru_.clear();
    subtree_cache_bytes_ = 0;
    subtree_cache_requests_.store(0, std::memory_order_relaxed);
    subtree_cache_hits_.store(0, std::memory_order_relaxed);
    subtree_cache_misses_.store(0, std::memory_order_relaxed);
}

void L1HybridIndex::PruneSubtreeCacheLocked() const {
    while (!subtree_cache_lru_.empty()) {
        const bool over_capacity = options_.subtree_cache_capacity > 0 &&
                                   subtree_cache_.size() > options_.subtree_cache_capacity;
        const bool over_bytes = options_.subtree_cache_max_bytes > 0 &&
                                subtree_cache_bytes_ > options_.subtree_cache_max_bytes;
        if (!over_capacity && !over_bytes) {
            break;
        }
        const SubtreeCacheKey evict_key = subtree_cache_lru_.back();
        subtree_cache_lru_.pop_back();
        const auto it = subtree_cache_.find(evict_key);
        if (it == subtree_cache_.end()) {
            continue;
        }
        if (subtree_cache_bytes_ >= it->second.estimated_bytes) {
            subtree_cache_bytes_ -= it->second.estimated_bytes;
        } else {
            subtree_cache_bytes_ = 0;
        }
        subtree_cache_.erase(it);
    }
}

std::shared_ptr<const L1SubtreeBPTree> L1HybridIndex::GetPartitionSubtree(size_t partition_idx) const {
    if (partition_idx >= partitions_.size()) {
        return {};
    }
    const auto& partition = partitions_[partition_idx];
    const auto snapshot = published_snapshot_;
    const bool has_disk_subtree =
        snapshot != nullptr && partition_idx < snapshot->routes.size() &&
        snapshot->routes[partition_idx].root_page_ptr != kInvalidSubtreePagePtr;
    if (!has_disk_subtree) {
        return {};
    }

    const bool cache_enabled = options_.enable_subtree_cache && options_.subtree_cache_capacity > 0;
    const SubtreeCacheKey cache_key{partition.prefix, partition.generation};
    if (cache_enabled) {
        subtree_cache_requests_.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(subtree_cache_mutex_);
        const auto it = subtree_cache_.find(cache_key);
        if (it != subtree_cache_.end()) {
            subtree_cache_lru_.erase(it->second.lru_it);
            subtree_cache_lru_.push_front(cache_key);
            it->second.lru_it = subtree_cache_lru_.begin();
            subtree_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return it->second.tree;
        }
        subtree_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    }

    auto loaded = std::make_shared<L1SubtreeBPTree>(options_.subtree_options);
    if (!LoadPartitionSubtree(partition_idx, *loaded)) {
        return {};
    }

    if (!cache_enabled) {
        return loaded;
    }

    const size_t estimated_bytes = loaded->EstimateMemoryUsage().TotalBytes();
    std::lock_guard<std::mutex> lk(subtree_cache_mutex_);
    const auto existing = subtree_cache_.find(cache_key);
    if (existing != subtree_cache_.end()) {
        subtree_cache_lru_.erase(existing->second.lru_it);
        subtree_cache_lru_.push_front(cache_key);
        existing->second.lru_it = subtree_cache_lru_.begin();
        return existing->second.tree;
    }

    subtree_cache_lru_.push_front(cache_key);
    SubtreeCacheEntry entry;
    entry.tree = loaded;
    entry.estimated_bytes = estimated_bytes;
    entry.lru_it = subtree_cache_lru_.begin();
    subtree_cache_bytes_ += estimated_bytes;
    subtree_cache_.emplace(cache_key, std::move(entry));
    PruneSubtreeCacheLocked();
    return loaded;
}

bool L1HybridIndex::LookupCandidatesFromDisk(SubtreePagePtr root_page_ptr,
                                             const KeyType& key,
                                             size_t limit,
                                             std::vector<SubtreeRecord>& out) const {
    if (root_page_ptr == kInvalidSubtreePagePtr || limit == 0) {
        return false;
    }

    try {
        const RouteSuffix suffix = ExtractSuffix(key);
        SubtreePagePtr current_page_ptr = root_page_ptr;
        size_t level_hops = 0;

        while (level_hops++ <= kMaxSubtreeTraversalHops) {
            const auto page_bytes = SubtreePageStore::LoadPageByPtr(
                *options_.segment_allocator, options_.subtree_page_size, current_page_ptr);

            SubtreeDecodedInternalPage internal_page;
            if (SubtreePageCodec::TryDecodeInternalPage(page_bytes, internal_page)) {
                if (internal_page.child_page_ptrs.empty() ||
                    internal_page.child_page_ptrs.size() != internal_page.child_high_keys.size()) {
                    return false;
                }
                size_t child_idx = internal_page.child_page_ptrs.size();
                for (size_t i = 0; i < internal_page.child_high_keys.size(); ++i) {
                    if (suffix <= internal_page.child_high_keys[i]) {
                        child_idx = i;
                        break;
                    }
                }
                if (child_idx == internal_page.child_page_ptrs.size()) {
                    return true;
                }
                current_page_ptr = internal_page.child_page_ptrs[child_idx];
                continue;
            }

            SubtreeDecodedLeafPage leaf_page;
            if (!SubtreePageCodec::TryDecodeLeafPage(page_bytes, leaf_page)) {
                return false;
            }

            std::unordered_set<SubtreePagePtr> visited_leaf_pages;
            SubtreePagePtr leaf_page_ptr = current_page_ptr;
            bool first_leaf = true;
            while (true) {
                if (!visited_leaf_pages.insert(leaf_page_ptr).second) {
                    return false;
                }
                size_t start_idx = 0;
                if (first_leaf) {
                    const auto begin_it = std::lower_bound(leaf_page.records.begin(),
                                                           leaf_page.records.end(),
                                                           suffix,
                                                           RecordRouteSuffixLess{});
                    start_idx = static_cast<size_t>(begin_it - leaf_page.records.begin());
                }

                for (size_t i = start_idx; i < leaf_page.records.size(); ++i) {
                    const auto& record = leaf_page.records[i];
                    if (record.ContainsSuffix(suffix)) {
                        out.push_back(record);
                        if (out.size() >= limit) {
                            return true;
                        }
                    } else if (record.route_min_suffix > suffix) {
                        return true;
                    }
                }

                if (leaf_page.next_page_ptr == kInvalidSubtreePagePtr) {
                    return true;
                }
                leaf_page_ptr = leaf_page.next_page_ptr;
                const auto next_page = SubtreePageStore::LoadPageByPtr(
                    *options_.segment_allocator, options_.subtree_page_size, leaf_page_ptr);
                if (!SubtreePageCodec::TryDecodeLeafPage(next_page, leaf_page)) {
                    return false;
                }
                first_leaf = false;
            }
        }
        return false;
    } catch (const std::exception&) {
        return false;
    }
}

bool L1HybridIndex::LoadPartitionSubtree(size_t partition_idx, L1SubtreeBPTree& out) const {
    if (partition_idx >= partitions_.size()) {
        return false;
    }
    const auto snapshot = published_snapshot_;
    if (snapshot != nullptr && partition_idx < snapshot->routes.size()) {
        const auto& route = snapshot->routes[partition_idx];
        if (route.root_page_ptr != kInvalidSubtreePagePtr && route.page_count != 0) {
            SubtreePageStoreHandle handle;
            handle.page_size = route.page_size;
            handle.flags = route.subtree_store_flags;
            handle.page_count = route.page_count;
            handle.record_count = route.record_count;
            handle.root_page_ptr = route.root_page_ptr;
            handle.manifest_page_ptr = route.manifest_page_ptr;
            const auto page_set = SubtreePageStore::Load(*options_.segment_allocator, handle);
            out.ImportPageSet(page_set);
            return true;
        }
    }
    return false;
}

void L1HybridIndex::BuildPublishedSnapshot() {
    auto snapshot = std::make_shared<PublishedSnapshot>();
    snapshot->routes.reserve(partitions_.size());

    for (const auto& partition : partitions_) {
        PublishedRoutePartition route;
        route.prefix = partition.prefix;
        route.generation = partition.generation;
        route.record_count = partition.record_count;
        route.governance_flags = EncodeGovernanceFlags(partition.governance);
        route.descriptor_mode = static_cast<uint8_t>(partition.descriptor_mode);
        route.tiny_enter_streak = partition.tiny_enter_streak;
        route.pack_slot_id = partition.pack_slot_id;
        route.subtree_store_flags = partition.subtree_store.flags;
        route.page_size = partition.subtree_store.page_size;
        route.page_count = partition.subtree_store.page_count;
        route.root_page_ptr = partition.subtree_store.root_page_ptr;
        route.manifest_page_ptr = partition.subtree_store.manifest_page_ptr;
        route.tiny_leaf_value = partition.tiny_leaf_value;
        route.pack_page_ptr = partition.pack_page_ptr;
        snapshot->routes.push_back(route);
    }

    snapshot->routes.shrink_to_fit();
    published_snapshot_ = std::move(snapshot);
}

const PublishedRoutePartition* L1HybridIndex::GetPublishedPartition(size_t partition_idx) const {
    const auto snapshot = published_snapshot_;
    if (snapshot == nullptr || partition_idx >= snapshot->routes.size()) {
        return nullptr;
    }
    return &snapshot->routes[partition_idx];
}

}  // namespace flowkv::hybrid_l1
