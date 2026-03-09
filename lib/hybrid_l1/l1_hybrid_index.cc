#include "lib/hybrid_l1/l1_hybrid_index.h"

#include <algorithm>
#include <future>
#include <functional>
#include <limits>
#include <queue>
#include <utility>
#include <unordered_set>

namespace flowkv::hybrid_l1 {

namespace {

bool PartitionHasSubtree(const RoutePartition& partition, bool allow_in_memory_fallback) {
    return partition.subtree_store.Valid() ||
           (allow_in_memory_fallback && partition.subtree_pages_cache != nullptr);
}

struct SubtreeDiskQueryMeta {
    uint32_t root_page_id = kInvalidSubtreePageId;
    uint32_t page_count = 0;
    uint64_t record_count = 0;
};

bool ResolveSubtreeDiskQueryMeta(const RoutePartition& partition,
                                 SegmentAllocator& allocator,
                                 SubtreeDiskQueryMeta& out) {
    if (partition.subtree_store.HasQueryMeta()) {
        out.root_page_id = partition.subtree_store.root_page_id;
        out.page_count = partition.subtree_store.page_count;
        out.record_count = partition.subtree_store.record_count;
        return true;
    }

    // Backward compatibility path: old snapshot payloads may not have root metadata in handle.
    const auto manifest_page = SubtreePageStore::LoadManifestPage(allocator, partition.subtree_store);
    std::vector<uint8_t> manifest_bytes;
    manifest_bytes.reserve(SubtreePageCodec::kManifestSize);
    manifest_bytes.insert(manifest_bytes.end(),
                          manifest_page.begin(),
                          manifest_page.begin() + SubtreePageCodec::kManifestSize);
    const SubtreePageManifest manifest = SubtreePageCodec::DecodeManifest(manifest_bytes);
    out.root_page_id = manifest.root_page_id;
    out.page_count = manifest.page_count;
    out.record_count = manifest.record_count;
    return true;
}

}  // namespace

size_t L1HybridIndex::SubtreeCacheKeyHash::operator()(const SubtreeCacheKey& key) const {
    const size_t h1 = std::hash<RoutePrefix>{}(key.prefix);
    const size_t h2 = std::hash<uint64_t>{}(key.generation);
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
}

L1HybridIndex::L1HybridIndex() : L1HybridIndex(BuildOptions{}) {}

L1HybridIndex::L1HybridIndex(BuildOptions options)
    : options_(options), layout_(options.route_partition_num) {
    layout_.InitializePartitions(partitions_);
}

L1HybridIndex::~L1HybridIndex() = default;

void L1HybridIndex::Clear() {
    InvalidateSubtreeCache();
    L1HybridRebuilder::ResetPartitions(partitions_, options_.segment_allocator);
    partitions_.shrink_to_fit();
    size_ = 0;
}

void L1HybridIndex::BulkLoad(const std::vector<SubtreeRecord>& sorted_records) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::BulkLoad(sorted_records, partitions_, layout_, options_.subtree_options,
                                options_.subtree_page_size, options_.segment_allocator,
                                options_.governance_policy, size_, generation_);
    partitions_.shrink_to_fit();
}

void L1HybridIndex::BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::BulkLoadFromTables(tables, partitions_, layout_, options_.subtree_options,
                                          options_.subtree_page_size, options_.segment_allocator,
                                          options_.governance_policy, size_, generation_);
    partitions_.shrink_to_fit();
}

void L1HybridIndex::RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                                const std::vector<KeyType>& changed_route_keys) {
    InvalidateSubtreeCache();
    L1HybridRebuilder::RebuildPartitionsFromTables(tables, changed_route_keys, partitions_, layout_,
                                                   options_.subtree_options, options_.subtree_page_size,
                                                   options_.segment_allocator, options_.update_policy,
                                                   options_.governance_policy, size_, generation_);
    partitions_.shrink_to_fit();
}

bool L1HybridIndex::LookupCandidate(const KeyType& key, SubtreeRecord& out) const {
    if (size_ == 0) {
        return false;
    }

    size_t partition_idx = 0;
    if (!layout_.FindPartitionByKey(partitions_, key, partition_idx)) {
        return false;
    }

    std::vector<SubtreeRecord> matches;
    if (!LookupCandidatesFromDisk(partition_idx, key, 1, matches)) {
        const auto subtree = GetPartitionSubtree(partition_idx);
        if (subtree == nullptr) {
            return false;
        }
        const RouteSuffix suffix = ExtractSuffix(key);
        auto cursor = subtree->LowerBound(key);
        while (cursor.Valid()) {
            const auto& record = cursor.record();
            if (record.Contains(key)) {
                out = record;
                return true;
            }
            if (record.route_min_suffix > suffix) {
                return false;
            }
            cursor.Next();
        }
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

    size_t partition_idx = 0;
    if (!layout_.FindPartitionByKey(partitions_, key, partition_idx)) {
        return;
    }

    if (LookupCandidatesFromDisk(partition_idx, key, limit, out)) {
        return;
    }

    const auto subtree = GetPartitionSubtree(partition_idx);
    if (subtree == nullptr) {
        return;
    }

    const RouteSuffix suffix = ExtractSuffix(key);
    auto cursor = subtree->LowerBound(key);
    while (cursor.Valid() && out.size() < limit) {
        const auto& record = cursor.record();
        if (record.Contains(key)) {
            out.push_back(record);
        } else if (record.route_min_suffix > suffix) {
            return;
        }
        cursor.Next();
    }
}

void L1HybridIndex::RangeScan(const KeyType& start,
                              const KeyType& end,
                              std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (CompareKeyType(start, end) > 0 || size_ == 0) {
        return;
    }

    std::vector<size_t> partition_indices;
    layout_.CollectPartitionsForRange(partitions_, start, end, partition_indices);
    if (partition_indices.empty()) {
        return;
    }

    bool prefer_parallel_scan = false;
    for (const size_t partition_idx : partition_indices) {
        if (partitions_[partition_idx].governance.prefer_parallel_scan) {
            prefer_parallel_scan = true;
            break;
        }
    }

    if (options_.enable_parallel_range_scan &&
        (partition_indices.size() >= options_.parallel_scan_min_partitions ||
         (prefer_parallel_scan && partition_indices.size() >= 2)) &&
        options_.parallel_scan_max_tasks > 1) {
        ParallelRangeScan(partition_indices, start, end, out);
    } else {
        SerialRangeScan(partition_indices, start, end, out);
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
    for (auto& partition : partitions_) {
        partition.subtree_pages_cache.reset();
    }

    const bool allow_in_memory_fallback = (options_.segment_allocator == nullptr);
    RoutePrefix prev_prefix = 0;
    bool first = true;
    for (const auto& partition : partitions_) {
        if (!first && partition.prefix <= prev_prefix) {
            return false;
        }
        if (!PartitionHasSubtree(partition, allow_in_memory_fallback) && partition.record_count != 0) {
            return false;
        }
        prev_prefix = partition.prefix;
        first = false;
    }

    layout_.RefreshPartitions(partitions_);
    partitions_.shrink_to_fit();
    size_ = logical_size;
    generation_ = generation;
    return true;
}

bool L1HybridIndex::Validate() const {
    const bool allow_in_memory_fallback = (options_.segment_allocator == nullptr);
    std::unordered_set<uint64_t> seen_table_idx;
    RoutePrefix last_prefix = 0;
    bool first = true;
    for (size_t i = 0; i < partitions_.size(); ++i) {
        const auto& partition = partitions_[i];
        if (!first && last_prefix >= partition.prefix) {
            return false;
        }
        if (!PartitionHasSubtree(partition, allow_in_memory_fallback)) {
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
    const bool allow_in_memory_fallback = (options_.segment_allocator == nullptr);
    MemoryUsageStats stats;
    stats.route_partition_bytes = partitions_.capacity() * sizeof(RoutePartition);
    // Route lookup now uses binary-search on sorted partitions; no extra in-memory route tree.
    stats.route_index_estimated_bytes = 0;
    stats.governance_bytes = partitions_.size() * sizeof(BucketGovernanceState);
    for (const auto& partition : partitions_) {
        if (!PartitionHasSubtree(partition, allow_in_memory_fallback)) {
            continue;
        }
        // Disk-resident mode: keep in-memory subtree footprint near zero.
        // Include metadata-handle size and allocated page-id list footprint.
        if (partition.subtree_store.Valid()) {
            stats.subtree_bytes += sizeof(SubtreePageStoreHandle);
            stats.subtree_bytes +=
                partition.subtree_store.pages.capacity() * sizeof(SubtreeStoredPageRef);
        }
        if (allow_in_memory_fallback && partition.subtree_pages_cache != nullptr) {
            stats.subtree_bytes += partition.subtree_pages_cache->manifest.capacity();
            for (const auto& page : partition.subtree_pages_cache->pages) {
                stats.subtree_bytes += page.bytes.capacity();
            }
        }
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
    size_t partition_idx = 0;
    if (!layout_.FindPartitionByKey(partitions_, key, partition_idx)) {
        return {};
    }
    return GetPartitionSubtree(partition_idx);
}

bool L1HybridIndex::DebugGetPartitionForPrefix(RoutePrefix prefix, RoutePartition& out) const {
    auto it = std::find_if(partitions_.begin(), partitions_.end(),
                           [prefix](const RoutePartition& partition) {
                               return partition.prefix == prefix;
                           });
    if (it == partitions_.end()) {
        return false;
    }
    out = *it;
    return true;
}

L1HybridIndex::PartitionScanResult L1HybridIndex::ScanSinglePartition(size_t partition_idx,
                                                                      const KeyType& start,
                                                                      const KeyType& end) const {
    PartitionScanResult result;
    result.partition_idx = partition_idx;

    const RoutePrefix start_prefix = ExtractPrefix(start);
    const RoutePrefix end_prefix = ExtractPrefix(end);
    const RoutePrefix partition_prefix = partitions_[partition_idx].prefix;
    const RouteSuffix local_start_suffix =
        partition_prefix == start_prefix ? ExtractSuffix(start) : 0;
    const RouteSuffix local_end_suffix =
        partition_prefix == end_prefix ? ExtractSuffix(end) : std::numeric_limits<RouteSuffix>::max();

    if (ScanPartitionRangeFromDisk(partition_idx, local_start_suffix, local_end_suffix, result.records)) {
        return result;
    }

    const auto subtree = GetPartitionSubtree(partition_idx);
    if (subtree == nullptr) {
        return result;
    }

    auto cursor = subtree->LowerBound(ComposeKey(partition_prefix, local_start_suffix));
    while (cursor.Valid()) {
        const auto& record = cursor.record();
        if (record.route_min_suffix > local_end_suffix) {
            break;
        }
        if (record.OverlapsLocalSuffixRange(local_start_suffix, local_end_suffix)) {
            result.records.push_back(record);
        }
        cursor.Next();
    }
    return result;
}

bool L1HybridIndex::ScanPartitionRangeFromDisk(size_t partition_idx,
                                               RouteSuffix local_start_suffix,
                                               RouteSuffix local_end_suffix,
                                               std::vector<SubtreeRecord>& out) const {
    if (partition_idx >= partitions_.size() || options_.segment_allocator == nullptr) {
        return false;
    }
    const auto& partition = partitions_[partition_idx];
    if (!partition.subtree_store.Valid()) {
        return false;
    }

    try {
        SubtreeDiskQueryMeta query_meta;
        if (!ResolveSubtreeDiskQueryMeta(partition, *options_.segment_allocator, query_meta)) {
            return false;
        }
        if (query_meta.record_count == 0 || query_meta.root_page_id == kInvalidSubtreePageId) {
            return true;
        }

        const KeyType start_key = ComposeKey(partition.prefix, local_start_suffix);
        uint32_t current_page_id = query_meta.root_page_id;
        size_t level_hops = 0;
        const size_t max_hops = std::max<size_t>(1, query_meta.page_count);

        while (level_hops++ <= max_hops) {
            const auto page_bytes = SubtreePageStore::LoadPageById(
                *options_.segment_allocator, partition.subtree_store, current_page_id);

            SubtreeDecodedInternalPage internal_page;
            if (SubtreePageCodec::TryDecodeInternalPage(page_bytes, internal_page)) {
                if (internal_page.child_page_ids.empty() ||
                    internal_page.child_page_ids.size() != internal_page.child_high_keys.size()) {
                    return false;
                }
                size_t child_idx = internal_page.child_page_ids.size();
                for (size_t i = 0; i < internal_page.child_high_keys.size(); ++i) {
                    if (CompareKeyType(start_key, internal_page.child_high_keys[i]) <= 0) {
                        child_idx = i;
                        break;
                    }
                }
                if (child_idx == internal_page.child_page_ids.size()) {
                    return true;
                }
                current_page_id = internal_page.child_page_ids[child_idx];
                continue;
            }

            SubtreeDecodedLeafPage leaf_page;
            if (!SubtreePageCodec::TryDecodeLeafPage(page_bytes, leaf_page)) {
                return false;
            }

            std::unordered_set<uint32_t> visited_leaf_pages;
            uint32_t leaf_page_id = current_page_id;
            bool first_leaf = true;
            while (true) {
                if (!visited_leaf_pages.insert(leaf_page_id).second) {
                    return false;
                }

                size_t start_idx = 0;
                if (first_leaf) {
                    const auto begin_it = std::lower_bound(leaf_page.records.begin(),
                                                           leaf_page.records.end(),
                                                           start_key,
                                                           RecordRouteKeyLess{});
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

                if (leaf_page.next_page_id == kInvalidSubtreePageId) {
                    return true;
                }
                leaf_page_id = leaf_page.next_page_id;
                const auto next_page = SubtreePageStore::LoadPageById(
                    *options_.segment_allocator, partition.subtree_store, leaf_page_id);
                if (!SubtreePageCodec::TryDecodeLeafPage(next_page, leaf_page)) {
                    return false;
                }
                first_leaf = false;
            }
        }
    } catch (const std::exception&) {
        return false;
    }

    return true;
}

void L1HybridIndex::SerialRangeScan(const std::vector<size_t>& partition_indices,
                                    const KeyType& start,
                                    const KeyType& end,
                                    std::vector<SubtreeRecord>& out) const {
    std::unordered_set<uint64_t> seen_table_idx;
    for (const size_t partition_idx : partition_indices) {
        auto result = ScanSinglePartition(partition_idx, start, end);
        for (const auto& record : result.records) {
            if (seen_table_idx.insert(record.table_idx).second) {
                out.push_back(record);
            }
        }
    }
    std::sort(out.begin(), out.end(), RecordRouteKeyLess{});
}

void L1HybridIndex::ParallelRangeScan(const std::vector<size_t>& partition_indices,
                                      const KeyType& start,
                                      const KeyType& end,
                                      std::vector<SubtreeRecord>& out) const {
    const size_t task_count = std::min(partition_indices.size(), options_.parallel_scan_max_tasks);
    const size_t chunk_size = (partition_indices.size() + task_count - 1) / task_count;

    std::vector<std::future<std::vector<PartitionScanResult>>> futures;
    futures.reserve(task_count);

    for (size_t offset = 0; offset < partition_indices.size(); offset += chunk_size) {
        const size_t chunk_end = std::min(offset + chunk_size, partition_indices.size());
        futures.emplace_back(std::async(std::launch::async,
                                        [this, &partition_indices, offset, chunk_end, start, end]() {
                                            std::vector<PartitionScanResult> chunk_results;
                                            chunk_results.reserve(chunk_end - offset);
                                            for (size_t i = offset; i < chunk_end; ++i) {
                                                chunk_results.push_back(
                                                    ScanSinglePartition(partition_indices[i], start, end));
                                            }
                                            return chunk_results;
                                        }));
    }

    std::vector<PartitionScanResult> results;
    results.reserve(partition_indices.size());
    for (auto& future : futures) {
        auto chunk_results = future.get();
        results.insert(results.end(),
                       std::make_move_iterator(chunk_results.begin()),
                       std::make_move_iterator(chunk_results.end()));
    }
    std::sort(results.begin(), results.end(),
              [](const PartitionScanResult& lhs, const PartitionScanResult& rhs) {
                  return lhs.partition_idx < rhs.partition_idx;
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
    const bool allow_in_memory_fallback = (options_.segment_allocator == nullptr);
    if (!PartitionHasSubtree(partition, allow_in_memory_fallback)) {
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

bool L1HybridIndex::LookupCandidatesFromDisk(size_t partition_idx,
                                             const KeyType& key,
                                             size_t limit,
                                             std::vector<SubtreeRecord>& out) const {
    if (partition_idx >= partitions_.size() || limit == 0 ||
        options_.segment_allocator == nullptr) {
        return false;
    }
    const auto& partition = partitions_[partition_idx];
    if (!partition.subtree_store.Valid()) {
        return false;
    }

    try {
        SubtreeDiskQueryMeta query_meta;
        if (!ResolveSubtreeDiskQueryMeta(partition, *options_.segment_allocator, query_meta)) {
            return false;
        }
        if (query_meta.record_count == 0 || query_meta.root_page_id == kInvalidSubtreePageId) {
            return true;
        }

        const RouteSuffix suffix = ExtractSuffix(key);
        uint32_t current_page_id = query_meta.root_page_id;
        size_t level_hops = 0;
        const size_t max_hops = std::max<size_t>(1, query_meta.page_count);

        while (level_hops++ <= max_hops) {
            const auto page_bytes = SubtreePageStore::LoadPageById(
                *options_.segment_allocator, partition.subtree_store, current_page_id);

            SubtreeDecodedInternalPage internal_page;
            if (SubtreePageCodec::TryDecodeInternalPage(page_bytes, internal_page)) {
                if (internal_page.child_page_ids.empty() ||
                    internal_page.child_page_ids.size() != internal_page.child_high_keys.size()) {
                    return false;
                }
                size_t child_idx = internal_page.child_page_ids.size();
                for (size_t i = 0; i < internal_page.child_high_keys.size(); ++i) {
                    if (CompareKeyType(key, internal_page.child_high_keys[i]) <= 0) {
                        child_idx = i;
                        break;
                    }
                }
                if (child_idx == internal_page.child_page_ids.size()) {
                    return true;
                }
                current_page_id = internal_page.child_page_ids[child_idx];
                continue;
            }

            SubtreeDecodedLeafPage leaf_page;
            if (!SubtreePageCodec::TryDecodeLeafPage(page_bytes, leaf_page)) {
                return false;
            }

            std::unordered_set<uint32_t> visited_leaf_pages;
            uint32_t leaf_page_id = current_page_id;
            bool first_leaf = true;
            while (true) {
                if (!visited_leaf_pages.insert(leaf_page_id).second) {
                    return false;
                }
                size_t start_idx = 0;
                if (first_leaf) {
                    const auto begin_it = std::lower_bound(leaf_page.records.begin(),
                                                           leaf_page.records.end(),
                                                           key,
                                                           RecordRouteKeyLess{});
                    start_idx = static_cast<size_t>(begin_it - leaf_page.records.begin());
                }

                for (size_t i = start_idx; i < leaf_page.records.size(); ++i) {
                    const auto& record = leaf_page.records[i];
                    if (record.Contains(key)) {
                        out.push_back(record);
                        if (out.size() >= limit) {
                            return true;
                        }
                    } else if (record.route_min_suffix > suffix) {
                        return true;
                    }
                }

                if (leaf_page.next_page_id == kInvalidSubtreePageId) {
                    return true;
                }
                leaf_page_id = leaf_page.next_page_id;
                const auto next_page = SubtreePageStore::LoadPageById(
                    *options_.segment_allocator, partition.subtree_store, leaf_page_id);
                if (!SubtreePageCodec::TryDecodeLeafPage(next_page, leaf_page)) {
                    return false;
                }
                first_leaf = false;
            }
        }
    } catch (const std::exception&) {
        return false;
    }

    return true;
}

bool L1HybridIndex::LoadPartitionSubtree(size_t partition_idx, L1SubtreeBPTree& out) const {
    if (partition_idx >= partitions_.size()) {
        return false;
    }
    const auto& partition = partitions_[partition_idx];
    if (partition.subtree_store.Valid() && options_.segment_allocator != nullptr) {
        const auto page_set = SubtreePageStore::Load(*options_.segment_allocator, partition.subtree_store);
        out.ImportPageSet(page_set);
        return true;
    }
    if (options_.segment_allocator == nullptr && partition.subtree_pages_cache != nullptr) {
        out.ImportPageSet(*partition.subtree_pages_cache);
        return true;
    }
    return false;
}

}  // namespace flowkv::hybrid_l1
