#pragma once

#include "l1_hybrid_rebuilder.h"

#include "db/allocator/segment_allocator.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace flowkv::hybrid_l1 {

class L1HybridIndex {
public:
    struct MemoryUsageStats {
        size_t route_partition_bytes = 0;
        size_t route_index_estimated_bytes = 0;
        size_t subtree_bytes = 0;
        size_t subtree_cache_bytes = 0;
        size_t governance_bytes = 0;
        uint64_t subtree_cache_requests = 0;
        uint64_t subtree_cache_hits = 0;
        uint64_t subtree_cache_misses = 0;
        uint64_t cow_persist_calls = 0;
        uint64_t cow_reused_pages = 0;
        uint64_t cow_written_pages = 0;
        uint64_t cow_reused_bytes = 0;
        uint64_t cow_written_bytes = 0;

        size_t TotalBytes() const {
            return route_partition_bytes + route_index_estimated_bytes +
                   subtree_bytes + subtree_cache_bytes + governance_bytes;
        }
    };

    struct BuildOptions {
        size_t route_partition_num = RANGE_PARTITION_NUM;
        L1SubtreeBPTree::BuildOptions subtree_options{};
        PartitionUpdatePolicy update_policy{};
        PrefixGovernancePolicy governance_policy{};
        bool enable_parallel_range_scan = true;
        size_t parallel_scan_min_partitions = 3;
        size_t parallel_scan_max_tasks = 4;
        bool enable_subtree_cache = true;
        size_t subtree_cache_capacity = 256;
        size_t subtree_cache_max_bytes = 256ULL << 20;
        uint32_t subtree_page_size = 16 * 1024;
        SegmentAllocator* segment_allocator = nullptr;
    };

    L1HybridIndex();
    explicit L1HybridIndex(BuildOptions options);
    ~L1HybridIndex();

    void Clear();

    void BulkLoad(const std::vector<SubtreeRecord>& sorted_records);
    void BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables);
    void RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                     const std::vector<KeyType>& changed_route_keys);

    bool Empty() const { return size_ == 0; }
    size_t Size() const { return size_; }
    size_t PartitionCount() const { return partitions_.size(); }

    bool LookupCandidate(const KeyType& key, SubtreeRecord& out) const;
    void LookupCandidates(const KeyType& key, size_t limit, std::vector<SubtreeRecord>& out) const;
    void RangeScan(const KeyType& start, const KeyType& end, std::vector<SubtreeRecord>& out) const;
    void ExportAll(std::vector<SubtreeRecord>& out) const;
    void ExportLocalFragments(std::vector<SubtreeRecord>& out) const;
    void ExportPersistedState(std::vector<RoutePartition>& out,
                              size_t& logical_size,
                              uint64_t& generation) const;
    bool ImportPersistedState(const std::vector<RoutePartition>& partitions,
                              size_t logical_size,
                              uint64_t generation);

    bool Validate() const;
    MemoryUsageStats EstimateMemoryUsage() const;

    // Debug/test helper: capture the current subtree reference for a route key.
    std::shared_ptr<const L1SubtreeBPTree> DebugCaptureSubtreeForKey(const KeyType& key) const;
    bool DebugGetPartitionForPrefix(RoutePrefix prefix, RoutePartition& out) const;

private:
    struct SubtreeCacheKey {
        RoutePrefix prefix = 0;
        uint64_t generation = 0;

        bool operator==(const SubtreeCacheKey& rhs) const {
            return prefix == rhs.prefix && generation == rhs.generation;
        }
    };

    struct SubtreeCacheKeyHash {
        size_t operator()(const SubtreeCacheKey& key) const;
    };

    struct SubtreeCacheEntry {
        std::shared_ptr<const L1SubtreeBPTree> tree;
        size_t estimated_bytes = 0;
        std::list<SubtreeCacheKey>::iterator lru_it;
    };

    struct PartitionScanResult {
        size_t partition_idx = 0;
        std::vector<SubtreeRecord> records;
    };

    void SerialRangeScan(const std::vector<size_t>& partition_indices,
                         const KeyType& start,
                         const KeyType& end,
                         std::vector<SubtreeRecord>& out) const;
    void ParallelRangeScan(const std::vector<size_t>& partition_indices,
                           const KeyType& start,
                           const KeyType& end,
                           std::vector<SubtreeRecord>& out) const;
    PartitionScanResult ScanSinglePartition(size_t partition_idx,
                                            const KeyType& start,
                                            const KeyType& end) const;
    bool ScanPartitionRangeFromDisk(size_t partition_idx,
                                    RouteSuffix local_start_suffix,
                                    RouteSuffix local_end_suffix,
                                    std::vector<SubtreeRecord>& out) const;
    std::shared_ptr<const L1SubtreeBPTree> GetPartitionSubtree(size_t partition_idx) const;
    bool LookupCandidatesFromDisk(size_t partition_idx,
                                  const KeyType& key,
                                  size_t limit,
                                  std::vector<SubtreeRecord>& out) const;
    bool LoadPartitionSubtree(size_t partition_idx, L1SubtreeBPTree& out) const;
    void InvalidateSubtreeCache() const;
    void PruneSubtreeCacheLocked() const;

    BuildOptions options_;
    FixedRouteLayout layout_;
    std::vector<RoutePartition> partitions_;
    size_t size_ = 0;
    uint64_t generation_ = 0;

    mutable std::mutex subtree_cache_mutex_;
    mutable size_t subtree_cache_bytes_ = 0;
    mutable std::list<SubtreeCacheKey> subtree_cache_lru_;
    mutable std::unordered_map<SubtreeCacheKey, SubtreeCacheEntry, SubtreeCacheKeyHash> subtree_cache_;
    mutable std::atomic<uint64_t> subtree_cache_requests_{0};
    mutable std::atomic<uint64_t> subtree_cache_hits_{0};
    mutable std::atomic<uint64_t> subtree_cache_misses_{0};
};

}  // namespace flowkv::hybrid_l1
