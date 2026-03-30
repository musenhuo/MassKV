#pragma once

#include "l1_hybrid_rebuilder.h"
#include "l1_delta_batch.h"

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

struct alignas(32) PublishedRoutePartition {
    RoutePrefix prefix = 0;
    uint32_t generation = 0;
    uint32_t record_count = 0;
    uint8_t governance_flags = 0;
    uint8_t descriptor_mode = static_cast<uint8_t>(RouteDescriptorMode::kNormalSubtree);
    uint8_t tiny_enter_streak = 0;
    uint8_t pack_slot_id = 0;
    uint8_t subtree_store_flags = 0;
    uint8_t descriptor_reserved0 = 0;
    uint8_t descriptor_reserved1 = 0;
    uint16_t page_size = 0;
    uint32_t page_count = 0;
    SubtreePagePtr root_page_ptr = kInvalidSubtreePagePtr;
    SubtreePagePtr manifest_page_ptr = kInvalidSubtreePagePtr;
    uint64_t tiny_leaf_value = 0;
    SubtreePagePtr pack_page_ptr = kInvalidSubtreePagePtr;
};

struct PublishedSnapshot {
    std::vector<PublishedRoutePartition> routes;
};

struct RangeScanRecordOptions {
    bool include_window_fragments = true;
    bool include_unique_blocks = true;
    bool dedup_windows = true;
};

struct RangeScanRecordResult {
    std::vector<SubtreeRecord> window_fragments;
    std::vector<uint64_t> unique_kv_block_ptrs;
};

class L1HybridIndex {
public:
    struct MemoryUsageStats {
        size_t route_partition_bytes = 0;
        size_t route_index_estimated_bytes = 0;
        size_t route_index_measured_bytes = 0;
        size_t route_hot_root_index_measured_bytes = 0;
        size_t route_hot_descriptor_index_measured_bytes = 0;
        size_t route_cold_stub_count = 0;
        size_t route_cold_ssd_bytes = 0;
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

        size_t ReadPathBytes() const {
            // L1 index memory is defined as route-layer Masstree overhead only.
            return route_index_estimated_bytes;
        }

        size_t ReadPathMeasuredBytes() const {
            return route_index_measured_bytes;
        }

        size_t ControlPlaneBytes() const {
            return route_partition_bytes;
        }

        size_t TotalBytes() const {
            return ReadPathBytes() + ControlPlaneBytes();
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
        // Route-layer Masstree hot leaf memory budget.
        // 0 means keep all route entries hot in memory (no swap).
        size_t route_hot_leaf_budget_bytes = 0;
        uint32_t subtree_page_size = 16 * 1024;
        SegmentAllocator* segment_allocator = nullptr;
    };

    explicit L1HybridIndex(BuildOptions options);
    ~L1HybridIndex();

    void Clear();

    void BulkLoad(const std::vector<SubtreeRecord>& sorted_records);
    void BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables);
    void RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                     const std::vector<KeyType>& changed_route_keys,
                                     const L1DeltaBatch* delta_batch = nullptr);
    void RebuildPartitionsFromDelta(const std::vector<KeyType>& changed_route_keys,
                                    const L1DeltaBatch& delta_batch);

    bool Empty() const { return size_ == 0; }
    size_t Size() const { return size_; }
    size_t PartitionCount() const { return partitions_.size(); }

    bool LookupCandidate(const KeyType& key, SubtreeRecord& out) const;
    void LookupCandidates(const KeyType& key, size_t limit, std::vector<SubtreeRecord>& out) const;
    void RangeScan(const KeyType& start, const KeyType& end, std::vector<SubtreeRecord>& out) const;
    void RangeScanRecords(const KeyType& start,
                          const KeyType& end,
                          const RangeScanRecordOptions& options,
                          RangeScanRecordResult& out) const;
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
        RoutePrefix prefix = 0;
        std::vector<SubtreeRecord> records;
    };

    void SerialRangeScan(const std::vector<RoutedRoot>& route_roots,
                         const KeyType& start,
                         const KeyType& end,
                         std::vector<SubtreeRecord>& out) const;
    void ParallelRangeScan(const std::vector<RoutedRoot>& route_roots,
                           const KeyType& start,
                           const KeyType& end,
                           std::vector<SubtreeRecord>& out) const;
    PartitionScanResult ScanSinglePartition(RoutePrefix prefix,
                                            SubtreePagePtr root_page_ptr,
                                            const KeyType& start,
                                            const KeyType& end) const;
    bool ScanPartitionRangeFromDisk(RoutePrefix prefix,
                                    SubtreePagePtr root_page_ptr,
                                    RouteSuffix local_start_suffix,
                                    RouteSuffix local_end_suffix,
                                    std::vector<SubtreeRecord>& out) const;
    std::shared_ptr<const L1SubtreeBPTree> GetPartitionSubtree(size_t partition_idx) const;
    bool LookupCandidatesFromDisk(SubtreePagePtr root_page_ptr,
                                  const KeyType& key,
                                  size_t limit,
                                  std::vector<SubtreeRecord>& out) const;
    bool LoadPartitionSubtree(size_t partition_idx, L1SubtreeBPTree& out) const;
    void InvalidateSubtreeCache() const;
    void PruneSubtreeCacheLocked() const;
    void BuildPublishedSnapshot();
    const PublishedRoutePartition* GetPublishedPartition(size_t partition_idx) const;

    BuildOptions options_;
    FixedRouteLayout layout_;
    std::vector<RoutePartition> partitions_;
    std::shared_ptr<const PublishedSnapshot> published_snapshot_;
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
