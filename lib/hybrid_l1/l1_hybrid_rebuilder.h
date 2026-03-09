#pragma once

#include "route_layout.h"
#include "subtree_record.h"

#include "db/allocator/segment_allocator.h"
#include <cstddef>
#include <cstdint>
#include <vector>

namespace flowkv::hybrid_l1 {

enum class PartitionUpdateMode : uint8_t {
    kBulkLoadRebuild = 0,
    kCowPatch = 1,
};

struct PartitionUpdatePolicy {
    bool enable_cow = true;
    size_t small_tree_record_threshold = 256;
    size_t cow_max_changed_records = 64;
    size_t cow_max_leaf_spans = 4;
    uint32_t cow_max_change_percent = 20;
    size_t hot_prefix_relaxed_cow_max_changed_records = 128;
    size_t hot_prefix_relaxed_cow_max_leaf_spans = 8;
    uint32_t hot_prefix_relaxed_cow_max_change_percent = 35;
};

struct PrefixGovernancePolicy {
    bool enable_lightweight_governance = true;
    size_t hot_prefix_record_threshold = 512;
    size_t force_cow_record_threshold = 1024;
    size_t parallel_scan_record_threshold = 512;
};

struct PartitionUpdateDecision {
    PartitionUpdateMode mode = PartitionUpdateMode::kBulkLoadRebuild;
    size_t existing_record_count = 0;
    size_t target_record_count = 0;
    size_t changed_record_count = 0;
    size_t changed_leaf_span = 0;
};

class L1HybridRebuilder {
public:
    static void ResetPartitions(std::vector<RoutePartition>& partitions);
    static void ResetPartitions(std::vector<RoutePartition>& partitions,
                                SegmentAllocator* segment_allocator);

    static void BulkLoad(const std::vector<SubtreeRecord>& sorted_records,
                         std::vector<RoutePartition>& partitions,
                         const FixedRouteLayout& layout,
                         const L1SubtreeBPTree::BuildOptions& subtree_options,
                         uint32_t subtree_page_size,
                         SegmentAllocator* segment_allocator,
                         const PrefixGovernancePolicy& governance_policy,
                         size_t& size,
                         uint64_t& generation);

    static void BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables,
                                   std::vector<RoutePartition>& partitions,
                                   const FixedRouteLayout& layout,
                                   const L1SubtreeBPTree::BuildOptions& subtree_options,
                                   uint32_t subtree_page_size,
                                   SegmentAllocator* segment_allocator,
                                   const PrefixGovernancePolicy& governance_policy,
                                   size_t& size,
                                   uint64_t& generation);

    static void RebuildPartitionsFromTables(const std::vector<TaggedPstMeta>& tables,
                                            const std::vector<KeyType>& changed_route_keys,
                                            std::vector<RoutePartition>& partitions,
                                            const FixedRouteLayout& layout,
                                            const L1SubtreeBPTree::BuildOptions& subtree_options,
                                            uint32_t subtree_page_size,
                                            SegmentAllocator* segment_allocator,
                                            const PartitionUpdatePolicy& update_policy,
                                            const PrefixGovernancePolicy& governance_policy,
                                            size_t& size,
                                            uint64_t& generation);

private:
    static void ValidateSortedRecords(const std::vector<SubtreeRecord>& sorted_records);
    static RoutePartition BuildPartition(RoutePrefix prefix,
                                         const std::vector<SubtreeRecord>& records,
                                         uint64_t generation,
                                         const L1SubtreeBPTree::BuildOptions& subtree_options,
                                         uint32_t subtree_page_size,
                                         SegmentAllocator* segment_allocator,
                                         const PrefixGovernancePolicy& governance_policy);
    static PartitionUpdateDecision ChooseUpdateMode(const RoutePartition* existing_partition,
                                                    const std::vector<SubtreeRecord>& target_records,
                                                    const L1SubtreeBPTree::BuildOptions& subtree_options,
                                                    const PartitionUpdatePolicy& update_policy);
    static RoutePartition ApplyBulkLoadUpdate(RoutePrefix prefix,
                                              const std::vector<SubtreeRecord>& records,
                                              uint64_t generation,
                                              const L1SubtreeBPTree::BuildOptions& subtree_options,
                                              uint32_t subtree_page_size,
                                              SegmentAllocator* segment_allocator,
                                              const PrefixGovernancePolicy& governance_policy);
    static RoutePartition ApplyCowUpdate(RoutePrefix prefix,
                                         const RoutePartition* existing_partition,
                                         const std::vector<SubtreeRecord>& records,
                                         uint64_t generation,
                                         const L1SubtreeBPTree::BuildOptions& subtree_options,
                                         uint32_t subtree_page_size,
                                         SegmentAllocator* segment_allocator,
                                         const PrefixGovernancePolicy& governance_policy);
    static void RebuildPartitionFromRecords(size_t partition_idx,
                                            const std::vector<SubtreeRecord>& records,
                                            uint64_t generation,
                                            std::vector<RoutePartition>& partitions,
                                            const L1SubtreeBPTree::BuildOptions& subtree_options,
                                            uint32_t subtree_page_size,
                                            SegmentAllocator* segment_allocator,
                                            const PrefixGovernancePolicy& governance_policy);
};

}  // namespace flowkv::hybrid_l1
