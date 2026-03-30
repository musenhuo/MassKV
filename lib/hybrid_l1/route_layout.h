#pragma once

#include "route_partition.h"
#include "subtree_page.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

class MasstreeWrapper;

namespace flowkv::hybrid_l1 {

struct RoutedRoot {
    RoutePrefix prefix = 0;
    SubtreePagePtr root_page_ptr = kInvalidSubtreePagePtr;
};

struct RouteSwapOptions {
    // 0 means keep all route entries hot in Masstree (no swap).
    size_t hot_leaf_budget_bytes = 0;
    SegmentAllocator* segment_allocator = nullptr;
    // Logical cold-leaf page size (4KB aligned). Physical write-out is batched by page store.
    uint32_t leaf_page_size = 4 * 1024;
};

struct RouteSnapshotEntry {
    RoutePrefix prefix = 0;
    SubtreePagePtr root_page_ptr = kInvalidSubtreePagePtr;
    uint64_t descriptor = 0;
};

class FixedRouteLayout {
public:
    explicit FixedRouteLayout(size_t route_partition_num = RANGE_PARTITION_NUM,
                              RouteSwapOptions swap_options = {});
    ~FixedRouteLayout();

    size_t PartitionCount() const { return route_partition_num_; }

    void InitializePartitions(std::vector<RoutePartition>& partitions) const;
    void RefreshPartitions(std::vector<RoutePartition>& partitions) const;
    bool FindDescriptorByKey(const KeyType& key, uint64_t& descriptor) const;
    bool FindRootByKey(const KeyType& key, SubtreePagePtr& root_page_ptr) const;
    void CollectDescriptorsForRange(const KeyType& start,
                                    const KeyType& end,
                                    std::vector<RouteSnapshotEntry>& out) const;
    void CollectRootsForRange(const KeyType& start,
                              const KeyType& end,
                              std::vector<RoutedRoot>& roots) const;
    size_t EstimateRouteIndexMemoryUsageBytes() const;
    size_t EstimateRouteDescriptorIndexMemoryUsageBytes() const;
    size_t ColdStubCount() const;
    size_t EstimateColdSsdBytes() const;
    static RoutePrefix MakeRouteKey(RoutePrefix prefix);

private:
    static constexpr size_t kRouteMasstreeBytesPerEntry = 32;

    void SwapColdLeaves() const;
    void ReleaseColdResources() const;

    size_t route_partition_num_;
    RouteSwapOptions swap_options_;
    mutable std::unique_ptr<MasstreeWrapper> route_descriptor_index_;
    // Cold stubs stored as void* to avoid Masstree header dependency.
    // Actual type: ColdLeafStub<MasstreeWrapper::table_params>*
    mutable std::vector<void*> cold_stubs_;
    mutable std::vector<SubtreePagePtr> cold_ssd_ptrs_;
    mutable bool has_cold_stubs_ = false;
    mutable size_t route_cold_ssd_bytes_ = 0;
    mutable size_t route_entry_count_ = 0;
};

}  // namespace flowkv::hybrid_l1
