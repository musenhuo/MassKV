#pragma once

#include "route_partition.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace flowkv::hybrid_l1 {

class FixedRouteLayout {
public:
    explicit FixedRouteLayout(size_t route_partition_num = RANGE_PARTITION_NUM);
    ~FixedRouteLayout();

    size_t PartitionCount() const { return route_partition_num_; }

    void InitializePartitions(std::vector<RoutePartition>& partitions) const;
    void RefreshPartitions(std::vector<RoutePartition>& partitions) const;
    bool FindPartitionByKey(const std::vector<RoutePartition>& partitions,
                            const KeyType& key,
                            size_t& partition_idx) const;
    void CollectPartitionsForRange(const std::vector<RoutePartition>& partitions,
                                   const KeyType& start,
                                   const KeyType& end,
                                   std::vector<size_t>& partition_indices) const;

private:
    size_t route_partition_num_;
};

}  // namespace flowkv::hybrid_l1
