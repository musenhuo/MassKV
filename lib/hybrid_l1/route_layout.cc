#include "lib/hybrid_l1/route_layout.h"

#include <algorithm>
#include <stdexcept>

namespace flowkv::hybrid_l1 {

FixedRouteLayout::FixedRouteLayout(size_t route_partition_num)
    : route_partition_num_(route_partition_num) {
    if (route_partition_num_ == 0) {
        throw std::invalid_argument("invalid route partition count");
    }
}

FixedRouteLayout::~FixedRouteLayout() = default;

void FixedRouteLayout::InitializePartitions(std::vector<RoutePartition>& partitions) const {
    partitions.clear();
}

void FixedRouteLayout::RefreshPartitions(std::vector<RoutePartition>& partitions) const {
    std::sort(partitions.begin(), partitions.end(),
              [](const RoutePartition& lhs, const RoutePartition& rhs) {
                  return lhs.prefix < rhs.prefix;
              });
}

bool FixedRouteLayout::FindPartitionByKey(const std::vector<RoutePartition>& partitions,
                                          const KeyType& key,
                                          size_t& partition_idx) const {
    if (partitions.empty()) {
        return false;
    }

    const RoutePrefix prefix = ExtractPrefix(key);
    auto it = std::lower_bound(
        partitions.begin(),
        partitions.end(),
        prefix,
        [](const RoutePartition& partition, RoutePrefix target_prefix) {
            return partition.prefix < target_prefix;
        });
    if (it == partitions.end() || it->prefix != prefix) {
        return false;
    }
    partition_idx = static_cast<size_t>(it - partitions.begin());
    return true;
}

void FixedRouteLayout::CollectPartitionsForRange(const std::vector<RoutePartition>& partitions,
                                                 const KeyType& start,
                                                 const KeyType& end,
                                                 std::vector<size_t>& partition_indices) const {
    partition_indices.clear();
    if (partitions.empty() || CompareKeyType(start, end) > 0) {
        return;
    }

    const RoutePrefix start_prefix = ExtractPrefix(start);
    const RoutePrefix end_prefix = ExtractPrefix(end);
    auto it = std::lower_bound(
        partitions.begin(),
        partitions.end(),
        start_prefix,
        [](const RoutePartition& partition, RoutePrefix target_prefix) {
            return partition.prefix < target_prefix;
        });
    while (it != partitions.end() && it->prefix <= end_prefix) {
        partition_indices.push_back(static_cast<size_t>(it - partitions.begin()));
        ++it;
    }
}

}  // namespace flowkv::hybrid_l1
