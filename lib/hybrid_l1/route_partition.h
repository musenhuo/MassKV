#pragma once

#include "prefix_suffix.h"
#include "route_descriptor.h"
#include "subtree_page_store.h"

#include <cstddef>
#include <cstdint>

namespace flowkv::hybrid_l1 {

struct BucketGovernanceState {
    bool hot_prefix = false;
    bool prefer_cow = false;
    bool prefer_parallel_scan = false;
};

struct RoutePartition {
    RoutePrefix prefix = 0;
    uint32_t generation = 0;
    uint32_t record_count = 0;
    RouteDescriptorMode descriptor_mode = RouteDescriptorMode::kNormalSubtree;
    uint8_t tiny_enter_streak = 0;
    uint8_t pack_slot_id = 0;
    uint16_t descriptor_reserved = 0;
    uint64_t tiny_leaf_value = 0;
    SubtreePagePtr pack_page_ptr = kInvalidSubtreePagePtr;
    BucketGovernanceState governance{};
    SubtreePageStoreHandle subtree_store{};
};

}  // namespace flowkv::hybrid_l1
