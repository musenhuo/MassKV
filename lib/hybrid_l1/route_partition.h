#pragma once

#include "prefix_suffix.h"
#include "subtree_page_store.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace flowkv::hybrid_l1 {

struct BucketGovernanceState {
    bool hot_prefix = false;
    bool prefer_cow = false;
    bool prefer_parallel_scan = false;
};

struct RoutePartition {
    RoutePrefix prefix = 0;
    uint64_t generation = 0;
    uint32_t record_count = 0;
    BucketGovernanceState governance{};
    SubtreePageStoreHandle subtree_store{};
    // Debug/unit-test only fallback when allocator is not wired.
    // In disk-resident mode this pointer stays null.
    std::shared_ptr<const SubtreePageSet> subtree_pages_cache;
};

}  // namespace flowkv::hybrid_l1
