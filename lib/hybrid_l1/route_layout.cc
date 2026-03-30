#include "lib/hybrid_l1/route_layout.h"

#include "lib/masstree/masstree_wrapper.h"
#include "lib/hybrid_l1/route_cold_leaf.h"
#include "lib/hybrid_l1/route_descriptor.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace flowkv::hybrid_l1 {
namespace {

using ColdStub = ColdLeafStub<MasstreeWrapper::table_params>;

uint64_t EncodeDescriptorForPartition(const RoutePartition& partition) {
    switch (partition.descriptor_mode) {
    case RouteDescriptorMode::kTinyDirect:
        return RouteDescriptor::EncodeTinyLeafValue(partition.tiny_leaf_value);
    case RouteDescriptorMode::kNormalPack:
        return RouteDescriptor::EncodeNormalPack(partition.pack_page_ptr, partition.pack_slot_id);
    case RouteDescriptorMode::kNormalSubtree:
    default:
        return RouteDescriptor::EncodeNormalSubtree(partition.subtree_store.root_page_ptr);
    }
}

SubtreePagePtr ResolveRootForDescriptor(uint64_t descriptor) {
    if (RouteDescriptor::IsNormalSubtree(descriptor)) {
        return RouteDescriptor::DecodeNormalRoot(descriptor);
    }
    return kInvalidSubtreePagePtr;
}

void PutRouteEntry(MasstreeWrapper& tree, RoutePrefix prefix, uint64_t value) {
    ValueHelper helper(value);
    tree.insert(prefix, helper);
}

}  // namespace

FixedRouteLayout::FixedRouteLayout(size_t route_partition_num, RouteSwapOptions swap_options)
    : route_partition_num_(route_partition_num), swap_options_(swap_options) {
    if (route_partition_num_ == 0) {
        throw std::invalid_argument("invalid route partition count");
    }
    if (swap_options_.leaf_page_size == 0 || swap_options_.leaf_page_size % 4096 != 0) {
        throw std::invalid_argument("route leaf page size must be 4KB-aligned and non-zero");
    }
    if (swap_options_.leaf_page_size < kSpilledLeafMinPageBytes) {
        throw std::invalid_argument("route leaf page size must be >=4KB");
    }
}

FixedRouteLayout::~FixedRouteLayout() {
    ReleaseColdResources();
}

void FixedRouteLayout::InitializePartitions(std::vector<RoutePartition>& partitions) const {
    partitions.clear();
    route_descriptor_index_.reset();
    route_entry_count_ = 0;
    ReleaseColdResources();
}

void FixedRouteLayout::RefreshPartitions(std::vector<RoutePartition>& partitions) const {
    std::sort(partitions.begin(), partitions.end(),
              [](const RoutePartition& lhs, const RoutePartition& rhs) {
                  return lhs.prefix < rhs.prefix;
              });

    std::vector<RouteSnapshotEntry> entries;
    entries.reserve(partitions.size());
    RoutePrefix prev = 0;
    bool first = true;
    for (const auto& partition : partitions) {
        if (!first && partition.prefix == prev) {
            continue;
        }
        const uint64_t descriptor = EncodeDescriptorForPartition(partition);
        const SubtreePagePtr root_ptr = ResolveRootForDescriptor(descriptor);
        entries.push_back(RouteSnapshotEntry{partition.prefix, root_ptr, descriptor});
        prev = partition.prefix;
        first = false;
    }
    route_entry_count_ = entries.size();

    // Release previous cold resources before rebuilding.
    ReleaseColdResources();

    route_descriptor_index_ = std::make_unique<MasstreeWrapper>();
    route_descriptor_index_->table_init();

    for (const auto& entry : entries) {
        PutRouteEntry(*route_descriptor_index_, entry.prefix, entry.descriptor);
    }

    // After building the full Masstree, swap cold leaves to SSD if budget exceeded.
    SwapColdLeaves();
}

bool FixedRouteLayout::FindDescriptorByKey(const KeyType& key, uint64_t& descriptor) const {
    descriptor = 0;
    if (route_descriptor_index_ == nullptr) {
        return false;
    }

    const RoutePrefix prefix = ExtractPrefix(key);

    if (!has_cold_stubs_) {
        // Fast path: no cold stubs, pure Masstree lookup.
        uint64_t value = 0;
        if (route_descriptor_index_->search(MakeRouteKey(prefix), value)) {
            descriptor = value;
            return descriptor != 0;
        }
        return false;
    }

    // Cold-aware path.
    uint64_t value = 0;
    MasstreeWrapper::node_type* cold_node = nullptr;
    if (route_descriptor_index_->search_cold_aware(MakeRouteKey(prefix), value, cold_node)) {
        descriptor = value;
        return descriptor != 0;
    }

    if (cold_node != nullptr) {
        // Hit a cold stub — load from SSD.
        auto* stub = AsColdStub<MasstreeWrapper::table_params>(cold_node);
        if (stub->validate() && swap_options_.segment_allocator != nullptr) {
            auto page = SubtreePageStore::LoadPageByPtr(
                *swap_options_.segment_allocator, swap_options_.leaf_page_size,
                stub->ssd_page_ptr);
            if (SearchSpilledLeafSlot(page.data(), page.size(), stub->slot_id, prefix, &descriptor)) {
                return descriptor != 0;
            }
        }
    }
    return false;
}

bool FixedRouteLayout::FindRootByKey(const KeyType& key, SubtreePagePtr& root_page_ptr) const {
    root_page_ptr = kInvalidSubtreePagePtr;
    uint64_t descriptor = 0;
    if (!FindDescriptorByKey(key, descriptor)) {
        return false;
    }
    root_page_ptr = RouteDescriptor::DecodeNormalRoot(descriptor);
    return root_page_ptr != kInvalidSubtreePagePtr;
}

void FixedRouteLayout::CollectDescriptorsForRange(const KeyType& start,
                                                  const KeyType& end,
                                                  std::vector<RouteSnapshotEntry>& out) const {
    out.clear();
    if (CompareKeyType(start, end) > 0) {
        return;
    }

    const RoutePrefix start_prefix = ExtractPrefix(start);
    const RoutePrefix end_prefix = ExtractPrefix(end);
    if (start_prefix > end_prefix) {
        return;
    }

    struct CandidateEntry {
        RouteSnapshotEntry entry{};
        uint8_t source_priority = 0;  // 0: hot, 1: cold
    };
    std::vector<CandidateEntry> candidates;

    // Collect from hot entries via Masstree scan.
    if (route_descriptor_index_ != nullptr) {
        std::vector<uint64_t> prefixes;
        std::vector<uint64_t> descriptors;
        route_descriptor_index_->scan(MakeRouteKey(start_prefix), MakeRouteKey(end_prefix),
                                      prefixes, descriptors);
        const size_t count = std::min(prefixes.size(), descriptors.size());
        candidates.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            const RoutePrefix prefix = static_cast<RoutePrefix>(prefixes[i]);
            if (prefix < start_prefix || prefix > end_prefix) {
                continue;
            }
            const uint64_t descriptor = descriptors[i];
            if (!RouteDescriptor::IsValid(descriptor)) {
                continue;
            }
            candidates.push_back(CandidateEntry{
                RouteSnapshotEntry{prefix, ResolveRootForDescriptor(descriptor), descriptor},
                0});
        }
    }

    // Collect from cold stubs.
    if (has_cold_stubs_ && swap_options_.segment_allocator != nullptr) {
        std::unordered_map<SubtreePagePtr, std::vector<const ColdStub*>> stubs_by_page;
        stubs_by_page.reserve(cold_stubs_.size());
        for (void* raw : cold_stubs_) {
            auto* stub = static_cast<ColdStub*>(raw);
            if (!stub->validate()) {
                continue;
            }
            stubs_by_page[stub->ssd_page_ptr].push_back(stub);
        }
        for (const auto& kv : stubs_by_page) {
            const SubtreePagePtr page_ptr = kv.first;
            const auto page = SubtreePageStore::LoadPageByPtr(
                *swap_options_.segment_allocator, swap_options_.leaf_page_size, page_ptr);
            for (const auto* stub : kv.second) {
                std::vector<SpilledLeafEntry> cold_entries;
                CollectFromSpilledLeafSlot(page.data(), page.size(), stub->slot_id,
                                           start_prefix, end_prefix, cold_entries);
                for (const auto& e : cold_entries) {
                    if (!RouteDescriptor::IsValid(e.descriptor)) {
                        continue;
                    }
                    candidates.push_back(CandidateEntry{
                        RouteSnapshotEntry{e.prefix, ResolveRootForDescriptor(e.descriptor),
                                           e.descriptor},
                        1});
                }
            }
        }
    }

    if (candidates.empty()) {
        return;
    }

    std::sort(candidates.begin(), candidates.end(),
              [](const CandidateEntry& lhs, const CandidateEntry& rhs) {
                  if (lhs.entry.prefix != rhs.entry.prefix) {
                      return lhs.entry.prefix < rhs.entry.prefix;
                  }
                  if (lhs.source_priority != rhs.source_priority) {
                      return lhs.source_priority < rhs.source_priority;
                  }
                  return lhs.entry.descriptor < rhs.entry.descriptor;
              });

    out.reserve(candidates.size());
    RoutePrefix last_prefix = 0;
    bool first = true;
    for (const auto& candidate : candidates) {
        if (!first && candidate.entry.prefix == last_prefix) {
            continue;
        }
        out.push_back(candidate.entry);
        last_prefix = candidate.entry.prefix;
        first = false;
    }
}

void FixedRouteLayout::CollectRootsForRange(const KeyType& start,
                                            const KeyType& end,
                                            std::vector<RoutedRoot>& roots) const {
    roots.clear();
    std::vector<RouteSnapshotEntry> descriptors;
    CollectDescriptorsForRange(start, end, descriptors);
    roots.reserve(descriptors.size());
    for (const auto& entry : descriptors) {
        if (entry.root_page_ptr == kInvalidSubtreePagePtr) {
            continue;
        }
        roots.push_back(RoutedRoot{entry.prefix, entry.root_page_ptr});
    }
}

size_t FixedRouteLayout::EstimateRouteIndexMemoryUsageBytes() const {
    size_t bytes = 0;
    if (route_descriptor_index_ != nullptr) {
        bytes += route_descriptor_index_->EstimateMemoryUsageBytes();
    }
    // Add cold stub memory.
    bytes += cold_stubs_.size() * sizeof(ColdStub);
    return bytes;
}

size_t FixedRouteLayout::EstimateRouteDescriptorIndexMemoryUsageBytes() const {
    if (route_descriptor_index_ == nullptr) {
        return 0;
    }
    return route_descriptor_index_->EstimateMemoryUsageBytes();
}

size_t FixedRouteLayout::ColdStubCount() const {
    return cold_stubs_.size();
}

size_t FixedRouteLayout::EstimateColdSsdBytes() const {
    return route_cold_ssd_bytes_;
}

RoutePrefix FixedRouteLayout::MakeRouteKey(RoutePrefix prefix) {
    return prefix;
}

// ============================================================
// SwapColdLeaves: convert cold Masstree leaves to ColdLeafStubs
// ============================================================

void FixedRouteLayout::SwapColdLeaves() const {
    if (route_descriptor_index_ == nullptr) return;
    if (swap_options_.hot_leaf_budget_bytes == 0) return;
    if (swap_options_.segment_allocator == nullptr) return;

    // Collect all leaves and their entries.
    struct LeafInfo {
        MasstreeWrapper::leaf_type* leaf = nullptr;
        MasstreeWrapper::internode_type* parent = nullptr;
        int parent_child_idx = -1;
        size_t alloc_bytes = 0;
        std::vector<SpilledLeafEntry> entries;
    };
    std::vector<LeafInfo> leaves;
    size_t total_leaf_bytes = 0;

    route_descriptor_index_->ForEachLeaf(
        [&](MasstreeWrapper::node_type* node, bool is_cold) {
            if (is_cold) return;  // shouldn't happen on fresh tree
            auto* leaf = static_cast<MasstreeWrapper::leaf_type*>(node);
            LeafInfo info;
            info.leaf = leaf;
            info.alloc_bytes = leaf->allocated_size();
            auto* parent = leaf->parent_;
            if (parent != nullptr && !parent->isleaf()) {
                auto* internode = static_cast<MasstreeWrapper::internode_type*>(parent);
                for (int c = 0; c <= internode->size(); ++c) {
                    if (internode->child_[c] == leaf) {
                        info.parent = internode;
                        info.parent_child_idx = c;
                        break;
                    }
                }
            }
            auto perm = leaf->permutation();
            for (int r = 0; r < perm.size(); ++r) {
                int p = perm[r];
                SpilledLeafEntry e;
                // Masstree stores comparable ikey (net_to_host_order(raw_key)).
                // Spill pages store raw route prefix so point lookup can search
                // by ExtractPrefix(key) directly.
                const auto comparable_prefix = static_cast<uint64_t>(leaf->ikey(p));
                e.prefix = static_cast<RoutePrefix>(host_to_net_order(comparable_prefix));
                e.descriptor = static_cast<uint64_t>(leaf->lv_[p].value());
                info.entries.push_back(e);
            }
            // Sort entries by prefix within this leaf.
            std::sort(info.entries.begin(), info.entries.end(),
                      [](const SpilledLeafEntry& a, const SpilledLeafEntry& b) {
                          return a.prefix < b.prefix;
                      });
            total_leaf_bytes += info.alloc_bytes;
            leaves.push_back(std::move(info));
        });

    if (total_leaf_bytes <= swap_options_.hot_leaf_budget_bytes) {
        return;  // Fits in budget, no swap needed.
    }

    // Decide which leaves to keep hot: keep from the front until budget exceeded.
    size_t hot_bytes = 0;
    size_t first_cold_idx = leaves.size();
    for (size_t i = 0; i < leaves.size(); ++i) {
        if (hot_bytes + leaves[i].alloc_bytes > swap_options_.hot_leaf_budget_bytes) {
            first_cold_idx = i;
            break;
        }
        hot_bytes += leaves[i].alloc_bytes;
    }

    if (first_cold_idx >= leaves.size()) {
        return;  // All fit.
    }

    // Keep correctness simple: swap only when the cold suffix is fully detachable.
    for (size_t i = first_cold_idx; i < leaves.size(); ++i) {
        if (leaves[i].parent == nullptr || leaves[i].parent_child_idx < 0 ||
            leaves[i].entries.empty()) {
            return;
        }
    }

    // Truncate the leaf linked list: set the last hot leaf's next_ to nullptr
    // so that Masstree scan doesn't walk into cold stubs (which lack leaf fields).
    if (first_cold_idx > 0) {
        auto* last_hot = leaves[first_cold_idx - 1].leaf;
        last_hot->next_.ptr = nullptr;
    }

    // Pack multiple cold leaves per logical cold page (4KB-aligned), then persist pages in batch.
    struct PackedPagePlan {
        std::vector<size_t> leaf_indices;
    };
    std::vector<PackedPagePlan> packed_page_plans;
    packed_page_plans.reserve(leaves.size() - first_cold_idx);
    for (size_t i = first_cold_idx; i < leaves.size();) {
        PackedPagePlan plan;
        size_t used = kSpilledLeafHeaderBytes;
        while (i < leaves.size()) {
            const auto& info = leaves[i];
            const size_t need = kSpilledLeafSlotBytes +
                                info.entries.size() * kSpilledLeafEntryBytes;
            if (need + kSpilledLeafHeaderBytes > swap_options_.leaf_page_size) {
                return;  // Leaf segment too large for configured page size.
            }
            if (!plan.leaf_indices.empty() && used + need > swap_options_.leaf_page_size) {
                break;
            }
            if (plan.leaf_indices.empty() && used + need > swap_options_.leaf_page_size) {
                return;
            }
            plan.leaf_indices.push_back(i);
            used += need;
            ++i;
        }
        if (plan.leaf_indices.empty()) {
            return;
        }
        packed_page_plans.push_back(std::move(plan));
    }

    std::vector<std::vector<uint8_t>> packed_pages;
    packed_pages.reserve(packed_page_plans.size());
    for (const auto& plan : packed_page_plans) {
        std::vector<const std::vector<SpilledLeafEntry>*> slots;
        slots.reserve(plan.leaf_indices.size());
        for (const auto leaf_idx : plan.leaf_indices) {
            slots.push_back(&leaves[leaf_idx].entries);
        }
        packed_pages.push_back(SerializePackedLeafEntries(slots, swap_options_.leaf_page_size));
    }

    const auto page_ptrs = SubtreePageStore::PersistOpaquePages(
        *swap_options_.segment_allocator, swap_options_.leaf_page_size, packed_pages);
    if (page_ptrs.size() != packed_page_plans.size()) {
        throw std::runtime_error("packed cold leaf persist page count mismatch");
    }

    auto* ti = route_descriptor_index_->get_ti();
    for (size_t page_idx = 0; page_idx < packed_page_plans.size(); ++page_idx) {
        const auto ssd_ptr = page_ptrs[page_idx];
        const auto& plan = packed_page_plans[page_idx];
        bool any_stub_installed = false;
        for (size_t slot = 0; slot < plan.leaf_indices.size(); ++slot) {
            auto& info = leaves[plan.leaf_indices[slot]];
            auto* stub = new ColdStub(
                ssd_ptr, static_cast<uint16_t>(info.entries.size()), static_cast<uint16_t>(slot));
            if (info.parent == nullptr || info.parent_child_idx < 0 ||
                info.parent->child_[info.parent_child_idx] != info.leaf) {
                delete stub;
                continue;
            }
            info.parent->child_[info.parent_child_idx] = stub;
            stub->parent_ptr = info.leaf->parent_;
            cold_stubs_.push_back(stub);
            info.leaf->deallocate(*ti);
            any_stub_installed = true;
        }
        if (any_stub_installed) {
            cold_ssd_ptrs_.push_back(ssd_ptr);
        } else {
            std::vector<SubtreePagePtr> rollback_pages{ssd_ptr};
            SubtreePageStore::DestroyOpaquePages(
                *swap_options_.segment_allocator, swap_options_.leaf_page_size, rollback_pages);
        }
    }

    has_cold_stubs_ = !cold_stubs_.empty();
    route_cold_ssd_bytes_ = cold_ssd_ptrs_.size() * swap_options_.leaf_page_size;
}

void FixedRouteLayout::ReleaseColdResources() const {
    // Free SSD pages.
    if (swap_options_.segment_allocator != nullptr && !cold_ssd_ptrs_.empty()) {
        SubtreePageStore::DestroyOpaquePages(
            *swap_options_.segment_allocator, swap_options_.leaf_page_size, cold_ssd_ptrs_);
    }
    cold_ssd_ptrs_.clear();
    cold_ssd_ptrs_.shrink_to_fit();

    // Free cold stubs.
    for (void* raw : cold_stubs_) {
        delete static_cast<ColdStub*>(raw);
    }
    cold_stubs_.clear();
    cold_stubs_.shrink_to_fit();

    has_cold_stubs_ = false;
    route_cold_ssd_bytes_ = 0;
}

}  // namespace flowkv::hybrid_l1
