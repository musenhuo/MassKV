#pragma once

#include "subtree_page.h"
#include "db/allocator/segment_allocator.h"

#include <cstdint>
#include <vector>

namespace flowkv::hybrid_l1 {

struct SubtreePageStoreHandle {
    static constexpr uint8_t kQueryMetaValid = 0x1;

    uint16_t page_size = 0;
    uint8_t flags = 0;
    uint8_t reserved = 0;
    uint32_t page_count = 0;
    uint64_t record_count = 0;
    SubtreePagePtr root_page_ptr = kInvalidSubtreePagePtr;
    SubtreePagePtr manifest_page_ptr = kInvalidSubtreePagePtr;

    bool Valid() const {
        if (page_size == 0) {
            return false;
        }
        if (page_count == 0) {
            return root_page_ptr == kInvalidSubtreePagePtr &&
                   manifest_page_ptr == kInvalidSubtreePagePtr;
        }
        return root_page_ptr != kInvalidSubtreePagePtr &&
               manifest_page_ptr != kInvalidSubtreePagePtr;
    }

    bool HasQueryMeta() const {
        return (flags & kQueryMetaValid) != 0;
    }

    void SetQueryMeta(SubtreePagePtr root_ptr, uint32_t pages, uint64_t records) {
        flags |= kQueryMetaValid;
        root_page_ptr = root_ptr;
        page_count = pages;
        record_count = records;
    }
};

struct SubtreeCowStats {
    uint64_t persist_calls = 0;
    uint64_t reused_pages = 0;
    uint64_t written_pages = 0;
    uint64_t reused_bytes = 0;
    uint64_t written_bytes = 0;
};

class SubtreePageStore {
public:
    static SubtreePageStoreHandle Persist(SegmentAllocator& allocator, const SubtreePageSet& page_set);
    static SubtreePageStoreHandle PersistCow(SegmentAllocator& allocator,
                                             const SubtreePageStoreHandle& base_handle,
                                             const SubtreePageSet& base_page_set,
                                             const SubtreePageSet& target_page_set,
                                             SubtreeCowStats* stats_out = nullptr);
    static std::vector<uint8_t> LoadManifestPage(SegmentAllocator& allocator,
                                                 const SubtreePageStoreHandle& handle);
    static std::vector<uint8_t> LoadPageByPtr(SegmentAllocator& allocator,
                                              uint32_t page_size,
                                              SubtreePagePtr page_ptr);
    static std::vector<uint8_t> LoadPageByPtr(SegmentAllocator& allocator,
                                              const SubtreePageStoreHandle& handle,
                                              SubtreePagePtr page_ptr);
    // Persist opaque 4KB-aligned pages (e.g., NormalPack pages) and return physical pointers.
    static std::vector<SubtreePagePtr> PersistOpaquePages(
        SegmentAllocator& allocator,
        uint32_t page_size,
        const std::vector<std::vector<uint8_t>>& pages);
    static void DestroyOpaquePages(SegmentAllocator& allocator,
                                   uint32_t page_size,
                                   const std::vector<SubtreePagePtr>& page_ptrs);
    static SubtreePageSet Load(SegmentAllocator& allocator, const SubtreePageStoreHandle& handle);
    static void Destroy(SegmentAllocator& allocator, const SubtreePageStoreHandle& handle);
    static void DestroyUnshared(SegmentAllocator& allocator,
                                const SubtreePageStoreHandle& old_handle,
                                const SubtreePageStoreHandle& keep_handle);
    static SubtreeCowStats GetCowStats();
    static void ResetCowStats();
    static uint64_t GetThreadLocalReadPages();
    static void ResetThreadLocalReadPages();
};

}  // namespace flowkv::hybrid_l1
