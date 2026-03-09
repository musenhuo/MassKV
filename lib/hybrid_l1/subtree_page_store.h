#pragma once

#include "subtree_page.h"
#include "db/allocator/segment_allocator.h"

#include <cstdint>
#include <limits>
#include <vector>

namespace flowkv::hybrid_l1 {

constexpr uint32_t kInvalidSubtreeSegmentId = std::numeric_limits<uint32_t>::max();

struct SubtreeStoredPageRef {
    uint32_t segment_id = kInvalidSubtreeSegmentId;
    uint32_t page_id = 0;
};

struct SubtreePageStoreHandle {
    static constexpr uint8_t kQueryMetaValid = 0x1;

    uint32_t page_size = 0;
    uint8_t flags = 0;
    uint32_t root_page_id = kInvalidSubtreePageId;
    uint32_t page_count = 0;
    uint64_t record_count = 0;
    std::vector<SubtreeStoredPageRef> pages;

    bool Valid() const {
        return page_size != 0 && !pages.empty();
    }

    bool HasQueryMeta() const {
        return (flags & kQueryMetaValid) != 0;
    }

    void SetQueryMeta(uint32_t root_id, uint32_t subtree_page_count, uint64_t subtree_record_count) {
        flags |= kQueryMetaValid;
        root_page_id = root_id;
        page_count = subtree_page_count;
        record_count = subtree_record_count;
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
    static std::vector<uint8_t> LoadPageById(SegmentAllocator& allocator,
                                             const SubtreePageStoreHandle& handle,
                                             uint32_t page_id);
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
