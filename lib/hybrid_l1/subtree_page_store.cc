#include "lib/hybrid_l1/subtree_page_store.h"

#include <atomic>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace flowkv::hybrid_l1 {
namespace {

std::atomic<uint64_t> g_cow_persist_calls{0};
std::atomic<uint64_t> g_cow_reused_pages{0};
std::atomic<uint64_t> g_cow_written_pages{0};
std::atomic<uint64_t> g_cow_reused_bytes{0};
std::atomic<uint64_t> g_cow_written_bytes{0};
thread_local uint64_t g_tls_read_pages = 0;

off_t PageOffset(const SortedSegment& segment, uint32_t page_id) {
    return static_cast<off_t>(segment.segment_id_ * SEGMENT_SIZE +
                              segment.EXTRA_PAGE_NUM * segment.PAGE_SIZE +
                              static_cast<size_t>(page_id) * segment.PAGE_SIZE);
}

size_t RoundUpTo(size_t value, size_t align) {
    if (align == 0) {
        return value;
    }
    return ((value + align - 1) / align) * align;
}

size_t ComputeExtraPageNum(uint32_t page_size) {
    const size_t page_num = SEGMENT_SIZE / page_size;
    const size_t bitmap_bytes = RoundUpTo(page_num / 8, page_size);
    return 1 + bitmap_bytes / page_size;
}

off_t PageOffset(uint32_t segment_id, uint32_t page_size, uint32_t page_id) {
    return static_cast<off_t>(segment_id * SEGMENT_SIZE +
                              ComputeExtraPageNum(page_size) * page_size +
                              static_cast<size_t>(page_id) * page_size);
}

void ReadFullPage(int fd, off_t offset, std::vector<uint8_t>& out) {
    const ssize_t ret = pread(fd, out.data(), out.size(), offset);
    if (ret != static_cast<ssize_t>(out.size())) {
        throw std::runtime_error("failed to read subtree page from segment");
    }
}

void WriteFullPage(int fd, off_t offset, const std::vector<uint8_t>& bytes) {
    const ssize_t ret = pwrite(fd, bytes.data(), bytes.size(), offset);
    if (ret != static_cast<ssize_t>(bytes.size())) {
        throw std::runtime_error("failed to write subtree page to segment");
    }
}

void ValidateHandle(const SubtreePageStoreHandle& handle) {
    if (!handle.Valid()) {
        throw std::invalid_argument("invalid subtree page store handle");
    }
    if (handle.page_size % 4096 != 0) {
        throw std::invalid_argument("subtree page store requires 4096-aligned page size");
    }
    for (const auto& page : handle.pages) {
        if (page.segment_id == kInvalidSubtreeSegmentId) {
            throw std::invalid_argument("subtree page store contains invalid segment id");
        }
    }
}

void ReadStoredPage(SegmentAllocator& allocator,
                    uint32_t page_size,
                    const SubtreeStoredPageRef& page_ref,
                    std::vector<uint8_t>& out) {
    if (page_ref.segment_id == kInvalidSubtreeSegmentId) {
        throw std::invalid_argument("invalid segment id in stored page reference");
    }
    out.assign(page_size, 0);
    ReadFullPage(allocator.Getfd(), PageOffset(page_ref.segment_id, page_size, page_ref.page_id), out);
    ++g_tls_read_pages;
}

struct StoredPageIdentity {
    uint32_t segment_id = kInvalidSubtreeSegmentId;
    uint32_t page_id = 0;

    bool operator==(const StoredPageIdentity& rhs) const {
        return segment_id == rhs.segment_id && page_id == rhs.page_id;
    }
};

struct StoredPageIdentityHash {
    size_t operator()(const StoredPageIdentity& key) const {
        const size_t h1 = std::hash<uint32_t>{}(key.segment_id);
        const size_t h2 = std::hash<uint32_t>{}(key.page_id);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

void CloseActiveSegment(SegmentAllocator& allocator, SortedSegment*& active_segment) {
    if (active_segment != nullptr) {
        allocator.CloseSegment(active_segment);
        active_segment = nullptr;
    }
}

void EnsureActiveSegment(SegmentAllocator& allocator,
                         uint32_t page_size,
                         SortedSegment*& active_segment) {
    if (active_segment != nullptr) {
        return;
    }
    active_segment = allocator.AllocSortedSegment(static_cast<int>(page_size));
    if (active_segment == nullptr) {
        throw std::runtime_error("failed to allocate subtree page segment");
    }
    if (active_segment->PAGE_SIZE != page_size) {
        allocator.CloseSegment(active_segment);
        active_segment = nullptr;
        throw std::runtime_error("allocator returned subtree segment with mismatched page size");
    }
}

SubtreeStoredPageRef AllocateAndWritePage(SegmentAllocator& allocator,
                                          uint32_t page_size,
                                          SortedSegment*& active_segment,
                                          const std::vector<uint8_t>& bytes) {
    while (true) {
        EnsureActiveSegment(allocator, page_size, active_segment);
        const off_t page_offset = active_segment->AllocatePage();
        if (page_offset < 0) {
            CloseActiveSegment(allocator, active_segment);
            continue;
        }
        const uint32_t page_id = static_cast<uint32_t>(
            active_segment->TrasformOffsetToPageId(static_cast<size_t>(page_offset)));
        WriteFullPage(allocator.Getfd(), PageOffset(*active_segment, page_id), bytes);
        if (active_segment->segment_id_ > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("subtree page segment id overflow");
        }
        return SubtreeStoredPageRef{static_cast<uint32_t>(active_segment->segment_id_), page_id};
    }
}

}  // namespace

SubtreePageStoreHandle SubtreePageStore::Persist(SegmentAllocator& allocator,
                                                 const SubtreePageSet& page_set) {
    if (!SubtreePageCodec::Validate(page_set)) {
        throw std::invalid_argument("cannot persist invalid subtree page set");
    }

    const SubtreePageManifest manifest = SubtreePageCodec::DecodeManifest(page_set.manifest);
    if (manifest.page_size == 0 || manifest.page_size % 4096 != 0) {
        throw std::invalid_argument("subtree page store requires 4096-aligned page size");
    }

    std::vector<SubtreeStoredPageRef> persisted_pages;
    persisted_pages.reserve(page_set.pages.size() + 1);
    SortedSegment* active_segment = nullptr;

    try {
        std::vector<uint8_t> manifest_page(manifest.page_size, 0);
        std::memcpy(manifest_page.data(), page_set.manifest.data(), page_set.manifest.size());
        persisted_pages.push_back(
            AllocateAndWritePage(allocator, manifest.page_size, active_segment, manifest_page));

        for (const auto& page : page_set.pages) {
            if (page.bytes.size() != manifest.page_size) {
                throw std::invalid_argument("subtree page size mismatch during persist");
            }
            persisted_pages.push_back(
                AllocateAndWritePage(allocator, manifest.page_size, active_segment, page.bytes));
        }
        CloseActiveSegment(allocator, active_segment);
    } catch (...) {
        CloseActiveSegment(allocator, active_segment);
        if (!persisted_pages.empty()) {
            SubtreePageStoreHandle cleanup_handle;
            cleanup_handle.page_size = manifest.page_size;
            cleanup_handle.pages = persisted_pages;
            try {
                Destroy(allocator, cleanup_handle);
            } catch (...) {
            }
        }
        throw;
    }

    SubtreePageStoreHandle handle;
    handle.page_size = manifest.page_size;
    handle.pages = std::move(persisted_pages);
    handle.SetQueryMeta(manifest.root_page_id, manifest.page_count, manifest.record_count);
    return handle;
}

SubtreePageStoreHandle SubtreePageStore::PersistCow(SegmentAllocator& allocator,
                                                    const SubtreePageStoreHandle& base_handle,
                                                    const SubtreePageSet& base_page_set,
                                                    const SubtreePageSet& target_page_set,
                                                    SubtreeCowStats* stats_out) {
    ValidateHandle(base_handle);
    if (!SubtreePageCodec::Validate(base_page_set) || !SubtreePageCodec::Validate(target_page_set)) {
        throw std::invalid_argument("PersistCow requires valid base/target page sets");
    }

    const SubtreePageManifest base_manifest = SubtreePageCodec::DecodeManifest(base_page_set.manifest);
    const SubtreePageManifest target_manifest = SubtreePageCodec::DecodeManifest(target_page_set.manifest);
    if (base_manifest.page_size != target_manifest.page_size || base_manifest.page_size != base_handle.page_size) {
        throw std::invalid_argument("PersistCow page size mismatch");
    }
    if (base_handle.pages.size() != base_page_set.pages.size() + 1) {
        throw std::invalid_argument("PersistCow base handle/page-set size mismatch");
    }

    std::unordered_map<uint32_t, size_t> base_page_index_by_page_id;
    base_page_index_by_page_id.reserve(base_page_set.pages.size());
    for (size_t i = 0; i < base_page_set.pages.size(); ++i) {
        base_page_index_by_page_id.emplace(base_page_set.pages[i].page_id, i);
    }

    std::vector<SubtreeStoredPageRef> result_pages;
    result_pages.reserve(target_page_set.pages.size() + 1);
    std::vector<SubtreeStoredPageRef> newly_allocated_pages;
    SortedSegment* active_segment = nullptr;

    auto maybe_reuse_or_allocate = [&](const std::vector<uint8_t>& bytes,
                                       const SubtreeStoredPageRef* reusable_ref) {
        if (reusable_ref != nullptr) {
            result_pages.push_back(*reusable_ref);
            return;
        }
        const auto new_ref =
            AllocateAndWritePage(allocator, target_manifest.page_size, active_segment, bytes);
        result_pages.push_back(new_ref);
        newly_allocated_pages.push_back(new_ref);
    };

    try {
        const SubtreeStoredPageRef* manifest_ref = nullptr;
        if (base_page_set.manifest == target_page_set.manifest) {
            manifest_ref = &base_handle.pages[0];
        }
        std::vector<uint8_t> target_manifest_page(target_manifest.page_size, 0);
        std::memcpy(target_manifest_page.data(),
                    target_page_set.manifest.data(),
                    target_page_set.manifest.size());
        maybe_reuse_or_allocate(target_manifest_page, manifest_ref);

        for (const auto& target_page : target_page_set.pages) {
            const SubtreeStoredPageRef* reusable_ref = nullptr;
            const auto it = base_page_index_by_page_id.find(target_page.page_id);
            if (it != base_page_index_by_page_id.end()) {
                const size_t base_idx = it->second;
                if (base_page_set.pages[base_idx].bytes == target_page.bytes) {
                    reusable_ref = &base_handle.pages[base_idx + 1];
                }
            }
            maybe_reuse_or_allocate(target_page.bytes, reusable_ref);
        }
        CloseActiveSegment(allocator, active_segment);
    } catch (...) {
        CloseActiveSegment(allocator, active_segment);
        if (!newly_allocated_pages.empty()) {
            SubtreePageStoreHandle cleanup_handle;
            cleanup_handle.page_size = target_manifest.page_size;
            cleanup_handle.pages = std::move(newly_allocated_pages);
            try {
                Destroy(allocator, cleanup_handle);
            } catch (...) {
            }
        }
        throw;
    }

    SubtreePageStoreHandle handle;
    handle.page_size = target_manifest.page_size;
    handle.pages = std::move(result_pages);
    handle.SetQueryMeta(target_manifest.root_page_id, target_manifest.page_count,
                        target_manifest.record_count);
    const uint64_t written_pages = static_cast<uint64_t>(newly_allocated_pages.size());
    const uint64_t total_pages = static_cast<uint64_t>(handle.pages.size());
    const uint64_t reused_pages = total_pages >= written_pages ? (total_pages - written_pages) : 0;
    const uint64_t page_size = static_cast<uint64_t>(target_manifest.page_size);

    g_cow_persist_calls.fetch_add(1, std::memory_order_relaxed);
    g_cow_reused_pages.fetch_add(reused_pages, std::memory_order_relaxed);
    g_cow_written_pages.fetch_add(written_pages, std::memory_order_relaxed);
    g_cow_reused_bytes.fetch_add(reused_pages * page_size, std::memory_order_relaxed);
    g_cow_written_bytes.fetch_add(written_pages * page_size, std::memory_order_relaxed);

    if (stats_out != nullptr) {
        stats_out->persist_calls = 1;
        stats_out->reused_pages = reused_pages;
        stats_out->written_pages = written_pages;
        stats_out->reused_bytes = reused_pages * page_size;
        stats_out->written_bytes = written_pages * page_size;
    }
    return handle;
}

SubtreePageSet SubtreePageStore::Load(SegmentAllocator& allocator,
                                      const SubtreePageStoreHandle& handle) {
    ValidateHandle(handle);

    std::unordered_map<uint32_t, SortedSegment*> opened_segments;
    auto cleanup_opened_segments = [&]() {
        for (auto& kv : opened_segments) {
            delete kv.second;
        }
        opened_segments.clear();
    };
    auto get_opened_segment = [&](uint32_t segment_id) -> SortedSegment* {
        const auto it = opened_segments.find(segment_id);
        if (it != opened_segments.end()) {
            return it->second;
        }
        SortedSegment* segment =
            allocator.GetSortedSegment(static_cast<size_t>(segment_id), handle.page_size);
        if (segment == nullptr) {
            throw std::runtime_error("failed to open subtree page segment for load");
        }
        opened_segments.emplace(segment_id, segment);
        return segment;
    };

    SubtreePageSet page_set;
    std::vector<uint8_t> manifest_page(handle.page_size, 0);
    const auto& manifest_ref = handle.pages.front();
    SortedSegment* manifest_segment = get_opened_segment(manifest_ref.segment_id);
    ReadFullPage(allocator.Getfd(), PageOffset(*manifest_segment, manifest_ref.page_id), manifest_page);
    page_set.manifest.assign(manifest_page.begin(),
                             manifest_page.begin() + SubtreePageCodec::kManifestSize);

    SubtreePageManifest manifest;
    try {
        manifest = SubtreePageCodec::DecodeManifest(page_set.manifest);
    } catch (const std::exception& e) {
        cleanup_opened_segments();
        throw std::runtime_error("failed to decode subtree manifest at seg=" +
                                 std::to_string(manifest_ref.segment_id) + ", page=" +
                                 std::to_string(manifest_ref.page_id) + ": " + e.what());
    }
    if (handle.pages.size() != manifest.page_count + 1) {
        cleanup_opened_segments();
        throw std::runtime_error("subtree page store handle length mismatch");
    }
    if (manifest.page_size != handle.page_size) {
        cleanup_opened_segments();
        throw std::runtime_error("subtree page store handle page size mismatch");
    }

    page_set.pages.reserve(manifest.page_count);
    for (uint32_t i = 0; i < manifest.page_count; ++i) {
        SubtreePage page;
        page.bytes.assign(handle.page_size, 0);
        const auto& stored_page_ref = handle.pages[i + 1];
        SortedSegment* stored_segment = get_opened_segment(stored_page_ref.segment_id);
        ReadFullPage(allocator.Getfd(), PageOffset(*stored_segment, stored_page_ref.page_id), page.bytes);
        page.page_id = static_cast<uint32_t>(page.bytes[8]) |
                       (static_cast<uint32_t>(page.bytes[9]) << 8) |
                       (static_cast<uint32_t>(page.bytes[10]) << 16) |
                       (static_cast<uint32_t>(page.bytes[11]) << 24);
        page_set.pages.push_back(std::move(page));
    }

    cleanup_opened_segments();
    if (!SubtreePageCodec::Validate(page_set)) {
        throw std::runtime_error("loaded subtree page set failed validation");
    }
    return page_set;
}

std::vector<uint8_t> SubtreePageStore::LoadManifestPage(SegmentAllocator& allocator,
                                                        const SubtreePageStoreHandle& handle) {
    ValidateHandle(handle);
    std::vector<uint8_t> manifest_page;
    ReadStoredPage(allocator, handle.page_size, handle.pages.front(), manifest_page);
    return manifest_page;
}

std::vector<uint8_t> SubtreePageStore::LoadPageById(SegmentAllocator& allocator,
                                                    const SubtreePageStoreHandle& handle,
                                                    uint32_t page_id) {
    ValidateHandle(handle);
    if (page_id + 1 >= handle.pages.size()) {
        throw std::out_of_range("subtree page id out of range");
    }
    std::vector<uint8_t> page_bytes;
    ReadStoredPage(allocator, handle.page_size, handle.pages[page_id + 1], page_bytes);
    return page_bytes;
}

void SubtreePageStore::Destroy(SegmentAllocator& allocator, const SubtreePageStoreHandle& handle) {
    ValidateHandle(handle);

    std::map<uint32_t, std::vector<uint32_t>> pages_by_segment;
    for (const auto& page : handle.pages) {
        pages_by_segment[page.segment_id].push_back(page.page_id);
    }

    for (const auto& [segment_id, page_ids] : pages_by_segment) {
        SortedSegment* segment =
            allocator.GetSortedSegmentForDelete(static_cast<size_t>(segment_id), handle.page_size);
        if (segment == nullptr) {
            continue;
        }
        for (const uint32_t page_id : page_ids) {
            segment->RecyclePage(page_id);
        }
        allocator.CloseSegmentForDelete(segment);
    }
}

void SubtreePageStore::DestroyUnshared(SegmentAllocator& allocator,
                                       const SubtreePageStoreHandle& old_handle,
                                       const SubtreePageStoreHandle& keep_handle) {
    ValidateHandle(old_handle);
    ValidateHandle(keep_handle);
    if (old_handle.page_size != keep_handle.page_size) {
        throw std::invalid_argument("DestroyUnshared page size mismatch");
    }

    std::unordered_set<StoredPageIdentity, StoredPageIdentityHash> keep_pages;
    keep_pages.reserve(keep_handle.pages.size());
    for (const auto& page : keep_handle.pages) {
        keep_pages.insert(StoredPageIdentity{page.segment_id, page.page_id});
    }

    std::map<uint32_t, std::vector<uint32_t>> pages_to_recycle;
    for (const auto& page : old_handle.pages) {
        const StoredPageIdentity key{page.segment_id, page.page_id};
        if (keep_pages.find(key) != keep_pages.end()) {
            continue;
        }
        pages_to_recycle[page.segment_id].push_back(page.page_id);
    }

    for (const auto& [segment_id, page_ids] : pages_to_recycle) {
        SortedSegment* segment =
            allocator.GetSortedSegmentForDelete(static_cast<size_t>(segment_id), old_handle.page_size);
        if (segment == nullptr) {
            continue;
        }
        for (const uint32_t page_id : page_ids) {
            segment->RecyclePage(page_id);
        }
        allocator.CloseSegmentForDelete(segment);
    }
}

SubtreeCowStats SubtreePageStore::GetCowStats() {
    SubtreeCowStats stats;
    stats.persist_calls = g_cow_persist_calls.load(std::memory_order_relaxed);
    stats.reused_pages = g_cow_reused_pages.load(std::memory_order_relaxed);
    stats.written_pages = g_cow_written_pages.load(std::memory_order_relaxed);
    stats.reused_bytes = g_cow_reused_bytes.load(std::memory_order_relaxed);
    stats.written_bytes = g_cow_written_bytes.load(std::memory_order_relaxed);
    return stats;
}

void SubtreePageStore::ResetCowStats() {
    g_cow_persist_calls.store(0, std::memory_order_relaxed);
    g_cow_reused_pages.store(0, std::memory_order_relaxed);
    g_cow_written_pages.store(0, std::memory_order_relaxed);
    g_cow_reused_bytes.store(0, std::memory_order_relaxed);
    g_cow_written_bytes.store(0, std::memory_order_relaxed);
}

uint64_t SubtreePageStore::GetThreadLocalReadPages() {
    return g_tls_read_pages;
}

void SubtreePageStore::ResetThreadLocalReadPages() {
    g_tls_read_pages = 0;
}

}  // namespace flowkv::hybrid_l1
