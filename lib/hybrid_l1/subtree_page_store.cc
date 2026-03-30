#include "lib/hybrid_l1/subtree_page_store.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

constexpr uint32_t kPageMagic = 0x5042544Cu;  // "LTBP"
constexpr uint16_t kLeafType = 2;
constexpr uint16_t kInternalType = 1;
constexpr size_t kPageHeaderSize = 20;

size_t EncodedKeySize() {
#if defined(FLOWKV_KEY16)
    return sizeof(uint64_t) * 2;
#else
    return sizeof(uint64_t);
#endif
}

size_t EncodedSuffixSize() {
    return sizeof(RouteSuffix);
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

uint16_t ReadU16(const std::vector<uint8_t>& in, size_t offset) {
    if (offset + sizeof(uint16_t) > in.size()) {
        throw std::runtime_error("subtree page decode overflow (u16)");
    }
    return static_cast<uint16_t>(in[offset]) |
           (static_cast<uint16_t>(in[offset + 1]) << 8);
}

uint32_t ReadU32(const std::vector<uint8_t>& in, size_t offset) {
    if (offset + sizeof(uint32_t) > in.size()) {
        throw std::runtime_error("subtree page decode overflow (u32)");
    }
    return static_cast<uint32_t>(in[offset]) |
           (static_cast<uint32_t>(in[offset + 1]) << 8) |
           (static_cast<uint32_t>(in[offset + 2]) << 16) |
           (static_cast<uint32_t>(in[offset + 3]) << 24);
}

uint64_t ReadU64(const std::vector<uint8_t>& in, size_t offset) {
    if (offset + sizeof(uint64_t) > in.size()) {
        throw std::runtime_error("subtree page decode overflow (u64)");
    }
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(in[offset + i]) << (i * 8);
    }
    return value;
}

void WriteU64(std::vector<uint8_t>& out, size_t offset, uint64_t value) {
    if (offset + sizeof(uint64_t) > out.size()) {
        throw std::runtime_error("subtree page encode overflow (u64)");
    }
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out[offset + i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFFu);
    }
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

struct PendingPageWrite {
    SubtreePagePtr page_ptr = kInvalidSubtreePagePtr;
    std::vector<uint8_t> bytes;
};

void WritePagesBatched(int fd,
                       uint32_t page_size,
                       std::vector<PendingPageWrite>& writes,
                       size_t max_batch_bytes = 128 * 1024,
                       size_t min_batch_bytes = 16 * 1024) {
    if (writes.empty()) {
        return;
    }
    std::sort(writes.begin(), writes.end(),
              [](const PendingPageWrite& lhs, const PendingPageWrite& rhs) {
                  return lhs.page_ptr < rhs.page_ptr;
              });

    const size_t safe_page_size = std::max<size_t>(1, page_size);
    const size_t max_pages_per_batch = std::max<size_t>(1, max_batch_bytes / safe_page_size);
    const size_t min_pages_per_batch = std::max<size_t>(
        1, (min_batch_bytes + safe_page_size - 1) / safe_page_size);

    size_t i = 0;
    while (i < writes.size()) {
        if (writes[i].bytes.size() != page_size) {
            throw std::runtime_error("batched subtree page write size mismatch");
        }

        size_t run_end = i + 1;
        while (run_end < writes.size()) {
            const auto expected_ptr =
                writes[run_end - 1].page_ptr + static_cast<SubtreePagePtr>(page_size);
            if (writes[run_end].page_ptr != expected_ptr) {
                break;
            }
            if (writes[run_end].bytes.size() != page_size) {
                throw std::runtime_error("batched subtree page write size mismatch");
            }
            ++run_end;
        }

        size_t run_pos = i;
        while (run_pos < run_end) {
            const size_t run_remaining = run_end - run_pos;
            size_t group_pages = std::min(run_remaining, max_pages_per_batch);

            // Physical aggregation policy:
            // 1) only aggregate contiguous logical pages (run already guarantees this);
            // 2) aggregated chunk size must be >= min_batch_bytes (default 16KB);
            // 3) prefer multiples of min_pages_per_batch (e.g., 4*4KB=16KB).
            if (run_remaining >= min_pages_per_batch) {
                if (group_pages < min_pages_per_batch) {
                    group_pages = min_pages_per_batch;
                } else {
                    const size_t rounded =
                        (group_pages / min_pages_per_batch) * min_pages_per_batch;
                    if (rounded > 0) {
                        group_pages = rounded;
                    }
                }
            }

            if (group_pages < min_pages_per_batch) {
                // Tail smaller than one physical aggregate target: fall back to per-page writes.
                WriteFullPage(fd, static_cast<off_t>(writes[run_pos].page_ptr), writes[run_pos].bytes);
                ++run_pos;
                continue;
            }

            const size_t total_bytes = group_pages * static_cast<size_t>(page_size);
            std::vector<uint8_t> batch_bytes(total_bytes, 0);
            for (size_t j = 0; j < group_pages; ++j) {
                std::memcpy(batch_bytes.data() + j * page_size,
                            writes[run_pos + j].bytes.data(),
                            page_size);
            }
            const ssize_t ret = pwrite(fd,
                                       batch_bytes.data(),
                                       batch_bytes.size(),
                                       static_cast<off_t>(writes[run_pos].page_ptr));
            if (ret != static_cast<ssize_t>(batch_bytes.size())) {
                throw std::runtime_error("failed batched subtree page write to segment");
            }
            run_pos += group_pages;
        }

        i = run_end;
    }
}

void ValidateHandleBasic(const SubtreePageStoreHandle& handle) {
    if (handle.page_size == 0 || handle.page_size % 4096 != 0) {
        throw std::invalid_argument("invalid subtree page size in store handle");
    }
}

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
        throw std::runtime_error("allocator returned mismatched subtree segment page size");
    }
}

struct AllocatedPageLoc {
    uint32_t segment_id = 0;
    uint32_t page_id = 0;
    SubtreePagePtr page_ptr = kInvalidSubtreePagePtr;
};

AllocatedPageLoc AllocatePageLocation(SegmentAllocator& allocator,
                                      uint32_t page_size,
                                      SortedSegment*& active_segment) {
    while (true) {
        EnsureActiveSegment(allocator, page_size, active_segment);
        const off_t page_offset = active_segment->AllocatePage();
        if (page_offset < 0) {
            CloseActiveSegment(allocator, active_segment);
            continue;
        }
        if (active_segment->segment_id_ > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("subtree page segment id overflow");
        }
        const uint32_t segment_id = static_cast<uint32_t>(active_segment->segment_id_);
        const uint32_t page_id = static_cast<uint32_t>(
            active_segment->TrasformOffsetToPageId(static_cast<size_t>(page_offset)));
        return AllocatedPageLoc{segment_id, page_id, static_cast<SubtreePagePtr>(page_offset)};
    }
}

void DecodeStoredPtr(SubtreePagePtr page_ptr,
                     uint32_t page_size,
                     uint32_t& segment_id_out,
                     uint32_t& page_id_out) {
    if (page_ptr == kInvalidSubtreePagePtr) {
        throw std::invalid_argument("invalid subtree page pointer");
    }
    const uint64_t segment_id_u64 = page_ptr / SEGMENT_SIZE;
    if (segment_id_u64 > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("subtree page pointer segment id overflow");
    }
    const uint64_t in_segment = page_ptr % SEGMENT_SIZE;
    const uint64_t extra_bytes = static_cast<uint64_t>(ComputeExtraPageNum(page_size)) * page_size;
    if (in_segment < extra_bytes) {
        throw std::invalid_argument("subtree page pointer points into segment metadata area");
    }
    const uint64_t payload_off = in_segment - extra_bytes;
    if (payload_off % page_size != 0) {
        throw std::invalid_argument("subtree page pointer is not page aligned");
    }
    const uint64_t page_id_u64 = payload_off / page_size;
    if (page_id_u64 > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("subtree page pointer page id overflow");
    }
    segment_id_out = static_cast<uint32_t>(segment_id_u64);
    page_id_out = static_cast<uint32_t>(page_id_u64);
}

void PatchPagePointersToAbsolute(
    std::vector<uint8_t>& bytes,
    const std::unordered_map<uint32_t, SubtreePagePtr>& logical_to_abs) {
    if (bytes.size() < kPageHeaderSize) {
        throw std::runtime_error("subtree page too small");
    }
    if (ReadU32(bytes, 0) != kPageMagic) {
        throw std::runtime_error("invalid subtree page magic");
    }

    const uint16_t page_type = ReadU16(bytes, 6);
    const uint32_t item_count = ReadU32(bytes, 12);
    size_t offset = kPageHeaderSize + EncodedSuffixSize();
    if (page_type == kLeafType) {
        const uint64_t prev = ReadU64(bytes, offset);
        const uint64_t next = ReadU64(bytes, offset + sizeof(uint64_t));
        const auto rewrite = [&](uint64_t logical) -> uint64_t {
            if (logical == kInvalidSubtreePagePtr) {
                return kInvalidSubtreePagePtr;
            }
            if (logical > std::numeric_limits<uint32_t>::max()) {
                throw std::runtime_error("leaf pointer logical id overflow");
            }
            const auto it = logical_to_abs.find(static_cast<uint32_t>(logical));
            if (it == logical_to_abs.end()) {
                throw std::runtime_error("leaf pointer references missing logical page id");
            }
            return it->second;
        };
        WriteU64(bytes, offset, rewrite(prev));
        WriteU64(bytes, offset + sizeof(uint64_t), rewrite(next));
        return;
    }
    if (page_type != kInternalType) {
        throw std::runtime_error("unknown subtree page type");
    }

    for (uint32_t i = 0; i < item_count; ++i) {
        const uint64_t logical_child = ReadU64(bytes, offset);
        if (logical_child == kInvalidSubtreePagePtr ||
            logical_child > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("invalid logical child pointer in internal page");
        }
        const auto it = logical_to_abs.find(static_cast<uint32_t>(logical_child));
        if (it == logical_to_abs.end()) {
            throw std::runtime_error("internal page child references missing logical page id");
        }
        WriteU64(bytes, offset, it->second);
        offset += sizeof(uint64_t) + EncodedSuffixSize();
    }
}

void ConvertPagePointersToLogical(
    std::vector<uint8_t>& bytes,
    const std::unordered_map<SubtreePagePtr, uint32_t>& abs_to_logical) {
    if (bytes.size() < kPageHeaderSize) {
        throw std::runtime_error("subtree page too small");
    }
    if (ReadU32(bytes, 0) != kPageMagic) {
        throw std::runtime_error("invalid subtree page magic");
    }

    const uint16_t page_type = ReadU16(bytes, 6);
    const uint32_t item_count = ReadU32(bytes, 12);
    size_t offset = kPageHeaderSize + EncodedSuffixSize();
    if (page_type == kLeafType) {
        const uint64_t prev = ReadU64(bytes, offset);
        const uint64_t next = ReadU64(bytes, offset + sizeof(uint64_t));
        const auto rewrite = [&](uint64_t ptr) -> uint64_t {
            if (ptr == kInvalidSubtreePagePtr) {
                return kInvalidSubtreePagePtr;
            }
            const auto it = abs_to_logical.find(ptr);
            if (it == abs_to_logical.end()) {
                throw std::runtime_error("leaf pointer references unknown physical page");
            }
            return it->second;
        };
        WriteU64(bytes, offset, rewrite(prev));
        WriteU64(bytes, offset + sizeof(uint64_t), rewrite(next));
        return;
    }
    if (page_type != kInternalType) {
        throw std::runtime_error("unknown subtree page type");
    }
    for (uint32_t i = 0; i < item_count; ++i) {
        const uint64_t child_ptr = ReadU64(bytes, offset);
        if (child_ptr == kInvalidSubtreePagePtr) {
            throw std::runtime_error("invalid internal child physical pointer");
        }
        const auto it = abs_to_logical.find(child_ptr);
        if (it == abs_to_logical.end()) {
            throw std::runtime_error("internal child pointer references unknown physical page");
        }
        WriteU64(bytes, offset, it->second);
        offset += sizeof(uint64_t) + EncodedSuffixSize();
    }
}

uint32_t PageIdFromPageBytes(const std::vector<uint8_t>& bytes) {
    if (bytes.size() < 12) {
        throw std::runtime_error("subtree page too small to read page id");
    }
    return ReadU32(bytes, 8);
}

void CollectReachableSubtreePages(
    SegmentAllocator& allocator,
    uint32_t page_size,
    SubtreePagePtr root_page_ptr,
    std::unordered_map<SubtreePagePtr, std::vector<uint8_t>>& page_bytes_out) {
    page_bytes_out.clear();
    if (root_page_ptr == kInvalidSubtreePagePtr) {
        return;
    }

    std::vector<SubtreePagePtr> stack;
    stack.push_back(root_page_ptr);
    while (!stack.empty()) {
        const SubtreePagePtr ptr = stack.back();
        stack.pop_back();
        if (ptr == kInvalidSubtreePagePtr) {
            continue;
        }
        if (page_bytes_out.find(ptr) != page_bytes_out.end()) {
            continue;
        }
        auto page = SubtreePageStore::LoadPageByPtr(allocator, page_size, ptr);
        SubtreeDecodedInternalPage internal_page;
        if (SubtreePageCodec::TryDecodeInternalPage(page, internal_page)) {
            for (const auto child_ptr : internal_page.child_page_ptrs) {
                if (child_ptr != kInvalidSubtreePagePtr) {
                    stack.push_back(child_ptr);
                }
            }
        }
        page_bytes_out.emplace(ptr, std::move(page));
    }
}

void CollectAllStoredPointers(SegmentAllocator& allocator,
                              const SubtreePageStoreHandle& handle,
                              std::unordered_set<SubtreePagePtr>& pointers_out) {
    pointers_out.clear();
    if (handle.page_size == 0) {
        return;
    }
    if (handle.manifest_page_ptr != kInvalidSubtreePagePtr) {
        pointers_out.insert(handle.manifest_page_ptr);
    }
    if (handle.root_page_ptr == kInvalidSubtreePagePtr) {
        return;
    }

    std::unordered_map<SubtreePagePtr, std::vector<uint8_t>> pages;
    CollectReachableSubtreePages(allocator, handle.page_size, handle.root_page_ptr, pages);
    for (const auto& [ptr, _] : pages) {
        (void)_;
        pointers_out.insert(ptr);
    }
}

void RecyclePagePointers(SegmentAllocator& allocator,
                         uint16_t page_size,
                         const std::unordered_set<SubtreePagePtr>& ptrs) {
    if (ptrs.empty()) {
        return;
    }
    std::map<uint32_t, std::vector<uint32_t>> pages_by_segment;
    for (const auto ptr : ptrs) {
        uint32_t segment_id = 0;
        uint32_t page_id = 0;
        DecodeStoredPtr(ptr, page_size, segment_id, page_id);
        pages_by_segment[segment_id].push_back(page_id);
    }
    for (auto& [segment_id, page_ids] : pages_by_segment) {
        SortedSegment* segment =
            allocator.GetSortedSegmentForDelete(static_cast<size_t>(segment_id), page_size);
        if (segment == nullptr) {
            continue;
        }
        for (const auto page_id : page_ids) {
            segment->RecyclePage(page_id);
        }
        allocator.CloseSegmentForDelete(segment);
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
    if (manifest.page_size > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument("subtree page size overflow");
    }

    SubtreePageStoreHandle handle;
    handle.page_size = static_cast<uint16_t>(manifest.page_size);
    handle.page_count = manifest.page_count;
    handle.record_count = manifest.record_count;

    if (page_set.pages.empty()) {
        handle.root_page_ptr = kInvalidSubtreePagePtr;
        handle.manifest_page_ptr = kInvalidSubtreePagePtr;
        handle.flags |= SubtreePageStoreHandle::kQueryMetaValid;
        return handle;
    }

    std::unordered_map<uint32_t, AllocatedPageLoc> logical_to_loc;
    logical_to_loc.reserve(page_set.pages.size());
    std::vector<SubtreePagePtr> allocated_ptrs;
    allocated_ptrs.reserve(page_set.pages.size() + 1);
    SortedSegment* active_segment = nullptr;

    try {
        const auto manifest_loc =
            AllocatePageLocation(allocator, manifest.page_size, active_segment);
        handle.manifest_page_ptr = manifest_loc.page_ptr;
        allocated_ptrs.push_back(manifest_loc.page_ptr);

        for (const auto& page : page_set.pages) {
            if (page.bytes.size() != manifest.page_size) {
                throw std::invalid_argument("subtree page size mismatch during persist");
            }
            const auto loc = AllocatePageLocation(allocator, manifest.page_size, active_segment);
            allocated_ptrs.push_back(loc.page_ptr);
            logical_to_loc.emplace(page.page_id, loc);
        }
        CloseActiveSegment(allocator, active_segment);

        std::unordered_map<uint32_t, SubtreePagePtr> logical_to_abs;
        logical_to_abs.reserve(logical_to_loc.size());
        for (const auto& [logical_id, loc] : logical_to_loc) {
            logical_to_abs.emplace(logical_id, loc.page_ptr);
        }
        const auto root_it = logical_to_abs.find(manifest.root_page_id);
        if (root_it == logical_to_abs.end()) {
            throw std::runtime_error("missing root logical page id during persist");
        }
        handle.SetQueryMeta(root_it->second, manifest.page_count, manifest.record_count);

        std::vector<PendingPageWrite> pending_writes;
        pending_writes.reserve(page_set.pages.size() + 1);

        std::vector<uint8_t> manifest_page(manifest.page_size, 0);
        std::memcpy(manifest_page.data(), page_set.manifest.data(), page_set.manifest.size());
        pending_writes.push_back(PendingPageWrite{handle.manifest_page_ptr, std::move(manifest_page)});

        for (const auto& page : page_set.pages) {
            auto bytes = page.bytes;
            PatchPagePointersToAbsolute(bytes, logical_to_abs);
            const auto& loc = logical_to_loc.at(page.page_id);
            pending_writes.push_back(PendingPageWrite{loc.page_ptr, std::move(bytes)});
        }
        WritePagesBatched(allocator.Getfd(), manifest.page_size, pending_writes);
    } catch (...) {
        CloseActiveSegment(allocator, active_segment);
        std::unordered_set<SubtreePagePtr> cleanup(allocated_ptrs.begin(), allocated_ptrs.end());
        try {
            RecyclePagePointers(allocator, handle.page_size, cleanup);
        } catch (...) {
        }
        throw;
    }

    return handle;
}

SubtreePageStoreHandle SubtreePageStore::PersistCow(SegmentAllocator& allocator,
                                                    const SubtreePageStoreHandle& base_handle,
                                                    const SubtreePageSet& base_page_set,
                                                    const SubtreePageSet& target_page_set,
                                                    SubtreeCowStats* stats_out) {
    ValidateHandleBasic(base_handle);
    if (!SubtreePageCodec::Validate(base_page_set) || !SubtreePageCodec::Validate(target_page_set)) {
        throw std::invalid_argument("PersistCow requires valid base/target page sets");
    }

    const SubtreePageManifest base_manifest = SubtreePageCodec::DecodeManifest(base_page_set.manifest);
    const SubtreePageManifest target_manifest = SubtreePageCodec::DecodeManifest(target_page_set.manifest);
    if (base_manifest.page_size != target_manifest.page_size ||
        base_manifest.page_size != base_handle.page_size) {
        throw std::invalid_argument("PersistCow page size mismatch");
    }
    if (target_manifest.page_size > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument("PersistCow page size overflow");
    }

    SubtreePageStoreHandle handle;
    handle.page_size = static_cast<uint16_t>(target_manifest.page_size);
    handle.page_count = target_manifest.page_count;
    handle.record_count = target_manifest.record_count;
    if (target_page_set.pages.empty()) {
        handle.root_page_ptr = kInvalidSubtreePagePtr;
        handle.manifest_page_ptr = kInvalidSubtreePagePtr;
        handle.flags |= SubtreePageStoreHandle::kQueryMetaValid;
        return handle;
    }

    std::unordered_map<uint32_t, std::vector<uint8_t>> base_page_bytes_by_id;
    base_page_bytes_by_id.reserve(base_page_set.pages.size());
    for (const auto& page : base_page_set.pages) {
        base_page_bytes_by_id.emplace(page.page_id, page.bytes);
    }

    std::unordered_map<uint32_t, SubtreePagePtr> base_logical_to_ptr;
    {
        std::unordered_map<SubtreePagePtr, std::vector<uint8_t>> base_disk_pages;
        CollectReachableSubtreePages(
            allocator, base_handle.page_size, base_handle.root_page_ptr, base_disk_pages);
        base_logical_to_ptr.reserve(base_disk_pages.size());
        for (const auto& [ptr, bytes] : base_disk_pages) {
            base_logical_to_ptr.emplace(PageIdFromPageBytes(bytes), ptr);
        }
    }

    std::unordered_map<uint32_t, SubtreePagePtr> target_logical_to_ptr;
    target_logical_to_ptr.reserve(target_page_set.pages.size());
    std::unordered_map<uint32_t, SubtreePagePtr> newly_allocated_target_pages;
    newly_allocated_target_pages.reserve(target_page_set.pages.size());
    std::vector<SubtreePagePtr> newly_allocated_ptrs;
    newly_allocated_ptrs.reserve(target_page_set.pages.size() + 1);
    SortedSegment* active_segment = nullptr;

    uint64_t reused_pages = 0;
    uint64_t written_pages = 0;

    try {
        for (const auto& target_page : target_page_set.pages) {
            bool reused = false;
            const auto base_bytes_it = base_page_bytes_by_id.find(target_page.page_id);
            const auto base_ptr_it = base_logical_to_ptr.find(target_page.page_id);
            if (base_bytes_it != base_page_bytes_by_id.end() &&
                base_ptr_it != base_logical_to_ptr.end() &&
                base_bytes_it->second == target_page.bytes) {
                target_logical_to_ptr.emplace(target_page.page_id, base_ptr_it->second);
                reused = true;
                ++reused_pages;
            }
            if (!reused) {
                const auto loc = AllocatePageLocation(allocator, target_manifest.page_size, active_segment);
                target_logical_to_ptr.emplace(target_page.page_id, loc.page_ptr);
                newly_allocated_target_pages.emplace(target_page.page_id, loc.page_ptr);
                newly_allocated_ptrs.push_back(loc.page_ptr);
                ++written_pages;
            }
        }

        const auto root_it = target_logical_to_ptr.find(target_manifest.root_page_id);
        if (root_it == target_logical_to_ptr.end()) {
            throw std::runtime_error("PersistCow missing target root logical page id");
        }
        handle.SetQueryMeta(root_it->second, target_manifest.page_count, target_manifest.record_count);

        std::vector<PendingPageWrite> pending_writes;
        pending_writes.reserve(newly_allocated_target_pages.size() + 1);

        if (base_page_set.manifest == target_page_set.manifest &&
            base_handle.manifest_page_ptr != kInvalidSubtreePagePtr) {
            handle.manifest_page_ptr = base_handle.manifest_page_ptr;
            ++reused_pages;
        } else {
            const auto manifest_loc =
                AllocatePageLocation(allocator, target_manifest.page_size, active_segment);
            handle.manifest_page_ptr = manifest_loc.page_ptr;
            newly_allocated_ptrs.push_back(manifest_loc.page_ptr);
            ++written_pages;
            std::vector<uint8_t> target_manifest_page(target_manifest.page_size, 0);
            std::memcpy(target_manifest_page.data(),
                        target_page_set.manifest.data(),
                        target_page_set.manifest.size());
            pending_writes.push_back(
                PendingPageWrite{handle.manifest_page_ptr, std::move(target_manifest_page)});
        }

        for (const auto& target_page : target_page_set.pages) {
            const auto new_it = newly_allocated_target_pages.find(target_page.page_id);
            if (new_it == newly_allocated_target_pages.end()) {
                continue;
            }
            auto bytes = target_page.bytes;
            PatchPagePointersToAbsolute(bytes, target_logical_to_ptr);
            pending_writes.push_back(PendingPageWrite{new_it->second, std::move(bytes)});
        }
        WritePagesBatched(allocator.Getfd(), target_manifest.page_size, pending_writes);
        CloseActiveSegment(allocator, active_segment);
    } catch (...) {
        CloseActiveSegment(allocator, active_segment);
        std::unordered_set<SubtreePagePtr> cleanup(newly_allocated_ptrs.begin(), newly_allocated_ptrs.end());
        try {
            RecyclePagePointers(allocator, handle.page_size, cleanup);
        } catch (...) {
        }
        throw;
    }

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

std::vector<uint8_t> SubtreePageStore::LoadManifestPage(SegmentAllocator& allocator,
                                                        const SubtreePageStoreHandle& handle) {
    ValidateHandleBasic(handle);
    if (handle.manifest_page_ptr == kInvalidSubtreePagePtr) {
        throw std::invalid_argument("subtree handle has no manifest page");
    }
    return LoadPageByPtr(allocator, handle.page_size, handle.manifest_page_ptr);
}

std::vector<uint8_t> SubtreePageStore::LoadPageByPtr(SegmentAllocator& allocator,
                                                     uint32_t page_size,
                                                     SubtreePagePtr page_ptr) {
    if (page_size == 0 || page_size % 4096 != 0) {
        throw std::invalid_argument("invalid page size for LoadPageByPtr");
    }
    if (page_ptr == kInvalidSubtreePagePtr) {
        throw std::invalid_argument("invalid page pointer for LoadPageByPtr");
    }
    std::vector<uint8_t> page(page_size, 0);
    ReadFullPage(allocator.Getfd(), static_cast<off_t>(page_ptr), page);
    ++g_tls_read_pages;
    return page;
}

std::vector<uint8_t> SubtreePageStore::LoadPageByPtr(SegmentAllocator& allocator,
                                                     const SubtreePageStoreHandle& handle,
                                                     SubtreePagePtr page_ptr) {
    ValidateHandleBasic(handle);
    return LoadPageByPtr(allocator, handle.page_size, page_ptr);
}

std::vector<SubtreePagePtr> SubtreePageStore::PersistOpaquePages(
    SegmentAllocator& allocator,
    uint32_t page_size,
    const std::vector<std::vector<uint8_t>>& pages) {
    if (page_size == 0 || page_size % 4096 != 0) {
        throw std::invalid_argument("invalid page size for PersistOpaquePages");
    }
    if (pages.empty()) {
        return {};
    }

    std::vector<SubtreePagePtr> page_ptrs;
    page_ptrs.reserve(pages.size());
    std::vector<PendingPageWrite> pending_writes;
    pending_writes.reserve(pages.size());
    SortedSegment* active_segment = nullptr;
    try {
        for (const auto& page : pages) {
            if (page.size() != page_size) {
                throw std::invalid_argument("opaque page size mismatch during persist");
            }
            const auto loc = AllocatePageLocation(allocator, page_size, active_segment);
            page_ptrs.push_back(loc.page_ptr);
            pending_writes.push_back(PendingPageWrite{loc.page_ptr, page});
        }
        WritePagesBatched(allocator.Getfd(), page_size, pending_writes);
        CloseActiveSegment(allocator, active_segment);
        return page_ptrs;
    } catch (...) {
        CloseActiveSegment(allocator, active_segment);
        try {
            std::unordered_set<SubtreePagePtr> cleanup(page_ptrs.begin(), page_ptrs.end());
            RecyclePagePointers(allocator, static_cast<uint16_t>(page_size), cleanup);
        } catch (...) {
        }
        throw;
    }
}

void SubtreePageStore::DestroyOpaquePages(SegmentAllocator& allocator,
                                          uint32_t page_size,
                                          const std::vector<SubtreePagePtr>& page_ptrs) {
    if (page_size == 0 || page_size % 4096 != 0) {
        throw std::invalid_argument("invalid page size for DestroyOpaquePages");
    }
    if (page_ptrs.empty()) {
        return;
    }
    std::unordered_set<SubtreePagePtr> ptrs(page_ptrs.begin(), page_ptrs.end());
    RecyclePagePointers(allocator, static_cast<uint16_t>(page_size), ptrs);
}

SubtreePageSet SubtreePageStore::Load(SegmentAllocator& allocator, const SubtreePageStoreHandle& handle) {
    ValidateHandleBasic(handle);
    SubtreePageSet page_set;
    if (handle.page_count == 0) {
        return page_set;
    }

    const auto manifest_page = LoadManifestPage(allocator, handle);
    page_set.manifest.assign(
        manifest_page.begin(), manifest_page.begin() + SubtreePageCodec::kManifestSize);
    const auto manifest = SubtreePageCodec::DecodeManifest(page_set.manifest);

    std::unordered_map<SubtreePagePtr, std::vector<uint8_t>> disk_pages;
    CollectReachableSubtreePages(allocator, handle.page_size, handle.root_page_ptr, disk_pages);
    if (disk_pages.size() != manifest.page_count) {
        throw std::runtime_error("loaded subtree page count mismatch");
    }

    std::unordered_map<SubtreePagePtr, uint32_t> abs_to_logical;
    abs_to_logical.reserve(disk_pages.size());
    for (const auto& [ptr, bytes] : disk_pages) {
        abs_to_logical.emplace(ptr, PageIdFromPageBytes(bytes));
    }

    page_set.pages.reserve(disk_pages.size());
    for (auto& [ptr, bytes] : disk_pages) {
        (void)ptr;
        SubtreePage page;
        page.page_id = PageIdFromPageBytes(bytes);
        ConvertPagePointersToLogical(bytes, abs_to_logical);
        page.bytes = std::move(bytes);
        page_set.pages.push_back(std::move(page));
    }
    std::sort(page_set.pages.begin(),
              page_set.pages.end(),
              [](const SubtreePage& lhs, const SubtreePage& rhs) {
                  return lhs.page_id < rhs.page_id;
              });
    if (!SubtreePageCodec::Validate(page_set)) {
        throw std::runtime_error("loaded subtree page set failed validation");
    }
    return page_set;
}

void SubtreePageStore::Destroy(SegmentAllocator& allocator, const SubtreePageStoreHandle& handle) {
    ValidateHandleBasic(handle);
    std::unordered_set<SubtreePagePtr> ptrs;
    CollectAllStoredPointers(allocator, handle, ptrs);
    RecyclePagePointers(allocator, handle.page_size, ptrs);
}

void SubtreePageStore::DestroyUnshared(SegmentAllocator& allocator,
                                       const SubtreePageStoreHandle& old_handle,
                                       const SubtreePageStoreHandle& keep_handle) {
    ValidateHandleBasic(old_handle);
    ValidateHandleBasic(keep_handle);
    if (old_handle.page_size != keep_handle.page_size) {
        throw std::invalid_argument("DestroyUnshared page size mismatch");
    }

    std::unordered_set<SubtreePagePtr> old_ptrs;
    std::unordered_set<SubtreePagePtr> keep_ptrs;
    CollectAllStoredPointers(allocator, old_handle, old_ptrs);
    CollectAllStoredPointers(allocator, keep_handle, keep_ptrs);

    std::unordered_set<SubtreePagePtr> reclaim;
    reclaim.reserve(old_ptrs.size());
    for (const auto ptr : old_ptrs) {
        if (keep_ptrs.find(ptr) == keep_ptrs.end()) {
            reclaim.insert(ptr);
        }
    }
    RecyclePagePointers(allocator, old_handle.page_size, reclaim);
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
