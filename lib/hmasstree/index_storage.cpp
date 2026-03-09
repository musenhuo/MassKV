/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * IndexStorageManager implementation
 */
#include "index_storage.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <ctime>

namespace Masstree {

// ========== Reserved Pages ==========
// Page 0: Superblock (256 bytes used, rest padding)
// Pages 1-16: Metadata region (segment bitmaps)
// Pages 17+: Data pages

static constexpr uint64_t SUPERBLOCK_PAGE = 0;
static constexpr uint64_t METADATA_START_PAGE = 1;
static constexpr uint64_t METADATA_PAGES = 16;  // 64KB for metadata
static constexpr uint64_t DATA_START_PAGE = METADATA_START_PAGE + METADATA_PAGES;

// Number of SegmentMetadata entries per page
static constexpr size_t SEGMENT_META_PER_PAGE = IndexSegment::PAGE_SIZE / sizeof(SegmentMetadata);

IndexStorageManager::IndexStorageManager(const IndexStorageConfig& config)
    : config_(config) {
    superblock_.init();
}

IndexStorageManager::~IndexStorageManager() {
    close();
}

bool IndexStorageManager::open() {
    if (fd_ >= 0) {
        return true;  // Already open
    }

    int flags = O_RDWR | O_CREAT;
    if (config_.use_direct_io) {
        flags |= O_DIRECT;
    }

    fd_ = ::open(config_.storage_path.c_str(), flags, 0666);
    if (fd_ < 0) {
        return false;
    }

    if (config_.recover) {
        return recover();
    }

    // Initialize new storage
    superblock_.init();
    superblock_.metadata_region_start = METADATA_START_PAGE;
    superblock_.metadata_region_size = METADATA_PAGES;
    superblock_.next_page_id = DATA_START_PAGE;
    superblock_.update_checksum();
    superblock_dirty_ = true;

    // Initialize first data segment
    next_page_id_.store(DATA_START_PAGE);
    
    {
        std::unique_lock<std::shared_mutex> lock(segments_mutex_);
        auto segment = std::make_unique<IndexSegment>(0);
        
        // Reserve pages 0 - (DATA_START_PAGE-1) for superblock and metadata
        for (uint16_t i = 0; i < DATA_START_PAGE; ++i) {
            segment->page_bitmap[i] = true;
            segment->allocated_count++;
            // Remove from free list
            if (!segment->free_pages.empty()) {
                segment->free_pages.pop();
            }
        }
        
        // Rebuild free list for remaining pages
        std::queue<uint16_t> new_free;
        for (uint16_t i = DATA_START_PAGE; i < IndexSegment::PAGES_PER_SEGMENT; ++i) {
            if (!segment->page_bitmap[i]) {
                new_free.push(i);
            }
        }
        segment->free_pages = std::move(new_free);
        
        segments_.push_back(std::move(segment));
        segments_with_space_.push(0);
    }

    // Persist initial superblock
    persist_metadata();

    return true;
}

void IndexStorageManager::close() {
    if (fd_ >= 0) {
        persist_metadata();
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

// ========== Node Allocation ==========

std::pair<uint64_t, uint8_t> IndexStorageManager::allocate_node_slot(NodeType type) {
    uint8_t type_key = static_cast<uint8_t>(type);

    // First, try to reuse a free slot
    {
        std::lock_guard<std::mutex> lock(free_slots_mutex_);
        auto it = free_slots_.find(type_key);
        if (it != free_slots_.end() && !it->second.empty()) {
            FreeSlot slot = it->second.front();
            it->second.pop();
            return {slot.page_id, slot.slot_index};
        }
    }

    // No free slot, allocate a new page and use slot 0
    uint64_t page_id = allocate_page();
    if (page_id == 0) {
        return {0, 0};
    }

    // Add remaining slots (1-7) to free list
    {
        std::lock_guard<std::mutex> lock(free_slots_mutex_);
        auto& free_queue = free_slots_[type_key];
        for (uint8_t i = 1; i < PackedPage::NODES_PER_PAGE; ++i) {
            free_queue.push({page_id, i});
        }
    }

    return {page_id, 0};
}

void IndexStorageManager::free_node_slot(NodeHandle handle) {
    if (handle.is_null()) return;

    uint8_t type_key = static_cast<uint8_t>(handle.type());
    
    std::lock_guard<std::mutex> lock(free_slots_mutex_);
    free_slots_[type_key].push({handle.page_id(), handle.slot_index()});
}

uint64_t IndexStorageManager::allocate_page() {
    std::unique_lock<std::shared_mutex> lock(segments_mutex_);

    // Find a segment with free pages
    IndexSegment* segment = find_segment_with_space();
    if (!segment) {
        // Create new segment
        uint64_t new_segment_id = segments_.size();
        segment = ensure_segment(new_segment_id);
        if (!segment) {
            return 0;
        }
    }

    // Allocate page within segment
    uint16_t page_index = segment->allocate_page();
    if (page_index == UINT16_MAX) {
        return 0;
    }

    // Calculate global page ID
    uint64_t page_id = segment->segment_id * IndexSegment::PAGES_PER_SEGMENT + page_index;

    // Update segments_with_space if segment is now full
    if (segment->is_full()) {
        // Remove from available list - handled lazily in find_segment_with_space
    }

    total_pages_allocated_.fetch_add(1, std::memory_order_relaxed);
    
    // Update next_page_id if needed
    uint64_t expected = next_page_id_.load();
    while (page_id >= expected) {
        if (next_page_id_.compare_exchange_weak(expected, page_id + 1)) {
            break;
        }
    }

    return page_id;
}

void IndexStorageManager::free_page(uint64_t page_id) {
    if (page_id == 0) return;

    uint64_t segment_id = page_to_segment(page_id);
    uint16_t page_index = page_index_in_segment(page_id);

    std::unique_lock<std::shared_mutex> lock(segments_mutex_);

    if (segment_id >= segments_.size() || !segments_[segment_id]) {
        return;
    }

    IndexSegment* segment = segments_[segment_id].get();
    bool was_full = segment->is_full();

    segment->free_page(page_index);

    // If segment was full and now has space, add to available list
    if (was_full && !segment->is_full()) {
        segments_with_space_.push(segment_id);
    }

    total_pages_freed_.fetch_add(1, std::memory_order_relaxed);
}

// ========== I/O Operations ==========

bool IndexStorageManager::read_page(uint64_t page_id, void* buffer) {
    if (fd_ < 0 || !buffer) return false;

    off_t offset = page_offset(page_id);
    ssize_t bytes = ::pread(fd_, buffer, IndexSegment::PAGE_SIZE, offset);

    if (bytes == static_cast<ssize_t>(IndexSegment::PAGE_SIZE)) {
        total_bytes_read_.fetch_add(bytes, std::memory_order_relaxed);
        return true;
    }

    // Page might not exist yet (new allocation)
    if (bytes < 0 && errno == EINVAL) {
        // This can happen with O_DIRECT if file is shorter than offset
        // Return false to indicate page doesn't exist
        return false;
    }

    // Partial read or EOF - treat as non-existent page
    return false;
}

bool IndexStorageManager::write_page(uint64_t page_id, const void* buffer) {
    if (fd_ < 0 || !buffer) return false;

    off_t offset = page_offset(page_id);
    
    ssize_t bytes = ::pwrite(fd_, buffer, IndexSegment::PAGE_SIZE, offset);

    if (bytes == static_cast<ssize_t>(IndexSegment::PAGE_SIZE)) {
        total_bytes_written_.fetch_add(bytes, std::memory_order_relaxed);
        return true;
    }

    return false;
}

void IndexStorageManager::sync() {
    if (fd_ >= 0) {
        ::fsync(fd_);
    }
}

// ========== Recovery ==========

bool IndexStorageManager::recover() {
    if (fd_ < 0) return false;

    // Get file size
    struct stat st;
    if (::fstat(fd_, &st) < 0) {
        return false;
    }

    off_t file_size = st.st_size;
    if (file_size == 0) {
        // Empty file, initialize fresh (same as open() non-recover path)
        superblock_.init();
        superblock_.metadata_region_start = METADATA_START_PAGE;
        superblock_.metadata_region_size = METADATA_PAGES;
        superblock_.next_page_id = DATA_START_PAGE;
        superblock_.update_checksum();
        superblock_dirty_ = true;
        
        next_page_id_.store(DATA_START_PAGE);
        
        // Create segment 0 with properly reserved metadata pages
        {
            std::unique_lock<std::shared_mutex> lock(segments_mutex_);
            auto segment = std::make_unique<IndexSegment>(0);
            
            // Reserve pages 0 - (DATA_START_PAGE-1) for superblock and metadata
            for (uint16_t i = 0; i < DATA_START_PAGE; ++i) {
                segment->page_bitmap[i] = true;
                segment->allocated_count++;
            }
            
            // Rebuild free list to exclude reserved pages
            std::queue<uint16_t> new_free;
            for (uint16_t i = DATA_START_PAGE; i < IndexSegment::PAGES_PER_SEGMENT; ++i) {
                new_free.push(i);
            }
            segment->free_pages = std::move(new_free);
            
            segments_.push_back(std::move(segment));
            segments_with_space_.push(0);
        }
        
        persist_metadata();
        return true;
    }

    // Read superblock from page 0
    alignas(4096) uint8_t sb_buffer[IndexSegment::PAGE_SIZE];
    ssize_t bytes = ::pread(fd_, sb_buffer, IndexSegment::PAGE_SIZE, 0);
    if (bytes != static_cast<ssize_t>(IndexSegment::PAGE_SIZE)) {
        return false;
    }

    // Copy superblock
    std::memcpy(&superblock_, sb_buffer, sizeof(IndexSuperblock));

    // Validate superblock
    if (!superblock_.is_valid()) {
        // Invalid magic - might be old format, try to recover conservatively
        return recover_without_metadata(file_size);
    }

    if (!superblock_.verify_checksum()) {
        // Checksum mismatch - try recovery
        return recover_without_metadata(file_size);
    }

    // Restore state from superblock
    next_page_id_.store(superblock_.next_page_id);
    total_pages_allocated_.store(superblock_.total_pages_allocated);
    total_pages_freed_.store(superblock_.total_pages_freed);

    // Read segment metadata
    std::unique_lock<std::shared_mutex> lock(segments_mutex_);

    size_t num_segments = superblock_.total_segments;
    if (num_segments == 0) {
        // No segments yet, initialize first one
        ensure_segment(0);
        return true;
    }

    // Read segment metadata from metadata region
    alignas(4096) uint8_t meta_buffer[IndexSegment::PAGE_SIZE];
    size_t segments_read = 0;

    for (uint64_t meta_page = 0; meta_page < METADATA_PAGES && segments_read < num_segments; ++meta_page) {
        off_t offset = (METADATA_START_PAGE + meta_page) * IndexSegment::PAGE_SIZE;
        bytes = ::pread(fd_, meta_buffer, IndexSegment::PAGE_SIZE, offset);
        if (bytes != static_cast<ssize_t>(IndexSegment::PAGE_SIZE)) {
            break;
        }

        // Parse segment metadata entries from this page
        SegmentMetadata* metas = reinterpret_cast<SegmentMetadata*>(meta_buffer);
        for (size_t i = 0; i < SEGMENT_META_PER_PAGE && segments_read < num_segments; ++i) {
            SegmentMetadata& meta = metas[i];
            
            // Create segment and restore bitmap
            auto segment = std::make_unique<IndexSegment>(meta.segment_id);
            segment->allocated_count = meta.allocated_count;
            
            // Restore page bitmap and free list
            std::queue<uint16_t> empty_queue;
            std::swap(segment->free_pages, empty_queue);  // Clear default free list
            
            for (uint16_t p = 0; p < IndexSegment::PAGES_PER_SEGMENT; ++p) {
                bool allocated = meta.is_page_set(p);
                segment->page_bitmap[p] = allocated;
                if (!allocated) {
                    segment->free_pages.push(p);
                }
            }
            
            // Grow segments vector if needed
            while (segments_.size() <= meta.segment_id) {
                segments_.push_back(nullptr);
            }
            segments_[meta.segment_id] = std::move(segment);
            
            // Track segment with space
            if (!segments_[meta.segment_id]->is_full()) {
                segments_with_space_.push(meta.segment_id);
            }
            
            segments_read++;
        }
    }

    superblock_dirty_ = false;
    return true;
}

bool IndexStorageManager::recover_without_metadata(off_t file_size) {
    // Fallback: conservative recovery without metadata
    // Assume all existing pages are allocated
    
    size_t num_pages = file_size / IndexSegment::PAGE_SIZE;
    size_t num_segments = (num_pages + IndexSegment::PAGES_PER_SEGMENT - 1) / IndexSegment::PAGES_PER_SEGMENT;

    std::unique_lock<std::shared_mutex> lock(segments_mutex_);

    for (size_t i = 0; i < num_segments; ++i) {
        auto segment = std::make_unique<IndexSegment>(i);
        
        // Mark all pages as allocated (conservative)
        std::queue<uint16_t> empty_queue;
        std::swap(segment->free_pages, empty_queue);
        
        for (uint16_t p = 0; p < IndexSegment::PAGES_PER_SEGMENT; ++p) {
            segment->page_bitmap[p] = true;
        }
        segment->allocated_count = IndexSegment::PAGES_PER_SEGMENT;
        
        segments_.push_back(std::move(segment));
    }

    // Update state
    next_page_id_.store(num_pages > DATA_START_PAGE ? num_pages : DATA_START_PAGE);
    total_pages_allocated_.store(num_pages > DATA_START_PAGE ? num_pages - DATA_START_PAGE : 0);

    // Initialize superblock
    superblock_.init();
    superblock_.total_segments = num_segments;
    superblock_.next_page_id = next_page_id_.load();
    superblock_.total_pages_allocated = total_pages_allocated_.load();
    superblock_dirty_ = true;

    return true;
}

void IndexStorageManager::persist_metadata() {
    if (fd_ < 0) return;

    // Update superblock with current state
    {
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        superblock_.total_segments = segments_.size();
    }
    superblock_.next_page_id = next_page_id_.load();
    superblock_.total_pages_allocated = total_pages_allocated_.load();
    superblock_.total_pages_freed = total_pages_freed_.load();
    superblock_.last_checkpoint_time = static_cast<uint64_t>(std::time(nullptr));
    superblock_.update_checksum();

    // Write superblock to page 0
    alignas(4096) uint8_t sb_buffer[IndexSegment::PAGE_SIZE];
    std::memset(sb_buffer, 0, IndexSegment::PAGE_SIZE);
    std::memcpy(sb_buffer, &superblock_, sizeof(IndexSuperblock));
    
    ::pwrite(fd_, sb_buffer, IndexSegment::PAGE_SIZE, 0);

    // Write segment metadata to metadata region
    std::shared_lock<std::shared_mutex> lock(segments_mutex_);

    alignas(4096) uint8_t meta_buffer[IndexSegment::PAGE_SIZE];
    size_t segments_written = 0;
    size_t total_segments = segments_.size();

    for (uint64_t meta_page = 0; meta_page < METADATA_PAGES && segments_written < total_segments; ++meta_page) {
        std::memset(meta_buffer, 0, IndexSegment::PAGE_SIZE);
        SegmentMetadata* metas = reinterpret_cast<SegmentMetadata*>(meta_buffer);

        for (size_t i = 0; i < SEGMENT_META_PER_PAGE && segments_written < total_segments; ++i) {
            if (segments_written < segments_.size() && segments_[segments_written]) {
                IndexSegment* seg = segments_[segments_written].get();
                SegmentMetadata& meta = metas[i];
                
                meta.init(seg->segment_id);
                meta.allocated_count = seg->allocated_count;
                
                // Copy page bitmap
                for (uint16_t p = 0; p < IndexSegment::PAGES_PER_SEGMENT; ++p) {
                    if (seg->page_bitmap[p]) {
                        meta.set_page(p);
                    }
                }
            }
            segments_written++;
        }

        off_t offset = (METADATA_START_PAGE + meta_page) * IndexSegment::PAGE_SIZE;
        ::pwrite(fd_, meta_buffer, IndexSegment::PAGE_SIZE, offset);
    }

    superblock_dirty_ = false;
    sync();
}

// ========== Private Helpers ==========

IndexSegment* IndexStorageManager::ensure_segment(uint64_t segment_id) {
    // Caller must hold segments_mutex_ exclusively

    // Grow vector if needed
    while (segments_.size() <= segment_id) {
        segments_.push_back(nullptr);
    }

    if (!segments_[segment_id]) {
        segments_[segment_id] = std::make_unique<IndexSegment>(segment_id);
        segments_with_space_.push(segment_id);
    }

    return segments_[segment_id].get();
}

IndexSegment* IndexStorageManager::find_segment_with_space() {
    // Caller must hold segments_mutex_

    while (!segments_with_space_.empty()) {
        uint64_t segment_id = segments_with_space_.front();
        segments_with_space_.pop();

        if (segment_id < segments_.size() && segments_[segment_id]) {
            IndexSegment* segment = segments_[segment_id].get();
            if (!segment->is_full()) {
                // Put back if still has space after allocation
                segments_with_space_.push(segment_id);
                return segment;
            }
        }
    }

    return nullptr;
}

}  // namespace Masstree
