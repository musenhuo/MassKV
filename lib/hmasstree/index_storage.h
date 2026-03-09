/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * IndexStorageManager: Manages on-disk storage for H-Masstree index nodes
 */
#ifndef HMASSTREE_INDEX_STORAGE_H
#define HMASSTREE_INDEX_STORAGE_H

#include "node_handle.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <ctime>
#include <sys/types.h>

namespace Masstree {

/**
 * @brief Superblock - persisted at the beginning of storage file
 *
 * Layout: Page 0 is reserved for superblock and metadata header
 */
struct alignas(64) IndexSuperblock {
    static constexpr uint64_t MAGIC = 0x464C4F574B565831ULL;  // "FLOWKVX1"
    static constexpr uint32_t VERSION = 1;

    // ===== Header (64 bytes) =====
    uint64_t magic;                    // Magic number for validation
    uint32_t version;                  // Format version
    uint32_t flags;                    // Feature flags
    uint64_t creation_time;            // Unix timestamp
    uint64_t last_checkpoint_time;     // Last checkpoint timestamp

    // ===== Storage info (64 bytes) =====
    uint64_t total_segments;           // Number of segments
    uint64_t next_page_id;             // Next available page ID
    uint64_t total_pages_allocated;    // Total pages ever allocated
    uint64_t total_pages_freed;        // Total pages ever freed
    uint64_t metadata_region_start;    // Offset to metadata region (page ID)
    uint64_t metadata_region_size;     // Size of metadata region in pages
    uint64_t reserved1[2];

    // ===== Root handle (64 bytes) =====
    uint64_t root_handle_raw;          // Root node handle (for future use)
    uint64_t reserved2[7];

    // ===== Checksum (64 bytes) =====
    uint64_t checksum;                 // CRC64 of above fields
    uint64_t reserved3[7];

    // Total: 256 bytes (4 cache lines)

    void init() {
        std::memset(this, 0, sizeof(*this));
        magic = MAGIC;
        version = VERSION;
        creation_time = static_cast<uint64_t>(std::time(nullptr));
        metadata_region_start = 1;     // Page 1 onwards for metadata
        metadata_region_size = 16;     // Reserve 16 pages (64KB) for metadata
    }

    bool is_valid() const {
        return magic == MAGIC && version <= VERSION;
    }

    uint64_t compute_checksum() const {
        // Simple XOR-based checksum (production would use CRC64)
        const uint64_t* data = reinterpret_cast<const uint64_t*>(this);
        uint64_t cs = 0;
        // Checksum all fields except the checksum field itself
        for (size_t i = 0; i < (offsetof(IndexSuperblock, checksum) / sizeof(uint64_t)); ++i) {
            cs ^= data[i];
        }
        return cs;
    }

    void update_checksum() {
        checksum = compute_checksum();
    }

    bool verify_checksum() const {
        return checksum == compute_checksum();
    }
};
static_assert(sizeof(IndexSuperblock) == 256, "Superblock must be 256 bytes");

/**
 * @brief Segment metadata persisted to disk
 *
 * Each segment has a metadata entry storing its allocation bitmap
 */
struct alignas(8) SegmentMetadata {
    uint64_t segment_id;
    uint64_t allocated_count;
    // Bitmap: 1024 pages / 8 = 128 bytes
    uint8_t page_bitmap[128];

    void init(uint64_t id) {
        segment_id = id;
        allocated_count = 0;
        std::memset(page_bitmap, 0, sizeof(page_bitmap));
    }

    void set_page(uint16_t page_index) {
        if (page_index < 1024) {
            page_bitmap[page_index / 8] |= (1 << (page_index % 8));
        }
    }

    void clear_page(uint16_t page_index) {
        if (page_index < 1024) {
            page_bitmap[page_index / 8] &= ~(1 << (page_index % 8));
        }
    }

    bool is_page_set(uint16_t page_index) const {
        if (page_index >= 1024) return false;
        return (page_bitmap[page_index / 8] & (1 << (page_index % 8))) != 0;
    }
};
static_assert(sizeof(SegmentMetadata) == 144, "SegmentMetadata size check");

/**
 * @brief Configuration for IndexStorageManager
 */
struct IndexStorageConfig {
    std::string storage_path;                           // Path to storage file/directory
    size_t segment_size = 4ULL * 1024 * 1024;           // 4MB segments
    size_t max_storage_size = 256ULL * 1024 * 1024 * 1024; // 256GB max
    bool use_direct_io = true;                          // Use O_DIRECT
    bool recover = false;                               // Recovery mode
};

/**
 * @brief Segment metadata for index storage
 *
 * Each segment is 4MB, containing multiple 4KB pages.
 */
struct IndexSegment {
    static constexpr size_t SEGMENT_SIZE = 4ULL * 1024 * 1024;  // 4MB
    static constexpr size_t PAGE_SIZE = 4096;                    // 4KB
    static constexpr size_t PAGES_PER_SEGMENT = SEGMENT_SIZE / PAGE_SIZE;  // 1024

    uint64_t segment_id;
    std::vector<bool> page_bitmap;  // true = allocated
    size_t allocated_count;

    // Free list for quick allocation
    std::queue<uint16_t> free_pages;

    IndexSegment(uint64_t id) : segment_id(id), page_bitmap(PAGES_PER_SEGMENT, false), allocated_count(0) {
        // Initialize free list
        // For segment 0, skip page 0 to reserve page_id=0 as "null/invalid"
        uint16_t start_page = (id == 0) ? 1 : 0;
        if (id == 0) {
            // Mark page 0 as allocated (reserved)
            page_bitmap[0] = true;
            allocated_count = 1;
        }
        for (uint16_t i = start_page; i < PAGES_PER_SEGMENT; ++i) {
            free_pages.push(i);
        }
    }

    bool is_full() const {
        return allocated_count >= PAGES_PER_SEGMENT;
    }

    bool is_empty() const {
        return allocated_count == 0;
    }

    uint16_t allocate_page() {
        if (free_pages.empty()) return UINT16_MAX;
        uint16_t page = free_pages.front();
        free_pages.pop();
        page_bitmap[page] = true;
        allocated_count++;
        return page;
    }

    void free_page(uint16_t page) {
        if (page < PAGES_PER_SEGMENT && page_bitmap[page]) {
            page_bitmap[page] = false;
            free_pages.push(page);
            allocated_count--;
        }
    }
};

/**
 * @brief Free slot entry for node packing
 */
struct FreeSlot {
    uint64_t page_id;
    uint8_t slot_index;
};

/**
 * @brief IndexStorageManager - Manages on-disk storage for index nodes
 *
 * Features:
 *   - Segment-based allocation (4MB segments, 4KB pages)
 *   - Node packing (8 nodes per 4KB page)
 *   - Free slot tracking for efficient allocation
 *   - Direct I/O support for high performance
 */
class IndexStorageManager {
private:
    // ===== Configuration =====
    IndexStorageConfig config_;

    // ===== File descriptor =====
    int fd_ = -1;

    // ===== Superblock =====
    IndexSuperblock superblock_;
    bool superblock_dirty_ = false;

    // ===== Segment management =====
    mutable std::shared_mutex segments_mutex_;
    std::vector<std::unique_ptr<IndexSegment>> segments_;
    std::queue<uint64_t> segments_with_space_;  // Segments that have free pages

    // ===== Page allocation tracking =====
    std::atomic<uint64_t> next_page_id_{1};  // 0 is reserved for null

    // ===== Free slot management (for node packing) =====
    // Key = NodeType, Value = list of free slots
    mutable std::mutex free_slots_mutex_;
    std::unordered_map<uint8_t, std::queue<FreeSlot>> free_slots_;

    // ===== Statistics =====
    std::atomic<uint64_t> total_pages_allocated_{0};
    std::atomic<uint64_t> total_pages_freed_{0};
    std::atomic<uint64_t> total_bytes_written_{0};
    std::atomic<uint64_t> total_bytes_read_{0};

public:
    explicit IndexStorageManager(const IndexStorageConfig& config);
    ~IndexStorageManager();

    // Non-copyable
    IndexStorageManager(const IndexStorageManager&) = delete;
    IndexStorageManager& operator=(const IndexStorageManager&) = delete;

    // ========== Initialization ==========

    /**
     * @brief Open or create storage file
     * @return true on success
     */
    bool open();

    /**
     * @brief Close storage file
     */
    void close();

    /**
     * @brief Check if storage is open
     */
    bool is_open() const { return fd_ >= 0; }

    // ========== Node Allocation ==========

    /**
     * @brief Allocate a slot for a new node
     * @param type Node type
     * @return Pair of (page_id, slot_index), (0, 0) on failure
     *
     * This allocates a slot within a page. If there's a free slot
     * in an existing page, it will be reused. Otherwise, a new page
     * is allocated.
     */
    std::pair<uint64_t, uint8_t> allocate_node_slot(NodeType type);

    /**
     * @brief Free a node slot
     * @param handle Node handle to free
     *
     * The slot is added to the free list for reuse.
     */
    void free_node_slot(NodeHandle handle);

    /**
     * @brief Allocate a new page
     * @return Page ID, 0 on failure
     */
    uint64_t allocate_page();

    /**
     * @brief Free a page
     * @param page_id Page to free
     */
    void free_page(uint64_t page_id);

    // ========== I/O Operations ==========

    /**
     * @brief Read a page from storage
     * @param page_id Page ID to read
     * @param buffer Output buffer (must be 4KB aligned)
     * @return true on success
     */
    bool read_page(uint64_t page_id, void* buffer);

    /**
     * @brief Write a page to storage
     * @param page_id Page ID to write
     * @param buffer Input buffer (must be 4KB aligned)
     * @return true on success
     */
    bool write_page(uint64_t page_id, const void* buffer);

    /**
     * @brief Sync all pending writes to disk
     */
    void sync();

    // ========== Recovery ==========

    /**
     * @brief Recover state from storage
     * @return true on success
     */
    bool recover();

    /**
     * @brief Fallback recovery without metadata (conservative)
     * @param file_size Size of the storage file
     * @return true on success
     */
    bool recover_without_metadata(off_t file_size);

    /**
     * @brief Persist metadata (segment info, free lists)
     */
    void persist_metadata();

    // ========== Root Handle Management ==========

    /**
     * @brief Set root handle for persistence
     * @param handle Root node handle
     */
    void set_root_handle(NodeHandle handle) {
        superblock_.root_handle_raw = handle.raw();
        superblock_dirty_ = true;
    }

    /**
     * @brief Get root handle
     * @return Root node handle
     */
    NodeHandle get_root_handle() const {
        return NodeHandle::from_raw(superblock_.root_handle_raw);
    }

    /**
     * @brief Check if storage has a saved root handle
     */
    bool has_root_handle() const {
        return superblock_.root_handle_raw != 0;
    }

    // ========== Statistics ==========

    uint64_t pages_allocated() const { return total_pages_allocated_.load(); }
    uint64_t pages_freed() const { return total_pages_freed_.load(); }
    uint64_t pages_in_use() const { return total_pages_allocated_.load() - total_pages_freed_.load(); }
    uint64_t bytes_written() const { return total_bytes_written_.load(); }
    uint64_t bytes_read() const { return total_bytes_read_.load(); }

    size_t storage_used_bytes() const {
        return pages_in_use() * IndexSegment::PAGE_SIZE;
    }

    // ========== Utility ==========

    /**
     * @brief Calculate file offset for a page
     */
    off_t page_offset(uint64_t page_id) const {
        return static_cast<off_t>(page_id) * IndexSegment::PAGE_SIZE;
    }

    /**
     * @brief Get segment ID for a page
     */
    uint64_t page_to_segment(uint64_t page_id) const {
        return page_id / IndexSegment::PAGES_PER_SEGMENT;
    }

    /**
     * @brief Get page index within segment
     */
    uint16_t page_index_in_segment(uint64_t page_id) const {
        return static_cast<uint16_t>(page_id % IndexSegment::PAGES_PER_SEGMENT);
    }

private:
    /**
     * @brief Ensure segment exists and is tracked
     */
    IndexSegment* ensure_segment(uint64_t segment_id);

    /**
     * @brief Find or create a segment with free pages
     */
    IndexSegment* find_segment_with_space();
};

/**
 * @brief Thread-local IndexStorage accessor
 *
 * Provides access to the current thread's storage manager.
 * Used for automatic handle allocation during node creation.
 */
class IndexStorageRegistry {
public:
    static IndexStorageManager* get() {
        return tls_storage_;
    }

    static void set(IndexStorageManager* storage) {
        tls_storage_ = storage;
    }

private:
    static inline thread_local IndexStorageManager* tls_storage_ = nullptr;
};

}  // namespace Masstree

#endif  // HMASSTREE_INDEX_STORAGE_H
