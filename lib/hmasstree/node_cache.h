/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeCache: In-memory cache for external storage nodes
 */
#ifndef HMASSTREE_NODE_CACHE_H
#define HMASSTREE_NODE_CACHE_H

#include "node_handle.h"
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <memory>
#include <functional>
#include <queue>

namespace Masstree {

// Forward declarations
class IndexStorageManager;
class threadinfo;

/**
 * @brief Cached page in memory
 *
 * Represents a 4KB page loaded from storage.
 * Includes pin counting for safe eviction.
 */
struct CachedPage {
    PackedPage* page_ptr;               // Pointer to page data (aligned allocation)
    uint64_t page_id;                   // Page ID in storage

    // ===== Pin protection =====
    std::atomic<uint32_t> pin_count{0};      // Reference count
    std::atomic<bool> evict_requested{false}; // Eviction pending

    // ===== Epoch information =====
    uint64_t load_epoch;                // Epoch when page was loaded

    // ===== Clock eviction =====
    std::atomic<uint8_t> reference_bit{0};   // For Clock algorithm

    // ===== Dirty tracking =====
    std::atomic<bool> is_dirty{false};

    CachedPage() : page_ptr(nullptr), page_id(0), load_epoch(0) {}

    ~CachedPage() {
        if (page_ptr) {
            free(page_ptr);
            page_ptr = nullptr;
        }
    }

    /**
     * @brief Attempt to pin the page
     * @return true if successfully pinned, false if eviction is pending
     */
    bool try_pin() {
        if (evict_requested.load(std::memory_order_acquire)) {
            return false;
        }
        pin_count.fetch_add(1, std::memory_order_acq_rel);
        // Double check after incrementing
        if (evict_requested.load(std::memory_order_acquire)) {
            pin_count.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
        return true;
    }

    void unpin() {
        pin_count.fetch_sub(1, std::memory_order_release);
    }

    void touch() {
        reference_bit.store(1, std::memory_order_relaxed);
    }

    void mark_dirty() {
        is_dirty.store(true, std::memory_order_release);
    }

    void clear_dirty() {
        is_dirty.store(false, std::memory_order_release);
    }

    bool is_pinned() const {
        return pin_count.load(std::memory_order_acquire) > 0;
    }

    template<typename NodeT>
    NodeT* get_node(uint8_t slot) {
        return page_ptr->get_node<NodeT>(slot);
    }
};

/**
 * @brief RAII guard for pinning a page during access
 */
class PagePinGuard {
private:
    CachedPage* page_;

public:
    explicit PagePinGuard(CachedPage* page) : page_(page) {}

    ~PagePinGuard() {
        if (page_) {
            page_->unpin();
        }
    }

    // Non-copyable
    PagePinGuard(const PagePinGuard&) = delete;
    PagePinGuard& operator=(const PagePinGuard&) = delete;

    // Movable
    PagePinGuard(PagePinGuard&& other) noexcept : page_(other.page_) {
        other.page_ = nullptr;
    }

    PagePinGuard& operator=(PagePinGuard&& other) noexcept {
        if (this != &other) {
            if (page_) {
                page_->unpin();
            }
            page_ = other.page_;
            other.page_ = nullptr;
        }
        return *this;
    }

    void release() {
        if (page_) {
            page_->unpin();
            page_ = nullptr;
        }
    }

    CachedPage* get() const { return page_; }
};

/**
 * @brief Per-thread epoch state for safe eviction
 */
struct alignas(64) ThreadEpochState {
    std::atomic<uint64_t> local_epoch{0};
    std::atomic<bool> in_critical{false};
    char padding[48];  // Avoid false sharing
};

/**
 * @brief Pending eviction entry
 */
struct PendingEviction {
    CachedPage* page;
    uint64_t request_epoch;
};

/**
 * @brief NodeCache configuration
 */
struct NodeCacheConfig {
    size_t max_memory_bytes = 16ULL * 1024 * 1024 * 1024;  // 16GB default
    size_t num_shards = 64;                                  // Number of hash shards
    size_t max_threads = 128;                                // Max concurrent threads
    size_t eviction_batch_size = 64;                         // Pages to evict per batch
    size_t dirty_flush_threshold = 1024;                     // Trigger flush when this many dirty pages
};

/**
 * @brief Callback type for page eviction notification
 *
 * Called before a page is evicted from cache, allowing dependent components
 * (e.g., NodeResolver) to clean up any cached pointers to nodes on that page.
 *
 * @param page_id The page ID being evicted
 * @param page_ptr Pointer to the page data (still valid during callback)
 */
using EvictCallback = std::function<void(uint64_t page_id, const PackedPage* page_ptr)>;

/**
 * @brief NodeCache - Sharded hash map with Clock eviction
 *
 * Key features:
 *   - Sharded for concurrent access
 *   - Clock algorithm for eviction
 *   - Pin/Unpin for safe access during reads
 *   - Epoch-based protection for safe eviction
 *   - Dirty page tracking for write-back
 */
class NodeCache {
private:
    // ===== Configuration =====
    NodeCacheConfig config_;

    // ===== Storage backend =====
    IndexStorageManager* storage_;

    // ===== Sharded hash map =====
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<uint64_t, std::unique_ptr<CachedPage>> pages;

        // Clock hand for this shard
        std::vector<uint64_t> clock_list;
        size_t clock_hand = 0;
    };
    std::vector<Shard> shards_;

    // ===== Memory tracking =====
    std::atomic<size_t> current_memory_bytes_{0};
    std::atomic<size_t> page_count_{0};

    // ===== Epoch management =====
    std::atomic<uint64_t> global_epoch_{0};
    std::vector<ThreadEpochState> thread_epochs_;

    // ===== Pending evictions =====
    std::mutex evict_queue_mutex_;
    std::queue<PendingEviction> evict_queue_;

    // ===== Statistics =====
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
    std::atomic<uint64_t> evictions_{0};

    // ===== State =====
    std::atomic<bool> shutdown_{false};

    // ===== Eviction callback =====
    EvictCallback evict_callback_;
    std::mutex evict_callback_mutex_;

public:
    explicit NodeCache(IndexStorageManager* storage, const NodeCacheConfig& config = {});
    ~NodeCache();

    // Non-copyable
    NodeCache(const NodeCache&) = delete;
    NodeCache& operator=(const NodeCache&) = delete;

    // ========== Core Operations ==========

    /**
     * @brief Look up a page without loading from storage
     * @return CachedPage pointer if in cache, nullptr otherwise
     * @note Does NOT pin the page - caller must handle
     */
    CachedPage* lookup_no_load(NodeHandle handle);

    /**
     * @brief Get a page, loading from storage if necessary
     * @param handle Node handle to look up
     * @param pin If true, pin the page before returning
     * @return CachedPage pointer (pinned if requested)
     * @note If pin=true, caller MUST unpin when done
     */
    CachedPage* get_or_load(NodeHandle handle, bool pin = true);

    /**
     * @brief Get a page, return nullptr if not in cache (for OCC restart pattern)
     */
    CachedPage* try_get(NodeHandle handle, bool pin = true);

    /**
     * @brief Allocate a new page for a new node
     * @param type Node type to allocate
     * @return Pair of (NodeHandle, slot pointer)
     */
    std::pair<NodeHandle, void*> allocate_node(NodeType type);

    /**
     * @brief Mark a node as dirty (needs write-back)
     */
    void mark_dirty(NodeHandle handle);

    /**
     * @brief Free a node (mark slot as available)
     */
    void free_node(NodeHandle handle);

    // ========== Epoch Protection ==========

    /**
     * @brief Enter critical section (called by reader threads)
     * @param thread_id Thread index (0-based)
     */
    void enter_critical_section(size_t thread_id);

    /**
     * @brief Exit critical section
     */
    void exit_critical_section(size_t thread_id);

    /**
     * @brief Advance global epoch (called by background thread)
     */
    void advance_epoch();

    /**
     * @brief Get minimum active epoch across all threads
     */
    uint64_t min_active_epoch() const;

    // ========== Eviction ==========

    /**
     * @brief Request eviction of a page (async)
     */
    void request_eviction(CachedPage* page);

    /**
     * @brief Process pending evictions (called by background thread)
     */
    void process_pending_evictions();

    /**
     * @brief Try to evict pages to free memory
     * @param target_bytes Target bytes to free
     * @return Actual bytes freed
     */
    size_t evict_pages(size_t target_bytes);

    // ========== Flush ==========

    /**
     * @brief Flush all dirty pages to storage
     */
    void flush_all();

    /**
     * @brief Flush dirty pages in a specific shard
     */
    void flush_shard(size_t shard_id);

    // ========== Statistics ==========

    size_t memory_usage() const { return current_memory_bytes_.load(); }
    size_t page_count() const { return page_count_.load(); }
    uint64_t cache_hits() const { return cache_hits_.load(); }
    uint64_t cache_misses() const { return cache_misses_.load(); }
    uint64_t eviction_count() const { return evictions_.load(); }

    double hit_rate() const {
        uint64_t hits = cache_hits_.load();
        uint64_t misses = cache_misses_.load();
        uint64_t total = hits + misses;
        return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }

    // ========== Shutdown ==========

    void shutdown() { shutdown_.store(true); }
    bool is_shutdown() const { return shutdown_.load(); }

    // ========== Eviction Callback ==========

    /**
     * @brief Set callback to be called before page eviction
     *
     * The callback is invoked with the page_id and page_ptr before the page
     * is removed from the cache. This allows NodeResolver to clean up any
     * cached pointers to nodes on that page before they become invalid.
     *
     * @param callback Function to call on eviction (nullptr to clear)
     */
    void set_evict_callback(EvictCallback callback) {
        std::lock_guard<std::mutex> lock(evict_callback_mutex_);
        evict_callback_ = std::move(callback);
    }

private:
    size_t shard_for_page(uint64_t page_id) const {
        return page_id % shards_.size();
    }

    CachedPage* load_page_from_storage(uint64_t page_id);
    void write_page_to_storage(CachedPage* page);

    // Clock eviction within a shard
    CachedPage* select_victim_clock(Shard& shard);
    void add_to_clock_list(Shard& shard, uint64_t page_id);
    void remove_from_clock_list(Shard& shard, uint64_t page_id);
};

/**
 * @brief RAII guard for critical section + multiple page pins
 *
 * Usage:
 *   ReadGuard guard(cache, thread_id);
 *   auto* node = guard.get_and_pin<LeafType>(handle);
 *   // ... use node ...
 *   // guard destructor releases all pins and exits critical section
 */
class ReadGuard {
private:
    NodeCache* cache_;
    size_t thread_id_;
    std::vector<CachedPage*> pinned_pages_;

public:
    ReadGuard(NodeCache* cache, size_t thread_id)
        : cache_(cache), thread_id_(thread_id) {
        cache_->enter_critical_section(thread_id_);
    }

    ~ReadGuard() {
        for (auto* page : pinned_pages_) {
            page->unpin();
        }
        cache_->exit_critical_section(thread_id_);
    }

    // Non-copyable
    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;

    /**
     * @brief Get a node and pin its page
     * @return Node pointer, or nullptr if page not in cache/being evicted
     */
    template<typename NodeT>
    NodeT* get_and_pin(NodeHandle handle) {
        CachedPage* page = cache_->try_get(handle, true);
        if (page) {
            pinned_pages_.push_back(page);
            return page->get_node<NodeT>(handle.slot_index());
        }
        return nullptr;
    }

    /**
     * @brief Release all pins (for restart pattern)
     */
    void release_all_pins() {
        for (auto* page : pinned_pages_) {
            page->unpin();
        }
        pinned_pages_.clear();
    }

    size_t pinned_count() const { return pinned_pages_.size(); }
};

/**
 * @brief Thread-local NodeCache accessor
 *
 * Provides access to the current thread's node cache.
 * Used for setting slot bitmap during node allocation.
 */
class NodeCacheRegistry {
public:
    static NodeCache* get() {
        return tls_cache_;
    }

    static void set(NodeCache* cache) {
        tls_cache_ = cache;
    }

private:
    static inline thread_local NodeCache* tls_cache_ = nullptr;
};

}  // namespace Masstree

#endif  // HMASSTREE_NODE_CACHE_H
