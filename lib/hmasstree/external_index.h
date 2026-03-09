/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * ExternalIndex: Integration layer for H-Masstree external storage mode
 */
#ifndef HMASSTREE_EXTERNAL_INDEX_H
#define HMASSTREE_EXTERNAL_INDEX_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
#include "node_handle.h"
#include "node_cache.h"
#include "index_storage.h"
#include "scan_context.h"
#include "node_factory.h"

namespace Masstree {

/**
 * @brief Configuration for external storage index
 */
struct ExternalIndexConfig {
    // Storage configuration
    std::string storage_path = "/tmp/hmasstree_index.dat";
    size_t storage_size_mb = 1024;  // 1GB default
    bool use_direct_io = true;
    
    // Cache configuration
    size_t cache_size_mb = 256;     // 256MB default cache
    size_t cache_shards = 64;       // Number of cache shards
    
    // Performance tuning
    size_t prefetch_depth = 2;      // Tree prefetch depth
    bool enable_background_flush = true;
    size_t flush_interval_ms = 1000;
    
    // Memory control
    size_t max_memory_mb = 512;     // Max memory before eviction
    double evict_ratio = 0.1;       // Evict 10% when threshold reached
    
    static ExternalIndexConfig default_config() {
        return ExternalIndexConfig{};
    }
};

/**
 * @brief Statistics for external storage index
 */
struct ExternalIndexStats {
    // Cache statistics
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
    uint64_t cache_evictions = 0;
    uint64_t cache_dirty_evictions = 0;
    
    // I/O statistics
    uint64_t pages_read = 0;
    uint64_t pages_written = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
    
    // Node statistics
    uint64_t nodes_allocated = 0;
    uint64_t nodes_freed = 0;
    uint64_t internodes_count = 0;
    uint64_t leaves_count = 0;
    
    double cache_hit_rate() const {
        uint64_t total = cache_hits + cache_misses;
        return total > 0 ? (double)cache_hits / total : 0.0;
    }
    
    void reset() {
        *this = ExternalIndexStats{};
    }
};

/**
 * @brief External Storage Manager for H-Masstree
 *
 * This class integrates NodeCache and IndexStorageManager to provide
 * a complete external storage solution for H-Masstree.
 *
 * Thread Safety:
 * - The class is thread-safe for concurrent operations
 * - Each thread should call set_thread_context() before using the index
 *
 * Usage:
 *   ExternalStorageManager mgr(config);
 *   mgr.initialize();
 *   // ... use index ...
 *   mgr.shutdown();
 */
class ExternalStorageManager {
private:
    ExternalIndexConfig config_;
    std::unique_ptr<NodeCache> cache_;
    std::unique_ptr<IndexStorageManager> storage_;
    ExternalIndexStats stats_;
    
    bool initialized_ = false;
    
    // Background flush thread
    std::thread flush_thread_;
    std::atomic<bool> stop_flush_thread_{false};
    
    // Flush thread implementation
    void flush_thread_loop() {
        while (!stop_flush_thread_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.flush_interval_ms));
            
            if (stop_flush_thread_.load(std::memory_order_relaxed)) break;
            
            if (cache_) {
                // Flush dirty pages to disk
                cache_->flush_all();
                // Advance epoch to allow reclamation
                cache_->advance_epoch();
            }
        }
    }
    
public:
    explicit ExternalStorageManager(const ExternalIndexConfig& config = ExternalIndexConfig::default_config())
        : config_(config) {}
    
    ~ExternalStorageManager() {
        if (initialized_) {
            shutdown();
        }
    }
    
    // Non-copyable
    ExternalStorageManager(const ExternalStorageManager&) = delete;
    ExternalStorageManager& operator=(const ExternalStorageManager&) = delete;
    
    /**
     * @brief Initialize the external storage manager
     * @return true on success
     */
    bool initialize() {
        if (initialized_) return true;
        
        // Create storage manager
        IndexStorageConfig storage_config;
        storage_config.storage_path = config_.storage_path;
        storage_config.max_storage_size = config_.storage_size_mb * 1024ULL * 1024;
        storage_config.use_direct_io = config_.use_direct_io;
        storage_config.recover = true;  // Always try to recover from existing file
        
        storage_ = std::make_unique<IndexStorageManager>(storage_config);
        if (!storage_->open()) {
            return false;
        }
        
        // Create cache with storage backend
        NodeCacheConfig cache_config;
        cache_config.max_memory_bytes = config_.cache_size_mb * 1024 * 1024;
        cache_config.num_shards = config_.cache_shards;
        
        cache_ = std::make_unique<NodeCache>(storage_.get(), cache_config);
        
        // Start background flush thread if enabled
        if (config_.enable_background_flush) {
            stop_flush_thread_.store(false, std::memory_order_relaxed);
            flush_thread_ = std::thread(&ExternalStorageManager::flush_thread_loop, this);
        }
        
        initialized_ = true;
        return true;
    }
    
    /**
     * @brief Shutdown the external storage manager
     *
     * Stops background threads, flushes all dirty pages and closes the storage file.
     */
    void shutdown() {
        if (!initialized_) return;
        
        // Stop background flush thread
        if (flush_thread_.joinable()) {
            stop_flush_thread_.store(true, std::memory_order_relaxed);
            flush_thread_.join();
        }
        
        // Flush all dirty pages
        if (cache_) {
            cache_->flush_all();
        }
        
        // Close storage
        if (storage_) {
            storage_->close();
        }
        
        initialized_ = false;
    }
    
    /**
     * @brief Check if manager is initialized
     */
    bool is_initialized() const { return initialized_; }
    
    /**
     * @brief Get the node cache
     */
    NodeCache* cache() { return cache_.get(); }
    const NodeCache* cache() const { return cache_.get(); }
    
    /**
     * @brief Get the storage manager
     */
    IndexStorageManager* storage() { return storage_.get(); }
    const IndexStorageManager* storage() const { return storage_.get(); }
    
    /**
     * @brief Get statistics
     */
    const ExternalIndexStats& stats() const { return stats_; }
    
    /**
     * @brief Reset statistics
     */
    void reset_stats() { stats_.reset(); }
    
    /**
     * @brief Create a node factory for the current thread
     */
    template <typename P>
    NodeFactory<P> create_node_factory() {
        if (!initialized_) {
            return NodeFactory<P>();
        }
        return NodeFactory<P>(cache_.get(), storage_.get());
    }
    
    /**
     * @brief Create a scan context for the current thread
     */
    ScanContext create_scan_context() {
        if (!initialized_) {
            return ScanContext();
        }
        return ScanContext(cache_.get());
    }
    
    /**
     * @brief Flush dirty pages to storage
     * @param sync If true, wait for I/O to complete
     */
    void flush(bool sync = true) {
        if (cache_) {
            cache_->flush_all();
            // TODO: Add sync parameter support
            (void)sync;
        }
    }
    
    /**
     * @brief Trigger cache eviction if memory usage exceeds threshold
     * @return Number of pages evicted
     */
    size_t maybe_evict() {
        if (!cache_) return 0;
        
        size_t current_pages = cache_->page_count();
        size_t max_pages = config_.cache_size_mb * 1024 * 1024 / IndexSegment::PAGE_SIZE;
        
        if (current_pages > max_pages) {
            size_t bytes_to_evict = static_cast<size_t>(max_pages * IndexSegment::PAGE_SIZE * config_.evict_ratio);
            size_t evicted_bytes = cache_->evict_pages(bytes_to_evict);
            size_t evicted_pages = evicted_bytes / IndexSegment::PAGE_SIZE;
            stats_.cache_evictions += evicted_pages;
            return evicted_pages;
        }
        return 0;
    }
    
    /**
     * @brief Get memory usage in bytes
     */
    size_t memory_usage() const {
        if (!cache_) return 0;
        return cache_->memory_usage();
    }
    
    /**
     * @brief Print statistics to stdout
     */
    void print_stats() const {
        printf("=== External Storage Statistics ===\n");
        printf("Cache Hit Rate: %.2f%%\n", stats_.cache_hit_rate() * 100);
        printf("Cache Hits: %lu, Misses: %lu\n", stats_.cache_hits, stats_.cache_misses);
        printf("Pages Read: %lu, Written: %lu\n", stats_.pages_read, stats_.pages_written);
        printf("Nodes: %lu allocated, %lu freed\n", stats_.nodes_allocated, stats_.nodes_freed);
        printf("Memory Usage: %.2f MB\n", memory_usage() / (1024.0 * 1024.0));
        printf("==================================\n");
    }
};

/**
 * @brief RAII guard for thread-local external storage context
 *
 * Usage:
 *   ExternalStorageGuard guard(mgr);
 *   // ... perform index operations ...
 *   // Context automatically cleaned up on scope exit
 */
class ExternalStorageGuard {
private:
    ScanContext scan_ctx_;
    ScanContext* prev_scan_ctx_;
    
public:
    explicit ExternalStorageGuard(ExternalStorageManager& mgr)
        : scan_ctx_(mgr.create_scan_context())
    {
        prev_scan_ctx_ = ScanContextRegistry::get();
        ScanContextRegistry::set(&scan_ctx_);
    }
    
    ~ExternalStorageGuard() {
        ScanContextRegistry::set(prev_scan_ctx_);
    }
    
    // Non-copyable
    ExternalStorageGuard(const ExternalStorageGuard&) = delete;
    ExternalStorageGuard& operator=(const ExternalStorageGuard&) = delete;
    
    ScanContext& scan_context() { return scan_ctx_; }
};

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_EXTERNAL_INDEX_H
