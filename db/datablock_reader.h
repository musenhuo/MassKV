#pragma once

#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"

#include <vector>
#include <atomic>
#include <cstdint>
#include <sys/types.h>
struct DataBlockMeta
{
    off_t block_off;
    size_t size;
    KeyType min_key;
    KeyType max_key;
    PBlockType type;
};

class DataBlockReader
{

private:

    int fd;
    int direct_fd = -1;        // O_DIRECT fd for bypassing page cache
    bool use_direct_io_ = false;
    bool nowait_poll_ = false;
    bool nowait_supported_ = true;
    std::string pool_path_;    // 用于打开 direct_fd
    off_t off_ = -1;
    off_t bufoff = -1;
    char *buf = nullptr;
    PDataBlock block;
    
    // Cache statistics (per-instance)
    mutable std::atomic<uint64_t> cache_hits_{0};
    mutable std::atomic<uint64_t> cache_misses_{0};
    
    // Global cache statistics (across all instances)
    static std::atomic<uint64_t> global_cache_hits_;
    static std::atomic<uint64_t> global_cache_misses_;
    static std::atomic<uint64_t> global_point_queries_;  // Total PointQuery calls
    static std::atomic<uint64_t> global_direct_unaligned_fallbacks_;
    static std::atomic<uint64_t> global_short_reads_;
    static std::atomic<uint64_t> global_nowait_eagain_retries_;
    static std::atomic<uint64_t> global_nowait_unsupported_fallbacks_;
    static thread_local uint64_t tls_cache_misses_;

public:
    DataBlockReader(SegmentAllocator *seg_allocator, bool use_direct_io = false);
    ~DataBlockReader();

    void TraverseDataBlock(uint64_t offset,std::vector<std::pair<KeyType, FixedValue16>>* results=nullptr);
    bool BinarySearch(uint64_t offset,Slice key,const char* value_out,int entry_num);
    bool BinarySearchWindow(uint64_t block_offset,
                            Slice key,
                            const char* value_out,
                            uint16_t start_entry,
                            uint16_t entry_count);
    
    // Get cache statistics
    uint64_t GetCacheHits() const { return cache_hits_.load(); }
    uint64_t GetCacheMisses() const { return cache_misses_.load(); }
    void ResetCacheStats() { cache_hits_ = 0; cache_misses_ = 0; }
    
    // Global cache statistics
    static uint64_t GetGlobalCacheHits() { return global_cache_hits_.load(); }
    static uint64_t GetGlobalCacheMisses() { return global_cache_misses_.load(); }
    static void ResetGlobalCacheStats() {
        global_cache_hits_ = 0;
        global_cache_misses_ = 0;
        global_point_queries_ = 0;
        global_direct_unaligned_fallbacks_ = 0;
        global_short_reads_ = 0;
        global_nowait_eagain_retries_ = 0;
        global_nowait_unsupported_fallbacks_ = 0;
    }
    static uint64_t GetGlobalPointQueries() { return global_point_queries_.load(); }
    static uint64_t GetGlobalDirectUnalignedFallbacks() {
        return global_direct_unaligned_fallbacks_.load();
    }
    static uint64_t GetGlobalShortReads() { return global_short_reads_.load(); }
    static uint64_t GetGlobalNowaitEagainRetries() {
        return global_nowait_eagain_retries_.load();
    }
    static uint64_t GetGlobalNowaitUnsupportedFallbacks() {
        return global_nowait_unsupported_fallbacks_.load();
    }
    static uint64_t GetThreadLocalCacheMisses() { return tls_cache_misses_; }
    static void ResetThreadLocalCacheStats() { tls_cache_misses_ = 0; }

private:
    bool ReadDataBlock(uint64_t offset);
    bool ReadBuf(uint64_t offset);
    ssize_t ReadWithPolicy(int read_fd, void* dst, size_t len, uint64_t offset);
};
