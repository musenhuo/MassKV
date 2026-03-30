#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "datablock_reader.h"
#include "util/binary_search.h"
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <limits>
#include <sched.h>
#include <sys/uio.h>
#include <unistd.h>

// Point query reads exactly one KV data block.
constexpr size_t kPointReadBufSize = sizeof(PDataBlock);
constexpr size_t kDirectIoAlign = 4096;
#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif

// Define static members
std::atomic<uint64_t> DataBlockReader::global_cache_hits_{0};
std::atomic<uint64_t> DataBlockReader::global_cache_misses_{0};
std::atomic<uint64_t> DataBlockReader::global_point_queries_{0};
std::atomic<uint64_t> DataBlockReader::global_direct_unaligned_fallbacks_{0};
std::atomic<uint64_t> DataBlockReader::global_short_reads_{0};
std::atomic<uint64_t> DataBlockReader::global_nowait_eagain_retries_{0};
std::atomic<uint64_t> DataBlockReader::global_nowait_unsupported_fallbacks_{0};
thread_local uint64_t DataBlockReader::tls_cache_misses_ = 0;

DataBlockReader::DataBlockReader(SegmentAllocator *seg_allocator, bool use_direct_io)
    : fd(seg_allocator->Getfd()), use_direct_io_(use_direct_io), pool_path_(seg_allocator->GetPoolPath())
{
    // Align to 4KB for O_DIRECT compatibility
    buf = static_cast<char*>(aligned_alloc(4096, kPointReadBufSize));

    const char* nowait_env = std::getenv("FLOWKV_PST_NOWAIT_POLL");
    if (nowait_env != nullptr && (nowait_env[0] == '1' || nowait_env[0] == 't' || nowait_env[0] == 'T')) {
        nowait_poll_ = true;
        printf("[INFO] DataBlockReader: NOWAIT polling mode enabled for PST reads\n");
    }
    
    // 如果需要 O_DIRECT，打开一个独立的 fd
    if (use_direct_io_) {
        direct_fd = open(pool_path_.c_str(), O_RDONLY | O_DIRECT);
        if (direct_fd < 0) {
            perror("[ERROR] Failed to open pool file with O_DIRECT");
            use_direct_io_ = false;  // 回退到普通 I/O
        } else {
            printf("[INFO] DataBlockReader: Using O_DIRECT for PST reads (bypassing page cache)\n");
        }
    }
}

DataBlockReader::~DataBlockReader()
{
    free(buf);
    if (direct_fd >= 0) {
        close(direct_fd);
    }
}

void DataBlockReader::TraverseDataBlock(uint64_t offset, std::vector<std::pair<KeyType, FixedValue16>> *results)
{
    // traverse
    if (!ReadDataBlock(offset)) {
        ERROR_EXIT("Failed to read data block.");
    }
    int i;
#if defined(FLOWKV_KEY16)
    Key16 last_key = {INVALID_PTR, INVALID_PTR};
    for (i = 0; i < PDataBlock::MAX_ENTRIES; i++)
    {
        if (block.entries[i].key_hi == INVALID_PTR &&
            block.entries[i].key_lo == INVALID_PTR &&
            block.entries[i].value_lo == INVALID_PTR &&
            block.entries[i].value_hi == INVALID_PTR)
        {
            break;
        }
        if (block.entries[i].key_hi == last_key.hi && block.entries[i].key_lo == last_key.lo)
        {
            break;
        }
        last_key = Key16{block.entries[i].key_hi, block.entries[i].key_lo};
        if (results)
        {
            results->emplace_back(last_key, FixedValue16{block.entries[i].value_lo, block.entries[i].value_hi});
        }
    }
#else
    size_t last_key = INVALID_PTR;
    for (i = 0; i < PDataBlock::MAX_ENTRIES; i++)
    {
        LOG("read entry %lu:%lu", block.entries[i].key, block.entries[i].value);
        if (block.entries[i].key == INVALID_PTR && block.entries[i].value == INVALID_PTR)
        {
            break;
        }
        if (block.entries[i].key == last_key)
        {
            break;
        }
        last_key = block.entries[i].key;
        if (results)
        {
            results->emplace_back(
                block.entries[i].key,
                FixedValue16{block.entries[i].value, 0});
        }
    }
#endif
    if (i == 0)
    {
        ERROR_EXIT("datablock have no entries");
    }
}
bool DataBlockReader::BinarySearch(uint64_t offset, Slice key, const char *value_out, int entry_num)
{
    return BinarySearchWindow(
        offset,
        key,
        value_out,
        0,
        static_cast<uint16_t>(std::min<int>(entry_num, std::numeric_limits<uint16_t>::max())));
}

bool DataBlockReader::BinarySearchWindow(uint64_t block_offset,
                                         Slice key,
                                         const char *value_out,
                                         uint16_t start_entry,
                                         uint16_t entry_count)
{
    global_point_queries_.fetch_add(1, std::memory_order_relaxed);
    if (!ReadBuf(block_offset)) {
        return false;
    }

    if (start_entry >= static_cast<uint16_t>(PDataBlock::MAX_ENTRIES)) {
        return false;
    }
    if (entry_count == 0) {
        return false;
    }
    const uint16_t capped_end = static_cast<uint16_t>(std::min<uint32_t>(
        static_cast<uint32_t>(PDataBlock::MAX_ENTRIES),
        static_cast<uint32_t>(start_entry) + static_cast<uint32_t>(entry_count)));
    if (capped_end <= start_entry) {
        return false;
    }
    const int left_boundary = static_cast<int>(start_entry);
    int left = left_boundary;
    int right = static_cast<int>(capped_end) - 1;

#if defined(FLOWKV_KEY16)
    // 16B key mode: each entry is 32 bytes (key_hi: 8B, key_lo: 8B, value: 16B)
    // Note: keys are stored in host order (not big-endian)
    constexpr size_t entry_size = 32;
    
    // Parse search key from big-endian bytes to Key16 (host order)
    Key16 search_key = Key16::FromBigEndianBytes(key.data());
    
    int mid = 0;
    while (left <= right)
    {
        mid = (left + right) / 2;
        // Read stored key (already in host order)
        uint64_t stored_hi, stored_lo;
        memcpy(&stored_hi, buf + mid * entry_size, sizeof(uint64_t));
        memcpy(&stored_lo, buf + mid * entry_size + sizeof(uint64_t), sizeof(uint64_t));
        Key16 stored_key{stored_hi, stored_lo};
        
        int cmp = CompareKeyType(stored_key, search_key);
        if (cmp == 0)
        {
            memcpy((void *)value_out, buf + mid * entry_size + 16, sizeof(FixedValue16));
            return true;
        }
        else if (cmp > 0)
        {
            right = mid - 1;
        }
        else
        {
            left = mid + 1;
        }
    }
#else
    // 8B key mode: each entry is 16 bytes (8B key + 8B value)
    constexpr size_t entry_size = 16;
    int mid = 0;
    int ret;
    while (left <= right)
    {
        mid = (left + right) / 2;
        ret = memcmp(buf + mid * entry_size, key.data(), 8);
        if (ret == 0)
        {
            memcpy((void *)value_out, buf + mid * entry_size + 8, 8);
            return true;
        }
        else if (ret > 0)
        {
            right = mid - 1;
        }
        else if (ret < 0)
        {
            left = mid + 1;
        }
    }
#endif
    return false;
}


// private
bool DataBlockReader::ReadDataBlock(uint64_t offset)
{
    if (offset == off_)
    {
        cache_hits_.fetch_add(1, std::memory_order_relaxed);
        global_cache_hits_.fetch_add(1, std::memory_order_relaxed);
        LOG("read datablock: cache hit %lu", offset);
        //std::cout<<"read datablock: cache hit "<<off_<<std::endl; 
        return true;
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    global_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    ++tls_cache_misses_;
    int read_fd = fd;
    if (use_direct_io_ && direct_fd >= 0) {
        if ((offset % kDirectIoAlign) == 0 && (sizeof(PDataBlock) % kDirectIoAlign) == 0) {
            read_fd = direct_fd;
        } else {
            global_direct_unaligned_fallbacks_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    auto ret = ReadWithPolicy(read_fd, &block, sizeof(PDataBlock), offset);
    if (ret != static_cast<ssize_t>(sizeof(PDataBlock)))
    {
        if (read_fd == direct_fd) {
            // Retry once using buffered I/O to isolate O_DIRECT alignment/device constraints.
            ret = ReadWithPolicy(fd, &block, sizeof(PDataBlock), offset);
        }
        if (ret != static_cast<ssize_t>(sizeof(PDataBlock))) {
            global_short_reads_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }
    //std::cout<<"off: "<<off_<<std::endl; 
    off_ = offset;
    //std::cout<<"read datablock: cache miss, read "<<offset<<std::endl; 
    LOG("read datablock: cache miss, read %lu", offset);
    return true;
}

bool DataBlockReader::ReadBuf(uint64_t offset)
{
    if (offset == bufoff)
    {
        cache_hits_.fetch_add(1, std::memory_order_relaxed);
        global_cache_hits_.fetch_add(1, std::memory_order_relaxed);
        LOG("read buf: cache hit %lu", offset);
        //std::cout<<"read datablock: cache hit "<<off_<<std::endl; 
        return true;
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    global_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    ++tls_cache_misses_;
    int read_fd = fd;
    if (use_direct_io_ && direct_fd >= 0) {
        if ((offset % kDirectIoAlign) == 0 && (kPointReadBufSize % kDirectIoAlign) == 0) {
            read_fd = direct_fd;
        } else {
            global_direct_unaligned_fallbacks_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    auto ret = ReadWithPolicy(read_fd, buf, kPointReadBufSize, offset);
    if (ret != static_cast<ssize_t>(kPointReadBufSize))
    {
        if (read_fd == direct_fd) {
            // Retry once using buffered I/O to isolate O_DIRECT alignment/device constraints.
            ret = ReadWithPolicy(fd, buf, kPointReadBufSize, offset);
        }
        if (ret != static_cast<ssize_t>(kPointReadBufSize)) {
            global_short_reads_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }
    //std::cout<<"off: "<<off_<<std::endl; 
    bufoff = offset;
    //std::cout<<"read datablock: cache miss, read "<<offset<<std::endl; 
    LOG("read buf: cache miss, read %lu", offset);
    return true;
}

ssize_t DataBlockReader::ReadWithPolicy(int read_fd, void* dst, size_t len, uint64_t offset)
{
    if (!(nowait_poll_ && nowait_supported_)) {
        return pread(read_fd, dst, len, offset);
    }

    struct iovec iov;
    iov.iov_base = dst;
    iov.iov_len = len;
    uint64_t spin_round = 0;
    while (true) {
        errno = 0;
        const ssize_t ret = preadv2(
            read_fd,
            &iov,
            1,
            static_cast<off_t>(offset),
            static_cast<int>(RWF_NOWAIT));
        if (ret >= 0) {
            return ret;
        }
        if (errno == EAGAIN) {
            global_nowait_eagain_retries_.fetch_add(1, std::memory_order_relaxed);
            ++spin_round;
            if ((spin_round & 0xFFu) == 0) {
                sched_yield();
            }
            continue;
        }
        if (errno == EOPNOTSUPP || errno == ENOSYS || errno == EINVAL) {
            nowait_supported_ = false;
            global_nowait_unsupported_fallbacks_.fetch_add(1, std::memory_order_relaxed);
            return pread(read_fd, dst, len, offset);
        }
        return ret;
    }
}
