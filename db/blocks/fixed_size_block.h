#pragma once
#include "db_common.h"

#define ALIGNED_COPY_256

struct alignas(4096) PDataBlock64KForFixed16B
{
#if defined(FLOWKV_KEY16)
    static constexpr int MAX_ENTRIES = 2048;  // 64KB / 32B (16B key + 16B value)
    struct Entry
    {
        uint64_t key_hi;
        uint64_t key_lo;
        uint64_t value_lo;
        uint64_t value_hi;
    };
#else
    static constexpr int MAX_ENTRIES = 4096;  // 64KB / 16B (8B key + 8B value)
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
#endif
    Entry entries[MAX_ENTRIES];
};

struct alignas(4096) PDataBlock16KForFixed16B
{
#if defined(FLOWKV_KEY16)
    static constexpr int MAX_ENTRIES = 512;   // 16KB / 32B (16B key + 16B value)
    struct Entry
    {
        uint64_t key_hi;
        uint64_t key_lo;
        uint64_t value_lo;
        uint64_t value_hi;
    };
#else
    static constexpr int MAX_ENTRIES = 1024;  // 16KB / 16B (8B key + 8B value)
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
#endif
    Entry entries[MAX_ENTRIES];
};

struct alignas(4096) PDataBlock4KForFixed16B
{
#if defined(FLOWKV_KEY16)
    static constexpr int MAX_ENTRIES = 128;   // 4KB / 32B (16B key + 16B value)
    struct Entry
    {
        uint64_t key_hi;
        uint64_t key_lo;
        uint64_t value_lo;
        uint64_t value_hi;
    };
#else
    static constexpr int MAX_ENTRIES = 256;   // 4KB / 16B (8B key + 8B value)
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
#endif
    Entry entries[MAX_ENTRIES];
};


// Wrappers

using PDataBlock = PDataBlock4KForFixed16B;

struct PDataBlockWrapper
{
    PDataBlock data_buf;
    int size = 0;
    off_t page_addr = -1;

    void clear()
    {
        size = 0;
        page_addr = -1;
    }
    bool valid()
    {
        return page_addr != -1;
    }
    void set_page(off_t addr)
    {
        page_addr = addr;
    }
#if defined(FLOWKV_KEY16)
    void add_entry(uint64_t key_hi, uint64_t key_lo, const FixedValue16 &value)
    {
        data_buf.entries[size].key_hi = key_hi;
        data_buf.entries[size].key_lo = key_lo;
        data_buf.entries[size].value_lo = value.lo;
        data_buf.entries[size].value_hi = value.hi;
        size++;
    }
#else
    void add_entry(uint64_t key, uint64_t ptr)
    {
        data_buf.entries[size].key = key;
        data_buf.entries[size].value = ptr;
        size++;
    }
#endif
    bool is_full()
    {
        return size == PDataBlock::MAX_ENTRIES;
    }
};
