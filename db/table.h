#pragma once
#include "db_common.h"
/**
 * @brief Contains the addresses of Indexblock and some metadata
 *
 */
struct PSTMeta
{
    uint64_t datablock_ptr_ = 0;
#if defined(FLOWKV_KEY16)
    uint64_t max_key_hi = 0;
    uint64_t max_key_lo = 0;
    uint64_t min_key_hi = MAX_UINT64;
    uint64_t min_key_lo = MAX_UINT64;
#else
    uint64_t max_key_ = 0;
    uint64_t min_key_ = MAX_UINT64;
#endif
    uint32_t seq_no_ = 0;
    uint16_t entry_num_ = 0;
    static PSTMeta InvalidTable() { return PSTMeta(); }
    bool Valid() const
    {
        return datablock_ptr_ != 0;
    }

    KeyType MinKey() const
    {
#if defined(FLOWKV_KEY16)
        return Key16{min_key_hi, min_key_lo};
#else
        return min_key_;
#endif
    }

    KeyType MaxKey() const
    {
#if defined(FLOWKV_KEY16)
        return Key16{max_key_hi, max_key_lo};
#else
        return max_key_;
#endif
    }
};


struct TaggedPstMeta
{
    PSTMeta meta;
    // optional information. maybe lost after recovery
    size_t level;
    size_t manifest_position;
    bool Valid() const
    {
        return meta.Valid();
    }
};

union TaggedPtr
{
    uint64_t valid : 1;
    uint64_t manifest_idx : 24;
    // 512-byte aligned
    uint64_t indexblock_ptr : 39;
};
