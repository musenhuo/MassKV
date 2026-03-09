#include "pst_reader.h"
#include <algorithm>
PSTReader::PSTReader(SegmentAllocator *allocator, bool use_direct_io) : datablock_reader_(allocator, use_direct_io)
{
}

PSTReader::~PSTReader()
{
}
//zwt todo
PSTMeta PSTReader::RecoverPSTMeta(uint64_t data_addr)
{
    PSTMeta meta;
    meta.datablock_ptr_ = data_addr;
    std::vector<std::pair<KeyType, FixedValue16>> kvlist;
    datablock_reader_.TraverseDataBlock(data_addr, &kvlist);
    
#if defined(FLOWKV_KEY16)
    Key16 min_k = kvlist[0].first;
    meta.min_key_hi = min_k.hi;
    meta.min_key_lo = min_k.lo;
    Key16 max_k = kvlist.back().first;
    meta.max_key_hi = max_k.hi;
    meta.max_key_lo = max_k.lo;
#else
    meta.min_key_ = kvlist[0].first;
    meta.max_key_ = kvlist.back().first;
#endif
    
    meta.entry_num_ = kvlist.size();
    return meta;
}
bool cmp(const std::pair<uint64_t, uint64_t> &a, const Slice &b)
{
    uint64_t key = a.first;
    return Slice(&key).compare(b) > 0;
};
bool cmp2(const Slice &b, const std::pair<uint64_t, uint64_t> &a)
{
    uint64_t key = a.first;
    return Slice(&key).compare(b) > 0;
};
bool PSTReader::PointQuery(uint64_t data_addr, Slice key, const char *value_out, int *value_size, uint16_t entry_num_)
{
    bool ret = datablock_reader_.BinarySearch(data_addr, key, value_out, entry_num_);
    *value_size = static_cast<int>(sizeof(FixedValue16));
    return ret;
}

bool PSTReader::PointQueryWindow(uint64_t block_addr,
                                 Slice key,
                                 const char *value_out,
                                 int *value_size,
                                 uint16_t entry_num,
                                 uint16_t start_entry,
                                 uint16_t entry_count)
{
    const bool ret = datablock_reader_.BinarySearchWindow(
        block_addr,
        key,
        value_out,
        static_cast<int>(entry_num),
        start_entry,
        entry_count);
    *value_size = static_cast<int>(sizeof(FixedValue16));
    return ret;
}
PSTReader::Iterator *PSTReader::GetIterator(uint64_t data_addr)
{
    return new PSTReader::Iterator(this, data_addr);
}
