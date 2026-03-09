#include "pst_builder.h"


PSTBuilder::PSTBuilder(SegmentAllocator *segment_allocator) 
{
    data_writer_ = new DataBlockWriter(segment_allocator);
}

PSTBuilder::~PSTBuilder()
{
    PersistCheckpoint();
    if (data_writer_)
    {
        delete (DataBlockWriter *)data_writer_;
    }
}

/**
 * @brief
 *
 * @param key
 * @param value
 * @return true ok
 * @return false all datablocks and indexblock is full, need flush PST
 */
bool PSTBuilder::AddEntry(Slice key, Slice value)
{
    //zwt todo
    auto ret = data_writer_->AddEntry(key, value);
    if (!ret)
    {
        return false;
    }
    // if (meta_.min_key_ == MAX_UINT64)
    //     meta_.min_key_ = key.ToUint64();
    // meta_.max_key_ = key.ToUint64();
    meta_.entry_num_++;
    return true;
}

// build a indexblock, then flush all of the datablocks and the indexblock
PSTMeta PSTBuilder::Flush()
{
	if(data_writer_->Empty())return PSTMeta::InvalidTable();
    meta_.datablock_ptr_ = data_writer_->Flush();
    
#if defined(FLOWKV_KEY16)
    Key16 max_k = data_writer_->GetKey(meta_.entry_num_-1);
    meta_.max_key_hi = max_k.hi;
    meta_.max_key_lo = max_k.lo;
    Key16 min_k = data_writer_->GetKey(0);
    meta_.min_key_hi = min_k.hi;
    meta_.min_key_lo = min_k.lo;
#else
    meta_.max_key_ = data_writer_->GetKey(meta_.entry_num_-1);
    meta_.min_key_ = data_writer_->GetKey(0);
#endif
    
    PSTMeta ret = meta_;
    Clear();
    return ret;
}

void PSTBuilder::Clear()
{
#if defined(FLOWKV_KEY16)
    meta_ =
        {
            .datablock_ptr_ = 0,
            .max_key_hi = 0,
            .max_key_lo = 0,
            .min_key_hi = MAX_UINT64,
            .min_key_lo = MAX_UINT64,
            .seq_no_ = 0,
            .entry_num_ = 0};
#else
    meta_ =
        {
            .datablock_ptr_ = 0,
            .max_key_ = 0,
            .min_key_ = MAX_UINT64,
            .seq_no_ = 0,
            .entry_num_ = 0};
#endif
}

/**
 * @brief need flush before this
 *
 */
void PSTBuilder::PersistCheckpoint()
{
    data_writer_->PersistCheckpoint();
}
