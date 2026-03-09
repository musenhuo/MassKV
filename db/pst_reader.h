/**
 * @file pst_reader.h
 * @author your name (you@domain.com)
 * @brief a wrapper  datablock reader which provides interfaces for reading a certain pst
 * @version 0.1
 * @date 2022-08-31
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "table.h"
#include "datablock_reader.h"

class PSTReader
{
private:
    DataBlockReader datablock_reader_;

public:
    PSTReader(SegmentAllocator *allocator, bool use_direct_io = false);
    ~PSTReader();

    PSTMeta RecoverPSTMeta(uint64_t data_addr);
    bool PointQuery(uint64_t data_addr, Slice key, const char *value_out, int *value_size, uint16_t entry_num_);
    bool PointQueryWindow(uint64_t block_addr,
                          Slice key,
                          const char *value_out,
                          int *value_size,
                          uint16_t entry_num,
                          uint16_t start_entry,
                          uint16_t entry_count);
    
    // Cache statistics
    uint64_t GetCacheHits() const { return datablock_reader_.GetCacheHits(); }
    uint64_t GetCacheMisses() const { return datablock_reader_.GetCacheMisses(); }
    
    class Iterator
    {
    public:
        PSTReader *reader_;
        std::vector<std::pair<KeyType, FixedValue16>> records_;
        int current_record_index_ = 0;
        Iterator(PSTReader *reader, uint64_t offset) : reader_(reader)
        {
            reader_->datablock_reader_.TraverseDataBlock(offset, &records_);
        };
        ~Iterator()
        {
            std::vector<std::pair<KeyType, FixedValue16>>().swap(records_);
        };
        bool Next()
        {
            if (current_record_index_ >= records_.size() - 1)
                return false;
            current_record_index_++;
            return true;        
        };
        KeyType Key() { 
            return records_[current_record_index_].first; }
        FixedValue16 Value() { return records_[current_record_index_].second; }
        bool LastOne()
        {
            if (current_record_index_ >= records_.size() - 1)
                return true;
            return false;
        }
        size_t RecordsSize() const { return records_.size(); }
    };
    Iterator *GetIterator(uint64_t data_addr);
};
#define NotOverlappedMark 100000
struct RowIterator
{
public:
    PSTReader *pst_reader_;
    PSTReader::Iterator *pst_iter_ = nullptr;
    std::vector<TaggedPstMeta> &pst_list_;
    int current_pst_idx_ = 0;

    RowIterator(PSTReader *pst_reader, std::vector<TaggedPstMeta> &pst_list) : pst_reader_(pst_reader), pst_list_(pst_list) {}
    ~RowIterator()
    {
        if (pst_iter_)
            delete pst_iter_;
    }
	/**
	 * @brief Mark the PST as an reused PST so that the cleaning step will not delete it from L1 index.
	 * 
	 */
    void MarkPst() { pst_list_[current_pst_idx_].level = NotOverlappedMark; }
    inline TaggedPstMeta GetPst() { return pst_list_[current_pst_idx_]; }

    bool NextPst()
    {
        current_pst_idx_++;
        if (current_pst_idx_ >= pst_list_.size())
            return false;
        return true;
    }
    void ResetPstIter()
    {
        assert(current_pst_idx_ < pst_list_.size());
        if (pst_iter_ != nullptr) {
            delete pst_iter_;
            pst_iter_ = nullptr;
        }
        pst_iter_ = pst_reader_->GetIterator(GetPst().meta.datablock_ptr_);
    }

    KeyType GetCurrentKey()
    {
        if (pst_iter_)
        {
            return pst_iter_->Key();
        }
        return GetPst().meta.MinKey();
    }

    FixedValue16 GetCurrentValue()
    {
        if (!pst_iter_)
            ResetPstIter();
        return pst_iter_->Value();
    }

    bool NextKey()
    {
        if (current_pst_idx_ >= pst_list_.size())
            return false;
        if (!pst_iter_)
            ResetPstIter();

        if (pst_iter_->Next())
        {
            return true;
        }
        delete pst_iter_;
        pst_iter_ = nullptr;
        return NextPst();
    }
    bool Valid()
    {
        return current_pst_idx_ < pst_list_.size();
    }

	/**
	 * @brief move current_pst_index_ to the pst which have the range with key
	 * 
	 * @param key 
	 * @return true 
	 * @return false 
	 */
    bool MoveTo(const KeyType &key){
        if(current_pst_idx_ >= pst_list_.size())return false;
        while (KeyTypeLess(pst_list_[current_pst_idx_].meta.MaxKey(), key))
        {
            current_pst_idx_++;
            if(current_pst_idx_ >=  pst_list_.size())return false;
        }
        return true;
    }
};
