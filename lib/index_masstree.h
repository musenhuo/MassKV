#pragma once

#include "util/util.h"
#include "masstree/masstree_wrapper.h"

class MasstreeIndex : public Index
{
public:
    MasstreeIndex() { mt_ = new MasstreeWrapper(); }

    virtual ~MasstreeIndex() override { delete mt_; }

    void MasstreeThreadInit(int thread_id) { MasstreeWrapper::thread_init(thread_id); }

    virtual void ThreadInit(int thread_id) override
    {
        MasstreeThreadInit(thread_id);
    };

    virtual ValueType Get(const KeyType key) override
    {
        ValueType val;
        bool found = mt_->search(key, val);
        if (found)
        {
            return val;
        }
        else
        {
            return INVALID_PTR;
        }
    }

    virtual void Put(const KeyType key, ValueHelper &le_helper)
    {
        mt_->insert(key, le_helper);
    }
    
    virtual void PutValidate(const KeyType key, ValueHelper &le_helper)
    {
        mt_->insert_validate(key, le_helper);
    }

    virtual void Delete(const KeyType key) override
    {
        mt_->remove(key);
    }

    virtual void Scan(const KeyType key, int cnt,
                      std::vector<ValueType> &vec) override
    {
        mt_->scan(key, cnt, vec);
    }
    virtual void Scan2(const KeyType key, int cnt, std::vector<KeyType> &kvec, std::vector<ValueType> &vvec) override
    {
        mt_->scan(key, cnt, kvec, vvec);
    }
    virtual void ScanByRange(const KeyType start, const KeyType end, std::vector<KeyType> &kvec, std::vector<ValueType> &vvec) override
    {
        mt_->scan(start, end, kvec, vvec);
    }

    virtual void ForEachEntry(std::function<bool(KeyType, ValueType)> callback) override
    {
#if defined(FLOWKV_KEY16)
        mt_->for_each(KeyType{0, 0}, std::move(callback));
#else
        mt_->for_each(0, std::move(callback));
#endif
    }

    virtual void ForEachEntryInRange(const KeyType start, const KeyType end,
                                     std::function<bool(KeyType, ValueType)> callback) override
    {
        mt_->for_each_range(start, end, std::move(callback));
    }

    size_t DebugEstimateTreeBytes() const
    {
        return mt_ != nullptr ? mt_->EstimateMemoryUsageBytes() : 0;
    }

    size_t DebugEstimateThreadPoolBytes() const
    {
        return mt_ != nullptr ? mt_->EstimateThreadInfoPoolBytes() : 0;
    }

    size_t DebugEstimateTotalBytes() const
    {
        return DebugEstimateTreeBytes() + DebugEstimateThreadPoolBytes();
    }

private:
    MasstreeWrapper *mt_;

    DISALLOW_COPY_AND_ASSIGN(MasstreeIndex);
};
