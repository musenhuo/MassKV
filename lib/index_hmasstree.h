#pragma once

#include "util/util.h"
#include "hmasstree/hmasstree_wrapper.h"

/**
 * @brief H-Masstree 索引适配层：将 HMasstreeWrapper 封装为 FlowKV 的 Index 接口。
 *
 * 该类与 MasstreeIndex 具备同等能力（Get/Put/Delete/Scan），但底层使用
 * 你后续修改后的 H-Masstree 版本。
 */
class HMasstreeIndex : public Index
{
public:
    HMasstreeIndex() { mt_ = new HMasstreeWrapper(); }

    virtual ~HMasstreeIndex() override { delete mt_; }

    void HMasstreeThreadInit(int thread_id) { HMasstreeWrapper::thread_init(thread_id); }

    virtual void ThreadInit(int thread_id) override
    {
        HMasstreeThreadInit(thread_id);
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

private:
    HMasstreeWrapper *mt_;

    DISALLOW_COPY_AND_ASSIGN(HMasstreeIndex);
};
