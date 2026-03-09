#include "db.h"
#include "db/log_writer.h"
#include "db/log_reader.h"
#include "db/compaction/version.h"
#include <mutex>
#include <algorithm>

namespace {

bool IsDeletedEncodedValue(uint64_t raw_value)
{
#ifdef KV_SEPARATE
    ValuePtr value_ptr{};
    value_ptr.data_ = raw_value;
    return value_ptr.detail_.valid == 0;
#else
    return raw_value == INVALID_PTR;
#endif
}

}

/*******************MYDBClient***********************/
MYDBClient::MYDBClient(MYDB *db, int tid) : db_(db), thread_id_(tid), log_writer_(new LogWriter(db->segment_allocator_, db_->current_memtable_idx_)), log_reader_(new LogReader(db->segment_allocator_)), pst_reader_(new PSTReader(db->segment_allocator_, db->use_direct_io_))
{
    current_memtable_idx_ = db_->current_memtable_idx_;
    db_->mem_index_[current_memtable_idx_]->ThreadInit(tid);
    for (auto &num : put_num_in_current_memtable_)
    {
        num = 0;
    }
}
MYDBClient::~MYDBClient()
{
    db_->AddTempMemtableSize(current_memtable_idx_, put_num_in_current_memtable_[current_memtable_idx_]);
    delete log_writer_;
    delete log_reader_;
    delete pst_reader_;
    std::lock_guard<SpinLock> lock(db_->client_lock_);
    db_->client_list_[thread_id_] = nullptr;
    DEBUG("close client %d", thread_id_);
}

/**
 * @brief 
 * 
 * @param key 
 * @param value 
 * @param slow add an latency between log writing and index updating to validate crash consistency
 * @return true 
 * @return false 
 */
bool MYDBClient::Put(const Slice key, const Slice value, bool slow)
{
    bool memtable_idx_changed = StartWrite();
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
#else
    KeyType int_key = key.ToUint64();
#endif
    // if active log_group is changed, first allocate new segment
    if (unlikely(memtable_idx_changed))
    {
        log_writer_->SwitchToNewSegment(current_memtable_idx_);
    }

    LSN lsn;
#ifdef INDEX_LOG_MEMTABLE
    lsn = db_->GetLSN(int_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    lsn = db_->LSN_lock(int_key);
#endif
    uint64_t log_ptr = log_writer_->WriteLogPut(key, value, lsn);
    LOG("put log_ptr = %lu", log_ptr);

    // index updade
#ifdef INDEX_LOG_MEMTABLE
    ValuePtr vp{.detail_ = {.valid = 1,
                            .ptr = log_ptr/logbuffersize*LogNumperBlock+log_ptr%logbuffersize/LOG_ENTRY_SIZE,
                            .lsn = lsn.lsn}};
    ValueHelper lh(vp.data_);
    db_->mem_index_[current_memtable_idx_]->PutValidate(int_key, lh);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    ValueHelper lh(value.ToUint64());
    db_->mem_index_[current_memtable_idx_]->Put(int_key, lh);
    db_->LSN_unlock(lsn.epoch);
#endif
    put_num_in_current_memtable_[current_memtable_idx_]++;
    FinishWrite();
    total_writes_.fetch_add(1);
    return true;
}
void MYDBClient::Persist_Log()
{
    log_writer_->PersistNowSegment();
}

bool MYDBClient::Delete(const Slice key)
{
    bool changed = StartWrite();
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
#else
    KeyType int_key = key.ToUint64();
#endif
    // if active log_group is changed, first allocate new segment
    if (unlikely(changed))
    {
        log_writer_->SwitchToNewSegment(current_memtable_idx_);
    }
    LSN lsn;
#ifdef INDEX_LOG_MEMTABLE
    lsn = db_->GetLSN(int_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    lsn = db_->LSN_lock(int_key);
#endif
    uint64_t log_ptr = log_writer_->WriteLogDelete(key, lsn);
    LOG("put log_ptr = %lu", log_ptr);
#ifdef INDEX_LOG_MEMTABLE
    ValuePtr vp{.detail_ = {.valid = 0,
                            .ptr = log_ptr/logbuffersize*LogNumperBlock+log_ptr%logbuffersize/LOG_ENTRY_SIZE,
                            .lsn = lsn.lsn}};
    ValueHelper lh(vp.data_);
    db_->mem_index_[current_memtable_idx_]->PutValidate(int_key, lh);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    ValueHelper lh(INVALID_PTR);
    db_->mem_index_[current_memtable_idx_]->Put(int_key, lh);
    db_->LSN_unlock(lsn.epoch);
#endif
    FinishWrite();
    total_writes_.fetch_add(1);
    return true;
}

bool MYDBClient::Get(const Slice key, Slice &value_out)
{
    total_reads_.fetch_add(1);
    const MemtableLookupResult memtable_result = GetFromMemtable(key, value_out);
    if (memtable_result == MemtableLookupResult::kFound)
    {
        MYDB::global_get_success_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (memtable_result == MemtableLookupResult::kDeleted)
    {
        MYDB::global_get_failure_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    int size;
    int ret = db_->current_version_->Get(key, value_out.data(), &size, pst_reader_);
    if (ret) {
        MYDB::global_get_success_.fetch_add(1, std::memory_order_relaxed);
    } else {
        MYDB::global_get_failure_.fetch_add(1, std::memory_order_relaxed);
    }
    return ret;
}

MYDBClient::MemtableLookupResult MYDBClient::GetFromMemtable(const Slice key, Slice &value_out)
{
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
    LOG("Get from memtable");
#else
    KeyType int_key = key.ToUint64();
    LOG("Get %lu(%lu) from memtable", key.ToUint64(), key.ToUint64Bswap());
#endif
    ValuePtr vptr;
    // NOTE:不同步到成员变量，成员变量current_memtable_idx只由写操作改变，避免出现bug
    int current_memtable_id = db_->current_memtable_idx_;
    // get from index
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        int memtable_id = (current_memtable_id - i + MAX_MEMTABLE_NUM) % MAX_MEMTABLE_NUM;
        if (db_->memtable_states_[memtable_id].state == MemTableStates::EMPTY)
        {
            break;
        }
        vptr.data_ = db_->mem_index_[memtable_id]->Get(int_key);

#ifdef INDEX_LOG_MEMTABLE
        if (vptr.data_ == INVALID_PTR) // check tombstone
            continue;
        if (vptr.detail_.valid == 0)
            return MemtableLookupResult::kDeleted;
        // get from log - 直接写入调用方缓冲区避免悬空指针
        size_t value_sz = log_writer_->ReadBufferedValue(key, vptr, (char*)value_out.data());
        if (value_sz == 0)
        {
            value_sz = log_reader_->ReadLogForValue(key, vptr, (char*)value_out.data());
        }
        if (value_sz == 0)
            return MemtableLookupResult::kMiss;
#endif
#ifdef BUFFER_WAL_MEMTABLE
        if (vptr.data_ == INVALID_PTR) // check tombstone
            continue;
        memcpy((void *)value_out.data(), &(vptr.data_), 8);
#endif
        return MemtableLookupResult::kFound;
    }
    LOG("memtable over");
    return MemtableLookupResult::kMiss;
}

int MYDBClient::Scan(const Slice start_key, int scan_sz, std::vector<KeyType> &key_out)
{
    // TODO: Wait for flush/compaction over and no level0_tree
    db_->WaitForFlushAndCompaction();
    ValuePtr vptr;
    std::vector<KeyType> keys_mem[MAX_MEMTABLE_NUM], keys_level;
    std::vector<uint64_t> values_mem[MAX_MEMTABLE_NUM], values_level;
#if defined(FLOWKV_KEY16)
    KeyType scan_start_key = start_key.ToKey16();
#else
    KeyType scan_start_key = start_key.ToUint64();
#endif
    // get from index
    int cur = db_->current_memtable_idx_;
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        int memidx = (cur + i) % MAX_MEMTABLE_NUM;
        if (db_->mem_index_[memidx] != nullptr)
            db_->mem_index_[memidx]->Scan2(scan_start_key, scan_sz, keys_mem[i], values_mem[i]);
    }
    std::vector<TaggedPstMeta> table_metas;
    RowIterator *level_row = db_->current_version_->GetLevel1Iter(start_key, pst_reader_, table_metas);
#if defined(FLOWKV_KEY16)
    KeyType int_start_key = start_key.ToKey16();
    LOG("level row.valid=%d, current_key=%lu:%lu\n", level_row->Valid(), level_row->GetCurrentKey().hi, level_row->GetCurrentKey().lo);
#else
    KeyType int_start_key = start_key.ToUint64();
    LOG("level row.valid=%d, current_key=%lu\n", level_row->Valid(), __bswap_64(level_row->GetCurrentKey()));
#endif
    // skip keys lower than start_key in the pst
    while (level_row->Valid() && KeyTypeLess(level_row->GetCurrentKey(), int_start_key))
    {
        // printf("skip %lu , %lu\n", __bswap_64(level_row->GetCurrentKey()),level_row->GetCurrentKey());
        level_row->NextKey();
        // TODO: if return false value, GetLevel1Iter maybe can't get enough KVs from 2 pst
    }

    struct KeyWithRowId
    {
        KeyType key;
        int row_id;
    };
    struct UintKeyComparator
    {
        bool operator()(const KeyWithRowId l, const KeyWithRowId r) const
        {
            if (unlikely(CompareKeyType(l.key, r.key) == 0))
            {
                return l.row_id < r.row_id;
            }
            return CompareKeyType(l.key, r.key) > 0;
        }
    } scancmp;

    // merge iterator
    KeyWithRowId ik;
    std::priority_queue<KeyWithRowId, std::vector<KeyWithRowId>, UintKeyComparator> key_heap(scancmp);
    int mem_idx[MAX_MEMTABLE_NUM];
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        if (!keys_mem[i].empty())
        {
            key_heap.push(KeyWithRowId{keys_mem[i][0], i});
            mem_idx[i] = 0;
        }
    }
    if (level_row->Valid())
    {
#if defined(FLOWKV_KEY16)
        LOG("push %lu:%lu\n", level_row->GetCurrentKey().hi, level_row->GetCurrentKey().lo);
#else
        LOG("push %lu\n", __bswap_64(level_row->GetCurrentKey()));
#endif
        key_heap.push(KeyWithRowId{level_row->GetCurrentKey(), 99});
    }

    while (scan_sz > 0 && !key_heap.empty())
    {
        auto topkey = key_heap.top();
        key_heap.pop();
        while (!key_heap.empty() && key_heap.top().key == topkey.key)
        {
            // 如果出现重合key，旧key直接next
            if (topkey.row_id == 99)
            {
                if (level_row->NextKey())
                {
                    key_heap.push(KeyWithRowId{level_row->GetCurrentKey(), 99});
                }
            }
            else if (keys_mem[topkey.row_id].size() > ++mem_idx[topkey.row_id])
            {
                key_heap.push(KeyWithRowId{keys_mem[topkey.row_id][mem_idx[topkey.row_id]], topkey.row_id});
            }
            topkey = key_heap.top();
            key_heap.pop();
        }
        // read the value of top_key
        // TODO: If kv separate, read log.
        bool deleted = false;
        if (topkey.row_id == 99)
        {
            deleted = level_row->GetCurrentValue().IsTombstone();
            if (level_row->NextKey())
            {
                key_heap.push(KeyWithRowId{level_row->GetCurrentKey(), 99});
            }
        }
        else
        {
            const uint64_t value = values_mem[topkey.row_id][mem_idx[topkey.row_id]];
            deleted = IsDeletedEncodedValue(value);
            mem_idx[topkey.row_id]++;
            if (mem_idx[topkey.row_id] < keys_mem[topkey.row_id].size())
            {
                key_heap.push(KeyWithRowId{keys_mem[topkey.row_id][mem_idx[topkey.row_id]], topkey.row_id});
            }
        }

        if (deleted)
        {
            continue;
        }

        key_out.push_back(topkey.key);
        scan_sz--;
    }
    delete level_row;
    return true;
}

inline bool MYDBClient::StartWrite()
{
    // 1 get memtable idx
    int old = current_memtable_idx_;
    current_memtable_idx_ = db_->current_memtable_idx_;
	// if memtable is half-full or full, slow down or stop
    // if(put_num_in_current_memtable_[current_memtable_idx_] > MAX_MEMTABLE_ENTRIES/8){
    //     usleep(10);
    // }
    // while (put_num_in_current_memtable_[current_memtable_idx_] > MAX_MEMTABLE_ENTRIES/4)
    // {
    //     std::this_thread::yield();
    //     current_memtable_idx_ = db_->current_memtable_idx_;
    // }

    // 2 modify thread state
    if (old != current_memtable_idx_)
    {
        // db_->mem_index_[current_memtable_idx_]->ThreadInit(thread_id_);
        return true;
    }
    return false;
}

inline void MYDBClient::FinishWrite()
{

}
