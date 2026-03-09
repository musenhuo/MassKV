#pragma once

#include "segment.h"
#include <filesystem>
#include <unordered_map>
#include <queue>
#include <atomic>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "util/atomic_vector.h"
class SegmentAllocator
{
public:
    const std::string& GetPoolPath() const { return pool_path_; }

private:
    std::string pool_path_;
    const size_t pool_size_;
    BitMap segment_bitmap_;     // persist in the tail of ssd pool
    BitMap log_segment_bitmap_; // a backup bitmap of log segments for fast recovery, persisted after segment_bitmap
    // TODO: modify these cache to a bitmap or a segment tree
    std::queue<SortedSegment *> data_segment_cache_;  // ditto
    AtomicVector<uint64_t> log_segment_group_[MAX_MEMTABLE_NUM];
    int current_log_group_;
    int fd;
    SpinLock mtx_i, mtx_d, mtx_s; // lock the cache for poping element

	std::atomic_uint64_t log_seg_num_=0,sort_seg_num_=0;
    size_t bitmap_persist_every_ = 1;
    std::atomic_uint64_t bitmap_dirty_ops_{0};

    inline void PersistBitmapsNow()
    {
        segment_bitmap_.PersistToSSD();
        log_segment_bitmap_.PersistToSSD();
        bitmap_dirty_ops_.store(0, std::memory_order_relaxed);
    }

    inline void NoteBitmapMutation()
    {
        if (bitmap_persist_every_ <= 1)
        {
            PersistBitmapsNow();
            return;
        }

        const uint64_t dirty = bitmap_dirty_ops_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (dirty < bitmap_persist_every_)
        {
            return;
        }

        std::lock_guard<SpinLock> lock(mtx_s);
        if (bitmap_dirty_ops_.load(std::memory_order_relaxed) >= bitmap_persist_every_)
        {
            PersistBitmapsNow();
        }
    }

public:
    SegmentAllocator(std::string pool_path, size_t pool_size, bool recover=false, bool use_direct_io=false) : pool_path_(pool_path), pool_size_(pool_size),  segment_bitmap_(pool_size_ / SEGMENT_SIZE, true), log_segment_bitmap_(pool_size_ / SEGMENT_SIZE, true), current_log_group_(0)
    {
        // NOTE: SegmentAllocator 始终使用普通 I/O（不用 O_DIRECT），因为恢复流程需要非对齐读取
        // O_DIRECT 仅在 DataBlockReader 中用于 PST 读取
        (void)use_direct_io;  // 参数保留以保持 API 兼容性
        size_t mapped_len;
        int open_flags = O_RDWR | O_CREAT;
        fd = open(pool_path.c_str(), open_flags, 0666);
        segment_bitmap_.Setoff(pool_size_);
        log_segment_bitmap_.Setoff(pool_size_ + segment_bitmap_.SizeInByte());
        segment_bitmap_.fd=fd;
        log_segment_bitmap_.fd=fd;
        const char *persist_every_env = std::getenv("FLOWKV_BITMAP_PERSIST_EVERY");
        if (persist_every_env != nullptr)
        {
            const auto parsed = std::strtoull(persist_every_env, nullptr, 10);
            if (parsed > 0)
            {
                bitmap_persist_every_ = static_cast<size_t>(parsed);
            }
        }
        INFO("SegmentAllocator bitmap persist interval=%lu", bitmap_persist_every_);

        if (recover)
        {
            // TODO: segment recover
            DEBUG("segment_bitmap_recover");
            segment_bitmap_.Recover();
            log_segment_bitmap_.Recover();
        }
        else
        {
            PersistBitmapsNow();
        }
    };
    ~SegmentAllocator()
    {
        while (!data_segment_cache_.empty())
        {
            auto &seg = data_segment_cache_.front();
            delete seg;
            data_segment_cache_.pop();
        }
        PersistBitmapsNow();
    };

    bool RecoverLogSegmentAndGetId(std::vector<uint64_t> &seg_id_list)
    {
        // TODO: for crash-consitency, check if elements of log_segment_bitmap exist in segment_bitamp. Make them consistent.
        std::vector<uint64_t> used_bits;
        bool ret = log_segment_bitmap_.GetUsedBits(used_bits);
        seg_id_list.clear();
        DEBUG("used_bits.size=%lu", used_bits.size());
        for (auto &id : used_bits)
        {
            LogSegment::Header header{};
            char header_buf[logbuffersize];
            auto read_ret = pread(fd, header_buf, logbuffersize, id * SEGMENT_SIZE);
            if (read_ret != logbuffersize)
            {
                continue;
            }
            memcpy(&header, header_buf, sizeof(header));
            if (header.segment_status == StatusAvailable || header.segment_status == StatusFree)
            {
                continue;
            }
            seg_id_list.push_back(id);
            log_segment_group_[current_log_group_].add(id);
        }
        return ret;
    }
    void PrintLogStats(){
        printf("-------valid log stats------\n");
        std::vector<uint64_t> seg_id_list;
        bool ret = log_segment_bitmap_.GetUsedBits(seg_id_list);
        DEBUG("used_bits.size=%lu(%lu)",seg_id_list.size(),log_segment_bitmap_.GetUsedBitsNum());
        for (auto &id : seg_id_list)
        {
            printf("%lu,",id);
        }
        printf("\n");
    }

    bool RedoFlushLog(std::vector<uint64_t> &deleted_seg_id_list)
    {
        for (auto &id : deleted_seg_id_list)
        {
            DEBUG("clean log segment %lu", id);
            auto log_seg = GetLogSegment(id);
            if (log_seg != nullptr)
            {
#ifdef KV_SEPARATE
                CloseSegment(log_seg, true);
#else
                FreeSegment(log_seg);
#endif
            }
        }
        return true;
    }

    LogSegment *AllocLogSegment(int group_id)
    {
        size_t id = segment_bitmap_.AllocateOne();
        log_segment_bitmap_.AllocatePos(id);
        NoteBitmapMutation();
        LOG("allocate log segment id=%lu", id);
        // printf("allocate log segment id=%lu\n", id);
        if (id == ERROR_CODE)
        {
            ERROR_EXIT("log segment allocation failed, space not enough");
        }
        log_segment_group_[group_id].add(id);
		log_seg_num_++;
        return new LogSegment(fd, id);
    };
    SortedSegment *AllocSortedSegment(int page_size)
    {
        // reuse unfilled segment with segment cache
        std::lock_guard<SpinLock> lock(mtx_d);
        const size_t cache_size = data_segment_cache_.size();
        for (size_t i = 0; i < cache_size; ++i) {
            auto* p = data_segment_cache_.front();
            data_segment_cache_.pop();
            if (static_cast<int>(p->PAGE_SIZE) == page_size) {
                p->Reuse();
                assert(!p->Full());
                return p;
            }
            // Keep non-matching page-size segments in cache for future compatible allocations.
            data_segment_cache_.push(p);
        }
        // alloc new segment
        size_t id = segment_bitmap_.AllocateOne();
        LOG("allocate sorted segment id=%lu", id);
        NoteBitmapMutation();
        if (id == ERROR_CODE)
        {
            ERROR_EXIT("index segment allocation failed, space not enough!");
        }
        PBlockType type = INVALID_NODE;
        type = PBlockType::DATABLOCK16K;
		sort_seg_num_++;
        SortedSegment *seg = new SortedSegment(fd, id, type, page_size);
        return seg;
    };

    bool CloseSegment(LogSegment *&seg, bool avail = 0)
    {
        if (avail)
        {
            seg->Avail();
        }
        else
        {
            seg->Close();
        }
        delete seg;
        seg = nullptr;
        return true;
    };
    bool CloseSegment(SortedSegment *&seg)
    {
        LOG("close segment id=%lu, full=%d", seg->segment_id_, seg->Full());
        if (!seg->Full())
        {
            seg->Freeze();
            auto type = seg->type();
            if (type == PBlockType::DATABLOCK16K)
            {
                std::lock_guard<SpinLock> lock(mtx_d);
                data_segment_cache_.emplace(seg);
            }
            return true;
        }
        seg->Close();
        delete seg;
        seg = nullptr;
        return true;
    };
    bool CloseSegmentForDelete(SortedSegment *&seg)
    {
        // TODO: may have bugs when remove this to allow concurrent allocate and free page in a shared segment
        //  while (seg->status() == StatusUsing)
        //  {
        //      usleep(1000);
        //      seg->RecoverHeader();
        //  }

        if (seg->status() == StatusAvailable || seg->status() == StatusUsing)
        {
            seg->PersistBitmapSoft();
            delete seg;
            seg = nullptr;
            return true;
        }
        if (seg->status() == StatusClosed)
        {
            seg->Freeze();
            auto type = seg->type();
            seg->SetForDelete(false);
            if (type == PBlockType::DATABLOCK16K)
            {
                std::lock_guard<SpinLock> lock(mtx_d);
                data_segment_cache_.emplace(seg);
				DEBUG("reuse segment %lu",seg->segment_id_);
            }
            return true;
        }
        ERROR_EXIT("segment open for delete have no data %d", seg->status());
    }

    bool FreeSegment(LogSegment *&seg)
    {
        // TODO: need finer-grainded persist I/O for bitmap
        LOG("free log segment id=%lu", seg->segment_id_);
        seg->Free();
        segment_bitmap_.Free(seg->segment_id_);
        auto ret=log_segment_bitmap_.Free(seg->segment_id_);
        assert(ret);
        NoteBitmapMutation();
        delete seg;
		log_seg_num_--;
        return true;
    };
    LogSegment *GetLogSegment(size_t id)
    {
        if (!segment_bitmap_.Exist(id))
        {
            // TODO: process return value at each call of this function
            return nullptr;
        }
        return new LogSegment(fd, id, true);
    };
    SortedSegment *GetSortedSegment(size_t id, size_t page_size)
    {
        if (!segment_bitmap_.Exist(id))
        {
            ERROR_EXIT("try to get unallocted log segment %lu", id);
        }
        SortedSegment *seg;
        seg = new SortedSegment(fd, id, INVALID_NODE, page_size, true);
        return seg;
    };

    inline size_t TrasformOffsetToId(size_t offset)
    {
        return offset / SEGMENT_SIZE;
    };

    SortedSegment *GetSortedSegmentForDelete(size_t id, size_t page_size)
    {
        if (!segment_bitmap_.Exist(id))
        {
            // Segment already freed (e.g., during previous run), safe to skip
            DEBUG("GetSortedSegmentForDelete: segment %lu not in bitmap, already freed", id);
            return nullptr;
        }
        SortedSegment *seg;
        seg = new SortedSegment(fd, id, OPEN_FOR_DELETE, page_size, true);
        return seg;
    }

    int Getfd() { return fd; };

    void ClearLogGroup(int idx)
    {
        LOG("clear log group: %lu log segment, %lu remaining",log_segment_group_[idx].size(),log_segment_bitmap_.GetUsedBitsNum());
        log_segment_group_[idx].clear();
    }

    void GetElementsFromLogGroup(int idx, std::vector<size_t> *list)
    {
        log_segment_group_[idx].get_elements(list);
    }


	void PrintSSDUsage(){
		size_t used = segment_bitmap_.GetUsedBitsNum();
		size_t freed = data_segment_cache_.size();
		size_t usage = (used - freed) * SEGMENT_SIZE;
		printf("[Segment allocator] SSD usage is %lu MB, inbitmap=%lu,instack=%lu,log=%lu,sort=%lu\n",usage / 1024 /1024,used,freed,log_seg_num_.load(),sort_seg_num_.load());
	}


private:
};
