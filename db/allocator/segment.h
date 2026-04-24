#pragma once

#include "bitmap.h"
#include <vector>
#include <map>
#include <assert.h>
#include <mutex>
#include <sys/param.h>
#include <queue>


#define logbuffersize 16384

// The meta data of basic segment can be persisted.
// The allocators(log allocator and sorted table allocator) only needs to maintain the shared global allocation table
class BaseSegmentMeta
{
public:
    BaseSegmentMeta(int fd_, size_t segment_id)
        : segment_id_(segment_id), off(segment_id * SEGMENT_SIZE), fd(fd_)
    {
        
    }

    virtual ~BaseSegmentMeta() {}

    const size_t segment_id_;

protected:
    int fd;
    off_t off;
    DISALLOW_COPY_AND_ASSIGN(BaseSegmentMeta);
};
enum SegmentStatus
{
    StatusFree = 0,
    StatusAvailable = 1, // allocated but not used
    StatusUsing = 2,     // writing, other thread cannot write/reopen for delete
    StatusClosed = 3     // closed, read-only
};
enum PBlockType
{
    // TODO: dont use this to denote index and data granularity
    INVALID_NODE = 0,
    INDEX512_TO_ENTRY = 1,
    INDEX512_TO_BLOCK512 = 2,
    INDEX512_TO_BLOCK4K = 3,
    DATABLOCK64 = 4,
    DATABLOCK128 = 5,
    DATABLOCK256 = 6,
    DATABLOCK16K = 7,
    DATABLOCK4K = 8,
    LOG = 9,
    OPEN_FOR_DELETE = 10
};
// The entries in log segment should be self-described (can be recovered if footer is loss).
// This can be realized by specific log entry format. See log_writer.h/cc.

class LogSegment : public BaseSegmentMeta
{
public:
    struct Header
    {
        // uint32_t offset; // only valid when status is closed
        uint32_t segment_status : 2;
        uint32_t segment_block_type : 6;
        uint32_t objects_tail_offset : 24;
        uint32_t magic : 32;
    };
    

public:
    LogSegment(int fd, size_t segment_id, bool exist = 0)
        : BaseSegmentMeta(fd, segment_id), tail_(off+logbuffersize)
    {
        buf_ = static_cast<char*>(aligned_alloc(4096,logbuffersize));
        // TODO: recover footer by read the persisted footer
        if (exist)
        {
            auto ret = pread(fd, buf_, logbuffersize, off);
            if(ret!=logbuffersize)
                std::cout<<"log header wrong"<<std::endl; 
            memcpy(&header_, buf_, sizeof(Header));
            header_.segment_status = StatusUsing;
            PersistHeader();
        }
        else
        {
            LOG("create log segment %lu at %lu(+%lu)", segment_id_, off, segment_id_ * SEGMENT_SIZE);
            header_.segment_status = StatusUsing;
            header_.segment_block_type = LOG;
            header_.objects_tail_offset = 0;
            PersistHeader();
        }
    };
    ~LogSegment() { free(buf_); }
    void Avail()
    {
        // index is persisted, can be gc
        header_.segment_status = StatusAvailable;
        PersistHeader();
    }
    void Close()
    {
        const uint32_t used_bytes = static_cast<uint32_t>(tail_ - off + buffer_offset_);
        if (buffer_offset_ > 0)
        {
            auto ret = pwrite(fd, buf_, logbuffersize, tail_);
            if(ret!=logbuffersize)
                std::cout<<"segment close wrong"<<std::endl;
            tail_ += logbuffersize;
        }
        header_.segment_status = StatusClosed;
        header_.objects_tail_offset = used_bytes;
        // TODO: compute checksum
        PersistHeader();
    }


    /**
     * @brief fast-persist log entry
     *
     * @param data
     * @param size
     * @return int return offset, -1 represent overflow
     */
    int Append(const char *data, size_t size)
    {
        // Check if the buffer is full
        if (buffer_offset_ + size > logbuffersize)
        {
            // Flush the buffer to the file
            auto ret = pwrite(fd, buf_, logbuffersize, tail_);
            if(ret!=logbuffersize)
                std::cout<<"segment append wrong"<<std::endl; 
            tail_ += logbuffersize;
            buffer_offset_ = 0;
        }
        if(tail_>=off+SEGMENT_SIZE)
            return -1;
        // Copy data to the buffer
        memcpy(buf_ + buffer_offset_, data, size);
        // auto ret = pwrite(fd, buf_, 4096, tail_);
        // if(ret!=4096)
        //     std::cout<<"segment append wrong"<<std::endl;         
        buffer_offset_ += size;
        return tail_ + buffer_offset_ - size - off;
    }


    bool Free()
    {
        LOG("free log segment %lu at %lu(+%lu)", segment_id_, off, segment_id_ * SEGMENT_SIZE);
        header_.segment_status = StatusFree;
        header_.objects_tail_offset = 0;
        header_.magic = 0;
        PersistHeader();
        return true;
    }

    Header GetHeader(){
        return header_;
    }

    int Getfd(){
        return fd;
    }

    off_t Getoff(){
        return off;
    }

    bool TryReadBuffered(off_t abs_offset, size_t size, char *output) const
    {
        if (output == nullptr)
        {
            return false;
        }
        if (abs_offset < tail_)
        {
            return false;
        }
        const off_t buffered_end = tail_ + static_cast<off_t>(buffer_offset_);
        if (abs_offset + static_cast<off_t>(size) > buffered_end)
        {
            return false;
        }
        std::memcpy(output, buf_ + (abs_offset - tail_), size);
        return true;
    }

private:
    Header header_;
    off_t tail_;
    char *buf_ = nullptr;
    size_t buffer_offset_ = 0;
    inline void PersistHeader()
    {
        memcpy(buf_, &header_, sizeof(Header));
        auto ret = pwrite(fd, buf_, logbuffersize, off);
        if(ret!=logbuffersize)
            std::cout<<"PersistHeader wrong"<<std::endl;        
    }
};


static SpinLock write_delete_locks[1024];
/**
 * @brief 512 Bytes header + 1024 Bytes bitmap + 8189 * 512-Byte blocks = 4 MB
 *
 */
class SortedSegment : public BaseSegmentMeta
{
public:
    struct Header // 8 Bytes
    {
        uint16_t segment_status : 2;
        uint64_t segment_block_type : 6;
    };
    const size_t PAGE_SIZE;
    const size_t EXTRA_PAGE_NUM;
    const size_t PAGE_NUM;

private:
    Header header_;
    BitMap bitmap_;
    char *buf_;
    bool for_delete_ = false;
    off_t data_;

public:
    /**
     * @brief Construct a new Sorted Segment object
     *
     * @param segment_pool_addr segment head addr
     * @param segment_id segment index
     * @param type data block type. if exist = true, this will be disabled
     * @param exist if exist, recover the header and bitmap from storage
     */
    SortedSegment(int fd, size_t segment_id, PBlockType type, int page_size, bool exist = 0)
        : BaseSegmentMeta(fd, segment_id),
          PAGE_SIZE(page_size),
          EXTRA_PAGE_NUM(1 + roundup(SEGMENT_SIZE / PAGE_SIZE / 8, PAGE_SIZE) / PAGE_SIZE),
          PAGE_NUM(SEGMENT_SIZE / PAGE_SIZE - EXTRA_PAGE_NUM),
          bitmap_(PAGE_NUM), 
          data_(off + EXTRA_PAGE_NUM * PAGE_SIZE)
    {
        LOG("new sorted segment, id=%lu,off=%lu", segment_id_, off);
        buf_ = static_cast<char*>(aligned_alloc(4096,PAGE_SIZE));
        bitmap_.Setoff(off+PAGE_SIZE);
        bitmap_.fd=fd;
        if (exist)
        {
            bitmap_.Recover();
            auto ret = pread(fd, buf_, PAGE_SIZE, off);
            if(ret!=PAGE_SIZE)
                std::cout<<"log header wrong"<<std::endl; 
            memcpy(&header_, buf_, sizeof(Header));
            if (type == OPEN_FOR_DELETE)
                for_delete_ = true;
        }
        else
        {
            if (type == INVALID_NODE)
            {
                ERROR_EXIT("error type of index segment");
            }
            LOG("create sorted segment %lu at %lu(+%lu)", segment_id, off, segment_id * SEGMENT_SIZE);
            header_.segment_status = StatusUsing;
            header_.segment_block_type = type;
            PersistBitmapHard();
        }
    }
    ~SortedSegment() { free(buf_); }
    SegmentStatus status() { return (SegmentStatus)header_.segment_status; }
    PBlockType type() { return (PBlockType)header_.segment_block_type; }
    void RecoverHeader() { 
        auto ret = pread(fd, buf_, PAGE_SIZE, off);
        if(ret!=PAGE_SIZE)
            std::cout<<"log header wrong"<<std::endl; 
        memcpy(&header_, buf_, sizeof(Header));
    }
    void SetForDelete(bool value) { for_delete_ = value; }
    /**
     * @brief
     *
     * @return char* page start addr, nullptr denotes no free page.
     */
    off_t AllocatePage()
    {
        size_t id = bitmap_.AllocateOne();
        if (id == ERROR_CODE)
        {
            return -1;
        }

        return data_ + id * PAGE_SIZE;
    }

    /**
     * @brief allocate consecutive pages
     *
     * @param num
     * @return char* start addr of the allocated pages, nullptr denotes no enough free pages.
     */
    off_t BatchAllocatePage(size_t num)
    {
        assert(num <= PAGE_NUM);
        size_t id = bitmap_.AllocateMany(num);
        if (id == ERROR_CODE)
            return -1;
        return data_ + id * PAGE_SIZE;
    }

    /**
     * @brief free a page
     *
     * @param id
     * @return true
     * @return false
     */
    bool RecyclePage(size_t id)
    {
        assert(id <= PAGE_NUM);
        LOG("recycle page seg %lu id=%lu", segment_id_, id);
        bool ret = bitmap_.Free(id);
        if (!ret)
        {
            // During recover, the page may have already been freed in a previous run
            // but manifest still has stale PST records. Warn instead of crash.
            LOG("[WARN] RecyclePage: page already free, offset=%lu,seg=%lu,id=%lu", 
                1536 + SEGMENT_SIZE * segment_id_ + PAGE_SIZE * id, segment_id_, id);
        }
        return ret;
    }
    void Close()
    {
        header_.segment_status = StatusClosed;
        PersistBitmapSoft();
        PersistHeader();
    }
    void Freeze()
    {
        header_.segment_status = StatusAvailable;
        PersistHeader();
        PersistBitmapSoft();
    }
    void Reuse()
    {
        header_.segment_status = StatusUsing;
        PersistHeader();
        bitmap_.Recover();
    }
    bool Full()
    {
        return bitmap_.IsFull();
    }
    size_t DebugBitmapFreeListCapacityBytes() const
    {
        return bitmap_.freed_bits_.capacity() * sizeof(size_t);
    }
    size_t DebugBitmapArrayBytes() const
    {
        return bitmap_.SizeInByte() * 2;
    }
    void PersistBitmapSoft()
    {
        std::lock_guard<SpinLock> lk(write_delete_locks[segment_id_ % 1024]);
        if (for_delete_)
        {
            bitmap_.PersistToSSDOnlyFree();
        }
        else
        {
            bitmap_.PersistToSSDOnlyAlloc();
        }
    }
    void PersistBitmapHard()
    {
        bitmap_.PersistToSSD();
    }

    inline size_t TrasformOffsetToPageId(size_t offset)
    {
        return ((offset % SEGMENT_SIZE) - EXTRA_PAGE_NUM * PAGE_SIZE) / PAGE_SIZE;
    };

private:
    inline void PersistHeader()
    {
        memcpy(buf_, &header_, sizeof(Header));
        auto ret = pwrite(fd, buf_, PAGE_SIZE, off);
        if(ret!=PAGE_SIZE)
            std::cout<<"PersistHeader wrong"<<std::endl;       
    }
};
