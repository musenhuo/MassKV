/**
 * @file datablock_writer.h
 * @author your name (you@domain.com)
 * @brief 负责持久数据块的写入。管理多种粒度datablock的写入（目前只支持定长类型）。
 * 在需要用到时lazy地通过segment allocator获得对应粒度的data segment的写权限。
 * @version 0.1
 * @date 2022-08-29
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"
#include <vector>

class DataBlockWriter
{
private:
    struct PendingBlockWrite
    {
        off_t page_addr = -1;
        PDataBlock block{};
    };

    static constexpr size_t kBlockBytes = sizeof(PDataBlock);
    static constexpr size_t kWriteBatchBytes = 128 * 1024;  // 128KB

    SegmentAllocator *seg_allocator_;
    SortedSegment *current_segment_;
    PDataBlockWrapper blocks_buf_;
    std::vector<SortedSegment*> used_segments_;
    std::vector<PendingBlockWrite> pending_writes_;
    size_t pending_bytes_ = 0;
	int num = 0;
    int fd;

public:
    DataBlockWriter(SegmentAllocator *allocator);
    ~DataBlockWriter();

    virtual bool AddEntry(Slice key, Slice value);
    virtual KeyType GetCurrentMinKey();
    virtual KeyType GetCurrentMaxKey();
    virtual KeyType GetKey(size_t idx);
    virtual uint64_t Flush();
    virtual int PersistCheckpoint();
	virtual int Empty();

private:
    virtual void allocate_block();
    void QueueCurrentBlockWrite(off_t block_addr);
    bool FlushPendingWrites(bool force);
};
