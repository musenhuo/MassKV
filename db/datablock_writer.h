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
    SegmentAllocator *seg_allocator_;
    SortedSegment *current_segment_;
    PDataBlockWrapper blocks_buf_;
    std::vector<SortedSegment*> used_segments_;
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
};

