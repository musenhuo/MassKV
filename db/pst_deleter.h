#pragma once
#include "table.h"
#include "allocator/segment_allocator.h"
#include "blocks/fixed_size_block.h"
class PSTDeleter
{
private:
    SegmentAllocator *seg_allocator_;
    std::vector<SortedSegment *> used_data_segments_;

public:
    PSTDeleter(SegmentAllocator *seg_allocator);
    ~PSTDeleter();

    bool DeletePST(PSTMeta meta);
    bool PersistCheckpoint();
};
