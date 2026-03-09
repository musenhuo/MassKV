#include "pst_deleter.h"

PSTDeleter::PSTDeleter(SegmentAllocator *seg_allocator) : seg_allocator_(seg_allocator) {}
PSTDeleter::~PSTDeleter() { PersistCheckpoint(); }

bool PSTDeleter::DeletePST(PSTMeta meta)
{
    if (meta.datablock_ptr_ == 0) {
        // Invalid PST, skip
        return true;
    }
    
    // Validate datablock_ptr: must be at least segment header size within segment
    // SEGMENT_SIZE = 4MB, minimum valid offset within segment is ~16KB (header+bitmap)
    size_t offset_in_seg = meta.datablock_ptr_ % SEGMENT_SIZE;
    if (offset_in_seg < 32768) {  // Skip if offset in segment is too small (invalid PST record)
        return true;
    }
    
    size_t data_seg_id = seg_allocator_->TrasformOffsetToId(meta.datablock_ptr_);
    SortedSegment *data_seg = nullptr;
    
    // Limit search to avoid O(n^2) on large recover
    size_t search_limit = std::min(used_data_segments_.size(), (size_t)1000);
    for (size_t i = used_data_segments_.size(); i > used_data_segments_.size() - search_limit; ) {
        --i;
        if (used_data_segments_[i] != nullptr && used_data_segments_[i]->segment_id_ == data_seg_id)
        {
            data_seg = used_data_segments_[i];
            break;
        }
    }
    if (data_seg == nullptr)
    {
        data_seg = seg_allocator_->GetSortedSegmentForDelete(data_seg_id,sizeof(PDataBlock));
        if (data_seg == nullptr) {
            // Segment already freed in previous run, skip deletion
            return true;
        }
        used_data_segments_.push_back(data_seg);
    }
    
    size_t page_id = data_seg->TrasformOffsetToPageId(meta.datablock_ptr_);
    // Validate page_id is within reasonable range
    if (page_id > 1000000) {  // Sanity check - no segment has millions of pages
        return true;  // Skip corrupted PST record
    }
    
    data_seg->RecyclePage(page_id);
    return true;
}

bool PSTDeleter::PersistCheckpoint()
{
    //just persist,don't modify the state
    for (auto &seg : used_data_segments_)
    {
        seg_allocator_->CloseSegmentForDelete(seg);
    }
    used_data_segments_.clear();
    return true;
}
