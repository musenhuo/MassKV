#include "db_common.h"
#include "db/allocator/segment_allocator.h"
#include "db/log_reader.h"
#include "db/pst_builder.h"
#include "db/pst_reader.h"
#include <vector>
class Version;
class Manifest;
class ThreadPoolImpl;
class FlushJob
{
private:
	// input
    Index *memtable_index_;
    int seg_group_id_;
    SegmentAllocator *seg_allocater_;
    Version *version_;
    LogReader log_reader_;
    PSTBuilder pst_builder_;
    PSTReader pst_reader_;
    Manifest *manifest_;
	PartitionInfo *partition_info_;
	// temp
	uint32_t tree_seq_no_;
	std::vector<TaggedPstMeta> output_pst_list_;
	int tree_idx_;
    ThreadPoolImpl* flush_thread_pool_;
    PSTBuilder* partition_pst_builder_[RANGE_PARTITION_NUM];
    std::vector<TaggedPstMeta> partition_outputs_[RANGE_PARTITION_NUM];
public:
    FlushJob(Index *index, int seg_group_id, SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest, PartitionInfo* partition_info,ThreadPoolImpl* thread_pool) : memtable_index_(index), seg_group_id_(seg_group_id), seg_allocater_(seg_alloc), version_(target_version), log_reader_(seg_allocater_), pst_builder_(seg_allocater_),pst_reader_(seg_allocater_), manifest_(manifest),partition_info_(partition_info),flush_thread_pool_(thread_pool)
    {	
    }
    ~FlushJob();

    //use index to build persistent index blocks
    bool run();
    bool subrun(int partition_id);
    bool subrunParallel();
    static void TriggerSubFlush(void *arg);
private:
	inline void FlushPST();
};
