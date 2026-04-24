#include "db_common.h"
#include "db/allocator/segment_allocator.h"
#include "db/log_reader.h"
#include "db/pst_builder.h"
#include "db/pst_reader.h"
#include "db/pst_deleter.h"
#include "lib/hybrid_l1/subtree_record.h"
#include <cstdint>
#include <vector>

class Version;
class Manifest;
class ThreadPoolImpl;
class CompactionJob
{
private:
    SegmentAllocator *seg_allocater_;
    Version *version_;
    Manifest *manifest_;
    PSTBuilder pst_builder_;
    PSTDeleter pst_deleter_;
    const unsigned output_seq_no_;
    bool use_l1_range_scan_records_ = true;
    bool use_l1_delete_covered_only_ = true;
    bool force_serial_compaction_ = false;

    std::vector<std::vector<TaggedPstMeta>> inputs_;
    std::vector<flowkv::hybrid_l1::SubtreeRecord> inputs_l1_records_;
    std::vector<uint64_t> inputs_l1_unique_blocks_;
    std::vector<flowkv::hybrid_l1::SubtreeRecord> add_patch_records_;
    std::vector<flowkv::hybrid_l1::SubtreeRecord> partition_add_patch_records_[RANGE_PARTITION_NUM];
    std::vector<TaggedPstMeta> outputs_;
	std::vector<TaggedPstMeta> partition_outputs_[RANGE_PARTITION_NUM];

    // temp variables
    #if defined(FLOWKV_KEY16)
    KeyType min_key_{MAX_UINT64, MAX_UINT64};
    KeyType max_key_{0, 0};
    #else
    KeyType min_key_ = MAX_UINT64;
    KeyType max_key_ = 0;
    #endif

	//for sub compaction
	PartitionInfo *partition_info_;
    PSTBuilder* partition_pst_builder_[RANGE_PARTITION_NUM];
	ThreadPoolImpl* compaction_thread_pool_;

public:
    CompactionJob(SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest,PartitionInfo* partition_info,ThreadPoolImpl* thread_pool);
    ~CompactionJob();

    bool CheckPmRoomEnough(); // with segment allocator
    /**
     * @brief get all overlapped input psts from version_ to inputs_
     * 
     * @return L0 tree number in the compaction
     */
    size_t PickCompaction();
    /**
     * @brief merge sorting inputs, writing all output psts to ssd
     *          currently, we persist manifests of outputs, but not persist data (for consistency check when recovery)
     * 
     */
    bool RunCompaction();
    bool RunSubCompactionParallel();
	void RunSubCompaction(int partition_id);
    bool ShouldUseSerialCompaction() const { return force_serial_compaction_; }
	/**
     * @brief persist data and metadata(manifest) of all output psts, and update L0 and L1 volatile indexes
     * 
     * @return true 
     * @return false 
     */
    void CleanCompaction();
	void CleanCompactionWhenUsingSubCompaction();
    bool RollbackCompaction();

	static void TriggerSubCompaction(void *arg);
};
