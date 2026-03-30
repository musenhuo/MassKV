/**
 * @file version.h
 * @author your name (you@domain.com)
 * @brief each version stores a view of the PSTs contained in each layer of the LSM-tree
 * @version 0.1
 * @date 2022-08-31
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "db/table.h"
#include "db/pst_reader.h"
#include "db/pst_builder.h"
#include "db/pst_deleter.h"
#include "lib/hybrid_l1/l1_delta_batch.h"
#include "lib/hybrid_l1/l1_hybrid_index.h"

#include <algorithm>
#include <queue>
#include <map>
#include <unordered_map>
#include <atomic>
#include <mutex>

struct TreeMeta
{
    bool valid = false;
    #if defined(FLOWKV_KEY16)
    KeyType min_key{MAX_UINT64, MAX_UINT64};
    KeyType max_key{0, 0};
    #else
    KeyType min_key = MAX_UINT64;
    KeyType max_key = 0;
    #endif
    uint64_t size = 0;
};

class Manifest;
class Version
{
private:
    /**
     * @brief level 0 structure
     *
     * | nullptr | nullptr | tree1 | tree2 | tree3 | tree4 | nullptr | nullptr
     *                        ↑                                ↑
     *                        head ->                         tail ->
     *                       oldest tree,compact           newest tree, flush
     */
    std::vector<std::vector<TaggedPstMeta>> level0_table_lists_;
    Index *level0_trees_[MAX_L0_TREE_NUM];
    int l0_tail_ = 0;
    int l0_head_ = 0;
    int l0_read_tail_ = 0;
    bool l0_tree_ready_[MAX_L0_TREE_NUM]{};
    mutable std::mutex l0_meta_lock_;

    TreeMeta level0_tree_meta_[MAX_L0_TREE_NUM];
    // TODO: recover it when recovering db
    int l0_tree_seq_ = 0;
    int l1_seq_ = 0;

    // level1
    std::unordered_map<uint64_t, TaggedPstMeta> level1_table_by_block_;
    flowkv::hybrid_l1::L1HybridIndex *level1_tree_;
    PSTReader pst_reader_;
    int l1_batch_update_depth_ = 0;
    std::vector<KeyType> pending_l1_changed_route_keys_;
    bool has_pending_l1_delta_batch_ = false;
    flowkv::hybrid_l1::L1DeltaBatch pending_l1_delta_batch_;

    void RebuildLevel1Tree();
    void RebuildLevel1Partitions(const std::vector<KeyType>& changed_route_keys);
    void RebuildLevel1Partitions(const std::vector<KeyType>& changed_route_keys,
                                 const flowkv::hybrid_l1::L1DeltaBatch* delta_batch);
    std::vector<KeyType> CollectChangedRouteKeysForTable(const PSTMeta& table) const;
    void RebuildLevel1TableMapFromRecords();
    void CollectActiveLevel1Tables(std::vector<TaggedPstMeta>& output) const;
    bool ResolveL1BlockToTableMeta(uint64_t kv_block_ptr, TaggedPstMeta& output) const;
    bool ResolveL1RecordToTableMeta(const flowkv::hybrid_l1::SubtreeRecord& record,
                                    TaggedPstMeta& output) const;
    void QueueOrApplyL1Rebuild(const std::vector<KeyType>& changed_route_keys);

public:
    Version(SegmentAllocator *seg_allocator);
    ~Version();

    int InsertTableToL0(TaggedPstMeta table, int tree_idx);
    int InsertTableToL1(TaggedPstMeta table);
    void BeginRecoverLevel1(uint32_t next_l1_seq, size_t expected_table_count = 0);
    void RecoverLevel1Table(const TaggedPstMeta& table);
    void FinalizeRecoverLevel1();
    void RecoverLevel1Tables(const std::vector<TaggedPstMeta>& tables, uint32_t next_l1_seq);
    bool DeleteTableInL1(PSTMeta table);
    void BeginL1BatchUpdate();
    void EndL1BatchUpdate();
    void SetPendingL1DeltaBatch(flowkv::hybrid_l1::L1DeltaBatch batch);
    // bool DeleteTable(int idx, int level_id);

    bool Get(Slice key, const char *value_out, int *value_size, PSTReader *pst_reader);
    RowIterator *GetLevel1Iter(Slice key, PSTReader *pst_reader,std::vector<TaggedPstMeta>& table_metas);
    int GetLevelSize(int level)
    {
        if (level == 1)
            return static_cast<int>(level1_table_by_block_.size());
        int count = 0;
        if (level == 0)
        {
            for (int i = 0; i < level0_table_lists_.size(); i++)
            {
                count += level0_table_lists_[i].size();
            }
        }
        return count;
    }

    /**
     * @brief
     *
     * @return int tree idx (determined by l0_tail_)
     */
    bool CheckSpaceForL0Tree();
    int AddLevel0Tree(uint32_t *seq_no_out = nullptr);
    uint32_t GetCurrentL0TreeSeq();
    void SetCurrentL0TreeSeq(uint32_t seq);
    void SetCurrentL1Seq(uint32_t seq);
    uint32_t GenerateL1Seq();
    bool FreeLevel0Tree();
    void PublishLevel0Tree(int tree_idx);
    void UpdateLevel0ReadTail();
    int GetLevel0TreeNum()
    {
        return (l0_read_tail_ + MAX_L0_TREE_NUM - l0_head_) % MAX_L0_TREE_NUM;
    };
    int PickLevel0Trees(std::vector<std::vector<TaggedPstMeta>> &outputs, std::vector<TreeMeta> &tree_metas, int max_size = MAX_L0_TREE_NUM);
    bool PickOverlappedL1Tables(const KeyType &min, const KeyType &max, std::vector<TaggedPstMeta> &output);
    bool PickOverlappedL1Records(const KeyType &min,
                                 const KeyType &max,
                                 std::vector<flowkv::hybrid_l1::SubtreeRecord> &records,
                                 std::vector<uint64_t> &unique_kv_block_ptrs) const;
    void ResolveL1RecordsToTables(const std::vector<flowkv::hybrid_l1::SubtreeRecord> &records,
                                  std::vector<TaggedPstMeta> &output) const;
    void ResolveL1BlocksToTables(const std::vector<uint64_t>& kv_block_ptrs,
                                 std::vector<TaggedPstMeta>& output) const;

    bool L1TreeConsistencyCheckAndFix(PSTDeleter* pst_deleter,Manifest* manifest);

    bool DebugValidateLevel1Structure() const;
    flowkv::hybrid_l1::L1HybridIndex::MemoryUsageStats DebugEstimateLevel1MemoryUsage() const;
    void DebugExportLevel1Records(std::vector<flowkv::hybrid_l1::SubtreeRecord> &output) const;
    void DebugExportLevel1LocalFragments(std::vector<flowkv::hybrid_l1::SubtreeRecord> &output) const;
    bool DebugResolveLevel1Record(const flowkv::hybrid_l1::SubtreeRecord &record, TaggedPstMeta &output) const;
    void DebugExportActiveLevel1Tables(std::vector<TaggedPstMeta> &output) const;
    void DebugCollectLevel1Candidates(const KeyType &key, std::vector<TaggedPstMeta> &output) const;
    void DebugCollectLevel1Overlaps(const KeyType &min, const KeyType &max, std::vector<TaggedPstMeta> &output) const;
    bool ExportL1HybridState(std::vector<uint8_t>& bytes_out, uint32_t& current_l1_seq_no) const;
    bool ImportL1HybridState(const std::vector<uint8_t>& bytes, uint32_t expected_l1_seq_no);
    
    // Cache statistics
    uint64_t GetCacheHits() const { return pst_reader_.GetCacheHits(); }
    uint64_t GetCacheMisses() const { return pst_reader_.GetCacheMisses(); }
    
    // L1 index statistics
    void PrintL1IndexStats();
};

// Debug stats for L1 lookup
void PrintL1DebugStats();
