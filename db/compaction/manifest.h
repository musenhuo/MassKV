/**
 * @file manifest.h
 * @author your name (you@domain.com)
 * @brief 
 *
 * @version 0.1
 * @date 2022-10-26
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "db/table.h"
#include <queue>
#include <unordered_map>
#include <mutex>
// FlowKV baseline tuned for 16KB table granularity; Hybrid-L1 currently uses 4KB
// table granularity and therefore produces ~4x more L0 table metadata under the
// same KV scale. Expand L0 manifest space by 4x to keep comparable headroom.
#define L0MetaSize 204800000
#define L1MetaSize 512000000
// Each log group can cotain MAX_USER_THREAD_NUM * 8 log segments, which have an 4-byte id
#define FlushLogSize (4 * MAX_MEMTABLE_NUM * MAX_USER_THREAD_NUM * 32)
// Tableless recovery persists the whole Hybrid-L1 snapshot into a double-buffered
// manifest region. With 2M prefixes, the exported snapshot can exceed the old
// 32MiB per-slot budget (64MiB total / 2 slots). Increase the area so the
// breakdown matrix can cover the historical 2M-prefix settings without hitting
// an artificial persist-size ceiling.
#define L1HybridStateSize (512 * 1024 * 1024)
// Quick fix for large-scale write experiments:
// one compaction batch may touch too many manifest pages and overflow the
// txn payload area; enlarge txn log region from 64MB to 256MB.
#define ManifestTxnLogSize (256 * 1024 * 1024)
#define ManifestSize (4096 + L0MetaSize + L1MetaSize + FlushLogSize + L1HybridStateSize + ManifestTxnLogSize)

class Version;
class SegmentAllocator;
struct ManifestSuperMeta
{
    uint32_t l0_min_valid_seq_no = 0;
    uint32_t l1_current_seq_no = 0;
    size_t l0_tail = 0;
    size_t l1_tail = 0;
    struct FlushLog
    {
        uint64_t is_valid : 1;
        uint64_t length : 63;
    } flush_log;
    struct L1HybridState
    {
        uint64_t is_valid : 1;
        uint64_t length : 47;
        uint64_t seq_no : 16;
    } l1_hybrid_state;
    char a[4056];
};
class Manifest
{
private:
    struct BatchPage
    {
        char *data = nullptr;  // 4KB aligned page buffer for O_DIRECT
        bool dirty = false;
    };

    int fd;
    off_t  l0_start_;
    off_t  l1_start_;
    off_t  flush_log_start_;
    off_t  l1_hybrid_state_start_;
    off_t  manifest_txn_start_;
    off_t end_;
    std::queue<int> l0_freelist_;
    ManifestSuperMeta super_;
    char *buf_;
    bool batch_active_ = false;
    bool batch_super_dirty_ = false;
    ManifestSuperMeta batch_super_;
    std::unordered_map<off_t, BatchPage> batch_pages_;
    mutable std::recursive_mutex manifest_mu_;

public:
    struct ResidentMemoryStats
    {
        size_t aligned_super_page_buffer_bytes = 0;
        size_t super_meta_bytes = 0;
        size_t batch_super_meta_bytes = 0;

        size_t l0_freelist_size = 0;
        size_t l0_freelist_estimated_bytes = 0;

        size_t batch_pages_count = 0;
        size_t batch_pages_data_bytes = 0;
        size_t batch_pages_map_node_estimated_bytes = 0;
        size_t batch_pages_map_bucket_bytes = 0;

        size_t total_estimated_bytes = 0;
    };

    Manifest(int fd, bool recover);
    ~Manifest();
    /**
     * @brief persist new pst in manifest
     *
     * @param meta PST metadata
     * @param level level
     * @return int the id in manifest (for fast delete);
     */
    int AddTable(PSTMeta meta, int level);

    void DeleteTable(int idx, int level);

    void UpdateL0Version(unsigned min_seq_no);

    void UpdateL1Version(unsigned current_seq_no);

    unsigned GetL0Version();

    bool BeginBatchUpdate();
    bool CommitBatchUpdate();
    void AbortBatchUpdate();

    void L0GC();

    void AddFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    void ClearFlushLog();

    bool GetFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    bool PersistL1HybridState(const std::vector<uint8_t>& bytes, uint32_t current_l1_seq_no);
    bool LoadL1HybridState(uint32_t expected_l1_seq_no, std::vector<uint8_t>& bytes_out);
    void ClearL1HybridState();

    Version *RecoverVersion(Version *source,SegmentAllocator* allocator);

    void PrintL1Info();
    ResidentMemoryStats DebugEstimateResidentMemory() const;
    size_t DebugReleaseVolatileStateForProbe();

private:
    inline const off_t Getoff(int idx, int level);
    inline ManifestSuperMeta &MutableSuper();
    inline const ManifestSuperMeta &CurrentSuper() const;
    char *AcquireBatchPage(off_t offset);
    bool PersistSuper(const ManifestSuperMeta &meta);
    void ClearBatchPages();
    bool ReplayPendingBatchTxn();
};
