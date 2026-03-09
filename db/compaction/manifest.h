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
#define L0MetaSize 51200000
#define L1MetaSize 512000000
// Each log group can cotain MAX_USER_THREAD_NUM * 8 log segments, which have an 4-byte id
#define OpLogSize (4 * MAX_MEMTABLE_NUM * MAX_USER_THREAD_NUM * 32)
#define L1HybridStateSize (64 * 1024 * 1024)
#define ManifestSize (4096 + L0MetaSize + L1MetaSize + OpLogSize + L1HybridStateSize)

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
    int fd;
    off_t  l0_start_;
    off_t  l1_start_;
    off_t  flush_log_start_;
    off_t  l1_hybrid_state_start_;
    off_t end_;
    std::queue<int> l0_freelist_;
    std::queue<int> l1_freelist_;
    ManifestSuperMeta super_;
    char *buf_;

public:
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

    void L0GC();

    void AddFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    void ClearFlushLog();

    bool GetFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    bool PersistL1HybridState(const std::vector<uint8_t>& bytes, uint32_t current_l1_seq_no);
    bool LoadL1HybridState(uint32_t expected_l1_seq_no, std::vector<uint8_t>& bytes_out);
    void ClearL1HybridState();

    Version *RecoverVersion(Version *source,SegmentAllocator* allocator);

	void PrintL1Info();

private:
    inline const off_t Getoff(int idx, int level);
};
