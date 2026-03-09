/**
 * all functions need lock. We add lock in PersistMeta() in flush and compaction with a global lock (db->manifest_lock_)
 *
 */
#include "version.h"
#include "manifest.h"
#include "db/pst_deleter.h"
#include <libpmem.h>
#include <set>
#include <vector>
#include <errno.h>
#include <stdio.h>
#include <string.h>

namespace {

constexpr uint32_t kL1HybridSnapshotEnvelopeMagic = 0x4D485331u;  // "MHS1"
constexpr uint16_t kL1HybridSnapshotEnvelopeVersion = 1;
constexpr size_t kL1HybridSnapshotEnvelopeHeaderSize = 24;
constexpr uint64_t kL1HybridSlotBit = (1ULL << 46);
constexpr uint64_t kL1HybridLengthMask = (kL1HybridSlotBit - 1);
constexpr size_t kL1HybridSlotCount = 2;
constexpr size_t kL1HybridSlotBytes = (L1HybridStateSize / kL1HybridSlotCount);

static_assert(L1HybridStateSize % kL1HybridSlotCount == 0,
              "L1 hybrid snapshot area must be divisible by slot count");

void PutU16(char* dst, uint16_t value) {
    dst[0] = static_cast<char>(value & 0xFFu);
    dst[1] = static_cast<char>((value >> 8) & 0xFFu);
}

void PutU32(char* dst, uint32_t value) {
    dst[0] = static_cast<char>(value & 0xFFu);
    dst[1] = static_cast<char>((value >> 8) & 0xFFu);
    dst[2] = static_cast<char>((value >> 16) & 0xFFu);
    dst[3] = static_cast<char>((value >> 24) & 0xFFu);
}

void PutU64(char* dst, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        dst[i] = static_cast<char>((value >> (i * 8)) & 0xFFu);
    }
}

uint16_t GetU16(const char* src) {
    return static_cast<uint16_t>(static_cast<uint8_t>(src[0])) |
           (static_cast<uint16_t>(static_cast<uint8_t>(src[1])) << 8);
}

uint32_t GetU32(const char* src) {
    return static_cast<uint32_t>(static_cast<uint8_t>(src[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[3])) << 24);
}

uint64_t GetU64(const char* src) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= (static_cast<uint64_t>(static_cast<uint8_t>(src[i])) << (i * 8));
    }
    return value;
}

uint64_t ComputeFnv1a64(const uint8_t* data, size_t size) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

size_t DecodeL1HybridStoredBytes(uint64_t encoded_length) {
    return static_cast<size_t>(encoded_length & kL1HybridLengthMask);
}

uint32_t DecodeL1HybridSlot(uint64_t encoded_length) {
    return (encoded_length & kL1HybridSlotBit) ? 1u : 0u;
}

uint64_t EncodeL1HybridLengthWithSlot(size_t stored_bytes, uint32_t slot) {
    uint64_t encoded = static_cast<uint64_t>(stored_bytes);
    if (slot != 0) {
        encoded |= kL1HybridSlotBit;
    }
    return encoded;
}

}  // namespace

Manifest::Manifest(int fd_, bool recover)
    : fd(fd_),
      l0_start_(4096),
      l1_start_(l0_start_ + L0MetaSize),
      flush_log_start_(l1_start_ + L1MetaSize),
      l1_hybrid_state_start_(flush_log_start_ + OpLogSize),
      end_(ManifestSize)
{
    buf_ = static_cast<char*>(aligned_alloc(4096,4096));
    
    // Pre-allocate manifest file to required size for O_DIRECT pread/pwrite
    if (ftruncate(fd, ManifestSize) != 0) {
        perror("ftruncate manifest");
    }
    
    if(recover)
    {
        // read full aligned page into buf_ and memcpy super_ to support O_DIRECT
        auto ret = pread(fd, buf_, 4096, 0);
        if(ret!=4096) {
            std::cout<<"super_ recover wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pread super_");
        }
        memcpy(&super_, buf_, sizeof(super_));
        printf("recover mode\n");
    }
    else
    {
        super_.l0_tail = 0;
        super_.l1_tail = 0;
        super_.flush_log = {0, 0};
        super_.l1_hybrid_state = {0, 0, 0};
        super_.l0_min_valid_seq_no = 0;
        super_.l1_current_seq_no = 0;
        // persist super_ as a full aligned page write to satisfy O_DIRECT
        memset(buf_, 0, 4096);
        memcpy(buf_, &super_, sizeof(super_));
        int ret = pwrite(fd, buf_, 4096, 0);
        if(ret!=4096) {
            std::cout<<"super_ init wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite super_");
        }
    }
    INFO("MANIFEST:l0tail=%lu,l1tail=%lu", super_.l0_tail, super_.l1_tail);
}
Manifest::~Manifest() {free(buf_);}

Version *Manifest::RecoverVersion(Version *version, SegmentAllocator *allocator)
{
    PSTDeleter pst_deleter(allocator);
    const size_t metas_per_page = 4096 / sizeof(PSTMeta);
    
    // recover level0
    size_t tail = super_.l0_tail;
    PSTMeta meta;
    uint32_t min_tree_seq = super_.l0_min_valid_seq_no;
    int max_tree_seq = min_tree_seq - 1;
    version->SetCurrentL0TreeSeq(min_tree_seq);
    DEBUG("l0_tree_seq=%u,manifest tail=%lu", min_tree_seq, tail);
    INFO("recovering L0: %lu entries (%lu pages)", tail, (tail + metas_per_page - 1) / metas_per_page);
    
    off_t last_offset = -1;
    for (size_t i = 0; i < tail; i++)
    {
        off_t offset = Getoff(i, 0);
        // Only read when entering a new page
        if (offset != last_offset) {
            auto ret = pread(fd, buf_, 4096, offset);
            if(ret!=4096)
                std::cout<<"L0 recover read wrong at offset "<<offset<<std::endl; 
            last_offset = offset;
        }
        memcpy(&meta, buf_+(i % metas_per_page)*sizeof(PSTMeta), sizeof(meta));
        if (meta.datablock_ptr_)
        {
            // DEBUG("recover L0 pst %u", meta.seq_no_);
            if (meta.seq_no_ >= min_tree_seq)
            {
                // valid pst
                while (max_tree_seq < (int)meta.seq_no_) // if no tree,create it
                {
                    max_tree_seq++;
                    version->AddLevel0Tree();
                    DEBUG("recover L0 tree %u", max_tree_seq);
                }
                TaggedPstMeta tmeta{
                    .meta = meta,
                    .level = 0,
                    .manifest_position = i};
                version->InsertTableToL0(tmeta, meta.seq_no_ - min_tree_seq);
            }
            else
            {
                pst_deleter.DeletePST(meta);
            }
        }
    }
    version->UpdateLevel0ReadTail();

    // recover level1
    tail = super_.l1_tail;  // Must use l1_tail for L1 recovery!
    unsigned current_L1_version = super_.l1_current_seq_no;
    DEBUG("l1_version=%u", current_L1_version);
    INFO("recovering L1: %lu entries (%lu pages)", tail, (tail + metas_per_page - 1) / metas_per_page);
    std::vector<TaggedPstMeta> recovered_l1_tables(tail);
    for (size_t i = 0; i < tail; ++i) {
        recovered_l1_tables[i] = TaggedPstMeta{
            .meta = PSTMeta::InvalidTable(),
            .level = 1,
            .manifest_position = static_cast<int>(i),
        };
    }
    
    last_offset = -1;
    for (size_t i = 0; i < tail; i++)
    {
        off_t offset = Getoff(i, 1);
        // Only read when entering a new page
        if (offset != last_offset) {
            auto ret = pread(fd, buf_, 4096, offset);
            if(ret!=4096)
                std::cout<<"L1 recover read wrong at offset "<<offset<<std::endl; 
            last_offset = offset;
        }
        memcpy(&meta, buf_+(i % metas_per_page)*sizeof(PSTMeta), sizeof(meta));
        if (meta.datablock_ptr_ != 0)
        {
            if (meta.seq_no_ > current_L1_version)
            {
                // Clean new PSTs generated by an unfinished compaction during crash
                pst_deleter.DeletePST(meta);
            }
            else
            {
                // insert it into L1 tree
                TaggedPstMeta tmeta{
                    .meta = meta,
                    .level = 1,
                    .manifest_position = static_cast<int>(i)};
                recovered_l1_tables[i] = tmeta;
            }
        }
    }
    version->RecoverLevel1Tables(std::move(recovered_l1_tables), current_L1_version + 1);
    std::vector<uint8_t> l1_hybrid_state_bytes;
    if (LoadL1HybridState(current_L1_version, l1_hybrid_state_bytes)) {
        if (version->ImportL1HybridState(l1_hybrid_state_bytes, current_L1_version)) {
            INFO("recover L1 hybrid state from manifest snapshot, seq=%u", current_L1_version);
        } else {
            INFO("L1 hybrid state snapshot invalid for seq=%u, fallback to rebuilt tree", current_L1_version);
            ClearL1HybridState();
        }
    }
    pst_deleter.PersistCheckpoint();

    // clean overlapped old PSTs in L1 tree which was not been cleaned in an unfinished compaction due to crash
    version->L1TreeConsistencyCheckAndFix(&pst_deleter, this);
    return version;
}

void Manifest::PrintL1Info() {
	printf("[Manifest] l1 tail=%lu, l1_freelist=%lu\n",super_.l1_tail,l1_freelist_.size());
}

int Manifest::AddTable(PSTMeta meta, int level)
{
    int idx = -1;
    switch (level)
    {
    case 0:
        if (!l0_freelist_.empty())
        {
            idx = l0_freelist_.front();
            l0_freelist_.pop();
        }
        else
        {
            idx = super_.l0_tail;
            if (idx * sizeof(PSTMeta) >= L0MetaSize)
            {
                L0GC();
                idx = l0_freelist_.front();
                l0_freelist_.pop();
                break;
            }
            (super_.l0_tail)++;
        }
        break;
    case 1:
        if (!l1_freelist_.empty())
        {
            idx = l1_freelist_.front();
            l1_freelist_.pop();
        }
        else
        {
            idx = super_.l1_tail;
            if (idx * sizeof(PSTMeta) >= L1MetaSize)
                ERROR_EXIT("Manifest L1 is full, idx=%d!",idx);
            super_.l1_tail++;
        }
        break;
    }
    off_t offset = Getoff(idx, level);
    auto ret = pread(fd, buf_, 4096, offset);
    if(ret!=4096)
        std::cout<<"AddTable read wrong"<<std::endl; 
    memcpy(buf_+idx%(4096/sizeof(PSTMeta))*sizeof(PSTMeta), &meta, sizeof(PSTMeta));
    ret = pwrite(fd, buf_, 4096, offset);
    if(ret!=4096) {
        std::cout<<"AddTable write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite AddTable buf_");
    }
    // update super_ as a full page
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    ret = pwrite(fd, buf_, 4096, 0);
    if(ret!=4096) {
        std::cout<<"AddTable super_ write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite AddTable super_");
    }
    return idx;
}

void Manifest::DeleteTable(int idx, int level)
{
    auto temp = PSTMeta::InvalidTable();
    off_t offset = Getoff(idx, level);
    auto ret = pread(fd, buf_, 4096, offset);
    if(ret!=4096)
        std::cout<<"DeleteTable read wrong"<<std::endl; 
    memcpy(buf_+idx%(4096/sizeof(PSTMeta))*sizeof(PSTMeta), &temp, sizeof(PSTMeta));
    ret = pwrite(fd, buf_, 4096, offset);
    if(ret!=4096) {
        std::cout<<"DeleteTable write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite DeleteTable buf_");
    }
    switch (level)
    {
    case 0:
        l0_freelist_.push(idx);
        break;
    case 1:
        l1_freelist_.push(idx);
        break;
    }
}

void Manifest::AddFlushLog(std::vector<uint64_t> &deleted_log_segment_ids)
{
    if (super_.flush_log.is_valid == 1)
    {
        ERROR_EXIT("double flushing");
    }
    LOG("add flush log %lu,%lu,%u", deleted_log_segment_ids.size(), deleted_log_segment_ids.size() * sizeof(uint64_t), OpLogSize);
    assert(flush_log_start_ + deleted_log_segment_ids.size() * sizeof(uint64_t) < end_);
    size_t buffer_size = roundup(deleted_log_segment_ids.size() * sizeof(uint64_t),4096);
    char *buffer = static_cast<char*>(aligned_alloc(4096,buffer_size));
    memcpy(buffer, deleted_log_segment_ids.data(), deleted_log_segment_ids.size() * sizeof(uint64_t)); 
    auto ret = pwrite(fd, buffer, buffer_size, flush_log_start_);
    if(ret!=buffer_size) {
        std::cout<<"AddFlushLog wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite AddFlushLog buffer");
    }
    ManifestSuperMeta::FlushLog fl{1, deleted_log_segment_ids.size()};
    super_.flush_log=fl;
    // write updated super_ as full page
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    ret = pwrite(fd, buf_, 4096, 0);
    if(ret!=4096) {
        std::cout<<"AddFlushLog super_ write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite AddFlushLog super_");
    }
    free(buffer);
}

void Manifest::ClearFlushLog()
{
    super_.flush_log.is_valid = 0;
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    int ret = pwrite(fd, buf_, 4096, 0);
    if(ret!=4096) {
        std::cout<<"ClearFlushLog wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite ClearFlushLog super_");
    }
}

bool Manifest::GetFlushLog(std::vector<uint64_t> &deleted_log_segment_ids)
{
    if (!super_.flush_log.is_valid)
        return false;
    size_t size = super_.flush_log.length;
    deleted_log_segment_ids.resize(size);
    size_t buffer_size = roundup(size * sizeof(uint64_t),4096);
    char *buffer = static_cast<char*>(aligned_alloc(4096,buffer_size));
    auto ret = pread(fd, buffer, buffer_size, flush_log_start_);
    if(ret!=buffer_size)
        std::cout<<"GetFlushLog wrong"<<std::endl;  
    memcpy(deleted_log_segment_ids.data(), buffer, size * sizeof(uint64_t)); 
    return true;
}

bool Manifest::PersistL1HybridState(const std::vector<uint8_t>& bytes, uint32_t current_l1_seq_no)
{
    const size_t stored_bytes = kL1HybridSnapshotEnvelopeHeaderSize + bytes.size();
    if (stored_bytes > kL1HybridSlotBytes) {
        return false;
    }
    const size_t aligned_size = roundup(stored_bytes, static_cast<size_t>(4096));
    if (aligned_size > kL1HybridSlotBytes) {
        return false;
    }

    uint32_t active_slot = 0;
    if (super_.l1_hybrid_state.is_valid) {
        active_slot = DecodeL1HybridSlot(static_cast<uint64_t>(super_.l1_hybrid_state.length));
    }
    const uint32_t target_slot = super_.l1_hybrid_state.is_valid ? (active_slot ^ 1u) : 0u;
    const off_t slot_offset =
        l1_hybrid_state_start_ + static_cast<off_t>(target_slot) * kL1HybridSlotBytes;

    char* write_buf = static_cast<char*>(aligned_alloc(4096, aligned_size));
    memset(write_buf, 0, aligned_size);
    PutU32(write_buf + 0, kL1HybridSnapshotEnvelopeMagic);
    PutU16(write_buf + 4, kL1HybridSnapshotEnvelopeVersion);
    PutU16(write_buf + 6, 0);
    PutU32(write_buf + 8, current_l1_seq_no);
    PutU32(write_buf + 12, static_cast<uint32_t>(bytes.size()));
    PutU64(write_buf + 16, ComputeFnv1a64(bytes.data(), bytes.size()));
    if (!bytes.empty()) {
        memcpy(write_buf + kL1HybridSnapshotEnvelopeHeaderSize, bytes.data(), bytes.size());
    }
    auto ret = pwrite(fd, write_buf, aligned_size, slot_offset);
    free(write_buf);
    if (ret != static_cast<ssize_t>(aligned_size)) {
        return false;
    }

    ManifestSuperMeta::L1HybridState l1_hybrid_state{};
    l1_hybrid_state.is_valid = 1;
    l1_hybrid_state.length =
        EncodeL1HybridLengthWithSlot(stored_bytes, target_slot);
    l1_hybrid_state.seq_no = static_cast<uint16_t>(current_l1_seq_no & 0xFFFFu);
    super_.l1_hybrid_state = l1_hybrid_state;

    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    auto super_ret = pwrite(fd, buf_, 4096, 0);
    return super_ret == 4096;
}

bool Manifest::LoadL1HybridState(uint32_t expected_l1_seq_no, std::vector<uint8_t>& bytes_out)
{
    bytes_out.clear();
    if (!super_.l1_hybrid_state.is_valid) {
        return false;
    }
    if (super_.l1_hybrid_state.seq_no != static_cast<uint16_t>(expected_l1_seq_no & 0xFFFFu)) {
        return false;
    }
    const uint64_t encoded_length = static_cast<uint64_t>(super_.l1_hybrid_state.length);
    const size_t stored_bytes = DecodeL1HybridStoredBytes(encoded_length);
    const uint32_t active_slot = DecodeL1HybridSlot(encoded_length);
    if (stored_bytes == 0 || stored_bytes > kL1HybridSlotBytes) {
        return false;
    }
    const size_t aligned_size = roundup(stored_bytes, static_cast<size_t>(4096));
    if (aligned_size > kL1HybridSlotBytes) {
        return false;
    }
    const off_t slot_offset =
        l1_hybrid_state_start_ + static_cast<off_t>(active_slot) * kL1HybridSlotBytes;
    char* read_buf = static_cast<char*>(aligned_alloc(4096, aligned_size));
    auto ret = pread(fd, read_buf, aligned_size, slot_offset);
    if (ret != static_cast<ssize_t>(aligned_size)) {
        free(read_buf);
        return false;
    }

    if (stored_bytes >= kL1HybridSnapshotEnvelopeHeaderSize &&
        GetU32(read_buf + 0) == kL1HybridSnapshotEnvelopeMagic) {
        const uint16_t version = GetU16(read_buf + 4);
        const uint16_t reserved = GetU16(read_buf + 6);
        const uint32_t seq_no = GetU32(read_buf + 8);
        const uint32_t payload_bytes = GetU32(read_buf + 12);
        const uint64_t checksum = GetU64(read_buf + 16);
        const size_t total_bytes =
            static_cast<size_t>(payload_bytes) + kL1HybridSnapshotEnvelopeHeaderSize;
        if (version != kL1HybridSnapshotEnvelopeVersion || reserved != 0 ||
            seq_no != expected_l1_seq_no || total_bytes != stored_bytes) {
            free(read_buf);
            return false;
        }
        const uint8_t* payload =
            reinterpret_cast<const uint8_t*>(read_buf + kL1HybridSnapshotEnvelopeHeaderSize);
        if (ComputeFnv1a64(payload, payload_bytes) != checksum) {
            free(read_buf);
            return false;
        }
        bytes_out.assign(payload, payload + payload_bytes);
    } else {
        // Legacy format compatibility: payload-only snapshot without envelope.
        bytes_out.assign(read_buf, read_buf + stored_bytes);
    }
    free(read_buf);
    return true;
}

void Manifest::ClearL1HybridState()
{
    super_.l1_hybrid_state.is_valid = 0;
    super_.l1_hybrid_state.length = 0;
    super_.l1_hybrid_state.seq_no = 0;
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    auto ret = pwrite(fd, buf_, 4096, 0);
    if (ret != 4096) {
        std::cout<<"ClearL1HybridState write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite ClearL1HybridState super_");
    }
}

inline const off_t Manifest::Getoff(int idx, int level)
{
    off_t start;
    switch (level)
    {
    case 0:
        start = l0_start_;
        break;
    case 1:
        start = l1_start_;
        break;
    default:
        ERROR_EXIT("invalid level");
    }
    assert(start + idx/(4096/sizeof(PSTMeta))*4096 < ManifestSize);
    return start + idx/(4096/sizeof(PSTMeta))*4096;
}

void Manifest::UpdateL0Version(unsigned min_seq_no)
{
    DEBUG("update L0 version = %u", min_seq_no);
    super_.l0_min_valid_seq_no = min_seq_no;
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    int ret = pwrite(fd, buf_, 4096, 0);
    if(ret!=4096) {
        std::cout<<"UpdateL0Version write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite UpdateL0Version super_");
    }
}

void Manifest::UpdateL1Version(unsigned current_seq_no)
{
    super_.l1_current_seq_no = current_seq_no;
    memset(buf_, 0, 4096);
    memcpy(buf_, &super_, sizeof(super_));
    int ret = pwrite(fd, buf_, 4096, 0);
    if(ret!=4096) {
        std::cout<<"UpdateL1Version write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite UpdateL1Version super_");
    }
}

unsigned Manifest::GetL0Version()
{
    return super_.l0_min_valid_seq_no;
}

void Manifest::L0GC()
{
    // clear freelist_l0_ and add all invalid position to freelist
    assert(super_.l0_tail * sizeof(PSTMeta) >= L0MetaSize);
    assert(l0_freelist_.empty());
    printf("manifest: l0 gc....\n");
    unsigned min_seq_no = super_.l0_min_valid_seq_no;
    PSTMeta meta;
    for (int i = 0; i < super_.l0_tail; i++)
    {
        off_t offset = Getoff(i, 0);
        auto ret = pread(fd, buf_, 4096, offset);
        if(ret!=4096)
            std::cout<<"LOGC wrong"<<std::endl; 
        memcpy(&meta, buf_+i%(4096/sizeof(PSTMeta))*sizeof(PSTMeta), sizeof(meta));
        if (meta.seq_no_ < min_seq_no)
        {
            l0_freelist_.push(i);
        }
    }
    if (l0_freelist_.empty())
    {
        ERROR_EXIT("Manifest L0 is full!");
    }
}
