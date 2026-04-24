/**
 * all functions need lock. We add lock in PersistMeta() in flush and compaction with a global lock (db->manifest_lock_)
 *
 */
#include "version.h"
#include "manifest.h"
#include "db/pst_deleter.h"
#include <libpmem.h>
#include <algorithm>
#include <cstdlib>
#include <unistd.h>
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
constexpr uint32_t kManifestBatchTxnMagic = 0x5854424Du;  // "MBTX"
constexpr uint16_t kManifestBatchTxnVersion = 1;
constexpr uint16_t kManifestBatchTxnStatePrepared = 1;

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

bool SyncManifestFd(int fd) {
    while (true) {
        if (fdatasync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

void MaybeCrashAtManifestFailpoint(const char* name) {
    const char* configured = std::getenv("FLOWKV_MANIFEST_TXN_FAILPOINT");
    if (configured == nullptr) {
        return;
    }
    if (std::strcmp(configured, name) == 0) {
        std::fprintf(stderr, "[manifest] hit failpoint=%s, simulate crash\n", name);
        std::fflush(stderr);
        _exit(99);
    }
}

void EncodeManifestBatchTxnHeader(char *dst,
                                  uint16_t state,
                                  uint32_t page_count,
                                  uint32_t has_super_page,
                                  uint64_t payload_bytes,
                                  uint64_t payload_checksum) {
    memset(dst, 0, 4096);
    PutU32(dst + 0, kManifestBatchTxnMagic);
    PutU16(dst + 4, kManifestBatchTxnVersion);
    PutU16(dst + 6, state);
    PutU32(dst + 8, page_count);
    PutU32(dst + 12, has_super_page);
    PutU64(dst + 16, payload_bytes);
    PutU64(dst + 24, payload_checksum);
}

bool ParseEnvBool(const char* name, bool default_value) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) {
        return default_value;
    }
    if (std::strcmp(raw, "1") == 0 || std::strcmp(raw, "true") == 0 ||
        std::strcmp(raw, "TRUE") == 0) {
        return true;
    }
    if (std::strcmp(raw, "0") == 0 || std::strcmp(raw, "false") == 0 ||
        std::strcmp(raw, "FALSE") == 0) {
        return false;
    }
    return default_value;
}

}  // namespace

Manifest::Manifest(int fd_, bool recover)
    : fd(fd_),
      l0_start_(4096),
      l1_start_(l0_start_ + L0MetaSize),
      flush_log_start_(l1_start_ + L1MetaSize),
      l1_hybrid_state_start_(flush_log_start_ + FlushLogSize),
      manifest_txn_start_(l1_hybrid_state_start_ + L1HybridStateSize),
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
        if (!ReplayPendingBatchTxn()) {
            ERROR_EXIT("manifest replay pending batch txn failed");
        }
        ret = pread(fd, buf_, 4096, 0);
        if (ret != 4096) {
            std::cout<<"super_ replay read wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pread super_ replay");
        } else {
            memcpy(&super_, buf_, sizeof(super_));
        }
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
        // Fresh init must clear any stale txn header from a previous run.
        memset(buf_, 0, 4096);
        ret = pwrite(fd, buf_, 4096, manifest_txn_start_);
        if (ret != 4096) {
            std::cout<<"txn header init clear wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite txn header init clear");
        }
        if (!SyncManifestFd(fd)) {
            std::cout<<"manifest init fdatasync failed, errno="<<errno<<std::endl;
            perror("fdatasync manifest init");
        }
    }
    INFO("MANIFEST:l0tail=%lu,l1tail=%lu", super_.l0_tail, super_.l1_tail);
}
Manifest::~Manifest() {
    AbortBatchUpdate();
    free(buf_);
}

inline ManifestSuperMeta &Manifest::MutableSuper()
{
    return batch_active_ ? batch_super_ : super_;
}

inline const ManifestSuperMeta &Manifest::CurrentSuper() const
{
    return batch_active_ ? batch_super_ : super_;
}

char *Manifest::AcquireBatchPage(off_t offset)
{
    auto it = batch_pages_.find(offset);
    if (it != batch_pages_.end()) {
        return it->second.data;
    }
    char *page = static_cast<char *>(aligned_alloc(4096, 4096));
    auto ret = pread(fd, page, 4096, offset);
    if (ret != 4096) {
        std::cout << "AcquireBatchPage read wrong, ret=" << ret << ", offset=" << offset << std::endl;
        memset(page, 0, 4096);
    }
    batch_pages_.emplace(offset, BatchPage{page, false});
    return page;
}

bool Manifest::PersistSuper(const ManifestSuperMeta &meta)
{
    memset(buf_, 0, 4096);
    memcpy(buf_, &meta, sizeof(meta));
    auto ret = pwrite(fd, buf_, 4096, 0);
    if (ret != 4096) {
        std::cout<<"PersistSuper write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
        perror("pwrite PersistSuper");
        return false;
    }
    return true;
}

void Manifest::ClearBatchPages()
{
    for (auto &kv : batch_pages_) {
        if (kv.second.data != nullptr) {
            free(kv.second.data);
        }
    }
    batch_pages_.clear();
}

bool Manifest::BeginBatchUpdate()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    if (batch_active_) {
        return false;
    }
    batch_active_ = true;
    batch_super_ = super_;
    batch_super_dirty_ = false;
    ClearBatchPages();
    return true;
}

bool Manifest::CommitBatchUpdate()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    if (!batch_active_) {
        return true;
    }

    std::vector<off_t> dirty_offsets;
    dirty_offsets.reserve(batch_pages_.size());
    for (const auto &kv : batch_pages_) {
        if (kv.second.dirty) {
            dirty_offsets.push_back(kv.first);
        }
    }
    std::sort(dirty_offsets.begin(), dirty_offsets.end());

    const bool has_super_page = batch_super_dirty_;
    const size_t dirty_page_count = dirty_offsets.size();
    const uint64_t payload_bytes =
        static_cast<uint64_t>(dirty_page_count) * static_cast<uint64_t>(sizeof(uint64_t) + 4096) +
        static_cast<uint64_t>(has_super_page ? 4096 : 0);
    const size_t aligned_payload_bytes =
        static_cast<size_t>(roundup(static_cast<size_t>(payload_bytes), static_cast<size_t>(4096)));
    const size_t payload_capacity = ManifestTxnLogSize - 4096;
    if (aligned_payload_bytes > payload_capacity) {
        std::cout << "CommitBatchUpdate txn payload overflow, payload=" << aligned_payload_bytes
                  << ", cap=" << payload_capacity << std::endl;
        AbortBatchUpdate();
        return false;
    }

    char *payload_buf = nullptr;
    if (aligned_payload_bytes > 0) {
        payload_buf = static_cast<char *>(aligned_alloc(4096, aligned_payload_bytes));
        memset(payload_buf, 0, aligned_payload_bytes);
        char *cursor = payload_buf;
        for (off_t offset : dirty_offsets) {
            PutU64(cursor, static_cast<uint64_t>(offset));
            cursor += sizeof(uint64_t);
            auto it = batch_pages_.find(offset);
            assert(it != batch_pages_.end());
            memcpy(cursor, it->second.data, 4096);
            cursor += 4096;
        }
        if (has_super_page) {
            memset(cursor, 0, 4096);
            memcpy(cursor, &batch_super_, sizeof(batch_super_));
        }
        const uint64_t payload_checksum =
            ComputeFnv1a64(reinterpret_cast<const uint8_t *>(payload_buf),
                           static_cast<size_t>(payload_bytes));

        const off_t payload_off = manifest_txn_start_ + 4096;
        auto ret = pwrite(fd, payload_buf, aligned_payload_bytes, payload_off);
        if (ret != static_cast<ssize_t>(aligned_payload_bytes)) {
            std::cout<<"CommitBatchUpdate txn payload write wrong, ret="<<ret
                     <<", errno="<<errno<<std::endl;
            perror("pwrite CommitBatchUpdate txn payload");
            free(payload_buf);
            AbortBatchUpdate();
            return false;
        }

        memset(buf_, 0, 4096);
        EncodeManifestBatchTxnHeader(buf_,
                                     kManifestBatchTxnStatePrepared,
                                     static_cast<uint32_t>(dirty_page_count),
                                     has_super_page ? 1u : 0u,
                                     payload_bytes,
                                     payload_checksum);
        ret = pwrite(fd, buf_, 4096, manifest_txn_start_);
        if (ret != 4096) {
            std::cout<<"CommitBatchUpdate txn header write wrong, ret="<<ret
                     <<", errno="<<errno<<std::endl;
            perror("pwrite CommitBatchUpdate txn header");
            free(payload_buf);
            AbortBatchUpdate();
            return false;
        }
        if (!SyncManifestFd(fd)) {
            std::cout<<"CommitBatchUpdate txn prepare fdatasync failed, errno="<<errno<<std::endl;
            perror("fdatasync CommitBatchUpdate txn prepare");
            free(payload_buf);
            AbortBatchUpdate();
            return false;
        }
        MaybeCrashAtManifestFailpoint("after_prepare_sync");
    }

    for (off_t offset : dirty_offsets) {
        auto it = batch_pages_.find(offset);
        assert(it != batch_pages_.end());
        auto ret = pwrite(fd, it->second.data, 4096, offset);
        if (ret != 4096) {
            std::cout<<"CommitBatchUpdate page write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite CommitBatchUpdate page");
            if (payload_buf != nullptr) {
                free(payload_buf);
            }
            AbortBatchUpdate();
            return false;
        }
    }

    if (has_super_page) {
        memset(buf_, 0, 4096);
        memcpy(buf_, &batch_super_, sizeof(batch_super_));
        auto ret = pwrite(fd, buf_, 4096, 0);
        if (ret != 4096) {
            std::cout<<"CommitBatchUpdate super write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite CommitBatchUpdate super");
            if (payload_buf != nullptr) {
                free(payload_buf);
            }
            AbortBatchUpdate();
            return false;
        }
        super_ = batch_super_;
    }

    MaybeCrashAtManifestFailpoint("after_apply_before_clear");

    if (aligned_payload_bytes > 0) {
        memset(buf_, 0, 4096);
        auto ret = pwrite(fd, buf_, 4096, manifest_txn_start_);
        if (ret != 4096) {
            std::cout<<"CommitBatchUpdate txn clear wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite CommitBatchUpdate txn clear");
            free(payload_buf);
            AbortBatchUpdate();
            return false;
        }
        free(payload_buf);
    }
    if (!SyncManifestFd(fd)) {
        std::cout<<"CommitBatchUpdate apply fdatasync failed, errno="<<errno<<std::endl;
        perror("fdatasync CommitBatchUpdate apply");
        AbortBatchUpdate();
        return false;
    }

    batch_active_ = false;
    batch_super_dirty_ = false;
    ClearBatchPages();
    return true;
}

void Manifest::AbortBatchUpdate()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    batch_active_ = false;
    batch_super_dirty_ = false;
    ClearBatchPages();
}

bool Manifest::ReplayPendingBatchTxn()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    char *header = static_cast<char *>(aligned_alloc(4096, 4096));
    memset(header, 0, 4096);
    auto ret = pread(fd, header, 4096, manifest_txn_start_);
    if (ret != 4096) {
        std::cout << "ReplayPendingBatchTxn read header wrong, ret=" << ret
                  << ", errno=" << errno << std::endl;
        free(header);
        return false;
    }

    const uint32_t magic = GetU32(header + 0);
    const uint16_t version = GetU16(header + 4);
    const uint16_t state = GetU16(header + 6);
    if (magic == 0 && state == 0) {
        free(header);
        return true;
    }
    if (magic != kManifestBatchTxnMagic || version != kManifestBatchTxnVersion) {
        std::cout << "ReplayPendingBatchTxn invalid header, magic=" << magic
                  << ", version=" << version << ", clear it" << std::endl;
        memset(header, 0, 4096);
        auto clr_ret = pwrite(fd, header, 4096, manifest_txn_start_);
        free(header);
        if (clr_ret != 4096) {
            return false;
        }
        return SyncManifestFd(fd);
    }
    if (state != kManifestBatchTxnStatePrepared) {
        free(header);
        return true;
    }

    const uint32_t page_count = GetU32(header + 8);
    const uint32_t has_super_page = GetU32(header + 12);
    const uint64_t payload_bytes = GetU64(header + 16);
    const uint64_t payload_checksum = GetU64(header + 24);
    free(header);

    const size_t aligned_payload_bytes =
        static_cast<size_t>(roundup(static_cast<size_t>(payload_bytes), static_cast<size_t>(4096)));
    const size_t payload_capacity = ManifestTxnLogSize - 4096;
    if (aligned_payload_bytes > payload_capacity) {
        std::cout << "ReplayPendingBatchTxn payload overflow, payload="
                  << aligned_payload_bytes << ", cap=" << payload_capacity << std::endl;
        return false;
    }
    const uint64_t min_payload_bytes =
        static_cast<uint64_t>(page_count) * static_cast<uint64_t>(sizeof(uint64_t) + 4096) +
        static_cast<uint64_t>(has_super_page ? 4096 : 0);
    if (payload_bytes < min_payload_bytes) {
        std::cout << "ReplayPendingBatchTxn payload too small, payload=" << payload_bytes
                  << ", expected>=" << min_payload_bytes << std::endl;
        return false;
    }

    char *payload = nullptr;
    if (aligned_payload_bytes > 0) {
        payload = static_cast<char *>(aligned_alloc(4096, aligned_payload_bytes));
        memset(payload, 0, aligned_payload_bytes);
        ret = pread(fd, payload, aligned_payload_bytes, manifest_txn_start_ + 4096);
        if (ret != static_cast<ssize_t>(aligned_payload_bytes)) {
            std::cout << "ReplayPendingBatchTxn read payload wrong, ret=" << ret
                      << ", errno=" << errno << std::endl;
            free(payload);
            return false;
        }
        const uint64_t actual_checksum =
            ComputeFnv1a64(reinterpret_cast<const uint8_t *>(payload),
                           static_cast<size_t>(payload_bytes));
        if (actual_checksum != payload_checksum) {
            std::cout << "ReplayPendingBatchTxn checksum mismatch, expected="
                      << payload_checksum << ", actual=" << actual_checksum << std::endl;
            free(payload);
            return false;
        }
    }

    const char *cursor = payload;
    for (uint32_t i = 0; i < page_count; ++i) {
        const uint64_t page_offset = GetU64(cursor);
        cursor += sizeof(uint64_t);
        if ((page_offset % 4096) != 0 ||
            page_offset < 4096 ||
            page_offset >= static_cast<uint64_t>(ManifestSize)) {
            std::cout << "ReplayPendingBatchTxn invalid page offset " << page_offset << std::endl;
            if (payload != nullptr) {
                free(payload);
            }
            return false;
        }
        ret = pwrite(fd, cursor, 4096, static_cast<off_t>(page_offset));
        if (ret != 4096) {
            std::cout << "ReplayPendingBatchTxn apply page wrong, ret=" << ret
                      << ", offset=" << page_offset << ", errno=" << errno << std::endl;
            if (payload != nullptr) {
                free(payload);
            }
            return false;
        }
        cursor += 4096;
    }

    if (has_super_page != 0u) {
        ret = pwrite(fd, cursor, 4096, 0);
        if (ret != 4096) {
            std::cout << "ReplayPendingBatchTxn apply super wrong, ret=" << ret
                      << ", errno=" << errno << std::endl;
            if (payload != nullptr) {
                free(payload);
            }
            return false;
        }
    }
    if (payload != nullptr) {
        free(payload);
    }

    memset(buf_, 0, 4096);
    ret = pwrite(fd, buf_, 4096, manifest_txn_start_);
    if (ret != 4096) {
        std::cout << "ReplayPendingBatchTxn clear header wrong, ret=" << ret
                  << ", errno=" << errno << std::endl;
        return false;
    }
    return SyncManifestFd(fd);
}

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
    tail = super_.l1_tail;  // Must use l1_tail for L1 recovery in compatibility mode.
    unsigned current_L1_version = super_.l1_current_seq_no;
    DEBUG("l1_version=%u", current_L1_version);
    std::vector<uint8_t> l1_hybrid_state_bytes;
    const bool has_l1_snapshot = LoadL1HybridState(current_L1_version, l1_hybrid_state_bytes);
    if (!has_l1_snapshot) {
        ERROR_EXIT("missing L1 hybrid state snapshot in tableless recovery mode");
    }
    version->BeginRecoverLevel1(current_L1_version + 1, 0);
    if (!version->ImportL1HybridState(l1_hybrid_state_bytes, current_L1_version)) {
        ERROR_EXIT("invalid L1 hybrid state snapshot in tableless recovery mode");
    }
    INFO("recover L1 hybrid state(tableless mode), seq=%u", current_L1_version);
    pst_deleter.PersistCheckpoint();
    INFO("skip L1 manifest consistency fix in tableless snapshot recovery mode");
    return version;
}

void Manifest::PrintL1Info() {
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
	printf("[Manifest] l1 tail=%lu, l1 snapshot tracking only\n",
           CurrentSuper().l1_tail);
}

int Manifest::AddTable(PSTMeta meta, int level)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    if (level != 0) {
        ERROR_EXIT("Manifest::AddTable only supports L0 metadata in tableless mode");
    }
    int idx = -1;
    ManifestSuperMeta &super_ref = MutableSuper();
    if (!l0_freelist_.empty())
    {
        idx = l0_freelist_.front();
        l0_freelist_.pop();
    }
    else
    {
        idx = super_ref.l0_tail;
        if (idx * sizeof(PSTMeta) >= L0MetaSize)
        {
            L0GC();
            idx = l0_freelist_.front();
            l0_freelist_.pop();
        }
        else
        {
            (super_ref.l0_tail)++;
        }
    }
    off_t offset = Getoff(idx, level);
    const size_t slot_offset = idx%(4096/sizeof(PSTMeta))*sizeof(PSTMeta);
    if (batch_active_) {
        char *page = AcquireBatchPage(offset);
        memcpy(page + slot_offset, &meta, sizeof(PSTMeta));
        auto it = batch_pages_.find(offset);
        assert(it != batch_pages_.end());
        it->second.dirty = true;
        batch_super_dirty_ = true;
    } else {
        auto ret = pread(fd, buf_, 4096, offset);
        if(ret!=4096)
            std::cout<<"AddTable read wrong"<<std::endl; 
        memcpy(buf_ + slot_offset, &meta, sizeof(PSTMeta));
        ret = pwrite(fd, buf_, 4096, offset);
        if(ret!=4096) {
            std::cout<<"AddTable write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite AddTable buf_");
        }
        PersistSuper(super_ref);
    }
    return idx;
}

void Manifest::DeleteTable(int idx, int level)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    if (level != 0) {
        ERROR_EXIT("Manifest::DeleteTable only supports L0 metadata in tableless mode");
    }
    auto temp = PSTMeta::InvalidTable();
    off_t offset = Getoff(idx, level);
    const size_t slot_offset = idx%(4096/sizeof(PSTMeta))*sizeof(PSTMeta);
    if (batch_active_) {
        char *page = AcquireBatchPage(offset);
        memcpy(page + slot_offset, &temp, sizeof(PSTMeta));
        auto it = batch_pages_.find(offset);
        assert(it != batch_pages_.end());
        it->second.dirty = true;
    } else {
        auto ret = pread(fd, buf_, 4096, offset);
        if(ret!=4096)
            std::cout<<"DeleteTable read wrong"<<std::endl; 
        memcpy(buf_ + slot_offset, &temp, sizeof(PSTMeta));
        ret = pwrite(fd, buf_, 4096, offset);
        if(ret!=4096) {
            std::cout<<"DeleteTable write wrong, ret="<<ret<<", errno="<<errno<<std::endl;
            perror("pwrite DeleteTable buf_");
        }
    }
    l0_freelist_.push(idx);
}

void Manifest::AddFlushLog(std::vector<uint64_t> &deleted_log_segment_ids)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    if (super_.flush_log.is_valid == 1)
    {
        ERROR_EXIT("double flushing");
    }
    LOG("add flush log %lu,%lu,%u", deleted_log_segment_ids.size(), deleted_log_segment_ids.size() * sizeof(uint64_t), FlushLogSize);
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
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
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
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
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
    free(buffer);
    return true;
}

bool Manifest::PersistL1HybridState(const std::vector<uint8_t>& bytes, uint32_t current_l1_seq_no)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    const size_t stored_bytes = kL1HybridSnapshotEnvelopeHeaderSize + bytes.size();
    if (stored_bytes > kL1HybridSlotBytes) {
        return false;
    }
    const size_t aligned_size = roundup(stored_bytes, static_cast<size_t>(4096));
    if (aligned_size > kL1HybridSlotBytes) {
        return false;
    }

    const auto& super_ref = CurrentSuper();
    uint32_t active_slot = 0;
    if (super_ref.l1_hybrid_state.is_valid) {
        active_slot = DecodeL1HybridSlot(static_cast<uint64_t>(super_ref.l1_hybrid_state.length));
    }
    const uint32_t target_slot = super_ref.l1_hybrid_state.is_valid ? (active_slot ^ 1u) : 0u;
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
    if (batch_active_) {
        for (size_t written = 0; written < aligned_size; written += 4096) {
            const off_t page_offset = slot_offset + static_cast<off_t>(written);
            char* page = AcquireBatchPage(page_offset);
            memcpy(page, write_buf + written, 4096);
            auto it = batch_pages_.find(page_offset);
            assert(it != batch_pages_.end());
            it->second.dirty = true;
        }
    } else {
        auto ret = pwrite(fd, write_buf, aligned_size, slot_offset);
        if (ret != static_cast<ssize_t>(aligned_size)) {
            free(write_buf);
            return false;
        }
    }
    free(write_buf);

    ManifestSuperMeta::L1HybridState l1_hybrid_state{};
    l1_hybrid_state.is_valid = 1;
    l1_hybrid_state.length =
        EncodeL1HybridLengthWithSlot(stored_bytes, target_slot);
    l1_hybrid_state.seq_no = static_cast<uint16_t>(current_l1_seq_no & 0xFFFFu);
    auto& super_mut = MutableSuper();
    super_mut.l1_hybrid_state = l1_hybrid_state;
    if (batch_active_) {
        batch_super_dirty_ = true;
        return true;
    }
    return PersistSuper(super_mut);
}

bool Manifest::LoadL1HybridState(uint32_t expected_l1_seq_no, std::vector<uint8_t>& bytes_out)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
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
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    auto& super_ref = MutableSuper();
    super_ref.l1_hybrid_state.is_valid = 0;
    super_ref.l1_hybrid_state.length = 0;
    super_ref.l1_hybrid_state.seq_no = 0;
    if (batch_active_) {
        batch_super_dirty_ = true;
        return;
    }
    if (!PersistSuper(super_ref)) {
        std::cout<<"ClearL1HybridState write wrong, errno="<<errno<<std::endl;
        perror("PersistSuper ClearL1HybridState");
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
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    DEBUG("update L0 version = %u", min_seq_no);
    auto &super_ref = MutableSuper();
    super_ref.l0_min_valid_seq_no = min_seq_no;
    if (batch_active_) {
        batch_super_dirty_ = true;
    } else {
        PersistSuper(super_ref);
    }
}

void Manifest::UpdateL1Version(unsigned current_seq_no)
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    auto &super_ref = MutableSuper();
    super_ref.l1_current_seq_no = current_seq_no;
    if (batch_active_) {
        batch_super_dirty_ = true;
    } else {
        PersistSuper(super_ref);
    }
}

unsigned Manifest::GetL0Version()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    return CurrentSuper().l0_min_valid_seq_no;
}

void Manifest::L0GC()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
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

Manifest::ResidentMemoryStats Manifest::DebugEstimateResidentMemory() const
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    ResidentMemoryStats stats;
    stats.aligned_super_page_buffer_bytes = 4096;
    stats.super_meta_bytes = sizeof(super_);
    stats.batch_super_meta_bytes = sizeof(batch_super_);

    stats.l0_freelist_size = l0_freelist_.size();
    stats.l0_freelist_estimated_bytes = stats.l0_freelist_size * sizeof(int);

    stats.batch_pages_count = batch_pages_.size();
    stats.batch_pages_data_bytes = batch_pages_.size() * 4096;
    stats.batch_pages_map_node_estimated_bytes =
        batch_pages_.size() * (sizeof(std::pair<const off_t, BatchPage>) + sizeof(void*) * 2);
    stats.batch_pages_map_bucket_bytes = batch_pages_.bucket_count() * sizeof(void*);

    stats.total_estimated_bytes =
        stats.aligned_super_page_buffer_bytes +
        stats.super_meta_bytes +
        stats.batch_super_meta_bytes +
        stats.l0_freelist_estimated_bytes +
        stats.batch_pages_data_bytes +
        stats.batch_pages_map_node_estimated_bytes +
        stats.batch_pages_map_bucket_bytes;
    return stats;
}

size_t Manifest::DebugReleaseVolatileStateForProbe()
{
    std::lock_guard<std::recursive_mutex> guard(manifest_mu_);
    const ResidentMemoryStats before = DebugEstimateResidentMemory();

    std::queue<int> empty_l0;
    std::swap(l0_freelist_, empty_l0);

    ClearBatchPages();
    batch_pages_.rehash(0);
    batch_active_ = false;
    batch_super_dirty_ = false;

    return before.l0_freelist_estimated_bytes +
           before.batch_pages_data_bytes +
           before.batch_pages_map_node_estimated_bytes +
           before.batch_pages_map_bucket_bytes;
}
