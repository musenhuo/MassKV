#include "db/compaction/manifest.h"

#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>
#include <fcntl.h>

namespace {

size_t RoundUp4K(size_t bytes) {
    return (bytes + 4095) / 4096 * 4096;
}

[[noreturn]] void Fail(const std::string& msg) {
    std::cerr << "[manifest_batch_txn_replay_test] " << msg << std::endl;
    std::exit(1);
}

void Check(bool ok, const std::string& msg) {
    if (!ok) {
        Fail(msg);
    }
}

uint16_t ReadU16(const char* src) {
    return static_cast<uint16_t>(static_cast<uint8_t>(src[0])) |
           (static_cast<uint16_t>(static_cast<uint8_t>(src[1])) << 8);
}

uint32_t ReadU32(const char* src) {
    return static_cast<uint32_t>(static_cast<uint8_t>(src[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(src[3])) << 24);
}

uint64_t Fnv1a64(const uint8_t* data, size_t size) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

void WriteU16(char* dst, uint16_t value) {
    dst[0] = static_cast<char>(value & 0xFFu);
    dst[1] = static_cast<char>((value >> 8) & 0xFFu);
}

void WriteU32(char* dst, uint32_t value) {
    dst[0] = static_cast<char>(value & 0xFFu);
    dst[1] = static_cast<char>((value >> 8) & 0xFFu);
    dst[2] = static_cast<char>((value >> 16) & 0xFFu);
    dst[3] = static_cast<char>((value >> 24) & 0xFFu);
}

void WriteU64(char* dst, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        dst[i] = static_cast<char>((value >> (i * 8)) & 0xFFu);
    }
}

bool SyncFd(int fd) {
    while (true) {
        if (fdatasync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

std::string TempManifestPath() {
    return "/tmp/flowkv_manifest_batch_txn_replay_test.manifest";
}

}  // namespace

int main() {
    const std::string manifest_path = TempManifestPath();
    std::error_code ec;
    std::filesystem::remove(manifest_path, ec);

    const int fd = open(manifest_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_TRUNC, 0666);
    Check(fd >= 0, "open manifest failed");

    {
        Manifest manifest(fd, false);
    }

    constexpr uint32_t kTxnMagic = 0x5854424Du;  // MBTX
    constexpr uint16_t kTxnVersion = 1;
    constexpr uint16_t kTxnPrepared = 1;
    const off_t l0_start = 4096;
    const off_t l1_start = l0_start + L0MetaSize;
    const off_t flush_log_start = l1_start + L1MetaSize;
    const off_t l1_hybrid_start = flush_log_start + FlushLogSize;
    const off_t txn_start = l1_hybrid_start + L1HybridStateSize;
    const off_t payload_start = txn_start + 4096;
    const off_t target_page_offset = l1_start;
    constexpr uint32_t kPageCount = 1;
    constexpr uint32_t kHasSuper = 1;

    PSTMeta expected{};
    expected.datablock_ptr_ = 0x12345000ULL;
#if defined(FLOWKV_KEY16)
    expected.min_key_hi = 7;
    expected.min_key_lo = 11;
    expected.max_key_hi = 7;
    expected.max_key_lo = 19;
#else
    expected.min_key_ = 11;
    expected.max_key_ = 19;
#endif
    expected.seq_no_ = 42;
    expected.entry_num_ = 3;

    constexpr size_t kPayloadBytes = static_cast<size_t>(8 + 4096 + 4096);
    const size_t kAlignedPayloadBytes = RoundUp4K(kPayloadBytes);
    auto* payload_page = static_cast<char*>(aligned_alloc(4096, kAlignedPayloadBytes));
    auto* header_page = static_cast<char*>(aligned_alloc(4096, 4096));
    auto* read_page = static_cast<char*>(aligned_alloc(4096, 4096));
    Check(payload_page != nullptr && header_page != nullptr && read_page != nullptr,
          "aligned_alloc failed");
    memset(payload_page, 0, kAlignedPayloadBytes);
    memset(header_page, 0, 4096);
    memset(read_page, 0, 4096);

    // txn payload: [page_offset(8) + page_data(4096)] + [super_page(4096)]
    WriteU64(payload_page, static_cast<uint64_t>(target_page_offset));
    memcpy(payload_page + 8, &expected, sizeof(expected));
    ManifestSuperMeta replay_super{};
    replay_super.l0_min_valid_seq_no = 5;
    replay_super.l1_current_seq_no = 99;
    replay_super.l0_tail = 0;
    replay_super.l1_tail = 17;
    replay_super.flush_log = {0, 0};
    replay_super.l1_hybrid_state = {0, 0, 0};
    memcpy(payload_page + 8 + 4096, &replay_super, sizeof(replay_super));

    const uint64_t payload_bytes = static_cast<uint64_t>(kPayloadBytes);
    const uint64_t checksum = Fnv1a64(reinterpret_cast<const uint8_t*>(payload_page),
                                      static_cast<size_t>(payload_bytes));
    WriteU32(header_page + 0, kTxnMagic);
    WriteU16(header_page + 4, kTxnVersion);
    WriteU16(header_page + 6, kTxnPrepared);
    WriteU32(header_page + 8, kPageCount);
    WriteU32(header_page + 12, kHasSuper);
    WriteU64(header_page + 16, payload_bytes);
    WriteU64(header_page + 24, checksum);

    ssize_t ret = pwrite(fd, payload_page, kAlignedPayloadBytes, payload_start);
    Check(ret == static_cast<ssize_t>(kAlignedPayloadBytes), "write txn payload failed");
    ret = pwrite(fd, header_page, 4096, txn_start);
    Check(ret == 4096, "write txn header failed");
    Check(SyncFd(fd), "fdatasync before replay failed");

    // recover path should replay txn and clear header automatically
    {
        Manifest recovered(fd, true);
        (void)recovered;
    }

    memset(read_page, 0, 4096);
    ret = pread(fd, read_page, 4096, target_page_offset);
    Check(ret == 4096, "read applied target page failed");
    PSTMeta actual{};
    memcpy(&actual, read_page, sizeof(actual));
    Check(actual.datablock_ptr_ == expected.datablock_ptr_, "applied PST datablock_ptr mismatch");
    Check(actual.seq_no_ == expected.seq_no_, "applied PST seq mismatch");
    Check(actual.entry_num_ == expected.entry_num_, "applied PST entry_num mismatch");

    memset(read_page, 0, 4096);
    ret = pread(fd, read_page, 4096, 0);
    Check(ret == 4096, "read super page failed");
    ManifestSuperMeta super_after{};
    memcpy(&super_after, read_page, sizeof(super_after));
    Check(super_after.l1_current_seq_no == replay_super.l1_current_seq_no, "super l1 seq mismatch");
    Check(super_after.l1_tail == replay_super.l1_tail, "super l1 tail mismatch");

    memset(read_page, 0, 4096);
    ret = pread(fd, read_page, 4096, txn_start);
    Check(ret == 4096, "read txn header after replay failed");
    Check(ReadU32(read_page + 0) == 0, "txn magic should be cleared");
    Check(ReadU16(read_page + 6) == 0, "txn state should be cleared");

    free(payload_page);
    free(header_page);
    free(read_page);
    close(fd);
    std::filesystem::remove(manifest_path, ec);

    std::cout << "[manifest_batch_txn_replay_test] PASS" << std::endl;
    return 0;
}
