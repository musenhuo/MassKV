#include "db/compaction/manifest.h"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

[[noreturn]] void Fail(const std::string& message) {
    std::cerr << "[manifest_l1_snapshot_test] " << message << std::endl;
    std::exit(1);
}

void Check(bool condition, const std::string& message) {
    if (!condition) {
        Fail(message);
    }
}

std::string MakeTempManifestPath() {
    return std::string("/tmp/flowkv_manifest_l1_snapshot_") + std::to_string(getpid()) + ".test";
}

void RemovePath(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

void FillPayload(std::vector<uint8_t>& payload) {
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>((i * 131u + 17u) & 0xFFu);
    }
}

void TestSnapshotRoundTripAndChecksumGuard() {
    constexpr uint32_t kSeqNo = 0x1234ABCDu;
    const std::string path = MakeTempManifestPath();
    RemovePath(path);

    std::vector<uint8_t> expected_payload(4096 + 37);
    FillPayload(expected_payload);

    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    Check(fd >= 0, "open manifest file for write should succeed");
    {
        Manifest manifest(fd, false);
        Check(manifest.PersistL1HybridState(expected_payload, kSeqNo),
              "PersistL1HybridState should succeed");

        std::vector<uint8_t> loaded_payload;
        Check(manifest.LoadL1HybridState(kSeqNo, loaded_payload),
              "LoadL1HybridState should succeed after persist");
        Check(loaded_payload == expected_payload, "loaded payload mismatch");

        loaded_payload.clear();
        Check(!manifest.LoadL1HybridState(kSeqNo + 1, loaded_payload),
              "LoadL1HybridState should reject wrong seq");
    }
    close(fd);

    fd = open(path.c_str(), O_RDWR, 0644);
    Check(fd >= 0, "open manifest file for recover should succeed");
    {
        Manifest manifest(fd, true);
        std::vector<uint8_t> loaded_payload;
        Check(manifest.LoadL1HybridState(kSeqNo, loaded_payload),
              "LoadL1HybridState should succeed after reopen");
        Check(loaded_payload == expected_payload, "recovered payload mismatch");

        constexpr off_t kHybridStateStart =
            static_cast<off_t>(4096 + L0MetaSize + L1MetaSize + FlushLogSize);
        uint8_t flipped = 0;
        const ssize_t read_ret = pread(fd, &flipped, 1, kHybridStateStart + 32);
        Check(read_ret == 1, "read snapshot byte for corruption should succeed");
        flipped ^= 0x5Au;
        const ssize_t write_ret = pwrite(fd, &flipped, 1, kHybridStateStart + 32);
        Check(write_ret == 1, "write snapshot byte for corruption should succeed");
        fsync(fd);

        loaded_payload.clear();
        Check(!manifest.LoadL1HybridState(kSeqNo, loaded_payload),
              "LoadL1HybridState should reject corrupted payload");
    }
    close(fd);
    RemovePath(path);
}

void TestSnapshotDualSlotAtomicSwitch() {
    constexpr uint32_t kSeqNo = 0x4567ABCDu;
    constexpr off_t kHybridStateStart =
        static_cast<off_t>(4096 + L0MetaSize + L1MetaSize + FlushLogSize);
    constexpr size_t kHybridSlotBytes = L1HybridStateSize / 2;
    const std::string path = MakeTempManifestPath();
    RemovePath(path);

    std::vector<uint8_t> payload_a(2048 + 11);
    std::vector<uint8_t> payload_b(3072 + 19);
    FillPayload(payload_a);
    FillPayload(payload_b);
    payload_b[0] ^= 0x7Fu;
    payload_b.back() ^= 0x3Cu;

    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    Check(fd >= 0, "open manifest file for dual-slot test should succeed");
    {
        Manifest manifest(fd, false);
        Check(manifest.PersistL1HybridState(payload_a, kSeqNo),
              "first PersistL1HybridState should succeed");
        Check(manifest.PersistL1HybridState(payload_b, kSeqNo),
              "second PersistL1HybridState should succeed");

        uint8_t flipped = 0;
        ssize_t read_ret = pread(fd, &flipped, 1, kHybridStateStart + 48);
        Check(read_ret == 1, "read inactive slot byte should succeed");
        flipped ^= 0x55u;
        ssize_t write_ret = pwrite(fd, &flipped, 1, kHybridStateStart + 48);
        Check(write_ret == 1, "write inactive slot byte should succeed");
        fsync(fd);

        std::vector<uint8_t> loaded_payload;
        Check(manifest.LoadL1HybridState(kSeqNo, loaded_payload),
              "LoadL1HybridState should survive inactive slot corruption");
        Check(loaded_payload == payload_b, "active slot payload mismatch after inactive slot corruption");

        read_ret = pread(fd, &flipped, 1, kHybridStateStart + static_cast<off_t>(kHybridSlotBytes) + 48);
        Check(read_ret == 1, "read active slot byte should succeed");
        flipped ^= 0x33u;
        write_ret = pwrite(fd, &flipped, 1, kHybridStateStart + static_cast<off_t>(kHybridSlotBytes) + 48);
        Check(write_ret == 1, "write active slot byte should succeed");
        fsync(fd);

        loaded_payload.clear();
        Check(!manifest.LoadL1HybridState(kSeqNo, loaded_payload),
              "LoadL1HybridState should fail after active slot corruption");
    }
    close(fd);
    RemovePath(path);
}

}  // namespace

int main() {
    TestSnapshotRoundTripAndChecksumGuard();
    TestSnapshotDualSlotAtomicSwitch();
    std::cout << "[manifest_l1_snapshot_test] all tests passed" << std::endl;
    return 0;
}
