#pragma once

#include "prefix_suffix.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace flowkv::hybrid_l1 {

struct NormalPackSlot {
    RoutePrefix prefix = 0;
    uint16_t entry_begin = 0;
    uint16_t entry_count = 0;
    uint32_t reserved = 0;
};

struct NormalPackEntry {
    RouteSuffix suffix_min = 0;
    RouteSuffix suffix_max = 0;
    uint64_t leaf_value = 0;
};

struct NormalPackPage {
    std::vector<NormalPackSlot> slots;
    std::vector<NormalPackEntry> entries;
};

class NormalPackCodec {
public:
    static constexpr uint32_t kMagic = 0x4B434150u;  // "PACK"
    static constexpr uint16_t kFormatVersion = 1;
    static constexpr uint32_t kDefaultPageSize = 16 * 1024;
    static constexpr uint16_t kMaxSlots = 256;
    static constexpr size_t kSlotBytes = 16;
    static constexpr size_t kEntryBytes = 24;
    static constexpr size_t kHeaderBytes = 32;

    static bool Encode(const NormalPackPage& pack,
                       uint32_t page_size,
                       std::vector<uint8_t>& out);
    static bool Decode(const std::vector<uint8_t>& bytes, NormalPackPage& out);

    static bool Lookup(const NormalPackPage& pack,
                       uint16_t slot_id,
                       RouteSuffix suffix,
                       RouteSuffix& suffix_min_out,
                       RouteSuffix& suffix_max_out,
                       uint64_t& leaf_value_out);
    static bool LookupFromBytes(const std::vector<uint8_t>& bytes,
                                uint16_t slot_id,
                                RouteSuffix suffix,
                                RouteSuffix& suffix_min_out,
                                RouteSuffix& suffix_max_out,
                                uint64_t& leaf_value_out);
};

}  // namespace flowkv::hybrid_l1
