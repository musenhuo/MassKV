#include "lib/hybrid_l1/normal_pack.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace flowkv::hybrid_l1 {

namespace {

void PutU16(std::vector<uint8_t>& out, size_t off, uint16_t value) {
    out[off] = static_cast<uint8_t>(value & 0xFFu);
    out[off + 1] = static_cast<uint8_t>((value >> 8) & 0xFFu);
}

void PutU32(std::vector<uint8_t>& out, size_t off, uint32_t value) {
    out[off] = static_cast<uint8_t>(value & 0xFFu);
    out[off + 1] = static_cast<uint8_t>((value >> 8) & 0xFFu);
    out[off + 2] = static_cast<uint8_t>((value >> 16) & 0xFFu);
    out[off + 3] = static_cast<uint8_t>((value >> 24) & 0xFFu);
}

void PutU64(std::vector<uint8_t>& out, size_t off, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        out[off + i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFFu);
    }
}

uint16_t GetU16(const std::vector<uint8_t>& in, size_t off) {
    return static_cast<uint16_t>(in[off]) |
           static_cast<uint16_t>(in[off + 1] << 8);
}

uint32_t GetU32(const std::vector<uint8_t>& in, size_t off) {
    return static_cast<uint32_t>(in[off]) |
           (static_cast<uint32_t>(in[off + 1]) << 8) |
           (static_cast<uint32_t>(in[off + 2]) << 16) |
           (static_cast<uint32_t>(in[off + 3]) << 24);
}

uint64_t GetU64(const std::vector<uint8_t>& in, size_t off) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(in[off + i]) << (i * 8);
    }
    return value;
}

}  // namespace

bool NormalPackCodec::Encode(const NormalPackPage& pack,
                             uint32_t page_size,
                             std::vector<uint8_t>& out) {
    if (page_size == 0 || page_size % 4096 != 0) {
        return false;
    }
    if (pack.slots.size() > kMaxSlots) {
        return false;
    }
    if (pack.entries.size() > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        return false;
    }
    for (const auto& slot : pack.slots) {
        const size_t begin = static_cast<size_t>(slot.entry_begin);
        const size_t end = begin + static_cast<size_t>(slot.entry_count);
        if (end > pack.entries.size()) {
            return false;
        }
    }

    const size_t payload_bytes =
        kHeaderBytes + pack.slots.size() * kSlotBytes + pack.entries.size() * kEntryBytes;
    if (payload_bytes > page_size) {
        return false;
    }

    out.assign(page_size, 0);
    PutU32(out, 0, kMagic);
    PutU16(out, 4, kFormatVersion);
    PutU16(out, 6, static_cast<uint16_t>(pack.slots.size()));
    PutU32(out, 8, static_cast<uint32_t>(pack.entries.size()));
    PutU32(out, 12, page_size);

    size_t off = kHeaderBytes;
    for (const auto& slot : pack.slots) {
        PutU64(out, off, slot.prefix);
        PutU16(out, off + 8, slot.entry_begin);
        PutU16(out, off + 10, slot.entry_count);
        PutU32(out, off + 12, slot.reserved);
        off += kSlotBytes;
    }
    for (const auto& entry : pack.entries) {
        PutU64(out, off, entry.suffix_min);
        PutU64(out, off + 8, entry.suffix_max);
        PutU64(out, off + 16, entry.leaf_value);
        off += kEntryBytes;
    }
    return true;
}

bool NormalPackCodec::Decode(const std::vector<uint8_t>& bytes, NormalPackPage& out) {
    out.slots.clear();
    out.entries.clear();
    if (bytes.size() < kHeaderBytes) {
        return false;
    }
    if (GetU32(bytes, 0) != kMagic) {
        return false;
    }
    if (GetU16(bytes, 4) != kFormatVersion) {
        return false;
    }
    const uint16_t slot_count = GetU16(bytes, 6);
    const uint32_t entry_count = GetU32(bytes, 8);
    const uint32_t page_size = GetU32(bytes, 12);
    if (page_size != bytes.size()) {
        return false;
    }
    if (slot_count > kMaxSlots) {
        return false;
    }
    const size_t need_bytes =
        kHeaderBytes + static_cast<size_t>(slot_count) * kSlotBytes +
        static_cast<size_t>(entry_count) * kEntryBytes;
    if (need_bytes > bytes.size()) {
        return false;
    }

    out.slots.resize(slot_count);
    out.entries.resize(entry_count);

    size_t off = kHeaderBytes;
    for (size_t i = 0; i < slot_count; ++i) {
        auto& slot = out.slots[i];
        slot.prefix = GetU64(bytes, off);
        slot.entry_begin = GetU16(bytes, off + 8);
        slot.entry_count = GetU16(bytes, off + 10);
        slot.reserved = GetU32(bytes, off + 12);
        const size_t begin = static_cast<size_t>(slot.entry_begin);
        const size_t end = begin + static_cast<size_t>(slot.entry_count);
        if (end > entry_count) {
            return false;
        }
        off += kSlotBytes;
    }
    for (size_t i = 0; i < entry_count; ++i) {
        auto& entry = out.entries[i];
        entry.suffix_min = GetU64(bytes, off);
        entry.suffix_max = GetU64(bytes, off + 8);
        entry.leaf_value = GetU64(bytes, off + 16);
        off += kEntryBytes;
    }
    return true;
}

bool NormalPackCodec::Lookup(const NormalPackPage& pack,
                             uint16_t slot_id,
                             RouteSuffix suffix,
                             RouteSuffix& suffix_min_out,
                             RouteSuffix& suffix_max_out,
                             uint64_t& leaf_value_out) {
    if (slot_id >= pack.slots.size()) {
        return false;
    }
    const auto& slot = pack.slots[slot_id];
    const size_t begin = static_cast<size_t>(slot.entry_begin);
    const size_t end = begin + static_cast<size_t>(slot.entry_count);
    if (begin > pack.entries.size() || end > pack.entries.size()) {
        return false;
    }
    const auto it = std::lower_bound(pack.entries.begin() + begin,
                                     pack.entries.begin() + end,
                                     suffix,
                                     [](const NormalPackEntry& entry, RouteSuffix target) {
                                         return entry.suffix_max < target;
                                     });
    if (it == pack.entries.begin() + end) {
        return false;
    }
    if (it->suffix_min > suffix) {
        return false;
    }
    suffix_min_out = it->suffix_min;
    suffix_max_out = it->suffix_max;
    leaf_value_out = it->leaf_value;
    return true;
}

bool NormalPackCodec::LookupFromBytes(const std::vector<uint8_t>& bytes,
                                      uint16_t slot_id,
                                      RouteSuffix suffix,
                                      RouteSuffix& suffix_min_out,
                                      RouteSuffix& suffix_max_out,
                                      uint64_t& leaf_value_out) {
    NormalPackPage page;
    if (!Decode(bytes, page)) {
        return false;
    }
    return Lookup(page, slot_id, suffix, suffix_min_out, suffix_max_out, leaf_value_out);
}

}  // namespace flowkv::hybrid_l1
