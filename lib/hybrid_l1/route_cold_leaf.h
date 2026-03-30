#pragma once

#include "subtree_page.h"
#include "prefix_suffix.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <vector>

namespace flowkv::hybrid_l1 {

// ============================================================
// ColdLeafStub: minimal node that replaces a spilled Masstree leaf.
//
// Inherits node_base so that reach_leaf() stops here (isleaf_bit set).
// The cold_bit in the version field lets find_unlocked() bail out
// before touching any leaf-specific memory.
// ============================================================

static constexpr uint32_t kColdLeafMagic = 0xC01D1EAFu;

// SSD page constants
static constexpr uint32_t kSpilledLeafPageMagic   = 0x4D4C5053u;  // "SPLM"
static constexpr uint16_t kSpilledLeafPageVersion  = 2;
static constexpr size_t   kSpilledLeafHeaderBytes  = 16;
static constexpr size_t   kSpilledLeafSlotBytes    = 4;   // begin_entry(2) + entry_count(2)
static constexpr size_t   kSpilledLeafEntryBytes   = 16;  // prefix(8) + descriptor(8)
// Logical cold-leaf page granularity is 4KB.
static constexpr size_t   kSpilledLeafMinPageBytes = 4 * 1024;

// Forward-declare the Masstree node_base we inherit from.
// MasstreeWrapper::table_params is the P used everywhere.
// We template ColdLeafStub on P so it works with any param set.

template <typename P>
struct ColdLeafStub : public Masstree::node_base<P> {
    SubtreePagePtr ssd_page_ptr;   // 8B: where the leaf data lives on SSD
    uint16_t entry_count;          // 2B: number of entries in the SSD page
    uint16_t slot_id;              // 2B: packed-slot id in shared spilled page
    uint32_t magic;                // 4B: kColdLeafMagic for validation
    Masstree::node_base<P>* parent_ptr;  // 8B: parent internode (for set_parent)

    ColdLeafStub(SubtreePagePtr ssd_ptr, uint16_t count, uint16_t slot)
        : Masstree::node_base<P>(true),  // isleaf = true
          ssd_page_ptr(ssd_ptr),
          entry_count(count),
          slot_id(slot),
          magic(kColdLeafMagic),
          parent_ptr(nullptr) {
        this->mark_cold();
    }

    bool validate() const {
        return magic == kColdLeafMagic && this->cold();
    }
};

// ============================================================
// Detection helpers (work on node_base pointers)
// ============================================================

template <typename P>
inline bool IsColdLeafStub(const Masstree::node_base<P>* node) {
    if (!node) return false;
    // Check version bits without accessing leaf-specific fields
    auto v = node->stable(relax_fence_function());
    return v.isleaf() && v.cold();
}

template <typename P>
inline ColdLeafStub<P>* AsColdStub(Masstree::node_base<P>* node) {
    return static_cast<ColdLeafStub<P>*>(node);
}

template <typename P>
inline const ColdLeafStub<P>* AsColdStub(const Masstree::node_base<P>* node) {
    return static_cast<const ColdLeafStub<P>*>(node);
}

// ============================================================
// SSD page serialization
// ============================================================

struct SpilledLeafEntry {
    RoutePrefix prefix;
    uint64_t descriptor;
};

inline std::vector<uint8_t> SerializePackedLeafEntries(
        const std::vector<const std::vector<SpilledLeafEntry>*>& slots,
        uint32_t page_size) {
    if (page_size < kSpilledLeafMinPageBytes || page_size % 4096 != 0) {
        throw std::invalid_argument("spilled leaf page size must be >=4KB and 4KB-aligned");
    }
    if (slots.empty()) {
        throw std::invalid_argument("SerializePackedLeafEntries requires non-empty slots");
    }

    const uint16_t slot_count = static_cast<uint16_t>(slots.size());
    uint32_t total_entries_u32 = 0;
    for (const auto* slot : slots) {
        if (slot == nullptr) {
            throw std::invalid_argument("SerializePackedLeafEntries received null slot");
        }
        total_entries_u32 += static_cast<uint32_t>(slot->size());
    }
    if (total_entries_u32 > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument("too many packed spilled-leaf entries");
    }

    const uint16_t total_entries = static_cast<uint16_t>(total_entries_u32);
    const size_t data_offset = kSpilledLeafHeaderBytes +
                               static_cast<size_t>(slot_count) * kSpilledLeafSlotBytes;
    const size_t required = data_offset +
                            static_cast<size_t>(total_entries) * kSpilledLeafEntryBytes;
    if (required > page_size) {
        throw std::invalid_argument("packed spilled-leaf page overflow");
    }

    std::vector<uint8_t> page(page_size, 0);
    memcpy(page.data() + 0, &kSpilledLeafPageMagic, 4);
    memcpy(page.data() + 4, &kSpilledLeafPageVersion, 2);
    memcpy(page.data() + 6, &slot_count, 2);
    memcpy(page.data() + 8, &total_entries, 2);

    uint16_t begin_entry = 0;
    for (uint16_t slot_id = 0; slot_id < slot_count; ++slot_id) {
        const auto* slot = slots[slot_id];
        const uint16_t count = static_cast<uint16_t>(slot->size());
        const size_t slot_off = kSpilledLeafHeaderBytes +
                                static_cast<size_t>(slot_id) * kSpilledLeafSlotBytes;
        memcpy(page.data() + slot_off + 0, &begin_entry, 2);
        memcpy(page.data() + slot_off + 2, &count, 2);
        begin_entry = static_cast<uint16_t>(begin_entry + count);
    }

    uint16_t cursor = 0;
    for (const auto* slot : slots) {
        for (const auto& e : *slot) {
            const size_t off = data_offset + static_cast<size_t>(cursor) * kSpilledLeafEntryBytes;
            memcpy(page.data() + off, &e.prefix, 8);
            memcpy(page.data() + off + 8, &e.descriptor, 8);
            ++cursor;
        }
    }
    return page;
}

inline bool ResolveSpilledLeafSlot(
        const uint8_t* page, size_t page_size, uint16_t slot_id,
        const uint8_t** slot_base, uint16_t* slot_count_out) {
    if (page == nullptr || page_size < kSpilledLeafHeaderBytes) {
        return false;
    }

    uint32_t magic;
    memcpy(&magic, page, 4);
    if (magic != kSpilledLeafPageMagic) {
        return false;
    }

    uint16_t version;
    memcpy(&version, page + 4, 2);
    if (version == 1) {
        if (slot_id != 0) {
            return false;
        }
        uint16_t count;
        memcpy(&count, page + 6, 2);
        const size_t data_bytes = static_cast<size_t>(count) * kSpilledLeafEntryBytes;
        if (kSpilledLeafHeaderBytes + data_bytes > page_size) {
            return false;
        }
        *slot_base = page + kSpilledLeafHeaderBytes;
        *slot_count_out = count;
        return true;
    }
    if (version != kSpilledLeafPageVersion) {
        return false;
    }

    uint16_t packed_slot_count;
    memcpy(&packed_slot_count, page + 6, 2);
    if (slot_id >= packed_slot_count) {
        return false;
    }
    uint16_t total_entries;
    memcpy(&total_entries, page + 8, 2);

    const size_t data_offset = kSpilledLeafHeaderBytes +
                               static_cast<size_t>(packed_slot_count) * kSpilledLeafSlotBytes;
    if (data_offset > page_size) {
        return false;
    }

    const size_t slot_off = kSpilledLeafHeaderBytes +
                            static_cast<size_t>(slot_id) * kSpilledLeafSlotBytes;
    uint16_t begin_entry = 0;
    uint16_t entry_count = 0;
    memcpy(&begin_entry, page + slot_off + 0, 2);
    memcpy(&entry_count, page + slot_off + 2, 2);
    if (begin_entry > total_entries ||
        static_cast<uint32_t>(begin_entry) + static_cast<uint32_t>(entry_count) > total_entries) {
        return false;
    }

    const size_t begin_off = data_offset + static_cast<size_t>(begin_entry) * kSpilledLeafEntryBytes;
    const size_t end_off = begin_off + static_cast<size_t>(entry_count) * kSpilledLeafEntryBytes;
    if (end_off > page_size) {
        return false;
    }

    *slot_base = page + begin_off;
    *slot_count_out = entry_count;
    return true;
}

// Binary search a spilled leaf slot for a given prefix.
// Returns true and sets *out_descriptor if found.
inline bool SearchSpilledLeafSlot(
        const uint8_t* page, size_t page_size, uint16_t slot_id,
        RoutePrefix target, uint64_t* out_descriptor) {
    const uint8_t* base = nullptr;
    uint16_t count = 0;
    if (!ResolveSpilledLeafSlot(page, page_size, slot_id, &base, &count)) {
        return false;
    }
    if (count == 0) {
        return false;
    }

    int lo = 0, hi = static_cast<int>(count) - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        RoutePrefix mid_prefix;
        memcpy(&mid_prefix, base + mid * kSpilledLeafEntryBytes, 8);
        if (mid_prefix == target) {
            memcpy(out_descriptor, base + mid * kSpilledLeafEntryBytes + 8, 8);
            return true;
        } else if (mid_prefix < target) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return false;
}

// Collect all entries from a spilled leaf slot within [start, end] prefix range.
inline void CollectFromSpilledLeafSlot(
        const uint8_t* page, size_t page_size, uint16_t slot_id,
        RoutePrefix start_prefix, RoutePrefix end_prefix,
        std::vector<SpilledLeafEntry>& out) {
    const uint8_t* base = nullptr;
    uint16_t count = 0;
    if (!ResolveSpilledLeafSlot(page, page_size, slot_id, &base, &count)) {
        return;
    }
    if (count == 0) {
        return;
    }

    int lo = 0, hi = static_cast<int>(count);
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        RoutePrefix p;
        memcpy(&p, base + mid * kSpilledLeafEntryBytes, 8);
        if (p < start_prefix) lo = mid + 1;
        else hi = mid;
    }

    // Scan forward collecting entries in range
    for (int i = lo; i < static_cast<int>(count); ++i) {
        SpilledLeafEntry e;
        memcpy(&e.prefix, base + i * kSpilledLeafEntryBytes, 8);
        if (e.prefix > end_prefix) break;
        memcpy(&e.descriptor, base + i * kSpilledLeafEntryBytes + 8, 8);
        out.push_back(e);
    }
}

// Backward-compatible helper for single-slot lookup.
inline bool SearchSpilledLeafPage(
        const uint8_t* page, size_t page_size,
        RoutePrefix target, uint64_t* out_descriptor) {
    return SearchSpilledLeafSlot(page, page_size, 0, target, out_descriptor);
}

inline void CollectFromSpilledLeafPage(
        const uint8_t* page, size_t page_size,
        RoutePrefix start_prefix, RoutePrefix end_prefix,
        std::vector<SpilledLeafEntry>& out) {
    CollectFromSpilledLeafSlot(page, page_size, 0, start_prefix, end_prefix, out);
}

}  // namespace flowkv::hybrid_l1
