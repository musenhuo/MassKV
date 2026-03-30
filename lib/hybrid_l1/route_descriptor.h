#pragma once

#include "subtree_page.h"
#include "subtree_record.h"

#include <cstdint>

namespace flowkv::hybrid_l1 {

enum class RouteDescriptorMode : uint8_t {
    kTinyDirect = 0,
    kNormalSubtree = 1,
    kNormalPack = 2,
};

struct RouteDescriptor {
    static constexpr uint64_t kModeBit = 1ULL << 63;
    static constexpr uint64_t kNormalPackBit = 1ULL << 62;
    static constexpr uint64_t kPayloadMask = kModeBit - 1ULL;
    static constexpr uint64_t kNormalPayloadMask = kNormalPackBit - 1ULL;
    static constexpr uint32_t kPackSlotBits = 8;
    static constexpr uint64_t kPackSlotMask = (1ULL << kPackSlotBits) - 1ULL;
    static constexpr uint32_t kPackPagePtrShift = 12;

    static bool IsNormal(uint64_t descriptor) {
        return (descriptor & kModeBit) != 0;
    }

    static bool IsTiny(uint64_t descriptor) {
        return descriptor != 0 && (descriptor & kModeBit) == 0;
    }

    static bool IsValid(uint64_t descriptor) {
        return descriptor != 0;
    }

    static bool IsNormalPack(uint64_t descriptor) {
        return IsNormal(descriptor) && (descriptor & kNormalPackBit) != 0;
    }

    static bool IsNormalSubtree(uint64_t descriptor) {
        return IsNormal(descriptor) && (descriptor & kNormalPackBit) == 0;
    }

    static uint64_t EncodeNormalSubtree(SubtreePagePtr root_page_ptr) {
        if (root_page_ptr == kInvalidSubtreePagePtr || (root_page_ptr & kModeBit) != 0) {
            return 0;
        }
        return kModeBit | static_cast<uint64_t>(root_page_ptr);
    }

    static uint64_t EncodeNormal(SubtreePagePtr root_page_ptr) {
        return EncodeNormalSubtree(root_page_ptr);
    }

    static SubtreePagePtr DecodeNormalRoot(uint64_t descriptor) {
        if (!IsNormalSubtree(descriptor)) {
            return kInvalidSubtreePagePtr;
        }
        return static_cast<SubtreePagePtr>(descriptor & kNormalPayloadMask);
    }

    static uint64_t EncodeNormalPack(SubtreePagePtr pack_page_ptr, uint8_t slot_id) {
        if (pack_page_ptr == kInvalidSubtreePagePtr || (pack_page_ptr & ((1ULL << kPackPagePtrShift) - 1ULL)) != 0) {
            return 0;
        }
        const uint64_t shifted = pack_page_ptr >> kPackPagePtrShift;
        const uint64_t max_shifted =
            (kNormalPayloadMask >> kPackSlotBits);
        if (shifted > max_shifted) {
            return 0;
        }
        const uint64_t payload = (shifted << kPackSlotBits) | static_cast<uint64_t>(slot_id);
        return kModeBit | kNormalPackBit | payload;
    }

    static bool DecodeNormalPack(uint64_t descriptor,
                                 SubtreePagePtr& pack_page_ptr_out,
                                 uint8_t& slot_id_out) {
        pack_page_ptr_out = kInvalidSubtreePagePtr;
        slot_id_out = 0;
        if (!IsNormalPack(descriptor)) {
            return false;
        }
        const uint64_t payload = descriptor & kNormalPayloadMask;
        const uint64_t shifted = payload >> kPackSlotBits;
        slot_id_out = static_cast<uint8_t>(payload & kPackSlotMask);
        pack_page_ptr_out = static_cast<SubtreePagePtr>(shifted << kPackPagePtrShift);
        return pack_page_ptr_out != kInvalidSubtreePagePtr;
    }

    static bool CanEncodeTinyLeafValue(uint64_t leaf_value) {
        return leaf_value != 0 && (leaf_value & kModeBit) == 0;
    }

    static uint64_t EncodeTinyLeafValue(uint64_t leaf_value) {
        if (!CanEncodeTinyLeafValue(leaf_value)) {
            return 0;
        }
        return leaf_value;
    }

    static SubtreeRecord::LeafValueParts DecodeTinyWindow(uint64_t descriptor) {
        return SubtreeRecord::UnpackLeafValue(descriptor);
    }
};

}  // namespace flowkv::hybrid_l1
