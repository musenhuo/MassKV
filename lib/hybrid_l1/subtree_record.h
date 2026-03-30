#pragma once

#include "db/blocks/fixed_size_block.h"
#include "db/table.h"
#include "prefix_suffix.h"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace flowkv::hybrid_l1 {

/**
 * @brief L1 子树叶子中的路由记录。
 *
 * 该结构不存真实 value。
 * 叶子 payload 直接编码最终 KV block 的物理块指针与块内窗口，
 * 用于在读路径中直接完成 PST 块查询。
 */
struct SubtreeRecord {
    struct LeafValueParts {
        uint64_t kv_block_ptr = 0;  // 4KB block id (not byte offset)
        uint16_t offset = 0;        // entry offset inside KV block
        uint8_t count = 0;          // valid entry count for this prefix fragment
    };

    static constexpr uint32_t kLeafValueCountBits = 8;
    static constexpr uint32_t kLeafValueOffsetBits = 12;
    static constexpr uint32_t kLeafValueBlockPtrBits = 44;
    static constexpr uint32_t kLeafValueOffsetShift = kLeafValueCountBits;
    static constexpr uint32_t kLeafValueBlockPtrShift =
        kLeafValueCountBits + kLeafValueOffsetBits;
    static constexpr uint64_t kLeafValueCountMask = (1ULL << kLeafValueCountBits) - 1ULL;
    static constexpr uint64_t kLeafValueOffsetMask = (1ULL << kLeafValueOffsetBits) - 1ULL;
    static constexpr uint64_t kLeafValueBlockPtrMask =
        (1ULL << kLeafValueBlockPtrBits) - 1ULL;
    static constexpr uint32_t kKvBlockShift = 12;
    static_assert(kLeafValueCountBits + kLeafValueOffsetBits + kLeafValueBlockPtrBits == 64,
                  "leaf_value bit layout must occupy exactly 64 bits");
    static_assert((1u << kKvBlockShift) == 4096, "KV block shift must encode 4KB blocks");
    static_assert(PDataBlock::MAX_ENTRIES <= (1u << kLeafValueCountBits),
                  "leaf window count bits are insufficient for PDataBlock entries");

    KeyType min_key{};
    KeyType max_key{};
    RoutePrefix route_prefix = 0;
    RouteSuffix route_min_suffix = 0;
    RouteSuffix route_max_suffix = 0;
    uint32_t seq_no = 0;
    uint64_t table_idx = INVALID_PTR;
    uint64_t leaf_value = 0;

    static uint64_t PackLeafValue(uint64_t kv_block_ptr, uint16_t offset, uint8_t count) {
        const uint64_t block_ptr = kv_block_ptr & kLeafValueBlockPtrMask;
        const uint64_t off = static_cast<uint64_t>(offset) & kLeafValueOffsetMask;
        const uint64_t cnt = static_cast<uint64_t>(count) & kLeafValueCountMask;
        return (block_ptr << kLeafValueBlockPtrShift) |
               (off << kLeafValueOffsetShift) | cnt;
    }

    static LeafValueParts UnpackLeafValue(uint64_t leaf_value) {
        LeafValueParts parts;
        parts.kv_block_ptr = (leaf_value >> kLeafValueBlockPtrShift) & kLeafValueBlockPtrMask;
        parts.offset = static_cast<uint16_t>(
            (leaf_value >> kLeafValueOffsetShift) & kLeafValueOffsetMask);
        parts.count = static_cast<uint8_t>(leaf_value & kLeafValueCountMask);
        return parts;
    }

    static uint64_t EncodeKvBlockPtr(uint64_t datablock_offset_bytes) {
        return datablock_offset_bytes >> kKvBlockShift;
    }

    static uint64_t DecodeKvBlockOffset(uint64_t kv_block_ptr) {
        return kv_block_ptr << kKvBlockShift;
    }

    void SetLeafWindow(uint64_t datablock_offset_bytes, uint16_t offset, uint8_t count) {
        leaf_value = PackLeafValue(EncodeKvBlockPtr(datablock_offset_bytes), offset, count);
    }

    void SetLeafWindowByBlockPtr(uint64_t kv_block_ptr, uint16_t offset, uint8_t count) {
        leaf_value = PackLeafValue(kv_block_ptr, offset, count);
    }

    LeafValueParts LeafWindow() const {
        return UnpackLeafValue(leaf_value);
    }

    bool HasLeafWindow() const {
        return LeafWindow().count != 0;
    }

    bool Contains(const KeyType& key) const {
        return ExtractPrefix(key) == route_prefix &&
               route_min_suffix <= ExtractSuffix(key) &&
               ExtractSuffix(key) <= route_max_suffix;
    }

    bool ContainsSuffix(RouteSuffix suffix) const {
        return route_min_suffix <= suffix && suffix <= route_max_suffix;
    }

    bool Valid() const {
        return table_idx != INVALID_PTR;
    }

    bool TouchesPrefix(RoutePrefix prefix) const {
        return ExtractPrefix(min_key) <= prefix && prefix <= ExtractPrefix(max_key);
    }

    bool MatchesLocalFragment(RoutePrefix prefix) const {
        if (route_prefix != prefix) {
            return false;
        }
        const RoutePrefix min_prefix = ExtractPrefix(min_key);
        const RoutePrefix max_prefix = ExtractPrefix(max_key);
        const RouteSuffix expected_min =
            min_prefix == prefix ? ExtractSuffix(min_key) : 0;
        const RouteSuffix expected_max =
            max_prefix == prefix ? ExtractSuffix(max_key) : std::numeric_limits<RouteSuffix>::max();
        return route_min_suffix == expected_min && route_max_suffix == expected_max &&
               route_min_suffix <= route_max_suffix;
    }

    bool OverlapsLocalSuffixRange(RouteSuffix start_suffix, RouteSuffix end_suffix) const {
        return route_min_suffix <= end_suffix && start_suffix <= route_max_suffix;
    }

    KeyType RouteMinKey() const {
        return ComposeKey(route_prefix, route_min_suffix);
    }

    KeyType RouteMaxKey() const {
        return ComposeKey(route_prefix, route_max_suffix);
    }

    static SubtreeRecord FromTaggedPstMeta(const TaggedPstMeta& table, uint64_t idx) {
        SubtreeRecord record;
        record.min_key = table.meta.MinKey();
        record.max_key = table.meta.MaxKey();
        record.route_prefix = ExtractPrefix(record.max_key);
        const RoutePrefix min_prefix = ExtractPrefix(record.min_key);
        const RoutePrefix max_prefix = ExtractPrefix(record.max_key);
        record.route_min_suffix = min_prefix == record.route_prefix ? ExtractSuffix(record.min_key) : 0;
        record.route_max_suffix = max_prefix == record.route_prefix
                                      ? ExtractSuffix(record.max_key)
                                      : std::numeric_limits<RouteSuffix>::max();
        record.seq_no = table.meta.seq_no_;
        record.table_idx = idx;
        record.SetLeafWindow(table.meta.datablock_ptr_, 0,
                             static_cast<uint8_t>(std::min<uint16_t>(
                                 table.meta.entry_num_,
                                 static_cast<uint16_t>(std::numeric_limits<uint8_t>::max()))));
        return record;
    }

    static SubtreeRecord FromTaggedPstMetaForPrefix(const TaggedPstMeta& table,
                                                    uint64_t idx,
                                                    RoutePrefix prefix) {
        SubtreeRecord record;
        record.min_key = table.meta.MinKey();
        record.max_key = table.meta.MaxKey();
        record.route_prefix = prefix;
        const RoutePrefix min_prefix = ExtractPrefix(record.min_key);
        const RoutePrefix max_prefix = ExtractPrefix(record.max_key);
        record.route_min_suffix = min_prefix == prefix ? ExtractSuffix(record.min_key) : 0;
        record.route_max_suffix = max_prefix == prefix
                                      ? ExtractSuffix(record.max_key)
                                      : std::numeric_limits<RouteSuffix>::max();
        record.seq_no = table.meta.seq_no_;
        record.table_idx = idx;
        record.SetLeafWindow(table.meta.datablock_ptr_, 0,
                             static_cast<uint8_t>(std::min<uint16_t>(
                                 table.meta.entry_num_,
                                 static_cast<uint16_t>(std::numeric_limits<uint8_t>::max()))));
        return record;
    }

    static SubtreeRecord FromExistingForPrefix(const SubtreeRecord& source, RoutePrefix prefix) {
        SubtreeRecord record = source;
        record.route_prefix = prefix;
        const RoutePrefix min_prefix = ExtractPrefix(record.min_key);
        const RoutePrefix max_prefix = ExtractPrefix(record.max_key);
        record.route_min_suffix = min_prefix == prefix ? ExtractSuffix(record.min_key) : 0;
        record.route_max_suffix = max_prefix == prefix
                                      ? ExtractSuffix(record.max_key)
                                      : std::numeric_limits<RouteSuffix>::max();
        return record;
    }
};

struct RecordMaxKeyLess {
    bool operator()(const SubtreeRecord& lhs, const KeyType& rhs) const {
        return CompareKeyType(lhs.max_key, rhs) < 0;
    }

    bool operator()(const KeyType& lhs, const SubtreeRecord& rhs) const {
        return CompareKeyType(lhs, rhs.max_key) < 0;
    }

    bool operator()(const SubtreeRecord& lhs, const SubtreeRecord& rhs) const {
        const int cmp = CompareKeyType(lhs.max_key, rhs.max_key);
        if (cmp != 0) {
            return cmp < 0;
        }
        if (lhs.seq_no != rhs.seq_no) {
            return lhs.seq_no > rhs.seq_no;
        }
        return lhs.table_idx < rhs.table_idx;
    }
};

struct RecordRouteKeyLess {
    bool operator()(const SubtreeRecord& lhs, const KeyType& rhs) const {
        return CompareKeyType(lhs.RouteMaxKey(), rhs) < 0;
    }

    bool operator()(const KeyType& lhs, const SubtreeRecord& rhs) const {
        return CompareKeyType(lhs, rhs.RouteMaxKey()) < 0;
    }

    bool operator()(const SubtreeRecord& lhs, const SubtreeRecord& rhs) const {
        const int cmp = CompareKeyType(lhs.RouteMaxKey(), rhs.RouteMaxKey());
        if (cmp != 0) {
            return cmp < 0;
        }
        if (lhs.seq_no != rhs.seq_no) {
            return lhs.seq_no > rhs.seq_no;
        }
        return lhs.table_idx < rhs.table_idx;
    }
};

struct RecordRouteSuffixLess {
    bool operator()(const SubtreeRecord& lhs, RouteSuffix rhs) const {
        return lhs.route_max_suffix < rhs;
    }

    bool operator()(RouteSuffix lhs, const SubtreeRecord& rhs) const {
        return lhs < rhs.route_max_suffix;
    }

    bool operator()(const SubtreeRecord& lhs, const SubtreeRecord& rhs) const {
        if (lhs.route_max_suffix != rhs.route_max_suffix) {
            return lhs.route_max_suffix < rhs.route_max_suffix;
        }
        if (lhs.seq_no != rhs.seq_no) {
            return lhs.seq_no > rhs.seq_no;
        }
        return lhs.table_idx < rhs.table_idx;
    }
};

}  // namespace flowkv::hybrid_l1
