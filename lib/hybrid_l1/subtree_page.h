#pragma once

#include "subtree_bptree.h"

#include <cstdint>
#include <limits>
#include <vector>

namespace flowkv::hybrid_l1 {

constexpr uint32_t kInvalidSubtreePageId = 0xFFFFFFFFu;
using SubtreePagePtr = uint64_t;
constexpr SubtreePagePtr kInvalidSubtreePagePtr = std::numeric_limits<SubtreePagePtr>::max();

enum class SubtreePageType : uint16_t {
    kInternal = 1,
    kLeaf = 2,
};

struct SubtreePageManifest {
    uint32_t page_size = 0;
    uint32_t root_page_id = kInvalidSubtreePageId;
    uint32_t page_count = 0;
    uint32_t leaf_page_count = 0;
    uint32_t internal_page_count = 0;
    uint64_t record_count = 0;
    L1SubtreeBPTree::BuildOptions build_options{};
};

struct SubtreePage {
    uint32_t page_id = kInvalidSubtreePageId;
    std::vector<uint8_t> bytes;
};

struct SubtreeDecodedLeafPage {
    RouteSuffix high_key = 0;
    SubtreePagePtr prev_page_ptr = kInvalidSubtreePagePtr;
    SubtreePagePtr next_page_ptr = kInvalidSubtreePagePtr;
    std::vector<SubtreeRecord> records;
};

struct SubtreeDecodedInternalPage {
    RouteSuffix high_key = 0;
    std::vector<SubtreePagePtr> child_page_ptrs;
    std::vector<RouteSuffix> child_high_keys;
};

struct SubtreePageSet {
    std::vector<uint8_t> manifest;
    std::vector<SubtreePage> pages;

    bool Empty() const { return pages.empty(); }
};

class SubtreePageCodec {
public:
    static constexpr uint32_t kManifestSize = 128;
    static constexpr uint32_t kDefaultPageSize = 16 * 1024;

    static SubtreePageSet Export(const L1SubtreeBPTree& tree, uint32_t page_size = kDefaultPageSize);
    static void Import(const SubtreePageSet& page_set, L1SubtreeBPTree& tree);
    static bool Validate(const SubtreePageSet& page_set);
    static SubtreePageManifest DecodeManifest(const std::vector<uint8_t>& manifest_bytes);
    static bool TryDecodeLeafPage(const std::vector<uint8_t>& bytes, SubtreeDecodedLeafPage& out);
    static bool TryDecodeInternalPage(const std::vector<uint8_t>& bytes, SubtreeDecodedInternalPage& out);
};

}  // namespace flowkv::hybrid_l1
