#include "lib/hybrid_l1/subtree_page.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace flowkv::hybrid_l1 {
namespace {

constexpr uint32_t kManifestMagic = 0x48314D4Cu;  // "LM1H"
constexpr uint32_t kPageMagic = 0x5042544Cu;      // "LTBP"
constexpr uint16_t kFormatVersion = 3;
constexpr size_t kPageHeaderSize = 20;

using Node = L1SubtreeBPTree::Node;
using LeafNode = L1SubtreeBPTree::LeafNode;
using InternalNode = L1SubtreeBPTree::InternalNode;

size_t EncodedKeySize() {
#if defined(FLOWKV_KEY16)
    return sizeof(uint64_t) * 2;
#else
    return sizeof(uint64_t);
#endif
}

size_t EncodedSuffixSize() {
    return sizeof(RouteSuffix);
}

size_t EncodedRecordSize() {
    return EncodedKeySize() * 2 + sizeof(uint64_t) * 3 + sizeof(uint32_t) + sizeof(uint64_t) +
           sizeof(uint64_t);
}

size_t EncodedLeafPayloadSize(size_t record_count) {
    return EncodedSuffixSize() + sizeof(uint64_t) * 2 + record_count * EncodedRecordSize();
}

size_t EncodedInternalPayloadSize(size_t child_count) {
    return EncodedSuffixSize() + child_count * (sizeof(uint64_t) + EncodedSuffixSize());
}

void EnsureSize(const std::vector<uint8_t>& bytes, size_t offset, size_t need, const char* what) {
    if (offset + need > bytes.size()) {
        throw std::runtime_error(what);
    }
}

void WriteU16(std::vector<uint8_t>& out, size_t offset, uint16_t value) {
    EnsureSize(out, offset, sizeof(value), "buffer too small for u16 write");
    out[offset + 0] = static_cast<uint8_t>(value & 0xFFu);
    out[offset + 1] = static_cast<uint8_t>((value >> 8) & 0xFFu);
}

void WriteU32(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
    EnsureSize(out, offset, sizeof(value), "buffer too small for u32 write");
    out[offset + 0] = static_cast<uint8_t>(value & 0xFFu);
    out[offset + 1] = static_cast<uint8_t>((value >> 8) & 0xFFu);
    out[offset + 2] = static_cast<uint8_t>((value >> 16) & 0xFFu);
    out[offset + 3] = static_cast<uint8_t>((value >> 24) & 0xFFu);
}

void WriteU64(std::vector<uint8_t>& out, size_t offset, uint64_t value) {
    EnsureSize(out, offset, sizeof(value), "buffer too small for u64 write");
    for (size_t i = 0; i < sizeof(value); ++i) {
        out[offset + i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFFu);
    }
}

uint16_t ReadU16(const std::vector<uint8_t>& in, size_t offset) {
    EnsureSize(in, offset, sizeof(uint16_t), "buffer too small for u16 read");
    return static_cast<uint16_t>(in[offset + 0]) |
           (static_cast<uint16_t>(in[offset + 1]) << 8);
}

uint32_t ReadU32(const std::vector<uint8_t>& in, size_t offset) {
    EnsureSize(in, offset, sizeof(uint32_t), "buffer too small for u32 read");
    return static_cast<uint32_t>(in[offset + 0]) |
           (static_cast<uint32_t>(in[offset + 1]) << 8) |
           (static_cast<uint32_t>(in[offset + 2]) << 16) |
           (static_cast<uint32_t>(in[offset + 3]) << 24);
}

uint64_t ReadU64(const std::vector<uint8_t>& in, size_t offset) {
    EnsureSize(in, offset, sizeof(uint64_t), "buffer too small for u64 read");
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint64_t>(in[offset + i]) << (i * 8);
    }
    return value;
}

void WriteKey(std::vector<uint8_t>& out, size_t offset, const KeyType& key) {
#if defined(FLOWKV_KEY16)
    WriteU64(out, offset, key.hi);
    WriteU64(out, offset + sizeof(uint64_t), key.lo);
#else
    WriteU64(out, offset, key);
#endif
}

KeyType ReadKey(const std::vector<uint8_t>& in, size_t offset) {
#if defined(FLOWKV_KEY16)
    return KeyType{ReadU64(in, offset), ReadU64(in, offset + sizeof(uint64_t))};
#else
    return ReadU64(in, offset);
#endif
}

void WriteSuffix(std::vector<uint8_t>& out, size_t offset, RouteSuffix suffix) {
    WriteU64(out, offset, suffix);
}

RouteSuffix ReadSuffix(const std::vector<uint8_t>& in, size_t offset) {
    return ReadU64(in, offset);
}

void EncodeRecord(std::vector<uint8_t>& out, size_t offset, const SubtreeRecord& record) {
    WriteKey(out, offset, record.min_key);
    offset += EncodedKeySize();
    WriteKey(out, offset, record.max_key);
    offset += EncodedKeySize();
    WriteU64(out, offset, record.route_prefix);
    offset += sizeof(uint64_t);
    WriteU64(out, offset, record.route_min_suffix);
    offset += sizeof(uint64_t);
    WriteU64(out, offset, record.route_max_suffix);
    offset += sizeof(uint64_t);
    WriteU32(out, offset, record.seq_no);
    offset += sizeof(uint32_t);
    WriteU64(out, offset, record.table_idx);
    offset += sizeof(uint64_t);
    WriteU64(out, offset, record.leaf_value);
}

SubtreeRecord DecodeRecord(const std::vector<uint8_t>& in, size_t offset) {
    SubtreeRecord record;
    record.min_key = ReadKey(in, offset);
    offset += EncodedKeySize();
    record.max_key = ReadKey(in, offset);
    offset += EncodedKeySize();
    record.route_prefix = ReadU64(in, offset);
    offset += sizeof(uint64_t);
    record.route_min_suffix = ReadU64(in, offset);
    offset += sizeof(uint64_t);
    record.route_max_suffix = ReadU64(in, offset);
    offset += sizeof(uint64_t);
    record.seq_no = ReadU32(in, offset);
    offset += sizeof(uint32_t);
    record.table_idx = ReadU64(in, offset);
    offset += sizeof(uint64_t);
    record.leaf_value = ReadU64(in, offset);
    return record;
}

struct DecodedPageHeader {
    SubtreePageType type = SubtreePageType::kLeaf;
    uint32_t page_id = kInvalidSubtreePageId;
    uint32_t item_count = 0;
    uint32_t payload_bytes = 0;
};

DecodedPageHeader DecodePageHeader(const std::vector<uint8_t>& bytes) {
    if (bytes.size() < kPageHeaderSize) {
        throw std::runtime_error("subtree page too small");
    }
    if (ReadU32(bytes, 0) != kPageMagic) {
        throw std::runtime_error("invalid subtree page magic");
    }
    if (ReadU16(bytes, 4) != kFormatVersion) {
        throw std::runtime_error("unsupported subtree page version");
    }
    DecodedPageHeader header;
    header.type = static_cast<SubtreePageType>(ReadU16(bytes, 6));
    header.page_id = ReadU32(bytes, 8);
    header.item_count = ReadU32(bytes, 12);
    header.payload_bytes = ReadU32(bytes, 16);
    if (header.payload_bytes > bytes.size() - kPageHeaderSize) {
        throw std::runtime_error("subtree page payload exceeds page size");
    }
    if (header.type != SubtreePageType::kLeaf && header.type != SubtreePageType::kInternal) {
        throw std::runtime_error("unknown subtree page type");
    }
    return header;
}

void EncodePageHeader(std::vector<uint8_t>& bytes,
                      SubtreePageType type,
                      uint32_t page_id,
                      uint32_t item_count,
                      uint32_t payload_bytes) {
    WriteU32(bytes, 0, kPageMagic);
    WriteU16(bytes, 4, kFormatVersion);
    WriteU16(bytes, 6, static_cast<uint16_t>(type));
    WriteU32(bytes, 8, page_id);
    WriteU32(bytes, 12, item_count);
    WriteU32(bytes, 16, payload_bytes);
}

void EncodeManifestBytes(const SubtreePageManifest& manifest, std::vector<uint8_t>& out) {
    out.assign(SubtreePageCodec::kManifestSize, 0);
    WriteU32(out, 0, kManifestMagic);
    WriteU16(out, 4, kFormatVersion);
    WriteU16(out, 6, 0);
    WriteU32(out, 8, manifest.page_size);
    WriteU32(out, 12, manifest.root_page_id);
    WriteU32(out, 16, manifest.page_count);
    WriteU32(out, 20, manifest.leaf_page_count);
    WriteU32(out, 24, manifest.internal_page_count);
    WriteU64(out, 28, manifest.record_count);
    WriteU64(out, 36, manifest.build_options.leaf_capacity);
    WriteU64(out, 44, manifest.build_options.internal_fanout);
}

SubtreePageManifest ParseManifestBytes(const std::vector<uint8_t>& manifest_bytes) {
    if (manifest_bytes.size() != SubtreePageCodec::kManifestSize) {
        throw std::runtime_error("invalid subtree manifest size");
    }
    if (ReadU32(manifest_bytes, 0) != kManifestMagic) {
        throw std::runtime_error("invalid subtree manifest magic");
    }
    if (ReadU16(manifest_bytes, 4) != kFormatVersion) {
        throw std::runtime_error("unsupported subtree manifest version");
    }

    SubtreePageManifest manifest;
    manifest.page_size = ReadU32(manifest_bytes, 8);
    manifest.root_page_id = ReadU32(manifest_bytes, 12);
    manifest.page_count = ReadU32(manifest_bytes, 16);
    manifest.leaf_page_count = ReadU32(manifest_bytes, 20);
    manifest.internal_page_count = ReadU32(manifest_bytes, 24);
    manifest.record_count = ReadU64(manifest_bytes, 28);
    manifest.build_options.leaf_capacity = static_cast<size_t>(ReadU64(manifest_bytes, 36));
    manifest.build_options.internal_fanout = static_cast<size_t>(ReadU64(manifest_bytes, 44));
    if (manifest.build_options.leaf_capacity == 0 || manifest.build_options.internal_fanout < 2) {
        throw std::runtime_error("invalid subtree manifest build options");
    }
    return manifest;
}

struct DecodedLeafPage {
    RouteSuffix high_key = 0;
    SubtreePagePtr prev_page_ptr = kInvalidSubtreePagePtr;
    SubtreePagePtr next_page_ptr = kInvalidSubtreePagePtr;
    std::vector<SubtreeRecord> records;
};

DecodedLeafPage DecodeLeafPage(const std::vector<uint8_t>& bytes, const DecodedPageHeader& header) {
    DecodedLeafPage page;
    size_t offset = kPageHeaderSize;
    page.high_key = ReadSuffix(bytes, offset);
    offset += EncodedSuffixSize();
    page.prev_page_ptr = ReadU64(bytes, offset);
    offset += sizeof(uint64_t);
    page.next_page_ptr = ReadU64(bytes, offset);
    offset += sizeof(uint64_t);
    page.records.reserve(header.item_count);
    for (uint32_t i = 0; i < header.item_count; ++i) {
        page.records.push_back(DecodeRecord(bytes, offset));
        offset += EncodedRecordSize();
    }
    if (offset != kPageHeaderSize + header.payload_bytes) {
        throw std::runtime_error("leaf page payload size mismatch");
    }
    return page;
}

struct DecodedInternalPage {
    RouteSuffix high_key = 0;
    std::vector<SubtreePagePtr> child_page_ptrs;
    std::vector<RouteSuffix> child_high_keys;
};

DecodedInternalPage DecodeInternalPage(const std::vector<uint8_t>& bytes,
                                       const DecodedPageHeader& header) {
    DecodedInternalPage page;
    size_t offset = kPageHeaderSize;
    page.high_key = ReadSuffix(bytes, offset);
    offset += EncodedSuffixSize();
    page.child_page_ptrs.reserve(header.item_count);
    page.child_high_keys.reserve(header.item_count);
    for (uint32_t i = 0; i < header.item_count; ++i) {
        page.child_page_ptrs.push_back(ReadU64(bytes, offset));
        offset += sizeof(uint64_t);
        page.child_high_keys.push_back(ReadSuffix(bytes, offset));
        offset += EncodedSuffixSize();
    }
    if (offset != kPageHeaderSize + header.payload_bytes) {
        throw std::runtime_error("internal page payload size mismatch");
    }
    return page;
}

void CollectNodesPreOrder(const Node* node,
                          std::vector<const Node*>& nodes,
                          std::unordered_map<const Node*, uint32_t>& page_ids) {
    page_ids.emplace(node, static_cast<uint32_t>(nodes.size()));
    nodes.push_back(node);
    if (!node->is_leaf) {
        const auto* internal = static_cast<const InternalNode*>(node);
        for (const auto& child : internal->children) {
            CollectNodesPreOrder(child.get(), nodes, page_ids);
        }
    }
}

}  // namespace

SubtreePageSet SubtreePageCodec::Export(const L1SubtreeBPTree& tree, uint32_t page_size) {
    if (page_size < kPageHeaderSize + EncodedSuffixSize() + sizeof(uint32_t) * 2) {
        throw std::invalid_argument("subtree page size too small");
    }

    SubtreePageSet page_set;
    if (tree.Empty()) {
        SubtreePageManifest manifest;
        manifest.page_size = page_size;
        manifest.build_options = tree.options_;
        EncodeManifestBytes(manifest, page_set.manifest);
        return page_set;
    }

    std::vector<const Node*> nodes;
    std::unordered_map<const Node*, uint32_t> page_ids;
    CollectNodesPreOrder(tree.root_.get(), nodes, page_ids);

    uint32_t leaf_page_count = 0;
    uint32_t internal_page_count = 0;
    page_set.pages.reserve(nodes.size());

    for (const Node* node : nodes) {
        const uint32_t page_id = page_ids.at(node);
        SubtreePage page;
        page.page_id = page_id;
        page.bytes.assign(page_size, 0);

        if (node->is_leaf) {
            ++leaf_page_count;
            const auto* leaf = static_cast<const LeafNode*>(node);
            const size_t payload_bytes = EncodedLeafPayloadSize(leaf->records.size());
            if (kPageHeaderSize + payload_bytes > page_size) {
                throw std::runtime_error("leaf page exceeds configured page size");
            }
            EncodePageHeader(page.bytes, SubtreePageType::kLeaf, page_id,
                             static_cast<uint32_t>(leaf->records.size()),
                             static_cast<uint32_t>(payload_bytes));

            size_t offset = kPageHeaderSize;
            WriteSuffix(page.bytes, offset, leaf->high_key);
            offset += EncodedSuffixSize();
            const auto leaf_pos_it = tree.leaf_positions_.find(leaf);
            if (leaf_pos_it == tree.leaf_positions_.end()) {
                throw std::runtime_error("leaf missing position during page export");
            }
            const size_t leaf_index = leaf_pos_it->second;
            WriteU64(page.bytes, offset,
                     leaf_index > 0
                         ? static_cast<uint64_t>(page_ids.at(tree.leaves_[leaf_index - 1].get()))
                         : kInvalidSubtreePagePtr);
            offset += sizeof(uint64_t);
            WriteU64(page.bytes, offset,
                     leaf_index + 1 < tree.leaves_.size()
                         ? static_cast<uint64_t>(page_ids.at(tree.leaves_[leaf_index + 1].get()))
                         : kInvalidSubtreePagePtr);
            offset += sizeof(uint64_t);
            for (const auto& record : leaf->records) {
                EncodeRecord(page.bytes, offset, record);
                offset += EncodedRecordSize();
            }
        } else {
            ++internal_page_count;
            const auto* internal = static_cast<const InternalNode*>(node);
            const size_t payload_bytes = EncodedInternalPayloadSize(internal->children.size());
            if (kPageHeaderSize + payload_bytes > page_size) {
                throw std::runtime_error("internal page exceeds configured page size");
            }
            EncodePageHeader(page.bytes, SubtreePageType::kInternal, page_id,
                             static_cast<uint32_t>(internal->children.size()),
                             static_cast<uint32_t>(payload_bytes));

            size_t offset = kPageHeaderSize;
            WriteSuffix(page.bytes, offset, internal->high_key);
            offset += EncodedSuffixSize();
            for (size_t i = 0; i < internal->children.size(); ++i) {
                WriteU64(page.bytes, offset, page_ids.at(internal->children[i].get()));
                offset += sizeof(uint64_t);
                WriteSuffix(page.bytes, offset, internal->high_keys[i]);
                offset += EncodedSuffixSize();
            }
        }

        page_set.pages.push_back(std::move(page));
    }

    SubtreePageManifest manifest;
    manifest.page_size = page_size;
    manifest.root_page_id = 0;
    manifest.page_count = static_cast<uint32_t>(page_set.pages.size());
    manifest.leaf_page_count = leaf_page_count;
    manifest.internal_page_count = internal_page_count;
    manifest.record_count = tree.size_;
    manifest.build_options = tree.options_;
    EncodeManifestBytes(manifest, page_set.manifest);
    return page_set;
}

void SubtreePageCodec::Import(const SubtreePageSet& page_set, L1SubtreeBPTree& tree) {
    const SubtreePageManifest manifest = DecodeManifest(page_set.manifest);
    if (manifest.record_count == 0) {
        tree.Clear();
        tree.options_ = manifest.build_options;
        return;
    }

    if (!Validate(page_set)) {
        throw std::runtime_error("invalid subtree page set");
    }

    std::unordered_map<uint32_t, const SubtreePage*> page_map;
    page_map.reserve(page_set.pages.size());
    for (const auto& page : page_set.pages) {
        page_map.emplace(page.page_id, &page);
    }

    std::vector<SubtreeRecord> records;
    records.reserve(manifest.record_count);

    uint32_t leftmost_leaf = kInvalidSubtreePageId;
    for (const auto& page : page_set.pages) {
        const DecodedPageHeader header = DecodePageHeader(page.bytes);
        if (header.type != SubtreePageType::kLeaf) {
            continue;
        }
        const DecodedLeafPage leaf = DecodeLeafPage(page.bytes, header);
        if (leaf.prev_page_ptr == kInvalidSubtreePagePtr) {
            leftmost_leaf = page.page_id;
            break;
        }
    }

    if (leftmost_leaf == kInvalidSubtreePageId) {
        throw std::runtime_error("subtree page set missing leftmost leaf");
    }

    std::unordered_set<uint32_t> visited;
    uint32_t current = leftmost_leaf;
    while (current != kInvalidSubtreePageId) {
        if (!visited.insert(current).second) {
            throw std::runtime_error("loop detected in subtree leaf chain");
        }
        const auto it = page_map.find(current);
        if (it == page_map.end()) {
            throw std::runtime_error("leaf chain references missing page");
        }
        const DecodedPageHeader header = DecodePageHeader(it->second->bytes);
        if (header.type != SubtreePageType::kLeaf) {
            throw std::runtime_error("leaf chain references non-leaf page");
        }
        const DecodedLeafPage leaf = DecodeLeafPage(it->second->bytes, header);
        records.insert(records.end(), leaf.records.begin(), leaf.records.end());
        if (leaf.next_page_ptr == kInvalidSubtreePagePtr) {
            current = kInvalidSubtreePageId;
        } else if (leaf.next_page_ptr <= std::numeric_limits<uint32_t>::max()) {
            current = static_cast<uint32_t>(leaf.next_page_ptr);
        } else {
            throw std::runtime_error("leaf chain points to invalid logical page id");
        }
    }

    if (records.size() != manifest.record_count) {
        throw std::runtime_error("subtree record count mismatch during import");
    }

    tree.Clear();
    tree.options_ = manifest.build_options;
    tree.BulkLoad(records);
}

bool SubtreePageCodec::Validate(const SubtreePageSet& page_set) {
    try {
        const SubtreePageManifest manifest = DecodeManifest(page_set.manifest);
        if (manifest.record_count == 0) {
            return page_set.pages.empty();
        }
        if (manifest.page_count != page_set.pages.size()) {
            return false;
        }
        if (manifest.page_size < kPageHeaderSize) {
            return false;
        }

        std::unordered_map<uint32_t, const SubtreePage*> page_map;
        page_map.reserve(page_set.pages.size());
        for (const auto& page : page_set.pages) {
            if (page.bytes.size() != manifest.page_size) {
                return false;
            }
            const DecodedPageHeader header = DecodePageHeader(page.bytes);
            if (header.page_id != page.page_id) {
                return false;
            }
            if (!page_map.emplace(page.page_id, &page).second) {
                return false;
            }
        }

        const auto root_it = page_map.find(manifest.root_page_id);
        if (root_it == page_map.end()) {
            return false;
        }

        uint32_t seen_leaf_pages = 0;
        uint32_t seen_internal_pages = 0;
        uint64_t seen_records = 0;
        std::unordered_set<uint32_t> reachable_pages;
        std::vector<uint32_t> stack{manifest.root_page_id};

        while (!stack.empty()) {
            const uint32_t page_id = stack.back();
            stack.pop_back();
            if (!reachable_pages.insert(page_id).second) {
                continue;
            }
            const auto it = page_map.find(page_id);
            if (it == page_map.end()) {
                return false;
            }
            const DecodedPageHeader header = DecodePageHeader(it->second->bytes);
            if (header.type == SubtreePageType::kLeaf) {
                ++seen_leaf_pages;
                seen_records += header.item_count;
            } else {
                ++seen_internal_pages;
                const DecodedInternalPage internal = DecodeInternalPage(it->second->bytes, header);
                if (internal.child_page_ptrs.size() != header.item_count ||
                    internal.child_high_keys.size() != header.item_count) {
                    return false;
                }
                for (size_t i = 0; i < internal.child_page_ptrs.size(); ++i) {
                    if (internal.child_page_ptrs[i] == kInvalidSubtreePagePtr ||
                        internal.child_page_ptrs[i] > std::numeric_limits<uint32_t>::max()) {
                        return false;
                    }
                    const uint32_t child_page_id = static_cast<uint32_t>(internal.child_page_ptrs[i]);
                    if (page_map.find(child_page_id) == page_map.end()) {
                        return false;
                    }
                    if (internal.child_high_keys[i] > internal.high_key) {
                        return false;
                    }
                    stack.push_back(child_page_id);
                }
            }
        }

        if (reachable_pages.size() != page_set.pages.size()) {
            return false;
        }
        if (seen_leaf_pages != manifest.leaf_page_count ||
            seen_internal_pages != manifest.internal_page_count ||
            seen_records != manifest.record_count) {
            return false;
        }

        std::vector<uint32_t> leftmost_candidates;
        for (const auto& page : page_set.pages) {
            const DecodedPageHeader header = DecodePageHeader(page.bytes);
            if (header.type != SubtreePageType::kLeaf) {
                continue;
            }
            const DecodedLeafPage leaf = DecodeLeafPage(page.bytes, header);
            if (leaf.records.empty()) {
                return false;
            }
            if (leaf.records.back().route_max_suffix != leaf.high_key) {
                return false;
            }
            if (leaf.prev_page_ptr == kInvalidSubtreePagePtr) {
                leftmost_candidates.push_back(page.page_id);
            }
        }
        if (leftmost_candidates.size() != 1) {
            return false;
        }

        std::unordered_set<uint32_t> leaf_chain_visited;
        bool first_record = true;
        SubtreeRecord last_record;
        uint64_t chain_records = 0;
        uint32_t current = leftmost_candidates.front();
        while (current != kInvalidSubtreePageId) {
            if (!leaf_chain_visited.insert(current).second) {
                return false;
            }
            const auto it = page_map.find(current);
            if (it == page_map.end()) {
                return false;
            }
            const DecodedPageHeader header = DecodePageHeader(it->second->bytes);
            if (header.type != SubtreePageType::kLeaf) {
                return false;
            }
            const DecodedLeafPage leaf = DecodeLeafPage(it->second->bytes, header);
            if (leaf.next_page_ptr != kInvalidSubtreePagePtr) {
                if (leaf.next_page_ptr > std::numeric_limits<uint32_t>::max()) {
                    return false;
                }
                const uint32_t next_page_id = static_cast<uint32_t>(leaf.next_page_ptr);
                const auto next_it = page_map.find(next_page_id);
                if (next_it == page_map.end()) {
                    return false;
                }
                const DecodedLeafPage next_leaf = DecodeLeafPage(
                    next_it->second->bytes, DecodePageHeader(next_it->second->bytes));
                if (next_leaf.prev_page_ptr != current) {
                    return false;
                }
            }
            for (const auto& record : leaf.records) {
                if (CompareKeyType(record.min_key, record.max_key) > 0) {
                    return false;
                }
                if (!record.MatchesLocalFragment(record.route_prefix)) {
                    return false;
                }
                if (!first_record && RecordRouteKeyLess{}(record, last_record)) {
                    return false;
                }
                last_record = record;
                first_record = false;
                ++chain_records;
            }
            if (leaf.next_page_ptr == kInvalidSubtreePagePtr) {
                current = kInvalidSubtreePageId;
            } else if (leaf.next_page_ptr <= std::numeric_limits<uint32_t>::max()) {
                current = static_cast<uint32_t>(leaf.next_page_ptr);
            } else {
                return false;
            }
        }

        return leaf_chain_visited.size() == manifest.leaf_page_count &&
               chain_records == manifest.record_count;
    } catch (const std::exception&) {
        return false;
    }
}

SubtreePageManifest SubtreePageCodec::DecodeManifest(const std::vector<uint8_t>& manifest_bytes) {
    return ParseManifestBytes(manifest_bytes);
}

bool SubtreePageCodec::TryDecodeLeafPage(const std::vector<uint8_t>& bytes,
                                         SubtreeDecodedLeafPage& out) {
    try {
        const DecodedPageHeader header = DecodePageHeader(bytes);
        if (header.type != SubtreePageType::kLeaf) {
            return false;
        }
        const DecodedLeafPage decoded = DecodeLeafPage(bytes, header);
        out.high_key = decoded.high_key;
        out.prev_page_ptr = decoded.prev_page_ptr;
        out.next_page_ptr = decoded.next_page_ptr;
        out.records = decoded.records;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool SubtreePageCodec::TryDecodeInternalPage(const std::vector<uint8_t>& bytes,
                                             SubtreeDecodedInternalPage& out) {
    try {
        const DecodedPageHeader header = DecodePageHeader(bytes);
        if (header.type != SubtreePageType::kInternal) {
            return false;
        }
        const DecodedInternalPage decoded = DecodeInternalPage(bytes, header);
        out.high_key = decoded.high_key;
        out.child_page_ptrs = decoded.child_page_ptrs;
        out.child_high_keys = decoded.child_high_keys;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

}  // namespace flowkv::hybrid_l1
