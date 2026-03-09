#include "lib/hybrid_l1/subtree_bptree.h"

#include "lib/hybrid_l1/subtree_page.h"

#include <algorithm>
#include <functional>
#include <unordered_set>
#include <stdexcept>

namespace flowkv::hybrid_l1 {

namespace {

bool SameFragment(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    return CompareKeyType(lhs.min_key, rhs.min_key) == 0 &&
           CompareKeyType(lhs.max_key, rhs.max_key) == 0 &&
           lhs.route_prefix == rhs.route_prefix &&
           lhs.route_min_suffix == rhs.route_min_suffix &&
           lhs.route_max_suffix == rhs.route_max_suffix &&
           lhs.seq_no == rhs.seq_no &&
           lhs.table_idx == rhs.table_idx &&
           lhs.leaf_value == rhs.leaf_value;
}

uint64_t HashKeyTypeValue(const KeyType& key) {
#if defined(FLOWKV_KEY16)
    return std::hash<uint64_t>{}(key.hi) ^ (std::hash<uint64_t>{}(key.lo) << 1);
#else
    return std::hash<uint64_t>{}(key);
#endif
}

uint64_t HashRecord(const SubtreeRecord& record) {
    uint64_t hash = HashKeyTypeValue(record.min_key);
    hash ^= HashKeyTypeValue(record.max_key) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(record.route_prefix) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(record.route_min_suffix) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(record.route_max_suffix) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint32_t>{}(record.seq_no) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(record.table_idx) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(record.leaf_value) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    return hash;
}

struct LeafMatchKey {
    size_t size = 0;
    uint64_t first_hash = 0;
    uint64_t last_hash = 0;

    bool operator==(const LeafMatchKey& other) const {
        return size == other.size && first_hash == other.first_hash && last_hash == other.last_hash;
    }
};

struct LeafMatchKeyHash {
    size_t operator()(const LeafMatchKey& key) const {
        size_t hash = std::hash<size_t>{}(key.size);
        hash ^= std::hash<uint64_t>{}(key.first_hash) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        hash ^= std::hash<uint64_t>{}(key.last_hash) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        return hash;
    }
};

LeafMatchKey MakeLeafMatchKey(const std::vector<SubtreeRecord>& records, size_t offset, size_t size) {
    return LeafMatchKey{size, HashRecord(records[offset]), HashRecord(records[offset + size - 1])};
}

bool LeafMatchesRecords(const L1SubtreeBPTree::LeafNode& leaf,
                        const std::vector<SubtreeRecord>& sorted_records,
                        size_t offset) {
    if (offset + leaf.records.size() > sorted_records.size()) {
        return false;
    }
    for (size_t i = 0; i < leaf.records.size(); ++i) {
        if (!SameFragment(leaf.records[i], sorted_records[offset + i])) {
            return false;
        }
    }
    return true;
}

using InternalSequenceKey = std::vector<const L1SubtreeBPTree::Node*>;

struct InternalSequenceKeyHash {
    size_t operator()(const InternalSequenceKey& key) const {
        size_t hash = std::hash<size_t>{}(key.size());
        for (const auto* node : key) {
            hash ^= std::hash<const void*>{}(node) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        }
        return hash;
    }
};

using InternalReuseIndex =
    std::unordered_map<InternalSequenceKey,
                       std::shared_ptr<const L1SubtreeBPTree::InternalNode>,
                       InternalSequenceKeyHash>;

void CollectReusableInternalNodes(const std::shared_ptr<const L1SubtreeBPTree::Node>& node,
                                  InternalReuseIndex& reuse_index) {
    if (!node || node->is_leaf) {
        return;
    }
    const auto internal = std::static_pointer_cast<const L1SubtreeBPTree::InternalNode>(node);
    InternalSequenceKey key;
    key.reserve(internal->children.size());
    for (const auto& child : internal->children) {
        key.push_back(child.get());
    }
    reuse_index.emplace(std::move(key), internal);
    for (const auto& child : internal->children) {
        CollectReusableInternalNodes(child, reuse_index);
    }
}

void CollectInternalNodePointers(const std::shared_ptr<const L1SubtreeBPTree::Node>& node,
                                 std::unordered_set<const L1SubtreeBPTree::InternalNode*>& out) {
    if (!node || node->is_leaf) {
        return;
    }
    const auto internal = std::static_pointer_cast<const L1SubtreeBPTree::InternalNode>(node);
    out.insert(internal.get());
    for (const auto& child : internal->children) {
        CollectInternalNodePointers(child, out);
    }
}

}  // namespace

bool L1SubtreeBPTree::Cursor::Valid() const {
    return tree_ != nullptr && leaf_idx_ < tree_->leaves_.size() &&
           slot_ < tree_->leaves_[leaf_idx_]->records.size();
}

const SubtreeRecord& L1SubtreeBPTree::Cursor::record() const {
    return tree_->leaves_[leaf_idx_]->records[slot_];
}

bool L1SubtreeBPTree::Cursor::Next() {
    if (!Valid()) {
        return false;
    }
    ++slot_;
    while (tree_ != nullptr && leaf_idx_ < tree_->leaves_.size() &&
           slot_ >= tree_->leaves_[leaf_idx_]->records.size()) {
        ++leaf_idx_;
        slot_ = 0;
    }
    return Valid();
}

L1SubtreeBPTree::L1SubtreeBPTree() : L1SubtreeBPTree(BuildOptions{}) {}

L1SubtreeBPTree::L1SubtreeBPTree(BuildOptions options) : options_(options) {
    if (options_.leaf_capacity == 0 || options_.internal_fanout < 2) {
        throw std::invalid_argument("invalid B+Tree build options");
    }
}

L1SubtreeBPTree::~L1SubtreeBPTree() = default;

void L1SubtreeBPTree::Clear() {
    root_.reset();
    leaves_.clear();
    leaf_positions_.clear();
    size_ = 0;
}

void L1SubtreeBPTree::BulkLoad(const std::vector<SubtreeRecord>& sorted_records) {
    Clear();
    if (sorted_records.empty()) {
        return;
    }

    RecordRouteKeyLess less;
    for (size_t i = 1; i < sorted_records.size(); ++i) {
        if (less(sorted_records[i], sorted_records[i - 1])) {
            throw std::invalid_argument("BulkLoad expects records sorted by RecordRouteKeyLess");
        }
    }

    std::vector<std::shared_ptr<const LeafNode>> built_leaves;
    std::vector<std::shared_ptr<const Node>> leaves;
    BuildLeafLevel(sorted_records, built_leaves, leaves);
    size_ = sorted_records.size();
    leaves_ = std::move(built_leaves);
    for (size_t i = 0; i < leaves_.size(); ++i) {
        leaf_positions_[leaves_[i].get()] = i;
    }

    if (leaves.size() == 1) {
        root_ = leaves.front();
        return;
    }

    root_ = BuildInternalLevels(std::move(leaves));
}

void L1SubtreeBPTree::BulkLoadCow(const L1SubtreeBPTree& base,
                                  const std::vector<SubtreeRecord>& sorted_records) {
    Clear();
    if (sorted_records.empty()) {
        return;
    }

    RecordRouteKeyLess less;
    for (size_t i = 1; i < sorted_records.size(); ++i) {
        if (less(sorted_records[i], sorted_records[i - 1])) {
            throw std::invalid_argument("BulkLoadCow expects records sorted by RecordRouteKeyLess");
        }
    }

    std::unordered_map<LeafMatchKey,
                       std::vector<std::shared_ptr<const LeafNode>>,
                       LeafMatchKeyHash>
        leaf_index;
    leaf_index.reserve(base.leaves_.size());
    for (const auto& leaf : base.leaves_) {
        if (leaf->records.empty()) {
            continue;
        }
        leaf_index[MakeLeafMatchKey(leaf->records, 0, leaf->records.size())].push_back(leaf);
    }

    std::vector<std::shared_ptr<const LeafNode>> all_leaves;
    std::vector<SubtreeRecord> pending_new_records;
    all_leaves.reserve((sorted_records.size() + options_.leaf_capacity - 1) / options_.leaf_capacity);
    pending_new_records.reserve(options_.leaf_capacity);

    auto flush_pending = [&]() {
        if (pending_new_records.empty()) {
            return;
        }
        std::vector<std::shared_ptr<const LeafNode>> built_leaves;
        std::vector<std::shared_ptr<const Node>> level_nodes;
        BuildLeafLevel(pending_new_records, built_leaves, level_nodes);
        all_leaves.insert(all_leaves.end(), built_leaves.begin(), built_leaves.end());
        pending_new_records.clear();
    };

    for (size_t offset = 0; offset < sorted_records.size();) {
        std::shared_ptr<const LeafNode> matched_leaf;
        const size_t max_candidate = std::min(options_.leaf_capacity, sorted_records.size() - offset);
        for (size_t candidate_size = max_candidate; candidate_size >= 1; --candidate_size) {
            const auto key = MakeLeafMatchKey(sorted_records, offset, candidate_size);
            const auto it = leaf_index.find(key);
            if (it == leaf_index.end()) {
                if (candidate_size == 1) {
                    break;
                }
                continue;
            }
            for (const auto& candidate_leaf : it->second) {
                if (candidate_leaf->records.size() == candidate_size &&
                    LeafMatchesRecords(*candidate_leaf, sorted_records, offset)) {
                    matched_leaf = candidate_leaf;
                    break;
                }
            }
            if (matched_leaf) {
                flush_pending();
                all_leaves.push_back(matched_leaf);
                offset += candidate_size;
                break;
            }
            if (candidate_size == 1) {
                break;
            }
        }
        if (!matched_leaf) {
            pending_new_records.push_back(sorted_records[offset]);
            ++offset;
            if (pending_new_records.size() == options_.leaf_capacity) {
                flush_pending();
            }
        }
    }
    flush_pending();

    BuildFromLeafSequenceCow(std::move(all_leaves), base);
    size_ = sorted_records.size();
}

void L1SubtreeBPTree::BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables) {
    std::vector<SubtreeRecord> records;
    records.reserve(tables.size());
    for (size_t i = 0; i < tables.size(); ++i) {
        if (!tables[i].Valid()) {
            continue;
        }
        records.push_back(SubtreeRecord::FromTaggedPstMeta(tables[i], i));
    }
    std::sort(records.begin(), records.end(), RecordRouteKeyLess{});
    BulkLoad(records);
}

L1SubtreeBPTree::Cursor L1SubtreeBPTree::Begin() const {
    if (leaves_.empty()) {
        return Cursor();
    }
    return Cursor(this, 0, 0);
}

L1SubtreeBPTree::Cursor L1SubtreeBPTree::LowerBound(const KeyType& key) const {
    if (root_ == nullptr) {
        return Cursor();
    }
    const LeafNode* leaf = DescendToLeaf(key);
    return LowerBoundInLeaf(leaf, key);
}

bool L1SubtreeBPTree::LookupCandidate(const KeyType& key, SubtreeRecord& out) const {
    Cursor cursor = LowerBound(key);
    while (cursor.Valid()) {
        const auto& record = cursor.record();
        if (record.Contains(key)) {
            out = record;
            return true;
        }
        if (CompareKeyType(record.RouteMinKey(), key) > 0) {
            return false;
        }
        cursor.Next();
    }
    return false;
}

void L1SubtreeBPTree::LookupCandidates(const KeyType& key, size_t limit,
                                       std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (limit == 0) {
        return;
    }
    Cursor cursor = LowerBound(key);
    while (cursor.Valid() && out.size() < limit) {
        const auto& record = cursor.record();
        if (record.Contains(key)) {
            out.push_back(record);
        } else if (CompareKeyType(record.RouteMinKey(), key) > 0) {
            break;
        }
        cursor.Next();
    }
}

void L1SubtreeBPTree::RangeScan(const KeyType& start, const KeyType& end,
                                std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (CompareKeyType(start, end) > 0) {
        return;
    }

    Cursor cursor = LowerBound(start);
    while (cursor.Valid()) {
        const auto& record = cursor.record();
        if (CompareKeyType(record.RouteMaxKey(), end) > 0 && CompareKeyType(record.RouteMinKey(), end) > 0) {
            break;
        }
        if (KeyTypeLessEq(record.RouteMinKey(), end) && KeyTypeLessEq(start, record.RouteMaxKey())) {
            out.push_back(record);
        }
        cursor.Next();
    }
}

void L1SubtreeBPTree::ExportAll(std::vector<SubtreeRecord>& out) const {
    out.clear();
    if (size_ == 0) {
        return;
    }
    out.reserve(size_);
    AppendCursorResults(Begin(), size_, out);
}

SubtreePageSet L1SubtreeBPTree::ExportPageSet(uint32_t page_size) const {
    return SubtreePageCodec::Export(*this, page_size);
}

void L1SubtreeBPTree::ImportPageSet(const SubtreePageSet& page_set) {
    SubtreePageCodec::Import(page_set, *this);
}

bool L1SubtreeBPTree::Validate() const {
    if (size_ == 0) {
        return root_ == nullptr && leaves_.empty();
    }
    if (root_ == nullptr || leaves_.empty()) {
        return false;
    }

    size_t seen = 0;
    bool first = true;
    KeyType last_max{};

    for (size_t leaf_idx = 0; leaf_idx < leaves_.size(); ++leaf_idx) {
        const LeafNode* leaf = leaves_[leaf_idx].get();
        if (leaf->records.empty()) {
            return false;
        }
        const auto pos_it = leaf_positions_.find(leaf);
        if (pos_it == leaf_positions_.end() || pos_it->second != leaf_idx) {
            return false;
        }
        for (const auto& record : leaf->records) {
            if (CompareKeyType(record.min_key, record.max_key) > 0) {
                return false;
            }
            if (!record.MatchesLocalFragment(record.route_prefix)) {
                return false;
            }
            if (record.route_prefix != ExtractPrefix(leaf->records.front().RouteMaxKey())) {
                return false;
            }
            if (!first && CompareKeyType(last_max, record.RouteMaxKey()) > 0) {
                return false;
            }
            last_max = record.RouteMaxKey();
            first = false;
            ++seen;
        }
    }
    return seen == size_;
}

size_t L1SubtreeBPTree::DebugCountSharedLeaves(const L1SubtreeBPTree& other) const {
    std::unordered_set<const LeafNode*> leaves(other.leaves_.size());
    for (const auto& leaf : other.leaves_) {
        leaves.insert(leaf.get());
    }
    size_t shared = 0;
    for (const auto& leaf : leaves_) {
        if (leaves.find(leaf.get()) != leaves.end()) {
            ++shared;
        }
    }
    return shared;
}

size_t L1SubtreeBPTree::DebugCountSharedInternalNodes(const L1SubtreeBPTree& other) const {
    std::unordered_set<const InternalNode*> other_nodes;
    CollectInternalNodePointers(other.root_, other_nodes);
    std::unordered_set<const InternalNode*> this_nodes;
    CollectInternalNodePointers(root_, this_nodes);
    size_t shared = 0;
    for (const auto* node : this_nodes) {
        if (other_nodes.find(node) != other_nodes.end()) {
            ++shared;
        }
    }
    return shared;
}

L1SubtreeBPTree::Cursor L1SubtreeBPTree::MakeCursorFromLeafPosition(const LeafNode* leaf,
                                                                    size_t slot) const {
    if (leaf == nullptr) {
        return Cursor();
    }
    auto it = leaf_positions_.find(leaf);
    if (it == leaf_positions_.end()) {
        return Cursor();
    }
    return Cursor(this, it->second, slot);
}

const L1SubtreeBPTree::LeafNode* L1SubtreeBPTree::LeftmostLeaf() const {
    return leaves_.empty() ? nullptr : leaves_.front().get();
}

L1SubtreeBPTree::Cursor L1SubtreeBPTree::LowerBoundInLeaf(const LeafNode* leaf,
                                                          const KeyType& key) const {
    if (leaf == nullptr) {
        return Cursor();
    }
    RecordRouteKeyLess less;
    auto it = std::lower_bound(leaf->records.begin(), leaf->records.end(), key, less);
    if (it != leaf->records.end()) {
        return MakeCursorFromLeafPosition(leaf, static_cast<size_t>(it - leaf->records.begin()));
    }
    const auto pos_it = leaf_positions_.find(leaf);
    if (pos_it == leaf_positions_.end()) {
        return Cursor();
    }
    const size_t next_idx = pos_it->second + 1;
    if (next_idx >= leaves_.size()) {
        return Cursor();
    }
    return Cursor(this, next_idx, 0);
}

const L1SubtreeBPTree::LeafNode* L1SubtreeBPTree::DescendToLeaf(const KeyType& key) const {
    const Node* node = root_.get();
    while (node != nullptr && !node->is_leaf) {
        const auto* internal = static_cast<const InternalNode*>(node);
        auto it = std::lower_bound(
            internal->high_keys.begin(), internal->high_keys.end(), key,
            [](const KeyType& lhs, const KeyType& rhs) { return CompareKeyType(lhs, rhs) < 0; });
        size_t child_idx = it == internal->high_keys.end()
                               ? internal->children.size() - 1
                               : static_cast<size_t>(it - internal->high_keys.begin());
        node = internal->children[child_idx].get();
    }
    return static_cast<const LeafNode*>(node);
}

void L1SubtreeBPTree::BuildLeafLevel(const std::vector<SubtreeRecord>& sorted_records,
                                     std::vector<std::shared_ptr<const LeafNode>>& built_leaves,
                                     std::vector<std::shared_ptr<const Node>>& level_nodes) {
    level_nodes.clear();
    built_leaves.clear();
    level_nodes.reserve((sorted_records.size() + options_.leaf_capacity - 1) / options_.leaf_capacity);
    built_leaves.reserve(level_nodes.capacity());

    for (size_t offset = 0; offset < sorted_records.size(); offset += options_.leaf_capacity) {
        auto leaf = std::make_shared<LeafNode>();
        const size_t end = std::min(offset + options_.leaf_capacity, sorted_records.size());
        leaf->records.insert(leaf->records.end(), sorted_records.begin() + offset, sorted_records.begin() + end);
        leaf->high_key = leaf->records.back().RouteMaxKey();
        built_leaves.push_back(leaf);
        level_nodes.push_back(leaf);
    }

}

std::shared_ptr<const L1SubtreeBPTree::Node> L1SubtreeBPTree::BuildInternalLevels(
    std::vector<std::shared_ptr<const Node>> level_nodes) {
    while (level_nodes.size() > 1) {
        std::vector<std::shared_ptr<const Node>> next_level;
        next_level.reserve((level_nodes.size() + options_.internal_fanout - 1) / options_.internal_fanout);

        for (size_t offset = 0; offset < level_nodes.size(); offset += options_.internal_fanout) {
            auto internal = std::make_shared<InternalNode>();
            const size_t end = std::min(offset + options_.internal_fanout, level_nodes.size());
            internal->children.reserve(end - offset);
            internal->high_keys.reserve(end - offset);
            for (size_t i = offset; i < end; ++i) {
                internal->high_keys.push_back(level_nodes[i]->high_key);
                internal->children.push_back(level_nodes[i]);
            }
            internal->high_key = internal->high_keys.back();
            next_level.push_back(internal);
        }
        level_nodes = std::move(next_level);
    }
    return level_nodes.front();
}

std::shared_ptr<const L1SubtreeBPTree::Node> L1SubtreeBPTree::BuildInternalLevelsCow(
    std::vector<std::shared_ptr<const Node>> level_nodes, const L1SubtreeBPTree& base) {
    InternalReuseIndex reuse_index;
    CollectReusableInternalNodes(base.root_, reuse_index);

    while (level_nodes.size() > 1) {
        std::vector<std::shared_ptr<const Node>> next_level;
        next_level.reserve((level_nodes.size() + options_.internal_fanout - 1) / options_.internal_fanout);

        for (size_t offset = 0; offset < level_nodes.size(); offset += options_.internal_fanout) {
            const size_t end = std::min(offset + options_.internal_fanout, level_nodes.size());
            InternalSequenceKey key;
            key.reserve(end - offset);
            for (size_t i = offset; i < end; ++i) {
                key.push_back(level_nodes[i].get());
            }

            const auto reuse_it = reuse_index.find(key);
            if (reuse_it != reuse_index.end()) {
                next_level.push_back(reuse_it->second);
                continue;
            }

            auto internal = std::make_shared<InternalNode>();
            internal->children.reserve(end - offset);
            internal->high_keys.reserve(end - offset);
            for (size_t i = offset; i < end; ++i) {
                internal->high_keys.push_back(level_nodes[i]->high_key);
                internal->children.push_back(level_nodes[i]);
            }
            internal->high_key = internal->high_keys.back();
            next_level.push_back(internal);
        }
        level_nodes = std::move(next_level);
    }
    return level_nodes.front();
}

void L1SubtreeBPTree::BuildFromLeafSequence(std::vector<std::shared_ptr<const LeafNode>> leaves) {
    Clear();
    if (leaves.empty()) {
        return;
    }
    leaves_ = std::move(leaves);
    leaf_positions_.clear();
    for (size_t i = 0; i < leaves_.size(); ++i) {
        leaf_positions_[leaves_[i].get()] = i;
    }
    std::vector<std::shared_ptr<const Node>> level_nodes;
    level_nodes.reserve(leaves_.size());
    for (const auto& leaf : leaves_) {
        level_nodes.push_back(leaf);
    }
    if (level_nodes.size() == 1) {
        root_ = level_nodes.front();
        return;
    }
    root_ = BuildInternalLevels(std::move(level_nodes));
}

void L1SubtreeBPTree::BuildFromLeafSequenceCow(std::vector<std::shared_ptr<const LeafNode>> leaves,
                                               const L1SubtreeBPTree& base) {
    Clear();
    if (leaves.empty()) {
        return;
    }
    leaves_ = std::move(leaves);
    leaf_positions_.clear();
    for (size_t i = 0; i < leaves_.size(); ++i) {
        leaf_positions_[leaves_[i].get()] = i;
    }
    std::vector<std::shared_ptr<const Node>> level_nodes;
    level_nodes.reserve(leaves_.size());
    for (const auto& leaf : leaves_) {
        level_nodes.push_back(leaf);
    }
    if (level_nodes.size() == 1) {
        root_ = level_nodes.front();
        return;
    }
    root_ = BuildInternalLevelsCow(std::move(level_nodes), base);
}

void L1SubtreeBPTree::AppendCursorResults(Cursor cursor, size_t limit,
                                         std::vector<SubtreeRecord>& out) const {
    while (cursor.Valid() && out.size() < limit) {
        out.push_back(cursor.record());
        cursor.Next();
    }
}

L1SubtreeBPTree::MemoryUsageStats L1SubtreeBPTree::EstimateMemoryUsage() const {
    MemoryUsageStats stats;
    std::unordered_set<const Node*> visited;
    std::function<void(const std::shared_ptr<const Node>&)> walk =
        [&](const std::shared_ptr<const Node>& node) {
            if (!node || !visited.insert(node.get()).second) {
                return;
            }
            if (node->is_leaf) {
                const auto leaf = std::static_pointer_cast<const LeafNode>(node);
                stats.leaf_node_bytes += sizeof(LeafNode);
                stats.vector_overhead_bytes += sizeof(std::vector<SubtreeRecord>);
                stats.record_bytes += leaf->records.capacity() * sizeof(SubtreeRecord);
                return;
            }
            const auto internal = std::static_pointer_cast<const InternalNode>(node);
            stats.internal_node_bytes += sizeof(InternalNode);
            stats.vector_overhead_bytes += sizeof(std::vector<KeyType>);
            stats.vector_overhead_bytes += sizeof(std::vector<std::shared_ptr<const Node>>);
            stats.vector_overhead_bytes +=
                internal->high_keys.capacity() * sizeof(KeyType) +
                internal->children.capacity() * sizeof(std::shared_ptr<const Node>);
            for (const auto& child : internal->children) {
                walk(child);
            }
        };
    walk(root_);
    stats.vector_overhead_bytes += leaves_.capacity() * sizeof(std::shared_ptr<const LeafNode>);
    stats.leaf_position_bytes +=
        leaf_positions_.size() * (sizeof(const LeafNode*) + sizeof(size_t) + sizeof(size_t));
    return stats;
}

}  // namespace flowkv::hybrid_l1
