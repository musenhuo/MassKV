#pragma once

#include "subtree_record.h"
#include "util/util.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace flowkv::hybrid_l1 {

struct SubtreePageSet;
class SubtreePageCodec;

/**
 * @brief 面向 L1 路由场景的只读 B+Tree 子树。
 *
 * 设计原则：
 * - 叶子按 `max_key` 升序存放 `SubtreeRecord`
 * - 仅支持 bulk-load 构建
 * - 仅支持点查 lower_bound、顺序扫描、范围扫描
 * - 不支持在线 insert/delete/split/merge
 */
class L1SubtreeBPTree {
public:
    struct MemoryUsageStats {
        size_t leaf_node_bytes = 0;
        size_t internal_node_bytes = 0;
        size_t record_bytes = 0;
        size_t vector_overhead_bytes = 0;
        size_t leaf_position_bytes = 0;

        size_t TotalBytes() const {
            return leaf_node_bytes + internal_node_bytes + record_bytes +
                   vector_overhead_bytes + leaf_position_bytes;
        }
    };

    struct BuildOptions {
        size_t leaf_capacity = 128;
        size_t internal_fanout = 256;
    };
    struct Node;
    struct LeafNode;
    struct InternalNode;

    class Cursor {
    public:
        Cursor() = default;

        bool Valid() const;
        const SubtreeRecord& record() const;
        bool Next();

    private:
        friend class L1SubtreeBPTree;

        Cursor(const L1SubtreeBPTree* tree, size_t leaf_idx, size_t slot)
            : tree_(tree), leaf_idx_(leaf_idx), slot_(slot) {}

        const L1SubtreeBPTree* tree_ = nullptr;
        size_t leaf_idx_ = 0;
        size_t slot_ = 0;
    };

    L1SubtreeBPTree();
    explicit L1SubtreeBPTree(BuildOptions options);
    ~L1SubtreeBPTree();

    void Clear();

    void BulkLoad(const std::vector<SubtreeRecord>& sorted_records);
    void BulkLoadCow(const L1SubtreeBPTree& base, const std::vector<SubtreeRecord>& sorted_records);
    void BulkLoadFromTables(const std::vector<TaggedPstMeta>& tables);

    bool Empty() const { return size_ == 0; }
    size_t Size() const { return size_; }

    Cursor Begin() const;
    Cursor LowerBound(const KeyType& key) const;

    bool LookupCandidate(const KeyType& key, SubtreeRecord& out) const;
    void LookupCandidates(const KeyType& key, size_t limit, std::vector<SubtreeRecord>& out) const;
    void RangeScan(const KeyType& start, const KeyType& end, std::vector<SubtreeRecord>& out) const;
    void ExportAll(std::vector<SubtreeRecord>& out) const;
    SubtreePageSet ExportPageSet(uint32_t page_size = 16 * 1024) const;
    void ImportPageSet(const SubtreePageSet& page_set);

    bool Validate() const;
    size_t DebugCountSharedLeaves(const L1SubtreeBPTree& other) const;
    size_t DebugCountSharedInternalNodes(const L1SubtreeBPTree& other) const;
    MemoryUsageStats EstimateMemoryUsage() const;

    struct Node {
        explicit Node(bool leaf) : is_leaf(leaf) {}
        virtual ~Node() = default;

        bool is_leaf;
        KeyType high_key{};
    };

    struct LeafNode final : public Node {
        LeafNode() : Node(true) {}

        std::vector<SubtreeRecord> records;
    };

    struct InternalNode final : public Node {
        InternalNode() : Node(false) {}

        std::vector<KeyType> high_keys;
        std::vector<std::shared_ptr<const Node>> children;
    };

    Cursor MakeCursorFromLeafPosition(const LeafNode* leaf, size_t slot) const;
    const LeafNode* LeftmostLeaf() const;
    Cursor LowerBoundInLeaf(const LeafNode* leaf, const KeyType& key) const;
    const LeafNode* DescendToLeaf(const KeyType& key) const;

    void BuildLeafLevel(const std::vector<SubtreeRecord>& sorted_records,
                        std::vector<std::shared_ptr<const LeafNode>>& built_leaves,
                        std::vector<std::shared_ptr<const Node>>& level_nodes);
    std::shared_ptr<const Node> BuildInternalLevels(std::vector<std::shared_ptr<const Node>> level_nodes);
    std::shared_ptr<const Node> BuildInternalLevelsCow(std::vector<std::shared_ptr<const Node>> level_nodes,
                                                       const L1SubtreeBPTree& base);
    void BuildFromLeafSequence(std::vector<std::shared_ptr<const LeafNode>> leaves);
    void BuildFromLeafSequenceCow(std::vector<std::shared_ptr<const LeafNode>> leaves,
                                  const L1SubtreeBPTree& base);

    void AppendCursorResults(Cursor cursor, size_t limit, std::vector<SubtreeRecord>& out) const;

    BuildOptions options_;
    std::shared_ptr<const Node> root_;
    std::vector<std::shared_ptr<const LeafNode>> leaves_;
    std::unordered_map<const LeafNode*, size_t> leaf_positions_;
    size_t size_ = 0;

    friend class SubtreePageCodec;

    DISALLOW_COPY_AND_ASSIGN(L1SubtreeBPTree);
};

}  // namespace flowkv::hybrid_l1
