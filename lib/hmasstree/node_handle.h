/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeHandle: 8-byte logical address for external storage nodes
 */
#ifndef HMASSTREE_NODE_HANDLE_H
#define HMASSTREE_NODE_HANDLE_H

#include <cstdint>
#include <cstring>
#include <atomic>
#include <functional>

namespace Masstree {

/**
 * @brief Node types in Masstree
 */
enum class NodeType : uint8_t {
    INVALID = 0,
    LEAF = 1,
    INTERNODE = 2,
    LAYER_ROOT = 3,  // Root of a layer (for long keys)
};

/**
 * @brief NodeHandle - 8-byte logical address for external storage nodes
 *
 * Encoding format (64 bits):
 *   [63]     : valid bit (1 = valid handle, 0 = null/invalid)
 *   [62:60]  : node_type (3 bits, NodeType enum)
 *   [59:23]  : page_id (37 bits, supports ~137 billion pages)
 *   [22:20]  : slot_index (3 bits, 0-7, for node packing)
 *   [19:0]   : generation (20 bits, for ABA prevention)
 *
 * Design goals:
 *   - Same size as raw pointer (8 bytes)
 *   - Supports node packing (8 nodes per 4KB page)
 *   - Generation counter prevents ABA problem
 *   - Can distinguish null from valid handle
 */
class NodeHandle {
public:
    static constexpr uint64_t VALID_BIT = 63;
    static constexpr uint64_t TYPE_SHIFT = 60;
    static constexpr uint64_t TYPE_MASK = 0x7ULL;  // 3 bits
    static constexpr uint64_t PAGE_ID_SHIFT = 23;
    static constexpr uint64_t PAGE_ID_MASK = 0x1FFFFFFFFFULL;  // 37 bits
    static constexpr uint64_t SLOT_SHIFT = 20;
    static constexpr uint64_t SLOT_MASK = 0x7ULL;  // 3 bits
    static constexpr uint64_t GEN_MASK = 0xFFFFFULL;  // 20 bits

    static constexpr uint64_t MAX_PAGE_ID = PAGE_ID_MASK;
    static constexpr uint64_t MAX_GENERATION = GEN_MASK;
    static constexpr uint8_t SLOTS_PER_PAGE = 8;

private:
    uint64_t data_;

public:
    // ========== Constructors ==========

    /** @brief Construct a null handle */
    NodeHandle() : data_(0) {}

    /** @brief Construct from raw data (for deserialization) */
    explicit NodeHandle(uint64_t raw) : data_(raw) {}

    /** @brief Construct a valid handle */
    NodeHandle(uint64_t page_id, uint8_t slot_index, uint32_t generation, NodeType type)
        : data_(0) {
        set_valid(true);
        set_type(type);
        set_page_id(page_id);
        set_slot_index(slot_index);
        set_generation(generation);
    }

    // ========== Static Factory Methods ==========

    /** @brief Create a null handle */
    static NodeHandle null() {
        return NodeHandle();
    }

    /** @brief Create a handle for a leaf node */
    static NodeHandle make_leaf(uint64_t page_id, uint8_t slot, uint32_t gen) {
        return NodeHandle(page_id, slot, gen, NodeType::LEAF);
    }

    /** @brief Create a handle for an internode */
    static NodeHandle make_internode(uint64_t page_id, uint8_t slot, uint32_t gen) {
        return NodeHandle(page_id, slot, gen, NodeType::INTERNODE);
    }

    /** @brief Create a handle from raw serialized data */
    static NodeHandle from_raw(uint64_t raw) {
        return NodeHandle(raw);
    }

    // ========== Accessors ==========

    /** @brief Check if handle is valid (not null) */
    bool is_valid() const {
        return (data_ >> VALID_BIT) & 1;
    }

    /** @brief Check if handle is null */
    bool is_null() const {
        return !is_valid();
    }

    /** @brief Get node type */
    NodeType type() const {
        return static_cast<NodeType>((data_ >> TYPE_SHIFT) & TYPE_MASK);
    }

    /** @brief Check if this is a leaf node */
    bool is_leaf() const {
        return type() == NodeType::LEAF;
    }

    /** @brief Check if this is an internode */
    bool is_internode() const {
        return type() == NodeType::INTERNODE;
    }

    /** @brief Get page ID */
    uint64_t page_id() const {
        return (data_ >> PAGE_ID_SHIFT) & PAGE_ID_MASK;
    }

    /** @brief Get slot index within page (0-7) */
    uint8_t slot_index() const {
        return static_cast<uint8_t>((data_ >> SLOT_SHIFT) & SLOT_MASK);
    }

    /** @brief Get generation counter */
    uint32_t generation() const {
        return static_cast<uint32_t>(data_ & GEN_MASK);
    }

    /** @brief Get raw data (for serialization) */
    uint64_t raw() const {
        return data_;
    }

    // ========== Mutators ==========

    void set_valid(bool valid) {
        if (valid) {
            data_ |= (1ULL << VALID_BIT);
        } else {
            data_ &= ~(1ULL << VALID_BIT);
        }
    }

    void set_type(NodeType type) {
        data_ &= ~(TYPE_MASK << TYPE_SHIFT);
        data_ |= (static_cast<uint64_t>(type) & TYPE_MASK) << TYPE_SHIFT;
    }

    void set_page_id(uint64_t page_id) {
        data_ &= ~(PAGE_ID_MASK << PAGE_ID_SHIFT);
        data_ |= (page_id & PAGE_ID_MASK) << PAGE_ID_SHIFT;
    }

    void set_slot_index(uint8_t slot) {
        data_ &= ~(SLOT_MASK << SLOT_SHIFT);
        data_ |= (static_cast<uint64_t>(slot) & SLOT_MASK) << SLOT_SHIFT;
    }

    void set_generation(uint32_t gen) {
        data_ &= ~GEN_MASK;
        data_ |= (gen & GEN_MASK);
    }

    /** @brief Increment generation (with wrap-around) */
    void increment_generation() {
        uint32_t gen = generation();
        set_generation((gen + 1) & GEN_MASK);
    }

    // ========== Comparison ==========

    bool operator==(const NodeHandle& other) const {
        return data_ == other.data_;
    }

    bool operator!=(const NodeHandle& other) const {
        return data_ != other.data_;
    }

    /** @brief Compare ignoring generation (for logical equality) */
    bool same_location(const NodeHandle& other) const {
        // Compare page_id, slot, and type only
        constexpr uint64_t LOC_MASK = ~GEN_MASK;
        return (data_ & LOC_MASK) == (other.data_ & LOC_MASK);
    }

    // ========== Operators for use as map key ==========

    bool operator<(const NodeHandle& other) const {
        return data_ < other.data_;
    }
};

/**
 * @brief Atomic wrapper for NodeHandle
 *
 * Used for concurrent access to child pointers in internode
 */
class AtomicNodeHandle {
private:
    std::atomic<uint64_t> data_;

public:
    AtomicNodeHandle() : data_(0) {}

    explicit AtomicNodeHandle(NodeHandle h) : data_(h.raw()) {}

    NodeHandle load(std::memory_order order = std::memory_order_acquire) const {
        return NodeHandle(data_.load(order));
    }

    void store(NodeHandle h, std::memory_order order = std::memory_order_release) {
        data_.store(h.raw(), order);
    }

    bool compare_exchange_strong(NodeHandle& expected, NodeHandle desired,
                                  std::memory_order success = std::memory_order_acq_rel,
                                  std::memory_order failure = std::memory_order_acquire) {
        uint64_t exp = expected.raw();
        bool result = data_.compare_exchange_strong(exp, desired.raw(), success, failure);
        if (!result) {
            expected = NodeHandle(exp);
        }
        return result;
    }

    bool compare_exchange_weak(NodeHandle& expected, NodeHandle desired,
                                std::memory_order success = std::memory_order_acq_rel,
                                std::memory_order failure = std::memory_order_acquire) {
        uint64_t exp = expected.raw();
        bool result = data_.compare_exchange_weak(exp, desired.raw(), success, failure);
        if (!result) {
            expected = NodeHandle(exp);
        }
        return result;
    }
};

/**
 * @brief 4KB page containing multiple packed nodes
 *
 * Layout:
 *   - PageHeader (16 bytes)
 *   - 8 node slots, each 504 bytes (total 4032 bytes)
 *   - Padding (48 bytes)
 */
struct PackedPage {
    static constexpr size_t PAGE_SIZE = 4096;
    static constexpr size_t HEADER_SIZE = 64;  // Aligned to cache line
    static constexpr size_t NODE_SLOT_SIZE = 504;  // (4096 - 64) / 8 = 504
    static constexpr size_t NODES_PER_PAGE = 8;

    struct PageHeader {
        uint64_t magic;           // 0x464C4F574B560001 ("FLOWKV" + version)
        uint64_t page_id;
        uint8_t  slot_bitmap;     // Which slots are allocated (bit mask)
        uint8_t  slot_types[8];   // NodeType for each slot
        uint8_t  reserved[39];    // Padding to 64 bytes
    };
    static_assert(sizeof(PageHeader) == HEADER_SIZE, "PageHeader must be 64 bytes");

    PageHeader header;
    alignas(8) uint8_t slots[NODES_PER_PAGE][NODE_SLOT_SIZE];

    static constexpr uint64_t MAGIC = 0x464C4F574B560001ULL;

    void init(uint64_t page_id) {
        header.magic = MAGIC;
        header.page_id = page_id;
        header.slot_bitmap = 0;
        std::memset(header.slot_types, 0, sizeof(header.slot_types));
        std::memset(header.reserved, 0, sizeof(header.reserved));
    }

    bool is_valid() const {
        return header.magic == MAGIC;
    }

    bool slot_allocated(uint8_t slot) const {
        return (header.slot_bitmap & (1 << slot)) != 0;
    }

    void allocate_slot(uint8_t slot, NodeType type) {
        header.slot_bitmap |= (1 << slot);
        header.slot_types[slot] = static_cast<uint8_t>(type);
    }

    void free_slot(uint8_t slot) {
        header.slot_bitmap &= ~(1 << slot);
        header.slot_types[slot] = 0;
    }

    uint8_t first_free_slot() const {
        for (uint8_t i = 0; i < NODES_PER_PAGE; ++i) {
            if (!slot_allocated(i)) {
                return i;
            }
        }
        return NODES_PER_PAGE;  // No free slot
    }

    bool is_full() const {
        return header.slot_bitmap == 0xFF;
    }

    bool is_empty() const {
        return header.slot_bitmap == 0;
    }

    template<typename NodeT>
    NodeT* get_node(uint8_t slot) {
        return reinterpret_cast<NodeT*>(slots[slot]);
    }

    template<typename NodeT>
    const NodeT* get_node(uint8_t slot) const {
        return reinterpret_cast<const NodeT*>(slots[slot]);
    }

    void* get_slot_ptr(uint8_t slot) {
        return slots[slot];
    }
};
static_assert(sizeof(PackedPage) == PackedPage::PAGE_SIZE, "PackedPage must be 4KB");

}  // namespace Masstree

// Hash function for NodeHandle (for use in std::unordered_map)
namespace std {
template<>
struct hash<Masstree::NodeHandle> {
    size_t operator()(const Masstree::NodeHandle& h) const {
        return std::hash<uint64_t>{}(h.raw());
    }
};
}  // namespace std

#endif  // HMASSTREE_NODE_HANDLE_H
