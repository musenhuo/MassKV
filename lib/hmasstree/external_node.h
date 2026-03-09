/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * External storage node type definitions and helpers
 */
#ifndef HMASSTREE_EXTERNAL_NODE_H
#define HMASSTREE_EXTERNAL_NODE_H

#include "node_handle.h"

namespace Masstree {

// Forward declarations
template <typename P> class node_base;
template <typename P> class internode;
template <typename P> class leaf;
class NodeCache;

/**
 * @brief Pointer type selector based on storage mode
 *
 * In memory mode: raw pointer (node_base<P>*)
 * In external mode: NodeHandle (8-byte logical address)
 */
#ifdef HMASSTREE_EXTERNAL_STORAGE

// External storage mode: use NodeHandle
template <typename P>
using NodePtr = NodeHandle;

template <typename P>
using InternodePtr = NodeHandle;

template <typename P>
using LeafPtr = NodeHandle;

// Atomic version for concurrent access
template <typename P>
using AtomicNodePtr = AtomicNodeHandle;

#else

// Memory mode: use raw pointers
template <typename P>
using NodePtr = node_base<P>*;

template <typename P>
using InternodePtr = internode<P>*;

template <typename P>
using LeafPtr = leaf<P>*;

// Atomic version - just atomic pointer
template <typename P>
using AtomicNodePtr = std::atomic<node_base<P>*>;

#endif  // HMASSTREE_EXTERNAL_STORAGE

/**
 * @brief Helper to check if a pointer/handle is null
 */
template <typename T>
inline bool ptr_is_null(T* ptr) {
    return ptr == nullptr;
}

inline bool ptr_is_null(NodeHandle h) {
    return h.is_null();
}

/**
 * @brief Helper to create null pointer/handle
 */
template <typename T>
inline T make_null_ptr_val() {
    return T();
}

template <>
inline NodeHandle make_null_ptr_val<NodeHandle>() {
    return NodeHandle::null();
}

/**
 * @brief Context for external storage operations
 *
 * Passed through threadinfo to provide access to NodeCache
 */
struct ExternalStorageContext {
    NodeCache* cache = nullptr;
    size_t thread_id = 0;

    ExternalStorageContext() = default;
    ExternalStorageContext(NodeCache* c, size_t tid) : cache(c), thread_id(tid) {}
};

#ifdef HMASSTREE_EXTERNAL_STORAGE

/**
 * @brief Resolve a NodeHandle to a node pointer
 *
 * This function is the core of external storage access.
 * It looks up the handle in the cache and loads from disk if needed.
 *
 * @param handle Node handle to resolve
 * @param ctx External storage context (contains cache reference)
 * @return Pointer to the node, or nullptr if resolution failed
 */
template <typename P>
node_base<P>* resolve_handle(NodeHandle handle, ExternalStorageContext& ctx);

/**
 * @brief Resolve with pinning for safe access
 *
 * Pins the page containing the node to prevent eviction during access.
 * Caller must unpin when done.
 */
template <typename P>
node_base<P>* resolve_handle_pinned(NodeHandle handle, ExternalStorageContext& ctx);

/**
 * @brief Create a handle for an existing in-memory node
 *
 * Used during allocation to get the handle for a newly created node.
 */
template <typename P>
NodeHandle create_handle_for_node(node_base<P>* node, NodeType type, ExternalStorageContext& ctx);

#endif  // HMASSTREE_EXTERNAL_STORAGE

/**
 * @brief External storage leafvalue variant
 *
 * In external storage mode, the layer pointer is a NodeHandle
 */
#ifdef HMASSTREE_EXTERNAL_STORAGE

template <typename P>
class external_leafvalue {
public:
    typedef typename P::value_type value_type;
    typedef typename make_prefetcher<P>::type prefetcher_type;

    external_leafvalue() : is_layer_(false) {
        u_.v = value_type();
    }

    external_leafvalue(value_type v) : is_layer_(false) {
        u_.v = v;
    }

    external_leafvalue(NodeHandle h) : is_layer_(true) {
        u_.layer_handle = h;
    }

    static external_leafvalue<P> make_empty() {
        return external_leafvalue<P>();
    }

    bool empty() const {
        return !is_layer_ && u_.v == value_type();
    }

    bool is_layer() const {
        return is_layer_;
    }

    value_type value() const {
        return u_.v;
    }

    value_type& value() {
        return u_.v;
    }

    NodeHandle layer_handle() const {
        return is_layer_ ? u_.layer_handle : NodeHandle::null();
    }

    void set_layer(NodeHandle h) {
        is_layer_ = true;
        u_.layer_handle = h;
    }

    void set_value(value_type v) {
        is_layer_ = false;
        u_.v = v;
    }

private:
    bool is_layer_;
    union {
        value_type v;
        NodeHandle layer_handle;
    } u_;
};

#endif  // HMASSTREE_EXTERNAL_STORAGE

/**
 * @brief Node serialization helpers
 */
struct NodeSerializer {
    /**
     * @brief Serialize an internode to a buffer
     * @param node Source internode
     * @param buffer Destination buffer (must be NODE_SLOT_SIZE bytes)
     * @return Bytes written
     */
    template <typename P>
    static size_t serialize_internode(const internode<P>* node, void* buffer);

    /**
     * @brief Deserialize an internode from a buffer
     * @param buffer Source buffer
     * @param node Destination internode
     * @return true on success
     */
    template <typename P>
    static bool deserialize_internode(const void* buffer, internode<P>* node);

    /**
     * @brief Serialize a leaf to a buffer
     * @param node Source leaf
     * @param buffer Destination buffer (must be NODE_SLOT_SIZE bytes)
     * @return Bytes written
     */
    template <typename P>
    static size_t serialize_leaf(const leaf<P>* node, void* buffer);

    /**
     * @brief Deserialize a leaf from a buffer
     * @param buffer Source buffer
     * @param node Destination leaf
     * @return true on success
     */
    template <typename P>
    static bool deserialize_leaf(const void* buffer, leaf<P>* node);
};

/**
 * @brief Internode layout for external storage
 *
 * Fixed-size layout that fits in a 504-byte slot.
 * Designed to support width up to 15 (default Masstree configuration).
 */
struct ExternalInternodeLayout {
    static constexpr size_t MAX_WIDTH = 15;
    static constexpr size_t SLOT_SIZE = 504;

    // Header (16 bytes)
    uint8_t  nkeys;
    uint8_t  reserved1;
    uint16_t reserved2;
    uint32_t height;
    uint64_t version;  // nodeversion for OCC

    // Keys (15 * 8 = 120 bytes)
    uint64_t ikey0[MAX_WIDTH];

    // Children (16 * 8 = 128 bytes) - handles, not pointers
    NodeHandle children[MAX_WIDTH + 1];

    // Parent (8 bytes)
    NodeHandle parent;

    // Padding to 504 bytes
    uint8_t padding[504 - 16 - 120 - 128 - 8];

    static_assert(sizeof(padding) > 0, "Layout exceeds slot size");
};
static_assert(sizeof(ExternalInternodeLayout) <= 504, "ExternalInternodeLayout too large");

/**
 * @brief Leaf layout for external storage
 *
 * Fixed-size layout that fits in a 504-byte slot.
 */
struct ExternalLeafLayout {
    static constexpr size_t MAX_WIDTH = 15;
    static constexpr size_t SLOT_SIZE = 504;

    // Header (34 bytes total, padded to 40)
    uint64_t version;       // 8 bytes - nodeversion for OCC
    uint64_t permutation;   // 8 bytes - key permutation
    int8_t   extrasize64;   // 1 byte
    uint8_t  modstate;      // 1 byte
    uint8_t  keylenx[MAX_WIDTH];  // 15 bytes
    uint8_t  reserved[5];   // 5 bytes padding to 40

    // Keys (15 * 8 = 120 bytes)
    uint64_t ikey0[MAX_WIDTH];

    // Values/layer handles (15 * 8 = 120 bytes)
    // Note: In external mode, layers are stored as NodeHandle
    union {
        uint64_t raw;
        NodeHandle layer_handle;  // if keylenx indicates layer
    } lv[MAX_WIDTH];

    // Navigation (24 bytes)
    NodeHandle next;
    NodeHandle prev;
    NodeHandle parent;

    // Key suffix pointer - handled separately
    // (external ksuf storage not in this layout)

    // Phantom epoch (8 bytes)
    uint64_t phantom_epoch;

    // Total: 40 + 120 + 120 + 24 + 8 = 312 bytes
    // Padding: 504 - 312 = 192 bytes
    uint8_t padding[192];
};
static_assert(sizeof(ExternalLeafLayout) == 504, "ExternalLeafLayout must be 504 bytes");

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_NODE_H
