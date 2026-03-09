/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeResolver: Resolve NodeHandle to in-memory node pointers
 * Supports on-demand loading from external storage
 */
#ifndef HMASSTREE_NODE_RESOLVER_H
#define HMASSTREE_NODE_RESOLVER_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

#include "node_handle.h"
#include "node_cache.h"
#include "node_serializer.h"
#include "node_resolver_fwd.h"
#include <unordered_map>
#include <shared_mutex>

namespace Masstree {

// Forward declarations
template <typename P> class node_base;
template <typename P> class internode;
template <typename P> class leaf;
class threadinfo;

// Forward declarations for deserialize helpers
template <typename P>
leaf<P>* deserialize_leaf_from_slot(const void* slot_data, typename P::threadinfo_type& ti);

template <typename P>
internode<P>* deserialize_internode_from_slot(const void* slot_data, typename P::threadinfo_type& ti);

/**
 * @brief NodeResolver - Resolves NodeHandle to in-memory node pointers
 *
 * This class provides the bridge between handle-based persistence and
 * pointer-based in-memory traversal. It maintains a mapping from handles
 * to loaded nodes and handles on-demand loading from storage.
 *
 * Thread-safety: Uses shared_mutex for concurrent reads, exclusive for writes.
 */
template <typename P>
class NodeResolver {
public:
    using node_type = node_base<P>;
    using internode_type = internode<P>;
    using leaf_type = leaf<P>;
    using threadinfo_type = typename P::threadinfo_type;

    /**
     * @brief Construct a resolver with NodeCache backend
     */
    explicit NodeResolver(NodeCache* cache) 
        : cache_(cache) {}

    /**
     * @brief Resolve a handle to an in-memory node pointer
     *
     * If the node is not in the handle-to-pointer map, loads it from cache.
     * The cache will load from storage if needed.
     *
     * @param handle Node handle to resolve
     * @param ti Thread info for memory allocation (for deserialization)
     * @return Pointer to node, or nullptr if resolution failed
     */
    node_type* resolve(NodeHandle handle, threadinfo_type& ti) {
        if (handle.is_null()) {
            return nullptr;
        }

        // Fast path: check if already loaded
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            auto it = handle_to_node_.find(handle.raw());
            if (it != handle_to_node_.end()) {
                return it->second;
            }
        }
        
        // Slow path: load from cache/storage
        return load_and_register(handle, ti);
    }

    /**
     * @brief Get node pointer without loading (returns nullptr if not loaded)
     */
    node_type* try_get(NodeHandle handle) const {
        if (handle.is_null()) {
            return nullptr;
        }
        
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = handle_to_node_.find(handle.raw());
        return (it != handle_to_node_.end()) ? it->second : nullptr;
    }

    /**
     * @brief Register an existing in-memory node with its handle
     * 
     * Called when a new node is created in memory with a pre-allocated handle.
     */
    void register_node(NodeHandle handle, node_type* node) {
        if (handle.is_null() || !node) return;

        std::unique_lock<std::shared_mutex> lock(mutex_);
        handle_to_node_[handle.raw()] = node;
    }

    /**
     * @brief Unregister a node (when it's deallocated)
     */
    void unregister_node(NodeHandle handle) {
        if (handle.is_null()) return;

        std::unique_lock<std::shared_mutex> lock(mutex_);
        handle_to_node_.erase(handle.raw());
    }

    /**
     * @brief Serialize a node to its storage slot
     *
     * @param node Node to serialize
     * @return true on success
     */
    bool serialize_node(node_type* node);

    /**
     * @brief Serialize all registered nodes
     *
     * @return Number of nodes serialized
     */
    size_t serialize_all();

    /**
     * @brief Clear all mappings (for shutdown or reset)
     */
    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        handle_to_node_.clear();
    }

    /**
     * @brief Handle page eviction notification
     *
     * Called by NodeCache when a page is about to be evicted.
     * Clears all cached node pointers that reference the evicted page.
     *
     * @param page_id The page ID being evicted
     * @param page_ptr Pointer to the page data (can be used to verify slots)
     * @return Number of entries removed
     *
     * @note The deserialized node memory is NOT freed here since it's in
     *       thread-local pool memory. This is acceptable because:
     *       1. Pool memory is freed when the thread exits
     *       2. Pool allocation typically doesn't return memory to OS anyway
     *       3. The key goal is to invalidate stale pointers, not memory reclamation
     */
    size_t on_page_evicted(uint64_t page_id, const PackedPage* page_ptr) {
        (void)page_ptr;  // Could be used for verification if needed
        
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        size_t removed = 0;
        auto it = handle_to_node_.begin();
        while (it != handle_to_node_.end()) {
            NodeHandle h(it->first);
            if (h.page_id() == page_id) {
                it = handle_to_node_.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
        
        return removed;
    }

    /**
     * @brief Get number of registered nodes
     */
    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return handle_to_node_.size();
    }

private:
    NodeCache* cache_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<uint64_t, node_type*> handle_to_node_;

    /**
     * @brief Load node from cache/storage and register it
     */
    node_type* load_and_register(NodeHandle handle, threadinfo_type& ti);
};

// ========== Template Implementations ==========

template <typename P>
typename NodeResolver<P>::node_type* 
NodeResolver<P>::load_and_register(NodeHandle handle, threadinfo_type& ti) {
    if (!cache_) {
        return nullptr;
    }

    // Get page from cache (loads from storage if needed)
    CachedPage* page = cache_->get_or_load(handle, true);  // pinned
    if (!page || !page->page_ptr) {
        return nullptr;
    }

    // Get slot data
    uint8_t slot = handle.slot_index();
    if (!page->page_ptr->slot_allocated(slot)) {
        page->unpin();
        return nullptr;
    }

    void* slot_data = page->page_ptr->get_slot_ptr(slot);
    node_type* node = nullptr;

    // Use page's slot type to determine node type (authoritative source)
    uint8_t page_slot_type = page->page_ptr->header.slot_types[slot];
    
    // Deserialize based on node type from page slot_type (not handle.type())
    if (page_slot_type == static_cast<uint8_t>(NodeType::LEAF)) {
        node = reinterpret_cast<node_type*>(
            deserialize_leaf_from_slot<P>(slot_data, ti));
    } else {
        node = reinterpret_cast<node_type*>(
            deserialize_internode_from_slot<P>(slot_data, ti));
    }

    page->unpin();

    if (node) {
        // Register the newly loaded node
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Double-check (another thread might have loaded it)
        auto it = handle_to_node_.find(handle.raw());
        if (it != handle_to_node_.end()) {
            // Another thread loaded it, deallocate our copy
            if (page_slot_type == static_cast<uint8_t>(NodeType::LEAF)) {
                reinterpret_cast<leaf_type*>(node)->deallocate(ti);
            } else {
                reinterpret_cast<internode_type*>(node)->deallocate(ti);
            }
            return it->second;
        }
        handle_to_node_[handle.raw()] = node;
    }

    return node;
}

template <typename P>
bool NodeResolver<P>::serialize_node(node_type* node) {
    if (!node || !cache_) return false;

    NodeHandle handle;
    NodeType type;
    
    if (node->isleaf()) {
        leaf_type* leaf = static_cast<leaf_type*>(node);
        handle = leaf->self_handle_;
        type = NodeType::LEAF;
    } else {
        internode_type* inode = static_cast<internode_type*>(node);
        handle = inode->self_handle_;
        type = NodeType::INTERNODE;
    }

    if (handle.is_null()) {
        return false;  // Node doesn't have a handle
    }

    // Get or create page in cache
    CachedPage* page = cache_->get_or_load(handle, true);
    if (!page || !page->page_ptr) {
        return false;
    }

    uint8_t slot = handle.slot_index();
    void* slot_data = page->page_ptr->get_slot_ptr(slot);

    // Serialize node to slot
    size_t bytes = 0;
    if (type == NodeType::LEAF) {
        bytes = serialize_leaf(static_cast<const leaf_type*>(node), slot_data);
    } else {
        bytes = serialize_internode(static_cast<const internode_type*>(node), slot_data);
    }

    if (bytes > 0) {
        page->page_ptr->allocate_slot(slot, type);
        page->mark_dirty();
    }

    page->unpin();
    return bytes > 0;
}

template <typename P>
size_t NodeResolver<P>::serialize_all() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t count = 0;
    
    for (auto& [handle_raw, node] : handle_to_node_) {
        if (serialize_node(node)) {
            ++count;
        }
    }
    
    return count;
}

/**
 * @brief Deserialize a leaf from slot data
 */
template <typename P>
leaf<P>* deserialize_leaf_from_slot(const void* slot_data, typename P::threadinfo_type& ti) {
    // Allocate empty leaf
    size_t sz = (sizeof(leaf<P>) + 63) & ~size_t(63);
    void* ptr = ti.pool_allocate(sz, memtag_masstree_leaf);
    leaf<P>* node = new(ptr) leaf<P>(sz, typename P::phantom_epoch_type());
    
    // Deserialize from slot
    if (!deserialize_leaf(slot_data, node)) {
        node->deallocate(ti);
        return nullptr;
    }
    
    return node;
}

/**
 * @brief Deserialize an internode from slot data
 */
template <typename P>
internode<P>* deserialize_internode_from_slot(const void* slot_data, typename P::threadinfo_type& ti) {
    const ExternalInternodeLayout* layout = 
        static_cast<const ExternalInternodeLayout*>(slot_data);
    
    // Allocate empty internode
    void* ptr = ti.pool_allocate(sizeof(internode<P>), memtag_masstree_internode);
    internode<P>* node = new(ptr) internode<P>(layout->height);
    
    // Deserialize from slot
    if (!deserialize_internode(slot_data, node)) {
        node->deallocate(ti);
        return nullptr;
    }
    
    // Initialize child pointers to null (will be resolved on demand)
    for (int i = 0; i <= node->nkeys_; ++i) {
        node->child_[i] = nullptr;
    }
    node->parent_ = nullptr;
    
    return node;
}

// NodeResolverRegistry is defined in node_resolver_fwd.h

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_NODE_RESOLVER_H
