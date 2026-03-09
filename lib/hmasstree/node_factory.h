/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeFactory: Factory for creating nodes with proper handle assignment
 */
#ifndef HMASSTREE_NODE_FACTORY_H
#define HMASSTREE_NODE_FACTORY_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

#include "node_handle.h"
#include "node_cache.h"
#include "index_storage.h"

namespace Masstree {

/**
 * @brief NodeFactory - Creates nodes and assigns handles in external storage mode
 *
 * This class bridges the gap between node creation and storage allocation.
 * In external storage mode, every node needs a handle that identifies its
 * storage location.
 */
template <typename P>
class NodeFactory {
public:
    using leaf_type = leaf<P>;
    using internode_type = internode<P>;
    using threadinfo = typename P::threadinfo_type;

private:
    NodeCache* cache_;
    IndexStorageManager* storage_;
    
public:
    NodeFactory() : cache_(nullptr), storage_(nullptr) {}
    
    NodeFactory(NodeCache* cache, IndexStorageManager* storage)
        : cache_(cache), storage_(storage) {}
    
    /**
     * @brief Check if factory is initialized
     */
    bool is_initialized() const {
        return cache_ != nullptr && storage_ != nullptr;
    }
    
    /**
     * @brief Create a leaf node and assign a handle
     * @param ksuf_capacity Key suffix capacity
     * @param phantom_epoch Phantom epoch for the node
     * @param ti Thread info
     * @return Pair of (leaf pointer, handle)
     */
    std::pair<leaf_type*, NodeHandle> make_leaf(
        size_t ksuf_capacity,
        typename P::phantom_epoch_type phantom_epoch,
        threadinfo& ti)
    {
        // First, allocate the leaf using original allocator
        leaf_type* leaf = leaf_type::make(ksuf_capacity, phantom_epoch, ti);
        
        if (!is_initialized()) {
            // Not initialized - return with null handle
            return {leaf, NodeHandle::null()};
        }
        
        // Allocate storage slot
        auto slot = storage_->allocate_node_slot(NodeType::LEAF);
        if (slot.first == 0) {
            // Storage allocation failed
            return {leaf, NodeHandle::null()};
        }
        
        // Create handle
        NodeHandle handle = NodeHandle::make_leaf(
            slot.first,   // page_id
            slot.second,  // slot_index
            0  // Initial generation
        );
        
        // Set self handle
        leaf->self_handle_ = handle;
        
        return {leaf, handle};
    }
    
    /**
     * @brief Create a root leaf node and assign a handle
     */
    std::pair<leaf_type*, NodeHandle> make_root_leaf(
        size_t ksuf_capacity,
        leaf_type* parent,
        threadinfo& ti)
    {
        leaf_type* leaf = leaf_type::make_root(ksuf_capacity, parent, ti);
        
        if (!is_initialized()) {
            return {leaf, NodeHandle::null()};
        }
        
        auto slot = storage_->allocate_node_slot(NodeType::LEAF);
        if (slot.first == 0) {
            return {leaf, NodeHandle::null()};
        }
        
        NodeHandle handle = NodeHandle::make_leaf(
            slot.first,
            slot.second,
            0
        );
        
        leaf->self_handle_ = handle;
        
        return {leaf, handle};
    }
    
    /**
     * @brief Create an internode and assign a handle
     * @param height Height of the internode
     * @param ti Thread info
     * @return Pair of (internode pointer, handle)
     */
    std::pair<internode_type*, NodeHandle> make_internode(
        int height,
        threadinfo& ti)
    {
        internode_type* internode = internode_type::make(height, ti);
        
        if (!is_initialized()) {
            return {internode, NodeHandle::null()};
        }
        
        auto slot = storage_->allocate_node_slot(NodeType::INTERNODE);
        if (slot.first == 0) {
            return {internode, NodeHandle::null()};
        }
        
        NodeHandle handle = NodeHandle::make_internode(
            slot.first,
            slot.second,
            0
        );
        
        internode->self_handle_ = handle;
        
        return {internode, handle};
    }
    
    /**
     * @brief Get handle for an existing node
     * @param node The node to get handle for
     * @return The node's handle
     */
    template <typename NodeType>
    static NodeHandle get_handle(const NodeType* node) {
        if (!node) return NodeHandle::null();
        return node->self_handle_;
    }
    
    /**
     * @brief Free a node's storage slot
     * @param handle Handle of the node to free
     */
    void free_slot(NodeHandle handle) {
        if (!is_initialized() || !handle.is_valid()) return;
        storage_->free_node_slot(handle);
    }
};

/**
 * @brief Thread-local node factory accessor
 *
 * Uses inline thread_local for per-thread factory storage.
 * In production, this would be initialized per-thread with proper
 * cache and storage references.
 */
template <typename P>
class NodeFactoryRegistry {
public:
    static NodeFactory<P>* get() {
        return tls_factory_;
    }
    
    static void set(NodeFactory<P>* factory) {
        tls_factory_ = factory;
    }
    
private:
    static inline thread_local NodeFactory<P>* tls_factory_ = nullptr;
};

/**
 * @brief Helper function to get node handle
 */
template <typename P>
inline NodeHandle node_to_handle(const node_base<P>* node) {
    if (!node) return NodeHandle::null();
    
    if (node->isleaf()) {
        return static_cast<const leaf<P>*>(node)->self_handle_;
    } else {
        return static_cast<const internode<P>*>(node)->self_handle_;
    }
}

/**
 * @brief Helper function to get leaf handle
 */
template <typename P>
inline NodeHandle leaf_to_handle(const leaf<P>* leaf) {
    if (!leaf) return NodeHandle::null();
    return leaf->self_handle_;
}

/**
 * @brief Helper function to get internode handle
 */
template <typename P>
inline NodeHandle internode_to_handle(const internode<P>* in) {
    if (!in) return NodeHandle::null();
    return in->self_handle_;
}

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_NODE_FACTORY_H
