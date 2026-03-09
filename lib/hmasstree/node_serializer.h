/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * Node serialization/deserialization for external storage
 */
#ifndef HMASSTREE_NODE_SERIALIZER_H
#define HMASSTREE_NODE_SERIALIZER_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

#include "node_handle.h"
#include "external_node.h"
#include <cstring>

namespace Masstree {

// Forward declarations
template <typename P> class internode;
template <typename P> class leaf;

/**
 * @brief Serialize an internode to external storage format
 *
 * The serialized format matches ExternalInternodeLayout.
 *
 * @param node Source internode
 * @param buffer Destination buffer (must be at least NODE_SLOT_SIZE bytes)
 * @return Number of bytes written
 */
template <typename P>
size_t serialize_internode(const internode<P>* node, void* buffer) {
    static_assert(P::internode_width <= ExternalInternodeLayout::MAX_WIDTH,
                  "Internode width exceeds maximum supported");

    ExternalInternodeLayout* layout = static_cast<ExternalInternodeLayout*>(buffer);
    std::memset(layout, 0, sizeof(ExternalInternodeLayout));

    // Header
    layout->nkeys = node->nkeys_;
    layout->height = node->height_;
    layout->version = node->version_value();

    // Keys
    for (int i = 0; i < node->nkeys_; ++i) {
        layout->ikey0[i] = node->ikey0_[i];
    }

    // Children (handles)
    for (int i = 0; i <= node->nkeys_; ++i) {
        layout->children[i] = node->child_handles_[i];
    }

    // Parent
    layout->parent = node->parent_handle_;

    return sizeof(ExternalInternodeLayout);
}

/**
 * @brief Deserialize an internode from external storage format
 *
 * @param buffer Source buffer
 * @param node Destination internode (must be pre-allocated)
 * @return true on success
 */
template <typename P>
bool deserialize_internode(const void* buffer, internode<P>* node) {
    const ExternalInternodeLayout* layout =
        static_cast<const ExternalInternodeLayout*>(buffer);

    node->nkeys_ = layout->nkeys;
    node->height_ = layout->height;
    
    // Restore version (preserves root_bit, isleaf bit etc.)
    node->set_version_value(layout->version);

    // Keys
    for (int i = 0; i < layout->nkeys; ++i) {
        node->ikey0_[i] = layout->ikey0[i];
    }

    // Children
    for (int i = 0; i <= layout->nkeys; ++i) {
        node->child_handles_[i] = layout->children[i];
    }

    // Parent
    node->parent_handle_ = layout->parent;

    return true;
}

/**
 * @brief Serialize a leaf to external storage format
 *
 * The serialized format matches ExternalLeafLayout.
 * Note: Key suffixes (ksuf) are handled separately.
 *
 * @param node Source leaf
 * @param buffer Destination buffer (must be at least NODE_SLOT_SIZE bytes)
 * @return Number of bytes written
 */
template <typename P>
size_t serialize_leaf(const leaf<P>* node, void* buffer) {
    static_assert(P::leaf_width <= ExternalLeafLayout::MAX_WIDTH,
                  "Leaf width exceeds maximum supported");

    ExternalLeafLayout* layout = static_cast<ExternalLeafLayout*>(buffer);
    std::memset(layout, 0, sizeof(ExternalLeafLayout));

    // Header
    layout->version = node->version_value();
    layout->permutation = node->permutation_;
    layout->extrasize64 = node->extrasize64_;
    layout->modstate = node->modstate_;

    // Keylenx
    std::memcpy(layout->keylenx, node->keylenx_, P::leaf_width);

    // Keys
    for (int i = 0; i < P::leaf_width; ++i) {
        layout->ikey0[i] = node->ikey0_[i];
    }

    // Values/layer handles
    // For layers (keylenx > 127), store the layer root's self_handle
    // For values, store the raw value bits
    for (int i = 0; i < P::leaf_width; ++i) {
        if (node->is_layer(i)) {
            // This is a layer - get the layer root's handle
            auto* layer_root = node->lv_[i].layer();
            if (layer_root) {
                layout->lv[i].layer_handle = layer_root->self_handle();
            } else {
                layout->lv[i].layer_handle = NodeHandle::null();
            }
        } else {
            // Regular value - store raw bits
            layout->lv[i].raw = reinterpret_cast<uint64_t>(node->lv_[i].value());
        }
    }

    // Navigation handles
    layout->next = node->next_handle_;
    layout->prev = node->prev_handle_;
    layout->parent = node->parent_handle_;

    // Phantom epoch
    if (P::need_phantom_epoch) {
        layout->phantom_epoch = node->phantom_epoch_[0];
    }

    return sizeof(ExternalLeafLayout);
}

/**
 * @brief Deserialize a leaf from external storage format
 *
 * @param buffer Source buffer
 * @param node Destination leaf (must be pre-allocated)
 * @return true on success
 */
template <typename P>
bool deserialize_leaf(const void* buffer, leaf<P>* node) {
    const ExternalLeafLayout* layout =
        static_cast<const ExternalLeafLayout*>(buffer);

    // Restore version (preserves root_bit, isleaf bit etc.)
    node->set_version_value(layout->version);
    
    node->permutation_ = layout->permutation;
    node->extrasize64_ = layout->extrasize64;
    node->modstate_ = layout->modstate;

    // Keylenx
    std::memcpy(node->keylenx_, layout->keylenx, P::leaf_width);

    // Keys
    for (int i = 0; i < P::leaf_width; ++i) {
        node->ikey0_[i] = layout->ikey0[i];
    }

    // Values/layer handles - restore based on keylenx
    // For layers (keylenx > 127), the handle will be loaded later
    // For values, restore from raw bits
    for (int i = 0; i < P::leaf_width; ++i) {
        if (leaf<P>::keylenx_is_layer(node->keylenx_[i])) {
            // Layer - store null pointer for now, layer_handle is stored in layout
            // The actual layer will be loaded by load_children_recursive
            // Store raw bits (which is the layer_handle) for later resolution
            node->lv_[i] = typename leaf<P>::leafvalue_type(
                reinterpret_cast<typename P::value_type>(layout->lv[i].raw));
        } else {
            // Regular value - restore from raw bits
            node->lv_[i] = typename leaf<P>::leafvalue_type(
                reinterpret_cast<typename P::value_type>(layout->lv[i].raw));
        }
    }

    // Navigation handles
    node->next_handle_ = layout->next;
    node->prev_handle_ = layout->prev;
    node->parent_handle_ = layout->parent;

    // Phantom epoch
    if (P::need_phantom_epoch) {
        node->phantom_epoch_[0] = layout->phantom_epoch;
    }

    return true;
}

/**
 * @brief Calculate the serialized size of a node
 */
template <typename P>
size_t serialized_size(const internode<P>* /*node*/) {
    return sizeof(ExternalInternodeLayout);
}

template <typename P>
size_t serialized_size(const leaf<P>* /*node*/) {
    // Note: This doesn't include external key suffixes
    return sizeof(ExternalLeafLayout);
}

/**
 * @brief Check if a node fits in a single slot
 */
template <typename P>
bool fits_in_slot(const internode<P>* node) {
    return serialized_size(node) <= PackedPage::NODE_SLOT_SIZE;
}

template <typename P>
bool fits_in_slot(const leaf<P>* node) {
    return serialized_size(node) <= PackedPage::NODE_SLOT_SIZE;
}

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_NODE_SERIALIZER_H
