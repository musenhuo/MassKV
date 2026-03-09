/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2016 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_STRUCT_HH
#define MASSTREE_STRUCT_HH
#include "masstree.hh"
#include "nodeversion.hh"
#include "stringbag.hh"
#include "mtcounters.hh"
#include "timestamp.hh"

// External storage support
#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "node_handle.h"
#include "scan_context.h"
#include "index_storage.h"
#include "node_resolver_fwd.h"
#endif

namespace Masstree {

template <typename P>
struct make_nodeversion {
    typedef nodeversion_parameters<typename P::nodeversion_value_type> parameters_type;
    typedef typename mass::conditional<P::concurrent,
                                       nodeversion<parameters_type>,
                                       singlethreaded_nodeversion<parameters_type> >::type type;
};

template <typename P>
struct make_prefetcher {
    typedef typename mass::conditional<P::prefetch,
                                       value_prefetcher<typename P::value_type>,
                                       do_nothing>::type type;
};

template <typename P>
class node_base : public make_nodeversion<P>::type {
  public:
    static constexpr bool concurrent = P::concurrent;
    static constexpr int nikey = 1;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef node_base<P> base_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename make_nodeversion<P>::type nodeversion_type;
    typedef typename P::threadinfo_type threadinfo;

    node_base(bool isleaf)
        : nodeversion_type(isleaf) {
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    // External storage mode: return handle instead of pointer
    inline NodeHandle parent_handle() const {
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->parent_handle_;
        else
            return static_cast<const internode_type*>(this)->parent_handle_;
    }
    inline bool parent_exists(NodeHandle h) const {
        return !h.is_null();
    }
    // Parent pointer/handle accessors
    inline bool parent_exists(base_type* p) const {
        return p != nullptr;
    }
    inline bool has_parent() const {
        return parent_exists(parent());
    }
    inline void set_parent_handle(NodeHandle h) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->parent_handle_ = h;
        else
            static_cast<internode_type*>(this)->parent_handle_ = h;
    }
    inline NodeHandle self_handle() const {
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->self_handle_;
        else
            return static_cast<const internode_type*>(this)->self_handle_;
    }
    inline void set_self_handle(NodeHandle h) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->self_handle_ = h;
        else
            static_cast<internode_type*>(this)->self_handle_ = h;
    }
    inline void make_layer_root() {
        set_parent(nullptr);
        set_parent_handle(NodeHandle::null());
        this->mark_root();
    }
    // Parent pointer accessors (same as memory mode)
    inline base_type* parent() const {
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->parent_;
        else
            return static_cast<const internode_type*>(this)->parent_;
    }
    inline void set_parent(base_type* p) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->parent_ = p;
        else
            static_cast<internode_type*>(this)->parent_ = p;
    }
#else
    inline base_type* parent() const {
        // almost always an internode
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->parent_;
        else
            return static_cast<const internode_type*>(this)->parent_;
    }
    inline bool parent_exists(base_type* p) const {
        return p != nullptr;
    }
    inline bool has_parent() const {
        return parent_exists(parent());
    }
    inline void set_parent(base_type* p) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->parent_ = p;
        else
            static_cast<internode_type*>(this)->parent_ = p;
    }
    inline void make_layer_root() {
        set_parent(nullptr);
        this->mark_root();
    }
#endif
    inline internode_type* locked_parent(threadinfo& ti) const;
    inline base_type* maybe_parent() const {
        // Always use pointer for parent traversal (works for both modes)
        base_type* x = parent();
        return parent_exists(x) ? x : const_cast<base_type*>(this);
    }

    inline leaf_type* reach_leaf(const key_type& k, nodeversion_type& version,
                                 threadinfo& ti) const;

    void prefetch_full() const {
        for (int i = 0; i < std::min(16 * std::min(P::leaf_width, P::internode_width) + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;
};

template <typename P>
class internode : public node_base<P> {
  public:
    static constexpr int width = P::internode_width;
    typedef P param_type;  // Expose template parameter for external storage resolve
    typedef typename node_base<P>::nodeversion_type nodeversion_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename P::ikey_type ikey_type;
    typedef typename key_bound<width, P::bound_method>::type bound_type;
    typedef typename P::threadinfo_type threadinfo;

    uint8_t nkeys_;
    uint32_t height_;
    ikey_type ikey0_[width];
#ifdef HMASSTREE_EXTERNAL_STORAGE
    node_base<P>* child_[width + 1];        // Always keep raw pointers for in-memory traversal
    node_base<P>* parent_;                  // Raw parent pointer  
    NodeHandle child_handles_[width + 1];  // External storage mode handles
    NodeHandle parent_handle_;              // External storage mode
    NodeHandle self_handle_;                // Handle pointing to self
#else
    node_base<P>* child_[width + 1];
    node_base<P>* parent_;
#endif
    kvtimestamp_t created_at_[P::debug_level > 0];

#ifdef HMASSTREE_EXTERNAL_STORAGE
    internode(uint32_t height)
        : node_base<P>(false), nkeys_(0), height_(height), parent_(nullptr), parent_handle_(), self_handle_() {
    }
#else
    internode(uint32_t height)
        : node_base<P>(false), nkeys_(0), height_(height), parent_() {
    }
#endif

    static internode<P>* make(uint32_t height, threadinfo& ti) {
        void* ptr = ti.pool_allocate(sizeof(internode<P>),
                                     memtag_masstree_internode);
        internode<P>* n = new(ptr) internode<P>(height);
        assert(n);
        if (P::debug_level > 0)
            n->created_at_[0] = ti.operation_timestamp();
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // Allocate handle for external storage
        IndexStorageManager* storage = IndexStorageRegistry::get();
        if (storage && storage->is_open()) {
            auto slot = storage->allocate_node_slot(NodeType::INTERNODE);
            if (slot.first != 0) {
                n->self_handle_ = NodeHandle::make_internode(slot.first, slot.second, 0);
                
                // CRITICAL: Set slot bitmap in cache page immediately
                // This ensures the slot is marked as allocated even if page is evicted
                NodeCache* cache = NodeCacheRegistry::get();
                if (cache) {
                    CachedPage* page = cache->get_or_load(n->self_handle_, true);
                    if (page && page->page_ptr) {
                        page->page_ptr->allocate_slot(slot.second, NodeType::INTERNODE);
                        page->mark_dirty();
                        page->unpin();
                    }
                }
            }
        }
#endif
        return n;
    }

    int size() const {
        return nkeys_;
    }

    key_type get_key(int p) const {
        return key_type(ikey0_[p]);
    }
    ikey_type ikey(int p) const {
        return ikey0_[p];
    }
    int compare_key(ikey_type a, int bp) const {
        return ::compare(a, ikey(bp));
    }
    int compare_key(const key_type& a, int bp) const {
        return ::compare(a.ikey(), ikey(bp));
    }
    inline int stable_last_key_compare(const key_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;

    void deallocate(threadinfo& ti) {
        ti.pool_deallocate(this, sizeof(*this), memtag_masstree_internode);
    }
    void deallocate_rcu(threadinfo& ti) {
        ti.pool_deallocate_rcu(this, sizeof(*this), memtag_masstree_internode);
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    // External storage accessors - both pointers and handles
    node_base<P>* child(int p) const { return child_[p]; }
    void set_child(int p, node_base<P>* n) { child_[p] = n; }
    node_base<P>** child_ptr() { return child_; }
    const node_base<P>* const* child_ptr() const { return child_; }
    // Handle accessors for persistence
    NodeHandle child_handle(int p) const { return child_handles_[p]; }
    void set_child_handle(int p, NodeHandle h) { child_handles_[p] = h; }
    NodeHandle* child_handles_ptr() { return child_handles_; }
    const NodeHandle* child_handles_ptr() const { return child_handles_; }
    // Unified setter - sets both pointer and handle
    void set_child_with_handle(int p, node_base<P>* n, NodeHandle h) {
        child_[p] = n;
        child_handles_[p] = h;
    }
    
    /**
     * @brief Get child, resolving from storage if needed
     *
     * If child pointer is null but handle is valid, attempts to load
     * the child from storage using the provided loader function.
     *
     * @param p Child index
     * @param loader Function to load node from handle: node_base<P>*(NodeHandle)
     * @return Pointer to child node
     */
    template <typename LoaderFunc>
    node_base<P>* child_or_load(int p, LoaderFunc loader) {
        if (child_[p]) {
            return child_[p];
        }
        if (child_handles_[p].is_valid()) {
            child_[p] = loader(child_handles_[p]);
            if (child_[p]) {
                child_[p]->set_parent(this);
            }
            return child_[p];
        }
        return nullptr;
    }
    
    /**
     * @brief Check if child needs loading from storage
     */
    bool child_needs_loading(int p) const {
        return child_[p] == nullptr && child_handles_[p].is_valid();
    }
#else
    // Memory mode accessors
    node_base<P>* child(int p) const { return child_[p]; }
    void set_child(int p, node_base<P>* n) { child_[p] = n; }
    node_base<P>** child_ptr() { return child_; }
    const node_base<P>* const* child_ptr() const { return child_; }
#endif

  private:
#ifdef HMASSTREE_EXTERNAL_STORAGE
    void assign(int p, ikey_type ikey, node_base<P>* child) {
        // In external mode, we store both pointer and handle
        child->set_parent(this);
        child_[p + 1] = child;
        // Handle will be set separately when needed via set_child_handle
        ikey0_[p] = ikey;
    }

    void shift_from(int p, const internode<P>* x, int xp, int n) {
        masstree_precondition(x != this);
        if (n) {
            memcpy(ikey0_ + p, x->ikey0_ + xp, sizeof(ikey0_[0]) * n);
            memcpy(child_ + p + 1, x->child_ + xp + 1, sizeof(child_[0]) * n);
            memcpy(child_handles_ + p + 1, x->child_handles_ + xp + 1, sizeof(child_handles_[0]) * n);
        }
    }
    void shift_up(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + n, **b = child_ + xp + n; n; --a, --b, --n)
            *a = *b;
        // Also shift handles
        for (NodeHandle *a = child_handles_ + p + n, *b = child_handles_ + xp + n; n; --a, --b, --n)
            *a = *b;
    }
    void shift_down(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + 1, **b = child_ + xp + 1; n; ++a, ++b, --n)
            *a = *b;
        // Also shift handles  
        for (NodeHandle *a = child_handles_ + p + 1, *b = child_handles_ + xp + 1; n; ++a, ++b, --n)
            *a = *b;
    }
#else
    void assign(int p, ikey_type ikey, node_base<P>* child) {
        child->set_parent(this);
        child_[p + 1] = child;
        ikey0_[p] = ikey;
    }

    void shift_from(int p, const internode<P>* x, int xp, int n) {
        masstree_precondition(x != this);
        if (n) {
            memcpy(ikey0_ + p, x->ikey0_ + xp, sizeof(ikey0_[0]) * n);
            memcpy(child_ + p + 1, x->child_ + xp + 1, sizeof(child_[0]) * n);
        }
    }
    void shift_up(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + n, **b = child_ + xp + n; n; --a, --b, --n)
            *a = *b;
    }
    void shift_down(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + 1, **b = child_ + xp + 1; n; ++a, ++b, --n)
            *a = *b;
    }
#endif

#ifdef HMASSTREE_EXTERNAL_STORAGE
    int split_into(internode<P>* nr, int p, ikey_type ka, 
                   node_base<P>* value, NodeHandle value_h,
                   ikey_type& split_ikey, int split_type);
#else
    int split_into(internode<P>* nr, int p, ikey_type ka, node_base<P>* value,
                   ikey_type& split_ikey, int split_type);
#endif

    template <typename PP> friend class tcursor;
};

template <typename P>
class leafvalue {
  public:
    typedef typename P::value_type value_type;
    typedef typename make_prefetcher<P>::type prefetcher_type;

    leafvalue() {
    }
    leafvalue(value_type v) {
        u_.v = v;
    }
    leafvalue(node_base<P>* n) {
        u_.x = reinterpret_cast<uintptr_t>(n);
    }

    static leafvalue<P> make_empty() {
        return leafvalue<P>(value_type());
    }

    typedef bool (leafvalue<P>::*unspecified_bool_type)() const;
    operator unspecified_bool_type() const {
        return u_.x ? &leafvalue<P>::empty : 0;
    }
    bool empty() const {
        return !u_.x;
    }

    value_type value() const {
        return u_.v;
    }
    value_type& value() {
        return u_.v;
    }

    node_base<P>* layer() const {
        return reinterpret_cast<node_base<P>*>(u_.x);
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    /**
     * @brief Check if this leafvalue contains an unresolved layer handle
     * 
     * In external storage mode, after cold restart, layer pointers are stored
     * as NodeHandle encoded values (not valid memory pointers).
     * NodeHandle format (64-bit):
     *   bit 63: valid flag (always 1 for valid handles)
     *   bits 62-60: type (LEAF=1, INTERNODE=2)
     *   bits 59-23: page_id
     *   bits 22-20: slot_index
     *   bits 19-0: generation
     * 
     * Valid heap pointers on 64-bit Linux are in range [0x7f0000000000, 0x7fffffffffff]
     * NodeHandle values have bit 63 set (0x8000000000000000), so they're > 0x8000000000000000
     * which is outside the valid user-space pointer range.
     */
    bool is_unresolved_layer() const {
        // Check if the value looks like a NodeHandle rather than a valid pointer
        // NodeHandle has bit 63 set (valid flag), making it >= 0x8000000000000000
        // Valid user-space pointers on Linux are < 0x800000000000 (typically 0x7f...)
        // Also check for small values that could be handles without valid bit
        constexpr uintptr_t KERNEL_SPACE_START = 0x800000000000ULL;  // Start of kernel space
        constexpr uintptr_t NODEHANDLE_VALID_BIT = 0x8000000000000000ULL;
        
        // It's an unresolved layer if:
        // 1. Value is 0 - null pointer (should not happen, but be safe)
        // 2. Value has the NodeHandle valid bit set (bit 63)
        // 3. Value is a small number (<4GB, could be handle without valid bit)
        return u_.x != 0 && ((u_.x & NODEHANDLE_VALID_BIT) != 0 || u_.x < 0x100000000ULL);
    }

    /**
     * @brief Get the layer handle if this is an unresolved layer
     */
    NodeHandle layer_handle() const {
        return NodeHandle(u_.x);
    }

    /**
     * @brief Set the resolved layer pointer
     */
    void set_layer(node_base<P>* n) {
        u_.n = n;
    }
#endif

    void prefetch(int keylenx) const {
        if (!leaf<P>::keylenx_is_layer(keylenx))
            prefetcher_type()(u_.v);
        else
            u_.n->prefetch_full();
    }

  private:
    union {
        node_base<P>* n;
        value_type v;
        uintptr_t x;
    } u_;
};

template <typename P>
class leaf : public node_base<P> {
  public:
    static constexpr int width = P::leaf_width;
    typedef P param_type;  // Expose template parameter for external storage resolve
    typedef typename node_base<P>::nodeversion_type nodeversion_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename node_base<P>::leafvalue_type leafvalue_type;
    typedef kpermuter<P::leaf_width> permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef typename key_bound<width, P::bound_method>::type bound_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef stringbag<uint8_t> internal_ksuf_type;
    typedef stringbag<uint16_t> external_ksuf_type;
    typedef typename P::phantom_epoch_type phantom_epoch_type;
    static constexpr int ksuf_keylenx = 64;
    static constexpr int layer_keylenx = 128;

    enum {
        modstate_insert = 0, modstate_remove = 1, modstate_deleted_layer = 2
    };

    int8_t extrasize64_;
    uint8_t modstate_;
    uint8_t keylenx_[width];
    typename permuter_type::storage_type permutation_;
    ikey_type ikey0_[width];
    leafvalue_type lv_[width];
    external_ksuf_type* ksuf_;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // Always keep raw pointers for in-memory traversal
    union {
        leaf<P>* ptr;
        uintptr_t x;
    } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
    // External storage handles for persistence
    NodeHandle self_handle_;    // Handle pointing to self
    NodeHandle next_handle_;    // External storage mode
    NodeHandle prev_handle_;    // External storage mode
    NodeHandle parent_handle_;  // External storage mode
#else
    union {
        leaf<P>* ptr;
        uintptr_t x;
    } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
#endif
    phantom_epoch_type phantom_epoch_[P::need_phantom_epoch];
    kvtimestamp_t created_at_[P::debug_level > 0];
    internal_ksuf_type iksuf_[0];

    leaf(size_t sz, phantom_epoch_type phantom_epoch)
        : node_base<P>(true), modstate_(modstate_insert),
          permutation_(permuter_type::make_empty()),
#ifdef HMASSTREE_EXTERNAL_STORAGE
          ksuf_(), parent_(), self_handle_(), parent_handle_(), iksuf_{} {
        next_.ptr = nullptr;
        prev_ = nullptr;
#else
          ksuf_(), parent_(), iksuf_{} {
#endif
        masstree_precondition(sz % 64 == 0 && sz / 64 < 128);
        extrasize64_ = (int(sz) >> 6) - ((int(sizeof(*this)) + 63) >> 6);
        if (extrasize64_ > 0) {
            new((void*) &iksuf_[0]) internal_ksuf_type(width, sz - sizeof(*this));
        }
        if (P::need_phantom_epoch) {
            phantom_epoch_[0] = phantom_epoch;
        }
    }

    static leaf<P>* make(int ksufsize, phantom_epoch_type phantom_epoch, threadinfo& ti) {
        size_t sz = iceil(sizeof(leaf<P>) + std::min(ksufsize, 128), 64);
        void* ptr = ti.pool_allocate(sz, memtag_masstree_leaf);
        leaf<P>* n = new(ptr) leaf<P>(sz, phantom_epoch);
        assert(n);
        if (P::debug_level > 0) {
            n->created_at_[0] = ti.operation_timestamp();
        }
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // Allocate handle for external storage
        IndexStorageManager* storage = IndexStorageRegistry::get();
        if (storage && storage->is_open()) {
            auto slot = storage->allocate_node_slot(NodeType::LEAF);
            if (slot.first != 0) {
                n->self_handle_ = NodeHandle::make_leaf(slot.first, slot.second, 0);
                
                // CRITICAL: Set slot bitmap in cache page immediately
                // This ensures the slot is marked as allocated even if page is evicted
                NodeCache* cache = NodeCacheRegistry::get();
                if (cache) {
                    CachedPage* page = cache->get_or_load(n->self_handle_, true);
                    if (page && page->page_ptr) {
                        page->page_ptr->allocate_slot(slot.second, NodeType::LEAF);
                        page->mark_dirty();
                        page->unpin();
                    }
                }
            }
        }
#endif
        return n;
    }
    static leaf<P>* make_root(int ksufsize, leaf<P>* parent, threadinfo& ti) {
        leaf<P>* n = make(ksufsize, parent ? parent->phantom_epoch() : phantom_epoch_type(), ti);
        n->next_.ptr = n->prev_ = 0;
#ifdef HMASSTREE_EXTERNAL_STORAGE
        n->next_handle_ = NodeHandle::null();
        n->prev_handle_ = NodeHandle::null();
#endif
        n->ikey0_[0] = 0; // to avoid undefined behavior
        n->make_layer_root();
        return n;
    }

    static size_t min_allocated_size() {
        return (sizeof(leaf<P>) + 63) & ~size_t(63);
    }
    size_t allocated_size() const {
        int es = (extrasize64_ >= 0 ? extrasize64_ : -extrasize64_ - 1);
        return (sizeof(*this) + es * 64 + 63) & ~size_t(63);
    }
    phantom_epoch_type phantom_epoch() const {
        return P::need_phantom_epoch ? phantom_epoch_[0] : phantom_epoch_type();
    }

    int size() const {
        return permuter_type::size(permutation_);
    }
    permuter_type permutation() const {
        return permuter_type(permutation_);
    }
    typename nodeversion_type::value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        return (this->version_value() << permuter_type::size_bits) + size();
    }
    typename nodeversion_type::value_type full_unlocked_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        typename node_base<P>::nodeversion_type v(*this);
        if (v.locked()) {
            // subtly, unlocked_version_value() is different than v.unlock();
            // v.version_value() because the latter will add a split bit if
            // we're doing a split. So we do the latter to get the fully
            // correct version.
            v.unlock();
        }
        return (v.version_value() << permuter_type::size_bits) + size();
    }

    using node_base<P>::has_changed;
    bool has_changed(nodeversion_type oldv,
                     typename permuter_type::storage_type oldperm) const {
        return this->has_changed(oldv) || oldperm != permutation_;
    }

    key_type get_key(int p) const {
        int keylenx = keylenx_[p];
        if (!keylenx_has_ksuf(keylenx))
            return key_type(ikey0_[p], keylenx);
        else
            return key_type(ikey0_[p], ksuf(p));
    }
    ikey_type ikey(int p) const {
        return ikey0_[p];
    }
    ikey_type ikey_bound() const {
        return ikey0_[0];
    }
    int compare_key(const key_type& a, int bp) const {
        return a.compare(ikey(bp), keylenx_[bp]);
    }
    inline int stable_last_key_compare(const key_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    inline leaf<P>* advance_to_key(const key_type& k, nodeversion_type& version,
                                   threadinfo& ti) const;

    static bool keylenx_is_layer(int keylenx) {
        return keylenx > 127;
    }
    static bool keylenx_has_ksuf(int keylenx) {
        return keylenx == ksuf_keylenx;
    }

    bool is_layer(int p) const {
        return keylenx_is_layer(keylenx_[p]);
    }
    bool has_ksuf(int p) const {
        return keylenx_has_ksuf(keylenx_[p]);
    }
    Str ksuf(int p, int keylenx) const {
        (void) keylenx;
        masstree_precondition(keylenx_has_ksuf(keylenx));
        return ksuf_ ? ksuf_->get(p) : iksuf_[0].get(p);
    }
    Str ksuf(int p) const {
        return ksuf(p, keylenx_[p]);
    }
    bool ksuf_equals(int p, const key_type& ka) const {
        return ksuf_equals(p, ka, keylenx_[p]);
    }
    bool ksuf_equals(int p, const key_type& ka, int keylenx) const {
        if (!keylenx_has_ksuf(keylenx))
            return true;
        Str s = ksuf(p, keylenx);
        return s.len == ka.suffix().len
            && string_slice<uintptr_t>::equals_sloppy(s.s, ka.suffix().s, s.len);
    }
    // Returns 1 if match & not layer, 0 if no match, <0 if match and layer
    int ksuf_matches(int p, const key_type& ka) const {
        int keylenx = keylenx_[p];
        if (keylenx < ksuf_keylenx)
            return 1;
        if (keylenx == layer_keylenx)
            return -(int) sizeof(ikey_type);
        Str s = ksuf(p, keylenx);
        return s.len == ka.suffix().len
            && string_slice<uintptr_t>::equals_sloppy(s.s, ka.suffix().s, s.len);
    }
    int ksuf_matches_shortcut(int p, const key_type& ka) const {
        int keylenx = keylenx_[p];
        if (keylenx == layer_keylenx)
            return -(int) sizeof(ikey_type);
        Str s = ksuf(p, keylenx);
        return s.len == ka.suffix().len
            && string_slice<uintptr_t>::equals_sloppy(s.s, ka.suffix().s, s.len);
    }
    int ksuf_compare(int p, const key_type& ka) const {
        int keylenx = keylenx_[p];
        if (!keylenx_has_ksuf(keylenx))
            return 0;
        return ksuf(p, keylenx).compare(ka.suffix());
    }

    size_t ksuf_used_capacity() const {
        if (ksuf_)
            return ksuf_->used_capacity();
        else if (extrasize64_ > 0)
            return iksuf_[0].used_capacity();
        else
            return 0;
    }
    size_t ksuf_capacity() const {
        if (ksuf_)
            return ksuf_->capacity();
        else if (extrasize64_ > 0)
            return iksuf_[0].capacity();
        else
            return 0;
    }
    bool ksuf_external() const {
        return ksuf_;
    }
    Str ksuf_storage(int p) const {
        if (ksuf_)
            return ksuf_->get(p);
        else if (extrasize64_ > 0)
            return iksuf_[0].get(p);
        else
            return Str();
    }

    bool deleted_layer() const {
        return modstate_ == modstate_deleted_layer;
    }

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
        if (extrasize64_ > 0)
            ::prefetch((const char *) &iksuf_[0]);
        else if (extrasize64_ < 0) {
            ::prefetch((const char *) ksuf_);
            ::prefetch((const char *) ksuf_ + CACHE_LINE_SIZE);
        }
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;

#ifdef HMASSTREE_EXTERNAL_STORAGE
    // External storage accessors for handles
    NodeHandle next_h() const { return next_handle_; }
    NodeHandle prev_h() const { return prev_handle_; }
    void set_next_h(NodeHandle h) { next_handle_ = h; }
    void set_prev_h(NodeHandle h) { prev_handle_ = h; }
    NodeHandle safe_next_h() const { return next_handle_; }
    
    // Pointer accessors for dual storage - same as memory mode
    leaf<P>* safe_next() const {
        return reinterpret_cast<leaf<P>*>(next_.x & ~(uintptr_t) 1);
    }
    leaf<P>* next_ptr() const { return next_.ptr; }
    leaf<P>* prev_ptr() const { return prev_; }
    void set_next_ptr(leaf<P>* p) { next_.ptr = p; }
    void set_prev_ptr(leaf<P>* p) { prev_ = p; }
    
    /**
     * @brief Get next leaf, loading from storage if needed
     */
    template <typename LoaderFunc>
    leaf<P>* next_or_load(LoaderFunc loader) {
        if (next_.ptr) {
            return next_.ptr;
        }
        if (next_handle_.is_valid()) {
            auto* node = loader(next_handle_);
            if (node && node->isleaf()) {
                next_.ptr = static_cast<leaf<P>*>(node);
                return next_.ptr;
            }
        }
        return nullptr;
    }
    
    /**
     * @brief Get prev leaf, loading from storage if needed
     */
    template <typename LoaderFunc>
    leaf<P>* prev_or_load(LoaderFunc loader) {
        if (prev_) {
            return prev_;
        }
        if (prev_handle_.is_valid()) {
            auto* node = loader(prev_handle_);
            if (node && node->isleaf()) {
                prev_ = static_cast<leaf<P>*>(node);
                return prev_;
            }
        }
        return nullptr;
    }
    
    /**
     * @brief Check if next needs loading
     */
    bool next_needs_loading() const {
        return next_.ptr == nullptr && next_handle_.is_valid();
    }
    
    /**
     * @brief Check if prev needs loading  
     */
    bool prev_needs_loading() const {
        return prev_ == nullptr && prev_handle_.is_valid();
    }
#else
    leaf<P>* safe_next() const {
        return reinterpret_cast<leaf<P>*>(next_.x & ~(uintptr_t) 1);
    }
    
    leaf<P>* next_ptr() const { return next_.ptr; }
    leaf<P>* prev_ptr() const { return prev_; }
    void set_next_ptr(leaf<P>* p) { next_.ptr = p; }
    void set_prev_ptr(leaf<P>* p) { prev_ = p; }
#endif

    void deallocate(threadinfo& ti) {
        if (ksuf_)
            ti.deallocate(ksuf_, ksuf_->capacity(),
                          memtag_masstree_ksuffixes);
        if (extrasize64_ != 0)
            iksuf_[0].~stringbag();
        ti.pool_deallocate(this, allocated_size(), memtag_masstree_leaf);
    }
    void deallocate_rcu(threadinfo& ti) {
        if (ksuf_)
            ti.deallocate_rcu(ksuf_, ksuf_->capacity(),
                              memtag_masstree_ksuffixes);
        ti.pool_deallocate_rcu(this, allocated_size(), memtag_masstree_leaf);
    }

  private:
    inline void mark_deleted_layer() {
        modstate_ = modstate_deleted_layer;
    }

    inline void assign(int p, const key_type& ka, threadinfo& ti) {
        lv_[p] = leafvalue_type::make_empty();
        ikey0_[p] = ka.ikey();
        if (!ka.has_suffix()) {
            keylenx_[p] = ka.length();
        } else {
            keylenx_[p] = ksuf_keylenx;
            assign_ksuf(p, ka.suffix(), false, ti);
        }
    }
    inline void assign_initialize(int p, const key_type& ka, threadinfo& ti) {
        lv_[p] = leafvalue_type::make_empty();
        ikey0_[p] = ka.ikey();
        if (!ka.has_suffix()) {
            keylenx_[p] = ka.length();
        } else {
            keylenx_[p] = ksuf_keylenx;
            assign_ksuf(p, ka.suffix(), true, ti);
        }
    }
    inline void assign_initialize(int p, leaf<P>* x, int xp, threadinfo& ti) {
        lv_[p] = x->lv_[xp];
        ikey0_[p] = x->ikey0_[xp];
        keylenx_[p] = x->keylenx_[xp];
        if (x->has_ksuf(xp)) {
            assign_ksuf(p, x->ksuf(xp), true, ti);
        }
    }
    inline void assign_initialize_for_layer(int p, const key_type& ka) {
        assert(ka.has_suffix());
        ikey0_[p] = ka.ikey();
        keylenx_[p] = layer_keylenx;
    }
    void assign_ksuf(int p, Str s, bool initializing, threadinfo& ti);

    inline ikey_type ikey_after_insert(const permuter_type& perm, int i,
                                       const tcursor<P>* cursor) const;
    int split_into(leaf<P>* nr, tcursor<P>* tcursor, ikey_type& split_ikey,
                   threadinfo& ti);

    template <typename PP> friend class tcursor;
};


template <typename P>
void basic_table<P>::initialize(threadinfo& ti) {
    masstree_precondition(!root_);
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // In external storage mode, initialize root node
    // IndexStorageRegistry should be set before this call
    root_ = node_type::leaf_type::make_root(0, 0, ti);
    if (root_) {
        // Use the handle allocated by make_root -> make
        root_handle_ = root_->self_handle();
    }
#else
    root_ = node_type::leaf_type::make_root(0, 0, ti);
#endif
}


/** @brief Return this node's parent in locked state.
    @pre this->locked()
    @post this->parent() == result && (!result || result->locked()) */
template <typename P>
internode<P>* node_base<P>::locked_parent(threadinfo& ti) const
{
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // In external storage mode, we maintain both pointers and handles.
    // Use pointer for parent traversal (same as memory mode)
    node_base<P>* p;
    masstree_precondition(!this->concurrent || this->locked());
    while (true) {
        p = this->parent();
        if (!this->parent_exists(p)) {
            break;
        }
        nodeversion_type pv = p->lock(*p, ti.lock_fence(tc_internode_lock));
        if (p == this->parent()) {
            masstree_invariant(!p->isleaf());
            break;
        }
        p->unlock(pv);
        relax_fence();
    }
    return static_cast<internode<P>*>(p);
#else
    node_base<P>* p;
    masstree_precondition(!this->concurrent || this->locked());
    while (true) {
        p = this->parent();
        if (!this->parent_exists(p)) {
            break;
        }
        nodeversion_type pv = p->lock(*p, ti.lock_fence(tc_internode_lock));
        if (p == this->parent()) {
            masstree_invariant(!p->isleaf());
            break;
        }
        p->unlock(pv);
        relax_fence();
    }
    return static_cast<internode<P>*>(p);
#endif
}


template <typename P>
void node_base<P>::print(FILE* f, const char* prefix, int depth, int kdepth) const
{
    if (this->isleaf())
        static_cast<const leaf<P>*>(this)->print(f, prefix, depth, kdepth);
    else
        static_cast<const internode<P>*>(this)->print(f, prefix, depth, kdepth);
}


/** @brief Return the result of compare_key(k, LAST KEY IN NODE).

    Reruns the comparison until a stable comparison is obtained. */
template <typename P>
inline int
internode<P>::stable_last_key_compare(const key_type& k, nodeversion_type v,
                                      threadinfo& ti) const
{
    while (true) {
        int n = this->size();
        int cmp = n ? compare_key(k, n - 1) : 1;
        if (likely(!this->has_changed(v))) {
            return cmp;
        }
        v = this->stable_annotated(ti.stable_fence());
    }
}

template <typename P>
inline int
leaf<P>::stable_last_key_compare(const key_type& k, nodeversion_type v,
                                 threadinfo& ti) const
{
    while (true) {
        typename leaf<P>::permuter_type perm(permutation_);
        int n = perm.size();
        // If `n == 0`, then this node is empty: it was deleted without ever
        // splitting, or it split and then was emptied.
        // - It is always safe to return 1, because then the caller will
        //   check more precisely whether `k` belongs in `this`.
        // - It is safe to return anything if `this->deleted()`, because
        //   viewing the deleted node will always cause a retry.
        // - Thus it is safe to return a comparison with the key stored in slot
        //   `perm[0]`. If the node ever had keys in it, then kpermuter ensures
        //   that slot holds the most recently deleted key, which would belong
        //   in this leaf. Otherwise, `perm[0]` is 0.
        int p = perm[n ? n - 1 : 0];
        int cmp = compare_key(k, p);
        if (likely(!this->has_changed(v))) {
            return cmp;
        }
        v = this->stable_annotated(ti.stable_fence());
    }
}


/** @brief Return the leaf in this tree layer responsible for @a ka.

    Returns a stable leaf. Sets @a version to the stable version. */
template <typename P>
inline leaf<P>* node_base<P>::reach_leaf(const key_type& ka,
                                         nodeversion_type& version,
                                         threadinfo& ti) const
{
    const node_base<P> *n[2];
    typename node_base<P>::nodeversion_type v[2];
    unsigned sense;

    // Get a non-stale root.
    // Detect staleness by checking whether n has ever split.
    // The true root has never split.
 retry:
    sense = 0;
    n[sense] = this;
    while (true) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (v[sense].is_root()) {
            break;
        }
        ti.mark(tc_root_retry);
        n[sense] = n[sense]->maybe_parent();
    }

    // Loop over internal nodes.
    while (!v[sense].isleaf()) {
        const internode<P> *in = static_cast<const internode<P>*>(n[sense]);
        in->prefetch();
        int kp = internode<P>::bound_type::upper(ka, *in);
        
#if defined(HMASSTREE_EXTERNAL_STORAGE) && defined(HMASSTREE_HANDLE_ONLY)
        // Handle-only mode: resolve child through ScanContext (slower but enables true external storage)
        ScanContext* scan_ctx = ScanContextRegistry::get();
        if (scan_ctx && scan_ctx->is_valid()) {
            NodeHandle child_h = in->child_handle(kp);
            n[sense ^ 1] = scan_ctx->resolve_node<P>(child_h);
        } else {
            // Fallback to pointer if no ScanContext
            n[sense ^ 1] = in->child_[kp];
        }
#else
        // Pointer mode with on-demand loading fallback for external storage
        n[sense ^ 1] = in->child_[kp];
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // On-demand loading: if child pointer is null but handle is valid, load it
        if (!n[sense ^ 1]) {
            NodeHandle child_h = in->child_handle(kp);
            if (child_h.is_valid()) {
                // Use NodeResolver for proper deserialization
                auto* resolver = NodeResolverRegistry<P>::get();
                if (resolver) {
                    n[sense ^ 1] = resolver->resolve(child_h, ti);
                    if (n[sense ^ 1]) {
                        // Update the child pointer for future fast access
                        const_cast<internode<P>*>(in)->child_[kp] = 
                            const_cast<node_base<P>*>(n[sense ^ 1]);
                    } else {
                        // Node resolution failed - return nullptr to signal error
                        return nullptr;
                    }
                } else {
                    // No resolver available
                    return nullptr;
                }
            } else {
                // Invalid handle
                return nullptr;
            }
        }
#endif
#endif
        
        if (!n[sense ^ 1]) {
            goto retry;
        }
        v[sense ^ 1] = n[sense ^ 1]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
            sense ^= 1;
            continue;
        }

        typename node_base<P>::nodeversion_type oldv = v[sense];
        v[sense] = in->stable_annotated(ti.stable_fence());
        if (unlikely(oldv.has_split(v[sense]))
            && in->stable_last_key_compare(ka, v[sense], ti) > 0) {
            ti.mark(tc_root_retry);
            goto retry;
        } else {
            ti.mark(tc_internode_retry);
        }
    }

    version = v[sense];
    return const_cast<leaf<P> *>(static_cast<const leaf<P> *>(n[sense]));
}

/** @brief Return the leaf at or after *this responsible for @a ka.
    @pre *this was responsible for @a ka at version @a v

    Checks whether *this has split since version @a v. If it has split, then
    advances through the leaves using the B^link-tree pointers and returns
    the relevant leaf, setting @a v to the stable version for that leaf. */
template <typename P>
leaf<P>* leaf<P>::advance_to_key(const key_type& ka, nodeversion_type& v,
                                 threadinfo& ti) const
{
    const leaf<P>* n = this;
    nodeversion_type oldv = v;
    v = n->stable_annotated(ti.stable_fence());
    if (unlikely(v.has_split(oldv))
        && n->stable_last_key_compare(ka, v, ti) > 0) {
        leaf<P> *next;
        ti.mark(tc_leaf_walk);
        
#if defined(HMASSTREE_EXTERNAL_STORAGE) && defined(HMASSTREE_HANDLE_ONLY)
        // Handle-only mode: resolve next through ScanContext
        ScanContext* scan_ctx = ScanContextRegistry::get();
        while (likely(!v.deleted())) {
            if (scan_ctx && scan_ctx->is_valid()) {
                NodeHandle next_h = n->safe_next_h();
                next = scan_ctx->resolve_leaf<P>(next_h);
            } else {
                next = n->safe_next();
            }
            if (!next || compare(ka.ikey(), next->ikey_bound()) < 0) break;
            n = next;
            v = n->stable_annotated(ti.stable_fence());
        }
#else
        // Pointer mode with on-demand loading fallback
        while (likely(!v.deleted())) {
            next = n->safe_next();
#ifdef HMASSTREE_EXTERNAL_STORAGE
            // On-demand loading: if next is null but handle is valid
            if (!next) {
                NodeHandle next_h = n->safe_next_h();
                if (next_h.is_valid()) {
                    // Use NodeResolver for proper deserialization
                    auto* resolver = NodeResolverRegistry<P>::get();
                    if (resolver) {
                        node_base<P>* node = resolver->resolve(next_h, ti);
                        if (node && node->isleaf()) {
                            next = static_cast<leaf<P>*>(node);
                            const_cast<leaf<P>*>(n)->next_.ptr = next;
                        }
                    }
                }
            }
#endif
            if (!next || compare(ka.ikey(), next->ikey_bound()) < 0) break;
            n = next;
            v = n->stable_annotated(ti.stable_fence());
        }
#endif
    }
    return const_cast<leaf<P>*>(n);
}


/** @brief Assign position @a p's keysuffix to @a s.

    This may allocate a new suffix container, copying suffixes over.

    The @a initializing parameter determines which suffixes are copied. If @a
    initializing is false, then this is an insertion into a live node. The
    live node's permutation indicates which keysuffixes are active, and only
    active suffixes are copied. If @a initializing is true, then this
    assignment is part of the initialization process for a new node. The
    permutation might not be set up yet. In this case, it is assumed that key
    positions [0,p) are ready: keysuffixes in that range are copied. In either
    case, the key at position p is NOT copied; it is assigned to @a s. */
template <typename P>
void leaf<P>::assign_ksuf(int p, Str s, bool initializing, threadinfo& ti) {
    if ((ksuf_ && ksuf_->assign(p, s))
        || (extrasize64_ > 0 && iksuf_[0].assign(p, s)))
        return;

    external_ksuf_type* oksuf = ksuf_;

    permuter_type perm(permutation_);
    int n = initializing ? p : perm.size();

    size_t csz = 0;
    for (int i = 0; i < n; ++i) {
        int mp = initializing ? i : perm[i];
        if (mp != p && has_ksuf(mp))
            csz += ksuf(mp).len;
    }

    size_t sz = iceil_log2(external_ksuf_type::safe_size(width, csz + s.len));
    if (oksuf)
        sz = std::max(sz, oksuf->capacity());

    void* ptr = ti.allocate(sz, memtag_masstree_ksuffixes);
    external_ksuf_type* nksuf = new(ptr) external_ksuf_type(width, sz);
    for (int i = 0; i < n; ++i) {
        int mp = initializing ? i : perm[i];
        if (mp != p && has_ksuf(mp)) {
            bool ok = nksuf->assign(mp, ksuf(mp));
            assert(ok); (void) ok;
        }
    }
    bool ok = nksuf->assign(p, s);
    assert(ok); (void) ok;
    fence();

    // removed ksufs aren't copied to the new ksuf, but observers
    // might need them. We ensure that observers must retry by
    // ensuring that we are not currently in the remove state.
    // State transitions are accompanied by mark_insert() so observers
    // will retry.
    masstree_invariant(modstate_ != modstate_remove);

    ksuf_ = nksuf;
    fence();

    if (extrasize64_ >= 0)      // now the new ksuf_ installed, mark old dead
        extrasize64_ = -extrasize64_ - 1;

    if (oksuf)
        ti.deallocate_rcu(oksuf, oksuf->capacity(),
                          memtag_masstree_ksuffixes);
}

template <typename P>
inline basic_table<P>::basic_table()
#ifdef HMASSTREE_EXTERNAL_STORAGE
    : root_handle_(), root_(0) {
#else
    : root_(0) {
#endif
}

template <typename P>
inline node_base<P>* basic_table<P>::root() const {
    return root_;
}

template <typename P>
inline node_base<P>* basic_table<P>::fix_root() {
    node_base<P>* root = root_;
    if (unlikely(!root->is_root())) {
        node_base<P>* old_root = root;
        root = root->maybe_parent();
        (void) cmpxchg(&root_, old_root, root);
    }
    return root;
}

} // namespace Masstree
#endif
