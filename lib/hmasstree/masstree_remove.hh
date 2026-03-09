/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2019 President and Fellows of Harvard College
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
#ifndef MASSTREE_REMOVE_HH
#define MASSTREE_REMOVE_HH
#include "masstree_get.hh"
#include "btree_leaflink.hh"
#include "circular_int.hh"
#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "scan_context.h"
#include <vector>
#endif
namespace Masstree {

template <typename P>
bool tcursor<P>::gc_layer(threadinfo& ti)
{
    find_locked(ti);
    masstree_precondition(!n_->deleted() && !n_->deleted_layer());

    // find_locked might return early if another gc_layer attempt has
    // succeeded at removing multiple tree layers. So check that the whole
    // key has been consumed
    if (ka_.has_suffix()) {
        return false;
    }

    // find the slot for the child tree
    // ka_ is a multiple of ikey_size bytes long. We are looking for the entry
    // for the next tree layer, which has keylenx_ corresponding to ikey_size+1.
    // So if has_value(), then we found an entry for the same ikey, but with
    // length ikey_size; we need to adjust ki_.
    kx_.i += has_value();
    if (kx_.i >= n_->size()) {
        return false;
    }
    permuter_type perm(n_->permutation_);
    kx_.p = perm[kx_.i];
    if (n_->ikey0_[kx_.p] != ka_.ikey() || !n_->is_layer(kx_.p)) {
        return false;
    }

    // remove redundant internode layers
    node_type* layer;
    while (true) {
        layer = n_->lv_[kx_.p].layer();
        if (!layer->is_root()) {
            n_->lv_[kx_.p] = layer->maybe_parent();
            continue;
        }

        if (layer->isleaf()) {
            break;
        }

        internode_type *in = static_cast<internode_type *>(layer);
        if (in->size() > 0) {
            return false;
        }
        in->lock(*layer, ti.lock_fence(tc_internode_lock));
        if (!in->is_root() || in->size() > 0) {
            goto unlock_layer;
        }

#ifdef HMASSTREE_EXTERNAL_STORAGE
        ScanContext* ctx = ScanContextRegistry::get();
        NodeHandle child_h = in->child_handle(0);
        node_type *child = nullptr;
        if (ctx && child_h.is_valid()) {
            if (child_h.is_leaf()) {
                child = reinterpret_cast<node_type*>(ctx->template resolve_leaf<P>(child_h));
            } else {
                child = ctx->template resolve_internode<P>(child_h);
            }
        }
        if (!child) {
            in->unlock();
            continue;
        }
#else
        node_type *child = in->child_[0];
#endif
        if (!child->try_lock(ti.lock_fence(tc_internode_lock))) {
            in->unlock();
            continue;
        }
        child->make_layer_root();
        n_->lv_[kx_.p] = child;
        child->unlock();
        in->mark_split();
        in->set_parent(child);  // ensure concurrent reader finds true root
        // NB: now in->parent() might weirdly be a LEAF!
        in->unlock();
        in->deallocate_rcu(ti);
    }

    {
        leaf_type* lf = static_cast<leaf_type*>(layer);
        if (lf->size() > 0) {
            return false;
        }
        lf->lock(*lf, ti.lock_fence(tc_leaf_lock));
        if (!lf->is_root() || lf->size() > 0) {
            goto unlock_layer;
        }

        // child is an empty leaf: kill it
#ifdef HMASSTREE_EXTERNAL_STORAGE
        masstree_invariant(!lf->prev_handle_.is_valid() && !lf->next_handle_.is_valid());
#else
        masstree_invariant(!lf->prev_ && !lf->next_.ptr);
#endif
        masstree_invariant(!lf->deleted());
        masstree_invariant(!lf->deleted_layer());
        if (P::need_phantom_epoch
            && circular_int<typename P::phantom_epoch_type>::less(n_->phantom_epoch_[0], lf->phantom_epoch_[0])) {
            n_->phantom_epoch_[0] = lf->phantom_epoch_[0];
        }
        lf->mark_deleted_layer();   // NB DO NOT mark as deleted (see above)
        lf->unlock();
        lf->deallocate_rcu(ti);
        return true;
    }

 unlock_layer:
    layer->unlock();
    return false;
}

template <typename P>
struct gc_layer_rcu_callback : public P::threadinfo_type::mrcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    node_base<P>* root_;
    int len_;
    char s_[0];
    gc_layer_rcu_callback(node_base<P>* root, Str prefix)
        : root_(root), len_(prefix.length()) {
        memcpy(s_, prefix.data(), len_);
    }
    void operator()(threadinfo& ti);
    size_t size() const {
        return len_ + sizeof(*this);
    }
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
};

template <typename P>
void gc_layer_rcu_callback<P>::operator()(threadinfo& ti)
{
    while (!root_->is_root()) {
        root_ = root_->maybe_parent();
    }
    if (!root_->deleted()) {    // if not destroying tree...
        tcursor<P> lp(root_, s_, len_);
        bool do_remove = lp.gc_layer(ti);
        if (!do_remove || !lp.finish_remove(ti)) {
            lp.n_->unlock();
        }
        ti.deallocate(this, size(), memtag_masstree_gc);
    }
}

template <typename P>
void gc_layer_rcu_callback<P>::make(node_base<P>* root, Str prefix,
                                    threadinfo& ti)
{
    size_t sz = prefix.len + sizeof(gc_layer_rcu_callback<P>);
    void *data = ti.allocate(sz, memtag_masstree_gc);
    gc_layer_rcu_callback<P> *cb =
        new(data) gc_layer_rcu_callback<P>(root, prefix);
    ti.rcu_register(cb);
}

template <typename P>
bool tcursor<P>::finish_remove(threadinfo& ti) {
    if (n_->modstate_ == leaf<P>::modstate_insert) {
        n_->mark_insert();
        n_->modstate_ = leaf<P>::modstate_remove;
    }

    permuter_type perm(n_->permutation_);
    perm.remove(kx_.i);
    n_->permutation_ = perm.value();
    if (perm.size()) {
        return false;
    } else {
        return remove_leaf(n_, root_, ka_.prefix_string(), ti);
    }
}

template <typename P>
bool tcursor<P>::remove_leaf(leaf_type* leaf, node_type* root,
                             Str prefix, threadinfo& ti)
{
#ifdef HMASSTREE_EXTERNAL_STORAGE
    if (!leaf->prev_handle_.is_valid()) {
        if (!leaf->next_handle_.is_valid() && !prefix.empty()) {
            gc_layer_rcu_callback<P>::make(root, prefix, ti);
        }
        return false;
    }
#else
    if (!leaf->prev_) {
        if (!leaf->next_.ptr && !prefix.empty()) {
            gc_layer_rcu_callback<P>::make(root, prefix, ti);
        }
        return false;
    }
#endif

    // mark leaf deleted, RCU-free
    leaf->mark_deleted();
    leaf->deallocate_rcu(ti);

    // Ensure node that becomes responsible for our keys has its phantom epoch
    // kept up to date
    while (P::need_phantom_epoch) {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        ScanContext* ctx = ScanContextRegistry::get();
        NodeHandle prev_h = leaf->prev_h();
        leaf_type *prev = nullptr;
        if (ctx && prev_h.is_valid()) {
            prev = ctx->template resolve_leaf<P>(prev_h);
        }
        if (!prev) break;
#else
        leaf_type *prev = leaf->prev_;
#endif
        typename P::phantom_epoch_type prev_ts = prev->phantom_epoch();
        while (circular_int<typename P::phantom_epoch_type>::less(prev_ts, leaf->phantom_epoch())
               && !bool_cmpxchg(&prev->phantom_epoch_[0], prev_ts, leaf->phantom_epoch())) {
            prev_ts = prev->phantom_epoch();
        }
        fence();
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // Compare handles instead of pointers
        if (leaf->prev_h() == prev_h) {
            break;
        }
#else
        if (prev == leaf->prev_) {
            break;
        }
#endif
    }

    // Unlink leaf from doubly-linked leaf list
    btree_leaflink<leaf_type>::unlink(leaf);

    // Remove leaf from tree, collapse trivial chains, and rewrite
    // ikey bounds.
    ikey_type ikey = leaf->ikey_bound();
    node_type* n = leaf;
    node_type* replacement = nullptr;

    while (true) {
        internode_type *p = n->locked_parent(ti);
        p->mark_insert();
        masstree_invariant(!p->deleted());

        int kp = internode_type::bound_type::upper(ikey, *p);
        masstree_invariant(kp == 0 || p->ikey0_[kp - 1] <= ikey); // NB ikey might not equal!
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // Compare handle for invariant
        masstree_invariant(n->self_handle().is_valid() && 
                          p->child_handles_[kp] == n->self_handle());
        // Set replacement handle or invalid handle
        if (replacement) {
            p->child_handles_[kp] = replacement->self_handle();
        } else {
            p->child_handles_[kp] = NodeHandle();
        }
#else
        masstree_invariant(p->child_[kp] == n);

        p->child_[kp] = replacement;
#endif

        if (replacement) {
            replacement->set_parent(p);
        } else if (kp > 0) {
            p->shift_down(kp - 1, kp, p->nkeys_ - kp);
            --p->nkeys_;
        }

#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (kp <= 1 && p->nkeys_ > 0 && !p->child_handles_[0].is_valid()) {
#else
        if (kp <= 1 && p->nkeys_ > 0 && !p->child_[0]) {
#endif
            redirect(p, ikey, p->ikey0_[0], ti);
            ikey = p->ikey0_[0];
        }

        n->unlock();
        n = p;

        if (p->nkeys_ || p->is_root()) {
            break;
        }

        p->mark_deleted();
        p->deallocate_rcu(ti);
#ifdef HMASSTREE_EXTERNAL_STORAGE
        ScanContext* ctx = ScanContextRegistry::get();
        NodeHandle child_h = p->child_handle(0);
        replacement = nullptr;
        if (ctx && child_h.is_valid()) {
            if (child_h.is_leaf()) {
                replacement = reinterpret_cast<node_type*>(ctx->template resolve_leaf<P>(child_h));
            } else {
                replacement = ctx->template resolve_internode<P>(child_h);
            }
        }
        p->child_handles_[0] = NodeHandle();
#else
        replacement = p->child_[0];
        p->child_[0] = nullptr;
#endif
    }

    n->unlock();
    return true;
}

template <typename P>
void tcursor<P>::redirect(internode_type* n, ikey_type ikey,
                          ikey_type replacement_ikey, threadinfo& ti)
{
    int kp = -1;
    do {
        internode_type* p = n->locked_parent(ti);
        if (kp >= 0) {
            n->unlock();
        }
        kp = internode_type::bound_type::upper(ikey, *p);
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // In external mode, we compare handles for the invariant check
        // Note: n->self_handle() should match p->child_handles_[kp]
        masstree_invariant(n->self_handle().is_valid() && 
                          p->child_handles_[kp] == n->self_handle());
#else
        masstree_invariant(p->child_[kp] == n);
#endif
        if (kp > 0) {
            // NB p->ikey0_[kp - 1] might not equal ikey
            p->ikey0_[kp - 1] = replacement_ikey;
        }
        n = p;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    } while (kp == 0 || (kp == 1 && !n->child_handles_[0].is_valid()));
#else
    } while (kp == 0 || (kp == 1 && !n->child_[0]));
#endif
    n->unlock();
}

template <typename P>
struct destroy_rcu_callback : public P::threadinfo_type::mrcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    typedef typename node_base<P>::leaf_type leaf_type;
    typedef typename node_base<P>::internode_type internode_type;
    node_base<P>* root_;
    int count_;
    destroy_rcu_callback(node_base<P>* root)
        : root_(root), count_(0) {
    }
    void operator()(threadinfo& ti);
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
  private:
    static inline node_base<P>** link_ptr(node_base<P>* n);
    static inline void enqueue(node_base<P>* n, node_base<P>**& tailp);
};

template <typename P>
inline node_base<P>** destroy_rcu_callback<P>::link_ptr(node_base<P>* n) {
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // External storage mode uses vector-based queue, this function not used
    // But keep for non-external storage branch compatibility
    (void)n;
    return nullptr;
#else
    if (n->isleaf())
        return &static_cast<leaf_type*>(n)->parent_;
    else
        return &static_cast<internode_type*>(n)->parent_;
#endif
}

template <typename P>
inline void destroy_rcu_callback<P>::enqueue(node_base<P>* n,
                                             node_base<P>**& tailp) {
    *tailp = n;
    tailp = link_ptr(n);
}

template <typename P>
void destroy_rcu_callback<P>::operator()(threadinfo& ti) {
    if (++count_ == 1) {
        while (!root_->is_root()) {
            root_ = root_->maybe_parent();
        }
        root_->lock();
        root_->mark_deleted_tree(); // i.e., deleted but not splitting
        root_->unlock();
        ti.rcu_register(this);
        return;
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    // External storage mode: use vector-based work queue
    // This avoids using parent_handle_ as linked list pointer
    std::vector<node_base<P>*> workq;
    workq.reserve(256);  // Pre-allocate for efficiency
    workq.push_back(root_);
    
    ScanContext* ctx = ScanContextRegistry::get();
    
    while (!workq.empty()) {
        node_base<P>* n = workq.back();
        workq.pop_back();
        
        if (n->isleaf()) {
            leaf_type* l = static_cast<leaf_type*>(n);
            typename leaf_type::permuter_type perm = l->permutation();
            for (int i = 0; i != l->size(); ++i) {
                int p = perm[i];
                if (l->is_layer(p)) {
                    workq.push_back(l->lv_[p].layer());
                }
            }
            l->deallocate(ti);
        } else {
            internode_type* in = static_cast<internode_type*>(n);
            for (int i = 0; i != in->size() + 1; ++i) {
                NodeHandle child_h = in->child_handles_[i];
                if (ctx && child_h.is_valid()) {
                    node_base<P>* child = nullptr;
                    if (child_h.is_leaf()) {
                        child = reinterpret_cast<node_base<P>*>(
                            ctx->template resolve_leaf<P>(child_h));
                    } else {
                        child = ctx->template resolve_internode<P>(child_h);
                    }
                    if (child) {
                        workq.push_back(child);
                    }
                }
            }
            in->deallocate(ti);
        }
    }
#else
    // Original linked-list based approach for non-external storage
    node_base<P>* workq;
    node_base<P>** tailp = &workq;
    enqueue(root_, tailp);

    while (node_base<P>* n = workq) {
        node_base<P>** linkp = link_ptr(n);
        if (linkp != tailp) {
            workq = *linkp;
        } else {
            workq = 0;
            tailp = &workq;
        }

        if (n->isleaf()) {
            leaf_type* l = static_cast<leaf_type*>(n);
            typename leaf_type::permuter_type perm = l->permutation();
            for (int i = 0; i != l->size(); ++i) {
                int p = perm[i];
                if (l->is_layer(p))
                    enqueue(l->lv_[p].layer(), tailp);
            }
            l->deallocate(ti);
        } else {
            internode_type* in = static_cast<internode_type*>(n);
            for (int i = 0; i != in->size() + 1; ++i) {
                if (in->child_[i])
                    enqueue(in->child_[i], tailp);
            }
            in->deallocate(ti);
        }
    }
#endif
    ti.deallocate(this, sizeof(this), memtag_masstree_gc);
}

template <typename P>
void basic_table<P>::destroy(threadinfo& ti) {
    if (root_) {
        void* data = ti.allocate(sizeof(destroy_rcu_callback<P>), memtag_masstree_gc);
        destroy_rcu_callback<P>* cb = new(data) destroy_rcu_callback<P>(root_);
        ti.rcu_register(cb);
        root_ = 0;
    }
}

} // namespace Masstree
#endif
