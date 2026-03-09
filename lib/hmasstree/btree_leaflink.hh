/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
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
#ifndef BTREE_LEAFLINK_HH
#define BTREE_LEAFLINK_HH 1
#include "compiler.hh"

/** @brief Operations to manage linked lists of B+tree leaves.

    N is the type of nodes. CONCURRENT is true to make operations
    concurrency-safe (e.g. compare-and-swaps, fences), false to leave them
    unsafe (only OK on single threaded code, but faster). */
template <typename N, bool CONCURRENT = N::concurrent> struct btree_leaflink {};

#ifdef HMASSTREE_EXTERNAL_STORAGE

// External storage mode: use NodeHandle-based doubly-linked list
// In external mode, we use handles for next/prev pointers, which allows
// node references to remain valid across memory eviction/reload cycles.
// 
// Note: Updating the next node's prev_handle_ requires cache access.
// This is handled at a higher level when the next node is resolved.

template <typename N> struct btree_leaflink<N, true> {
  public:
    /** @brief Insert a new node @a nr at the right of node @a n.
        @pre @a n is locked.
        @pre @a nr has a valid self_handle_ assigned
        
        In external storage mode, we maintain both pointers AND handles.
        Pointers are used for in-memory traversal, handles for persistence. */
    static void link_split(N *n, N *nr) {
        // Update pointers for in-memory traversal
        nr->prev_ = n;
        N* old_next = n->next_.ptr;
        nr->next_.ptr = old_next;
        if (old_next) {
            old_next->prev_ = nr;
        }
        fence();
        n->next_.ptr = nr;
        
        // Also update handles for persistence
        nr->prev_handle_ = n->self_handle_;
        nr->next_handle_ = n->next_handle_;
        n->next_handle_ = nr->self_handle_;
        if (old_next) {
            old_next->prev_handle_ = nr->self_handle_;
        }
    }
    
    template <typename SF>
    static void link_split(N *n, N *nr, SF) {
        link_split(n, nr);
    }
    
    /** @brief Unlink @a n from the list.
        @pre @a n is locked.
        
        Update both pointers and handles. */
    static void unlink(N *n) {
        N* prev = n->prev_;
        N* next = n->next_.ptr;
        
        // Update pointers
        if (prev) prev->next_.ptr = next;
        if (next) next->prev_ = prev;
        n->next_.ptr = nullptr;
        n->prev_ = nullptr;
        
        // Update handles
        if (prev) prev->next_handle_ = n->next_handle_;
        if (next) next->prev_handle_ = n->prev_handle_;
        n->next_handle_ = decltype(n->next_handle_){};
        n->prev_handle_ = decltype(n->prev_handle_){};
    }
    
    template <typename SF>
    static void unlink(N *n, SF) {
        unlink(n);
    }
    
    /** @brief Complete the unlink operation with cache access.
        @param prev The previous node (resolved from n->prev_handle_)
        @param n The node being unlinked
        @param next The next node (resolved from n->next_handle_, may be null)
        
        This should be called by higher-level code that has cache access. */
    static void complete_unlink(N *prev, N *n, N *next) {
        if (prev) {
            prev->next_handle_ = n->next_handle_;
        }
        if (next) {
            next->prev_handle_ = n->prev_handle_;
        }
        fence();
    }
};

template <typename N> struct btree_leaflink<N, false> {
    // Non-concurrent external storage mode
    // Maintain both pointers and handles
    static void link_split(N *n, N *nr) {
        // Update pointers
        nr->prev_ = n;
        N* old_next = n->next_.ptr;
        nr->next_.ptr = old_next;
        if (old_next) old_next->prev_ = nr;
        n->next_.ptr = nr;
        
        // Update handles
        nr->prev_handle_ = n->self_handle_;
        nr->next_handle_ = n->next_handle_;
        n->next_handle_ = nr->self_handle_;
        if (old_next) old_next->prev_handle_ = nr->self_handle_;
    }
    
    template <typename SF>
    static void link_split(N *n, N *nr, SF) {
        link_split(n, nr);
    }
    
    static void unlink(N *n) {
        N* prev = n->prev_;
        N* next = n->next_.ptr;
        
        if (prev) prev->next_.ptr = next;
        if (next) next->prev_ = prev;
        n->next_.ptr = nullptr;
        n->prev_ = nullptr;
        
        if (prev) prev->next_handle_ = n->next_handle_;
        if (next) next->prev_handle_ = n->prev_handle_;
        n->next_handle_ = decltype(n->next_handle_){};
        n->prev_handle_ = decltype(n->prev_handle_){};
    }
    
    template <typename SF>
    static void unlink(N *n, SF) {
        unlink(n);
    }
    
    static void complete_unlink(N *prev, N *n, N *next) {
        if (prev) {
            prev->next_handle_ = n->next_handle_;
        }
        if (next) {
            next->prev_handle_ = n->prev_handle_;
        }
    }
};

#else


// This is the normal version of btree_leaflink; it uses lock-free linked list
// operations.
template <typename N> struct btree_leaflink<N, true> {
  private:
    static inline N *mark(N *n) {
        return reinterpret_cast<N *>(reinterpret_cast<uintptr_t>(n) + 1);
    }
    static inline bool is_marked(N *n) {
        return reinterpret_cast<uintptr_t>(n) & 1;
    }
    template <typename SF>
    static inline N *lock_next(N *n, SF spin_function) {
        while (1) {
            N *next = n->next_.ptr;
            if (!next
                || (!is_marked(next)
                    && bool_cmpxchg(&n->next_.ptr, next, mark(next))))
                return next;
            spin_function();
        }
    }

  public:
    /** @brief Insert a new node @a nr at the right of node @a n.
        @pre @a n is locked.

        Concurrency correctness: Ensures that all "next" pointers are always
        valid, even if @a n's successor is deleted concurrently. */
    static void link_split(N *n, N *nr) {
        link_split(n, nr, relax_fence_function());
    }
    /** @overload */
    template <typename SF>
    static void link_split(N *n, N *nr, SF spin_function) {
        nr->prev_ = n;
        N *next = lock_next(n, spin_function);
        nr->next_.ptr = next;
        if (next)
            next->prev_ = nr;
        fence();
        n->next_.ptr = nr;
    }

    /** @brief Unlink @a n from the list.
        @pre @a n is locked.

        Concurrency correctness: Works even in the presence of concurrent
        splits and deletes. */
    static void unlink(N *n) {
        unlink(n, relax_fence_function());
    }
    /** @overload */
    template <typename SF>
    static void unlink(N *n, SF spin_function) {
        // Assume node order A <-> N <-> B. Since n is locked, n cannot split;
        // next node will always be B or one of its successors.
        N *next = lock_next(n, spin_function);
        N *prev;
        while (1) {
            prev = n->prev_;
            if (bool_cmpxchg(&prev->next_.ptr, n, mark(n)))
                break;
            spin_function();
        }
        if (next)
            next->prev_ = prev;
        fence();
        prev->next_.ptr = next;
    }
};


// This is the single-threaded-only fast version of btree_leaflink.
template <typename N> struct btree_leaflink<N, false> {
    static void link_split(N *n, N *nr) {
        link_split(n, nr, do_nothing());
    }
    template <typename SF>
    static void link_split(N *n, N *nr, SF) {
        nr->prev_ = n;
        nr->next_.ptr = n->next_.ptr;
        n->next_.ptr = nr;
        if (nr->next_.ptr)
            nr->next_.ptr->prev_ = nr;
    }
    static void unlink(N *n) {
        unlink(n, do_nothing());
    }
    template <typename SF>
    static void unlink(N *n, SF) {
        if (n->next_.ptr)
            n->next_.ptr->prev_ = n->prev_;
        n->prev_->next_.ptr = n->next_.ptr;
    }
};

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif
