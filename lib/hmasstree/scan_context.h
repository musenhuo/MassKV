/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * ScanContext: Context for scan operations with external storage support
 */
#ifndef HMASSTREE_SCAN_CONTEXT_H
#define HMASSTREE_SCAN_CONTEXT_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

#include "node_handle.h"
#include "node_cache.h"

namespace Masstree {

// Thread-local scan context storage
inline thread_local class ScanContext* tls_scan_context_ = nullptr;

/**
 * @brief ScanContext - Provides cache access for scan operations
 *
 * In external storage mode, scan operations need access to the node cache
 * to resolve handles to node pointers. This class provides that context.
 */
class ScanContext {
private:
    NodeCache* cache_;
    size_t thread_id_;
    std::vector<CachedPage*> pinned_pages_;
    
public:
    ScanContext() : cache_(nullptr), thread_id_(0) {}
    explicit ScanContext(NodeCache* cache, size_t thread_id = 0) 
        : cache_(cache), thread_id_(thread_id) {}
    
    ~ScanContext() {
        release_all();
    }
    
    /**
     * @brief Check if context is valid
     */
    bool is_valid() const { return cache_ != nullptr; }
    
    /**
     * @brief Get the cache
     */
    NodeCache* cache() const { return cache_; }
    
    /**
     * @brief Release all pinned pages
     */
    void release_all() {
        for (auto* page : pinned_pages_) {
            if (page) page->unpin();
        }
        pinned_pages_.clear();
    }
    
    /**
     * @brief Resolve a handle to a leaf node pointer
     * @param handle The handle to resolve
     * @return Pointer to the leaf node, or nullptr if invalid/not cached
     */
    template <typename P>
    leaf<P>* resolve_leaf(NodeHandle handle) {
        if (!cache_ || !handle.is_valid()) return nullptr;
        
        CachedPage* page = cache_->get_or_load(handle, true);
        if (!page) return nullptr;
        
        pinned_pages_.push_back(page);
        return page->get_node<leaf<P>>(handle.slot_index());
    }
    
    /**
     * @brief Resolve a handle to an internode pointer
     * @param handle The handle to resolve
     * @return Pointer to the internode, or nullptr if invalid/not cached
     */
    template <typename P>
    internode<P>* resolve_internode(NodeHandle handle) {
        if (!cache_ || !handle.is_valid()) return nullptr;
        
        CachedPage* page = cache_->get_or_load(handle, true);
        if (!page) return nullptr;
        
        pinned_pages_.push_back(page);
        return page->get_node<internode<P>>(handle.slot_index());
    }
    
    /**
     * @brief Resolve next leaf in forward scan
     * @param current Current leaf node
     * @return Pointer to next leaf, or nullptr if no next
     */
    template <typename P>
    leaf<P>* resolve_next(const leaf<P>* current) {
        if (!current) return nullptr;
        NodeHandle next_h = current->safe_next_h();
        return resolve_leaf<P>(next_h);
    }
    
    /**
     * @brief Resolve a handle to a generic node_base pointer
     * @param handle The handle to resolve
     * @return Pointer to the node, or nullptr if invalid/not cached
     */
    template <typename P>
    node_base<P>* resolve_node(NodeHandle handle) {
        if (!cache_ || !handle.is_valid()) return nullptr;
        
        CachedPage* page = cache_->get_or_load(handle, true);
        if (!page) return nullptr;
        
        pinned_pages_.push_back(page);
        return page->get_node<node_base<P>>(handle.slot_index());
    }
    
    /**
     * @brief Resolve previous leaf in reverse scan
     * @param current Current leaf node
     * @return Pointer to previous leaf, or nullptr if no prev
     */
    template <typename P>
    leaf<P>* resolve_prev(const leaf<P>* current) {
        if (!current) return nullptr;
        NodeHandle prev_h = current->prev_h();
        return resolve_leaf<P>(prev_h);
    }
    
    /**
     * @brief Get pinned page count
     */
    size_t pinned_count() const { return pinned_pages_.size(); }
};

/**
 * @brief Thread-local scan context accessor
 *
 * Provides access to the current thread's scan context.
 * This would be set up before scan operations begin.
 */
class ScanContextRegistry {
public:
    static ScanContext* get() {
        return tls_scan_context_;
    }
    
    static void set(ScanContext* ctx) {
        tls_scan_context_ = ctx;
    }
};

/**
 * @brief RAII guard for setting scan context
 */
class ScanContextGuard {
private:
    ScanContext* prev_ctx_;
    
public:
    explicit ScanContextGuard(ScanContext* ctx) {
        prev_ctx_ = ScanContextRegistry::get();
        ScanContextRegistry::set(ctx);
    }
    
    ~ScanContextGuard() {
        ScanContextRegistry::set(prev_ctx_);
    }
    
    // Non-copyable
    ScanContextGuard(const ScanContextGuard&) = delete;
    ScanContextGuard& operator=(const ScanContextGuard&) = delete;
};

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_SCAN_CONTEXT_H
