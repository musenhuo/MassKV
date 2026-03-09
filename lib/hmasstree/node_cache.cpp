/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeCache implementation
 */
#include "node_cache.h"
#include "index_storage.h"
#include <algorithm>
#include <cstring>

namespace Masstree {

NodeCache::NodeCache(IndexStorageManager* storage, const NodeCacheConfig& config)
    : config_(config)
    , storage_(storage)
    , shards_(config.num_shards)
    , thread_epochs_(config.max_threads) {
    // Reserve space in clock lists
    size_t estimated_pages_per_shard = 
        (config.max_memory_bytes / PackedPage::PAGE_SIZE) / config.num_shards;
    for (auto& shard : shards_) {
        shard.clock_list.reserve(estimated_pages_per_shard);
    }
}

NodeCache::~NodeCache() {
    shutdown();
    flush_all();
}

// ========== Core Operations ==========

CachedPage* NodeCache::lookup_no_load(NodeHandle handle) {
    if (handle.is_null()) {
        return nullptr;
    }

    uint64_t page_id = handle.page_id();
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        CachedPage* page = it->second.get();
        page->touch();
        return page;
    }
    return nullptr;
}

CachedPage* NodeCache::try_get(NodeHandle handle, bool pin) {
    if (handle.is_null()) {
        return nullptr;
    }

    uint64_t page_id = handle.page_id();
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        CachedPage* page = it->second.get();
        if (pin) {
            if (!page->try_pin()) {
                // Page is being evicted
                return nullptr;
            }
        }
        page->touch();
        cache_hits_.fetch_add(1, std::memory_order_relaxed);
        return page;
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
}

CachedPage* NodeCache::get_or_load(NodeHandle handle, bool pin) {
    if (handle.is_null()) {
        return nullptr;
    }

    uint64_t page_id = handle.page_id();
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    // Fast path: check cache with shared lock
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.pages.find(page_id);
        if (it != shard.pages.end()) {
            CachedPage* page = it->second.get();
            if (pin) {
                if (!page->try_pin()) {
                    // Page is being evicted, fall through to reload
                    cache_misses_.fetch_add(1, std::memory_order_relaxed);
                } else {
                    page->touch();
                    cache_hits_.fetch_add(1, std::memory_order_relaxed);
                    return page;
                }
            } else {
                page->touch();
                cache_hits_.fetch_add(1, std::memory_order_relaxed);
                return page;
            }
        } else {
            cache_misses_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Slow path: need to load from storage
    // Check if we need to evict first
    if (current_memory_bytes_.load() + PackedPage::PAGE_SIZE > config_.max_memory_bytes) {
        evict_pages(PackedPage::PAGE_SIZE * config_.eviction_batch_size);
    }

    // Load page from storage
    CachedPage* new_page = load_page_from_storage(page_id);
    if (!new_page) {
        return nullptr;
    }

    // Insert into cache with exclusive lock
    std::unique_lock<std::shared_mutex> lock(shard.mutex);

    // Double-check: another thread might have loaded it
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        // Another thread loaded it, discard our copy
        delete new_page;
        CachedPage* page = it->second.get();
        if (pin) {
            if (!page->try_pin()) {
                return nullptr;
            }
        }
        page->touch();
        return page;
    }

    // Insert our page
    if (pin) {
        new_page->try_pin();  // Pre-pin before inserting
    }
    shard.pages[page_id] = std::unique_ptr<CachedPage>(new_page);
    add_to_clock_list(shard, page_id);

    current_memory_bytes_.fetch_add(PackedPage::PAGE_SIZE, std::memory_order_relaxed);
    page_count_.fetch_add(1, std::memory_order_relaxed);

    return new_page;
}

std::pair<NodeHandle, void*> NodeCache::allocate_node(NodeType type) {
    if (!storage_) {
        return {NodeHandle::null(), nullptr};
    }

    // Allocate from storage
    auto [page_id, slot_index] = storage_->allocate_node_slot(type);
    if (page_id == 0) {
        return {NodeHandle::null(), nullptr};
    }

    // Create handle
    uint32_t generation = 1;  // Initial generation
    NodeHandle handle(page_id, slot_index, generation, type);

    // Get or create the page in cache
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    std::unique_lock<std::shared_mutex> lock(shard.mutex);

    CachedPage* page = nullptr;
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        page = it->second.get();
    } else {
        // Create new cached page
        page = new CachedPage();
        page->page_ptr = static_cast<PackedPage*>(aligned_alloc(4096, PackedPage::PAGE_SIZE));
        page->page_ptr->init(page_id);
        page->page_id = page_id;
        page->load_epoch = global_epoch_.load();

        shard.pages[page_id] = std::unique_ptr<CachedPage>(page);
        add_to_clock_list(shard, page_id);

        current_memory_bytes_.fetch_add(PackedPage::PAGE_SIZE, std::memory_order_relaxed);
        page_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // Allocate slot in page
    page->page_ptr->allocate_slot(slot_index, type);
    page->mark_dirty();

    void* slot_ptr = page->page_ptr->get_slot_ptr(slot_index);
    return {handle, slot_ptr};
}

void NodeCache::mark_dirty(NodeHandle handle) {
    if (handle.is_null()) return;

    uint64_t page_id = handle.page_id();
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        it->second->mark_dirty();
    }
}

void NodeCache::free_node(NodeHandle handle) {
    if (handle.is_null()) return;

    uint64_t page_id = handle.page_id();
    uint8_t slot = handle.slot_index();
    size_t shard_id = shard_for_page(page_id);
    Shard& shard = shards_[shard_id];

    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.pages.find(page_id);
    if (it != shard.pages.end()) {
        CachedPage* page = it->second.get();
        page->page_ptr->free_slot(slot);
        page->mark_dirty();

        // If page is now empty, we could free it
        // But for simplicity, let eviction handle it
    }

    // Notify storage of freed slot
    if (storage_) {
        storage_->free_node_slot(handle);
    }
}

// ========== Epoch Protection ==========

void NodeCache::enter_critical_section(size_t thread_id) {
    if (thread_id >= thread_epochs_.size()) return;

    auto& te = thread_epochs_[thread_id];
    te.local_epoch.store(global_epoch_.load(std::memory_order_acquire),
                         std::memory_order_release);
    te.in_critical.store(true, std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

void NodeCache::exit_critical_section(size_t thread_id) {
    if (thread_id >= thread_epochs_.size()) return;

    auto& te = thread_epochs_[thread_id];
    te.in_critical.store(false, std::memory_order_release);
}

void NodeCache::advance_epoch() {
    global_epoch_.fetch_add(1, std::memory_order_acq_rel);
}

uint64_t NodeCache::min_active_epoch() const {
    uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);
    for (const auto& te : thread_epochs_) {
        if (te.in_critical.load(std::memory_order_acquire)) {
            uint64_t e = te.local_epoch.load(std::memory_order_acquire);
            if (e < min_epoch) {
                min_epoch = e;
            }
        }
    }
    return min_epoch;
}

// ========== Eviction ==========

void NodeCache::request_eviction(CachedPage* page) {
    // Mark eviction requested (blocks new pins)
    page->evict_requested.store(true, std::memory_order_release);

    // Add to pending queue
    std::lock_guard<std::mutex> lock(evict_queue_mutex_);
    evict_queue_.push({page, global_epoch_.load()});
}

void NodeCache::process_pending_evictions() {
    std::lock_guard<std::mutex> lock(evict_queue_mutex_);
    
    uint64_t safe_epoch = min_active_epoch();

    while (!evict_queue_.empty()) {
        auto& pending = evict_queue_.front();

        // Check epoch safety
        if (pending.request_epoch >= safe_epoch) {
            // Still readers that might access this page
            break;
        }

        // Check pin count
        if (pending.page->is_pinned()) {
            // Move to back and try later
            evict_queue_.push(pending);
            evict_queue_.pop();
            continue;
        }

        CachedPage* page = pending.page;
        uint64_t page_id = page->page_id;
        evict_queue_.pop();

        // Call eviction callback BEFORE freeing the page
        // This allows NodeResolver to clean up cached pointers
        {
            std::lock_guard<std::mutex> cb_lock(evict_callback_mutex_);
            if (evict_callback_ && page->page_ptr) {
                evict_callback_(page_id, page->page_ptr);
            }
        }

        // Write back if dirty
        if (page->is_dirty.load()) {
            write_page_to_storage(page);
        }

        // Remove from cache
        size_t shard_id = shard_for_page(page_id);
        Shard& shard = shards_[shard_id];

        std::unique_lock<std::shared_mutex> shard_lock(shard.mutex);
        remove_from_clock_list(shard, page_id);
        shard.pages.erase(page_id);

        current_memory_bytes_.fetch_sub(PackedPage::PAGE_SIZE, std::memory_order_relaxed);
        page_count_.fetch_sub(1, std::memory_order_relaxed);
        evictions_.fetch_add(1, std::memory_order_relaxed);
    }
}

size_t NodeCache::evict_pages(size_t target_bytes) {
    size_t freed = 0;
    size_t pages_to_evict = (target_bytes + PackedPage::PAGE_SIZE - 1) / PackedPage::PAGE_SIZE;

    for (size_t i = 0; i < shards_.size() && freed < target_bytes; ++i) {
        Shard& shard = shards_[i];

        std::unique_lock<std::shared_mutex> lock(shard.mutex);

        size_t attempts = 0;
        while (freed < target_bytes && attempts < shard.clock_list.size() * 2) {
            CachedPage* victim = select_victim_clock(shard);
            if (!victim) break;

            // Check if safe to evict
            if (victim->is_pinned() || victim->evict_requested.load()) {
                attempts++;
                continue;
            }

            // Request eviction (async via epoch)
            request_eviction(victim);
            freed += PackedPage::PAGE_SIZE;
            attempts++;
        }
    }

    // Process pending evictions
    process_pending_evictions();

    return freed;
}

CachedPage* NodeCache::select_victim_clock(Shard& shard) {
    if (shard.clock_list.empty()) return nullptr;

    size_t start = shard.clock_hand;
    do {
        uint64_t page_id = shard.clock_list[shard.clock_hand];
        auto it = shard.pages.find(page_id);
        
        shard.clock_hand = (shard.clock_hand + 1) % shard.clock_list.size();

        if (it == shard.pages.end()) continue;

        CachedPage* page = it->second.get();

        // Check reference bit
        uint8_t ref = page->reference_bit.load(std::memory_order_relaxed);
        if (ref) {
            // Give second chance
            page->reference_bit.store(0, std::memory_order_relaxed);
            continue;
        }

        // Found victim
        return page;

    } while (shard.clock_hand != start);

    // All pages have reference bit set, clear all and retry
    for (uint64_t page_id : shard.clock_list) {
        auto it = shard.pages.find(page_id);
        if (it != shard.pages.end()) {
            it->second->reference_bit.store(0, std::memory_order_relaxed);
        }
    }

    // Return first non-pinned page
    for (uint64_t page_id : shard.clock_list) {
        auto it = shard.pages.find(page_id);
        if (it != shard.pages.end() && !it->second->is_pinned()) {
            return it->second.get();
        }
    }

    return nullptr;
}

void NodeCache::add_to_clock_list(Shard& shard, uint64_t page_id) {
    shard.clock_list.push_back(page_id);
}

void NodeCache::remove_from_clock_list(Shard& shard, uint64_t page_id) {
    auto it = std::find(shard.clock_list.begin(), shard.clock_list.end(), page_id);
    if (it != shard.clock_list.end()) {
        // Swap with last and pop (O(1) removal)
        std::swap(*it, shard.clock_list.back());
        shard.clock_list.pop_back();

        // Adjust clock hand if needed
        if (shard.clock_hand >= shard.clock_list.size() && !shard.clock_list.empty()) {
            shard.clock_hand = 0;
        }
    }
}

// ========== Flush ==========

void NodeCache::flush_all() {
    for (size_t i = 0; i < shards_.size(); ++i) {
        flush_shard(i);
    }
}

void NodeCache::flush_shard(size_t shard_id) {
    if (shard_id >= shards_.size()) return;

    Shard& shard = shards_[shard_id];
    std::shared_lock<std::shared_mutex> lock(shard.mutex);

    for (auto& [page_id, cached_page] : shard.pages) {
        if (cached_page->is_dirty.load()) {
            write_page_to_storage(cached_page.get());
            cached_page->clear_dirty();
        }
    }
}

// ========== Storage I/O ==========

CachedPage* NodeCache::load_page_from_storage(uint64_t page_id) {
    if (!storage_) return nullptr;

    auto* page = new CachedPage();
    page->page_ptr = static_cast<PackedPage*>(aligned_alloc(4096, PackedPage::PAGE_SIZE));
    if (!page->page_ptr) {
        delete page;
        return nullptr;
    }

    page->page_id = page_id;
    page->load_epoch = global_epoch_.load();

    bool success = storage_->read_page(page_id, page->page_ptr);
    
    // Check if page has valid magic - sparse file reads return zeros
    if (!success || !page->page_ptr->is_valid()) {
        // Page not found in storage or has invalid magic, initialize as new
        page->page_ptr->init(page_id);
    }

    return page;
}

void NodeCache::write_page_to_storage(CachedPage* page) {
    if (!storage_ || !page || !page->page_ptr) return;

    storage_->write_page(page->page_id, page->page_ptr);
}

}  // namespace Masstree
