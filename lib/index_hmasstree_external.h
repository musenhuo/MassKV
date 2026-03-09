#pragma once

/**
 * @brief H-Masstree External Storage Index Adapter for FlowKV
 *
 * This file provides an optional external storage mode for H-Masstree.
 * When HMASSTREE_EXTERNAL_STORAGE is defined, it uses the external
 * storage backend. Otherwise, it falls back to the standard in-memory
 * implementation.
 */

#include "util/util.h"
#include "hmasstree/hmasstree_wrapper.h"

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "hmasstree/external_index.h"
#endif

/**
 * @brief H-Masstree External Storage Index for FlowKV
 *
 * This class extends HMasstreeIndex with external storage support.
 * In external storage mode, it manages a NodeCache and IndexStorageManager
 * for storing index nodes on SSD.
 *
 * Usage (external mode):
 *   Masstree::ExternalIndexConfig config;
 *   config.storage_path = "/data/index.dat";
 *   config.cache_size_mb = 512;
 *
 *   HMasstreeExternalIndex index(config);
 *   index.Initialize();
 *   // ... use index ...
 *   index.Shutdown();
 *
 * Usage (in-memory mode - HMASSTREE_EXTERNAL_STORAGE not defined):
 *   HMasstreeExternalIndex index;
 *   // Works the same as HMasstreeIndex
 */
class HMasstreeExternalIndex : public Index
{
public:
#ifdef HMASSTREE_EXTERNAL_STORAGE
    HMasstreeExternalIndex()
        : mt_(nullptr), storage_mgr_(nullptr), initialized_(false) {}

    explicit HMasstreeExternalIndex(const Masstree::ExternalIndexConfig& config)
        : mt_(nullptr), config_(config), storage_mgr_(nullptr), initialized_(false) {}
#else
    HMasstreeExternalIndex() : mt_(new HMasstreeWrapper()) {}
#endif

    virtual ~HMasstreeExternalIndex() override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        Shutdown();
#else
        delete mt_;
#endif
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    /**
     * @brief Initialize external storage and create the index
     * @return true on success
     */
    bool Initialize() {
        if (initialized_) return true;

        // Initialize storage manager
        storage_mgr_ = std::make_unique<Masstree::ExternalStorageManager>(config_);
        if (!storage_mgr_->initialize()) {
            return false;
        }

        // Create the masstree wrapper
        mt_ = new HMasstreeWrapper();
        initialized_ = true;
        return true;
    }

    /**
     * @brief Shutdown external storage cleanly
     */
    void Shutdown() {
        if (!initialized_) return;

        if (storage_mgr_) {
            storage_mgr_->shutdown();
            storage_mgr_.reset();
        }

        if (mt_) {
            delete mt_;
            mt_ = nullptr;
        }

        initialized_ = false;
    }

    /**
     * @brief Check if index is initialized
     */
    bool IsInitialized() const { return initialized_; }

    /**
     * @brief Get storage manager (for advanced usage)
     */
    Masstree::ExternalStorageManager* GetStorageManager() {
        return storage_mgr_.get();
    }

    /**
     * @brief Print external storage statistics
     */
    void PrintStats() const {
        if (storage_mgr_) {
            storage_mgr_->print_stats();
        }
    }

    /**
     * @brief Flush dirty pages to storage
     */
    void Flush() {
        if (storage_mgr_) {
            storage_mgr_->flush(true);
        }
    }
#endif

    void ThreadInit(int thread_id) override {
        HMasstreeWrapper::thread_init(thread_id);
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // Set up thread-local external storage context
        if (storage_mgr_ && storage_mgr_->cache()) {
            thread_ctx_ = Masstree::ScanContext(storage_mgr_->cache(), thread_id);
            Masstree::ScanContextRegistry::set(&thread_ctx_);
        }
#endif
    }

    ValueType Get(const KeyType key) override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return INVALID_PTR;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        ValueType val;
        bool found = mt_->search(key, val);
        return found ? val : INVALID_PTR;
    }

    virtual void Put(const KeyType key, ValueHelper& le_helper) {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->insert(key, le_helper);
    }

    virtual void PutValidate(const KeyType key, ValueHelper& le_helper) {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->insert_validate(key, le_helper);
    }

    void Delete(const KeyType key) override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->remove(key);
    }

    void Scan(const KeyType key, int cnt,
              std::vector<ValueType>& vec) override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->scan(key, cnt, vec);
    }

    void Scan2(const KeyType key, int cnt,
               std::vector<KeyType>& kvec,
               std::vector<ValueType>& vvec) override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->scan(key, cnt, kvec, vvec);
    }

    void ScanByRange(const KeyType start, const KeyType end,
                     std::vector<KeyType>& kvec,
                     std::vector<ValueType>& vvec) override {
#ifdef HMASSTREE_EXTERNAL_STORAGE
        if (!initialized_ || !mt_) return;
        Masstree::ScanContextGuard guard(&thread_ctx_);
#endif
        mt_->scan(start, end, kvec, vvec);
    }

private:
    HMasstreeWrapper* mt_;

#ifdef HMASSTREE_EXTERNAL_STORAGE
    Masstree::ExternalIndexConfig config_;
    std::unique_ptr<Masstree::ExternalStorageManager> storage_mgr_;
    bool initialized_;
    static inline thread_local Masstree::ScanContext thread_ctx_;
#endif

    DISALLOW_COPY_AND_ASSIGN(HMasstreeExternalIndex);
};
