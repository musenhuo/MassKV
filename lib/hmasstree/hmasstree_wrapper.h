#pragma once

#include <iostream>
#include <random>
#include <vector>
#include <thread>

#include <pthread.h>

#include "config.h"
#include "compiler.hh"

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "string.hh"

#include "db_common.h"

#include <unistd.h>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <regex>

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
#include "node_serializer.h"
#include "node_resolver.h"
#endif

/**
 * @brief 打印当前进程的 DRAM（RSS）占用。
 *
 * 通过读取 `/proc/<pid>/status` 中的 `VmRSS` 字段估算当前进程常驻内存。
 *
 * @note 仅适用于 Linux。
 * @throw std::runtime_error 当 popen 失败时抛出。
 */
static void print_dram_consuption()
{
    auto pid = getpid();
    std::array<char, 128> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(
        popen(("cat /proc/" + std::to_string(pid) + "/status").c_str(), "r"),
        pclose);
    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        std::string result = buffer.data();
        if (result.find("VmRSS") != std::string::npos)
        {
            // std::cout << result << std::endl;
            std::string mem_ocp = std::regex_replace(
                result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
            DEBUG("DRAM consumption: %.3f GB.", stof(mem_ocp) / 1024 / 1024);
            break;
        }
    }
}

class key_unparse_unsigned
{
public:
    /**
     * @brief 将 Masstree 的整型 key 反序列化为可打印字符串。
     *
     * 该函数用于 Masstree 的调试/打印路径（key_unparse_type）。
     *
     * @param key Masstree 内部 key（此处以 uint64_t ikey 为主）。
     * @param buf 输出缓冲区。
     * @param buflen 缓冲区大小。
     * @return 实际写入（或需要写入）的字符数，语义同 snprintf。
     */
    static int unparse_key(Masstree::key<uint64_t> key, char *buf, int buflen)
    {
        return snprintf(buf, buflen, "%" PRIu64, key.ikey());
    }
};

/**
 * @brief FlowKV 对 Masstree 的轻量封装。
 *
 * 该封装将 Masstree 的 `basic_table` + `threadinfo` 生命周期管理隐藏起来，
 * 提供面向 FlowKV 的整数 Key API：Get/Put/Delete/Scan。
 *
 * 约定：
 * - Key 使用 uint64_t，并以字典序（big-endian 比较意义）参与 Masstree 的比较。
 * - Value 也是 uint64_t（FlowKV 内部通常用其承载 ValuePtr/地址/元信息）。
 * - 每个工作线程需通过 `thread_init(tid)` 设置线程 id，以获得对应 threadinfo。
 */
class HMasstreeWrapper
{
public:
    /**
     * @brief 插入/生成 key 的上界（用于压测或特定 workload）。
     *
     * @note 该常量是否生效取决于调用方使用方式。
     */
    static constexpr uint64_t insert_bound = 0xfffff; // 0xffffff;

    /**
     * @brief Masstree 表参数配置。
     *
     * 指定节点宽度、并发与预取策略，以及本工程使用的 value/threadinfo/key 打印策略。
     */
    struct table_params : public Masstree::nodeparams<15, 15>
    {
        /// Masstree 叶子存储的值类型（FlowKV 用 uint64_t 承载 ValuePtr/地址等）。
        typedef uint64_t value_type;
        /// Masstree 打印 value 的策略。
        typedef Masstree::value_print<value_type> value_print_type;
        /// 线程上下文（内存分配器、RCU、统计计数等）。
        typedef threadinfo threadinfo_type;
        /// key 打印/反序列化策略（用于调试输出）。
        typedef key_unparse_unsigned key_unparse_type;
        static constexpr ssize_t print_max_indent_depth = 12;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;
    typedef Masstree::internode<table_params> internode_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    /**
     * @brief Scan 回调：仅收集 value。
     *
     * 语义：从起始 key 开始，按 Masstree 的有序遍历顺序最多收集 cnt 个 value。
     */
    struct Scanner
    {
        /// 需要收集的最大条数。
        const int cnt;
        /// value 输出容器引用。
        std::vector<table_params::value_type> &vec;

        Scanner(int cnt, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v)
        {
            vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV。
         * @param key 当前 key（Masstree::Str 视图）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return 返回 false 表示提前终止扫描；true 表示继续。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
            vec.push_back(val);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    /**
     * @brief Scan 回调：同时收集 key 与 value。
     *
     * 支持 8B (uint64_t) 和 16B (Key16) key 的条件编译。
     */
    struct Scanner2
    {
        const int cnt;
        std::vector<table_params::value_type> &vec;
        std::vector<KeyType> &k_vec;

        Scanner2(int cnt, std::vector<KeyType> &k, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v), k_vec(k)
        {
            vec.reserve(cnt);
            k_vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV。
         * @param key 当前 key（Masstree Str，8B 或 16B）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return 返回 false 表示提前终止扫描；true 表示继续。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
#if defined(FLOWKV_KEY16)
            // 16B key: 从大端序字节串还原 Key16
            Key16 kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            // 8B key: 直接读取 uint64_t
            uint64_t kint = *(uint64_t *)key.data();
#endif
            vec.push_back(val);
            k_vec.emplace_back(kint);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    /**
     * @brief Range Scan 回调：收集 [start, end] 范围内的 key/value。
     *
     * 实现方式：从 start 开始 scan，当 key 超过 end 时终止。
     * 支持 8B (uint64_t) 和 16B (Key16) key 的条件编译。
     */
    struct Scanner3
    {
        /// 扫描终止 key（开区间判断：key > end 则停止）。
        Str end;
        std::vector<table_params::value_type> &vec;
        std::vector<KeyType> &k_vec;

        Scanner3(Str end_key, std::vector<KeyType> &k, std::vector<table_params::value_type> &v)
            : end(end_key), vec(v), k_vec(k)
        {
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV，并根据 end key 决定是否停止。
         * @param key 当前 key（Masstree Str，8B 或 16B）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return key 超过 end 时返回 false 终止，否则返回 true。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
#if defined(FLOWKV_KEY16)
            // 16B key: 从大端序字节串还原 Key16
            Key16 kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            // 8B key: 直接读取 uint64_t
            uint64_t kint = *(uint64_t *)key.data();
#endif
            if (key > end)
            {
                return false;
            }
            vec.push_back(val);
            k_vec.emplace_back(kint);
            return true;
        }
    };

    // static thread_local typename table_params::threadinfo_type *ti;

    /**
     * @brief 当前线程的逻辑 id（1..64），用于索引 `tis[]`。
     *
     * 约定：0 号 slot 保留给 TI_MAIN（初始化/管理线程）。
     */
    static thread_local int thread_id;

    /**
     * @brief 线程上下文数组。
     *
     * `tis[tid]` 由 `get_ti()` 懒加载创建；析构时统一释放。
     */
    typename table_params::threadinfo_type *tis[65];

    /**
     * @brief 构造函数：初始化 threadinfo 数组并初始化 Masstree 表。
     */
    HMasstreeWrapper()
    {
        for (int i = 0; i < 65; i++)
        {
            tis[i] = nullptr;
        }
#ifndef HMASSTREE_EXTERNAL_STORAGE
        // 非外存模式下立即初始化表
        this->table_init();
#endif
        // 外存模式下延迟初始化，在 thread_init_external() 后调用
    }

    /**
     * @brief 析构函数：释放所有已创建的 threadinfo。
     */
    ~HMasstreeWrapper()
    {
        // printf("before free ti\n");
        // print_dram_consuption();
        if (tis[0] != nullptr) {
            // Ensure tree nodes (and their ksuffix malloc allocations) enter
            // the reclaim path before threadinfo pools are released.
            table_.destroy(*tis[0]);
        }
        for (int i = 0; i < 65; i++)
        {
            if (tis[i] != nullptr)
            {
                threadinfo::free_ti(tis[i]);
            }
        }
#ifdef HMASSTREE_EXTERNAL_STORAGE
        // 清理ScanContext
        for (int i = 0; i < 65; i++) {
            if (scan_contexts_[i] != nullptr) {
                delete scan_contexts_[i];
                scan_contexts_[i] = nullptr;
            }
        }
        // 清理NodeResolver
        for (int i = 0; i < 65; i++) {
            if (node_resolvers_[i] != nullptr) {
                delete node_resolvers_[i];
                node_resolvers_[i] = nullptr;
            }
        }
        // 关闭外存管理器
        if (ext_storage_ != nullptr) {
            ext_storage_->shutdown();
            delete ext_storage_;
            ext_storage_ = nullptr;
        }
#endif
        // print_dram_consuption();
        // printf("after free ti\n");
    }

    void table_init()
    {

        if (tis[0] == nullptr)
        {
            tis[0] = threadinfo::make(threadinfo::TI_MAIN, -1);
        }
        table_params::threadinfo_type *ti = tis[0];
        table_.initialize(*ti);
        key_gen_ = 0;
    }

    /** @brief 重置内部 key 生成器（若上层使用 key_gen_）。 */
    void keygen_reset()
    {
        key_gen_ = 0;
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    /**
     * @brief 初始化外存存储模式
     * @param config 外存配置
     * @return 是否成功
     */
    bool init_external_storage(const Masstree::ExternalIndexConfig& config = 
                               Masstree::ExternalIndexConfig::default_config())
    {
        if (ext_storage_ != nullptr) {
            return true;  // 已初始化
        }
        ext_storage_ = new Masstree::ExternalStorageManager(config);
        if (!ext_storage_->initialize()) {
            return false;
        }
        
        // Set up eviction callback to notify all NodeResolvers when pages are evicted
        // This ensures cached node pointers are invalidated before pages are freed
        ext_storage_->cache()->set_evict_callback(
            [this](uint64_t page_id, const Masstree::PackedPage* page_ptr) {
                // Iterate through all thread-local NodeResolvers
                for (int i = 0; i <= 64; ++i) {
                    if (node_resolvers_[i] != nullptr) {
                        node_resolvers_[i]->on_page_evicted(page_id, page_ptr);
                    }
                }
            });
        
        return true;
    }
    
    /**
     * @brief 检查外存是否已初始化
     */
    bool is_external_storage_enabled() const {
        return ext_storage_ != nullptr && ext_storage_->is_initialized();
    }
    
    /**
     * @brief 获取外存管理器
     */
    Masstree::ExternalStorageManager* get_external_storage() {
        return ext_storage_;
    }
    
    /**
     * @brief 刷新所有脏页到磁盘
     */
    void flush_external_storage() {
        if (ext_storage_) {
            ext_storage_->flush(true);
        }
    }
    
    /**
     * @brief 持久化树结构（root handle）到存储
     * @return 是否成功
     */
    bool persist_tree_structure() {
        if (!ext_storage_ || !ext_storage_->is_initialized()) {
            return false;
        }
        // Use fix_root() to get the actual current root (in case of splits)
        auto* fixed_root = table_.fix_root();
        
        auto* root = fixed_root;
        if (!root) {
            return false;
        }
        
        Masstree::NodeHandle root_h;
        if (root->isleaf()) {
            root_h = static_cast<leaf_type*>(root)->self_handle_;
        } else {
            root_h = static_cast<internode_type*>(root)->self_handle_;
        }
        
        ext_storage_->storage()->set_root_handle(root_h);
        ext_storage_->storage()->persist_metadata();
        ext_storage_->storage()->sync();
        return true;
    }
    
    /**
     * @brief 检查存储中是否有保存的树结构
     */
    bool has_persisted_tree() const {
        if (!ext_storage_ || !ext_storage_->is_initialized()) {
            return false;
        }
        return ext_storage_->storage()->has_root_handle();
    }
    
    /**
     * @brief 获取当前内存中的 root handle
     */
    Masstree::NodeHandle get_current_root_handle() const {
        return table_.root_handle();
    }
    
    /**
     * @brief 获取存储中保存的 root handle
     */
    Masstree::NodeHandle get_persisted_root_handle() const {
        if (!ext_storage_ || !ext_storage_->is_initialized()) {
            return Masstree::NodeHandle::null();
        }
        return ext_storage_->storage()->get_root_handle();
    }
    
    /**
     * @brief 打印外存统计信息
     */
    void print_external_stats() {
        if (ext_storage_) {
            ext_storage_->print_stats();
        }
    }
    
    /**
     * @brief 序列化所有节点到存储
     * 
     * 遍历整个树，将所有节点序列化到各自的存储槽中
     * @return 序列化的节点数量
     */
    size_t serialize_all_nodes() {
        if (!ext_storage_ || !ext_storage_->is_initialized()) {
            return 0;
        }
        
        table_params::threadinfo_type* ti = get_ti();
        size_t count = 0;
        
        // Get root node - use fix_root() to try to get the actual root
        auto* root = table_.fix_root();
        if (!root) {
            return 0;
        }
        
        // If root is still a leaf with is_root() flag, try to find the real root
        // by checking if there's an internode parent in any split path
        if (root->isleaf()) {
            auto* leaf = static_cast<leaf_type*>(root);
            // Check if this leaf has parent - if so, traverse up
            node_type* parent = leaf->parent();
            while (parent && parent != root) {
                root = parent;
                if (root->isleaf()) {
                    parent = static_cast<leaf_type*>(root)->parent();
                } else {
                    parent = static_cast<internode_type*>(root)->parent_;
                }
            }
        }
        
        // Serialize recursively
        count = serialize_node_recursive(root, ti);
        
        // Persist metadata (including root handle)
        persist_tree_structure();
        
        // Flush all dirty pages
        flush_external_storage();
        
        return count;
    }
    
    /**
     * @brief 从存储恢复树结构（冷启动）
     * 
     * 从存储加载根节点句柄，设置树结构。
     * 只加载根节点，其他节点按需加载（On-demand loading）。
     * @return 是否成功恢复
     */
    bool restore_from_storage() {
        if (!ext_storage_ || !ext_storage_->is_initialized()) {
            return false;
        }
        
        // Check if there's a persisted tree
        if (!has_persisted_tree()) {
            return false;
        }
        
        Masstree::NodeHandle root_h = get_persisted_root_handle();
        if (!root_h.is_valid()) {
            return false;
        }
        
        // Load root node from storage (only root, children loaded on-demand)
        table_params::threadinfo_type* ti = get_ti();
        
        auto* root = load_node_from_storage(root_h, ti);
        if (!root) {
            return false;
        }
        
        // Mark root node as root (sets root_bit in version)
        root->mark_root();
        
        // NOTE: Do NOT recursively load all children!
        // Children will be loaded on-demand through ScanContext::resolve_node()
        // This enables memory-constrained operation where only accessed nodes
        // are kept in memory (via LRU cache).
        
        // Set as tree root
        table_.set_root(root);
        table_.set_root_handle(root_h);
        
        return true;
    }
    
    /**
     * @brief 从存储完全恢复树结构（预加载所有节点）
     * 
     * 与 restore_from_storage() 不同，这会加载整棵树到内存。
     * 适用于内存充足、需要最快访问速度的场景。
     * @return 是否成功恢复
     */
    bool restore_from_storage_full() {
        if (!restore_from_storage()) {
            return false;
        }
        
        // Recursively load all children into memory
        table_params::threadinfo_type* ti = get_ti();
        load_children_recursive(table_.root(), ti);
        
        return true;
    }
    
    /**
     * @brief 递归加载子节点
     */
    size_t load_children_recursive(typename table_type::node_type* node,
                                   table_params::threadinfo_type* ti) {
        if (!node) return 0;
        
        size_t count = 0;
        
        if (!node->isleaf()) {
            // Internode - load all children
            auto* inode = static_cast<internode_type*>(node);
            for (int i = 0; i <= inode->nkeys_; ++i) {
                Masstree::NodeHandle child_h = inode->child_handles_[i];
                if (child_h.is_valid() && inode->child_[i] == nullptr) {
                    auto* child = load_node_from_storage(child_h, ti);
                    if (child) {
                        inode->child_[i] = child;
                        child->set_parent(inode);
                        count++;
                        // Recursively load grandchildren
                        count += load_children_recursive(child, ti);
                    }
                }
            }
        } else {
            // Leaf - check for layer pointers (nested trees)
            auto* leaf = static_cast<leaf_type*>(node);
            
            // Skip loading next/prev for now to avoid potential infinite loops
            // The linked list will be restored lazily if needed
            
            // Load layer pointers (nested trees)
            for (int i = 0; i < leaf_type::width; ++i) {
                if (leaf->is_layer(i)) {
                    // Get layer handle from lv_[i] (stored as raw bits)
                    // The serializer stored layer_handle in lv[i].raw
                    Masstree::NodeHandle layer_h = 
                        Masstree::NodeHandle::from_raw(
                            reinterpret_cast<uint64_t>(leaf->lv_[i].value()));
                    
                    if (layer_h.is_valid()) {
                        auto* layer_root = load_node_from_storage(layer_h, ti);
                        if (layer_root) {
                            // Set the layer pointer
                            leaf->lv_[i] = typename leaf_type::leafvalue_type(layer_root);
                            // Mark layer root as root (sets root_bit)
                            layer_root->mark_root();
                            count++;
                            // Recursively load layer's children
                            count += load_children_recursive(layer_root, ti);
                        }
                    }
                }
            }
        }
        
        return count;
    }
    
    /**
     * @brief 从存储加载单个节点
     */
    typename table_type::node_type* load_node_from_storage(
            Masstree::NodeHandle handle, 
            table_params::threadinfo_type* ti) {
        if (!ext_storage_ || handle.is_null()) {
            return nullptr;
        }
        
        // Get page from cache (loads from storage if needed)
        Masstree::CachedPage* page = ext_storage_->cache()->get_or_load(handle, true);
        if (!page || !page->page_ptr) {
            return nullptr;
        }
        
        uint8_t slot = handle.slot_index();
        if (!page->page_ptr->slot_allocated(slot)) {
            page->unpin();
            return nullptr;
        }
        
        void* slot_data = page->page_ptr->get_slot_ptr(slot);
        
        // Use page's slot type to determine node type (authoritative source)
        uint8_t page_slot_type = page->page_ptr->header.slot_types[slot];
        
        typename table_type::node_type* node = nullptr;
        
        // Use page's slot type instead of handle type (handle type may be stale)
        if (page_slot_type == static_cast<uint8_t>(Masstree::NodeType::LEAF)) {
            // Deserialize leaf
            auto* leaf = deserialize_leaf_node(slot_data, ti);
            node = leaf;
        } else {
            // Deserialize internode  
            auto* inode = deserialize_internode_node(slot_data, ti);
            node = inode;
        }
        
        page->unpin();
        return node;
    }

private:
    /**
     * @brief 递归序列化节点
     */
    size_t serialize_node_recursive(typename table_type::node_type* node,
                                    table_params::threadinfo_type* ti) {
        if (!node) return 0;
        
        static size_t leaf_count = 0, internode_count = 0;
        size_t count = 0;
        
        if (node->isleaf()) {
            // Serialize leaf
            auto* leaf = static_cast<leaf_type*>(node);
            if (serialize_leaf_to_storage(leaf)) {
                count = 1;
                leaf_count++;
            }
            
            // Check for layer pointers (nested trees)
            for (int i = 0; i < table_type::leaf_type::width; ++i) {
                if (leaf->is_layer(i)) {
                    auto* layer = leaf->lv_[i].layer();
                    if (layer) {
                        count += serialize_node_recursive(layer, ti);
                    }
                }
            }
        } else {
            // Serialize internode
            auto* inode = static_cast<internode_type*>(node);
            if (serialize_internode_to_storage(inode)) {
                count = 1;
                internode_count++;
            }
            
            // Recursively serialize children
            for (int i = 0; i <= inode->nkeys_; ++i) {
                auto* child = inode->child_[i];
                if (child) {
                    count += serialize_node_recursive(child, ti);
                }
            }
        }
        
        return count;
    }
    
    /**
     * @brief 序列化 leaf 节点到存储
     */
    bool serialize_leaf_to_storage(leaf_type* leaf) {
        if (!leaf || leaf->self_handle_.is_null()) {
            return false;
        }
        
        Masstree::NodeHandle handle = leaf->self_handle_;
        Masstree::CachedPage* page = ext_storage_->cache()->get_or_load(handle, true);
        if (!page || !page->page_ptr) {
            std::cerr << "[serialize_leaf] Failed to get page for handle: " 
                      << handle.page_id() << ":" << (int)handle.slot_index() << std::endl;
            return false;
        }
        
        uint8_t slot = handle.slot_index();
        void* slot_data = page->page_ptr->get_slot_ptr(slot);
        
        // Serialize leaf to slot
        size_t bytes = Masstree::serialize_leaf(leaf, slot_data);
        
        if (bytes > 0) {
            page->page_ptr->allocate_slot(slot, Masstree::NodeType::LEAF);
            page->mark_dirty();
            
            // Debug: verify slot type was set
            uint8_t set_type = page->page_ptr->header.slot_types[slot];
            if (set_type != static_cast<uint8_t>(Masstree::NodeType::LEAF)) {
                std::cerr << "[serialize_leaf] ERROR: slot_type not set! Expected 1, got " 
                          << (int)set_type << std::endl;
            }
        }
        
        page->unpin();
        return bytes > 0;
    }
    
    /**
     * @brief 序列化 internode 到存储
     */
    bool serialize_internode_to_storage(internode_type* inode) {
        if (!inode || inode->self_handle_.is_null()) {
            return false;
        }
        
        // First, update child handles
        for (int i = 0; i <= inode->nkeys_; ++i) {
            auto* child = inode->child_[i];
            if (child) {
                if (child->isleaf()) {
                    inode->child_handles_[i] = 
                        static_cast<leaf_type*>(child)->self_handle_;
                } else {
                    inode->child_handles_[i] = 
                        static_cast<internode_type*>(child)->self_handle_;
                }
            }
        }
        
        Masstree::NodeHandle handle = inode->self_handle_;
        Masstree::CachedPage* page = ext_storage_->cache()->get_or_load(handle, true);
        if (!page || !page->page_ptr) {
            return false;
        }
        
        uint8_t slot = handle.slot_index();
        void* slot_data = page->page_ptr->get_slot_ptr(slot);
        
        // Serialize internode to slot
        size_t bytes = Masstree::serialize_internode(inode, slot_data);
        
        if (bytes > 0) {
            page->page_ptr->allocate_slot(slot, Masstree::NodeType::INTERNODE);
            page->mark_dirty();
        }
        
        page->unpin();
        return bytes > 0;
    }
    
    /**
     * @brief 从存储反序列化 leaf 节点
     */
    leaf_type* deserialize_leaf_node(
            const void* slot_data, 
            table_params::threadinfo_type* ti) {
        // Allocate new leaf
        size_t sz = (sizeof(leaf_type) + 63) & ~size_t(63);
        void* ptr = ti->pool_allocate(sz, memtag_masstree_leaf);
        auto* leaf = new(ptr) leaf_type(
            sz, typename table_params::phantom_epoch_type());
        
        // Deserialize
        if (!Masstree::deserialize_leaf(slot_data, leaf)) {
            leaf->deallocate(*ti);
            return nullptr;
        }
        
        // Initialize pointers to null (will be resolved on demand)
        leaf->next_.ptr = nullptr;
        leaf->prev_ = nullptr;
        leaf->parent_ = nullptr;
        
        return leaf;
    }
    
    /**
     * @brief 从存储反序列化 internode
     */
    internode_type* deserialize_internode_node(
            const void* slot_data,
            table_params::threadinfo_type* ti) {
        const Masstree::ExternalInternodeLayout* layout = 
            static_cast<const Masstree::ExternalInternodeLayout*>(slot_data);
        
        // Allocate new internode
        void* ptr = ti->pool_allocate(sizeof(internode_type),
                                      memtag_masstree_internode);
        auto* inode = new(ptr) internode_type(layout->height);
        
        // Deserialize
        if (!Masstree::deserialize_internode(slot_data, inode)) {
            inode->deallocate(*ti);
            return nullptr;
        }
        
        // Initialize child pointers to null (will be resolved on demand)
        for (int i = 0; i <= inode->nkeys_; ++i) {
            inode->child_[i] = nullptr;
        }
        inode->parent_ = nullptr;
        
        return inode;
    }
    
public:
#endif

    /**
     * @brief 初始化当前线程的 Masstree 线程 id。
     * @param tid 线程 id，范围 1..64。
     */
    static void thread_init(int tid)
    {
        assert(tid > 0 && tid <= 64);
        thread_id = tid;
    }

#ifdef HMASSTREE_EXTERNAL_STORAGE
    /**
     * @brief 初始化当前线程，并设置外存上下文
     * @param tid 线程 id，范围 1..64
     * 
     * 外存模式下应使用此方法而非静态的thread_init()
     */
    void thread_init_external(int tid)
    {
        assert(tid > 0 && tid <= 64);
        thread_id = tid;
        
        if (ext_storage_ && ext_storage_->is_initialized()) {
            // 设置线程本地的 IndexStorage 用于自动分配节点 handle
            Masstree::IndexStorageRegistry::set(ext_storage_->storage());
            
            // 设置线程本地的 NodeCache 用于在节点分配时立即设置 slot bitmap
            Masstree::NodeCacheRegistry::set(ext_storage_->cache());
            
            // 创建并设置线程本地的ScanContext
            if (scan_contexts_[tid] == nullptr) {
                scan_contexts_[tid] = new Masstree::ScanContext(ext_storage_->cache());
            }
            Masstree::ScanContextRegistry::set(scan_contexts_[tid]);
            
            // 创建并设置线程本地的NodeResolver (用于按需加载)
            if (node_resolvers_[tid] == nullptr) {
                node_resolvers_[tid] = new Masstree::NodeResolver<table_params>(ext_storage_->cache());
            }
            Masstree::NodeResolverRegistry<table_params>::set(node_resolvers_[tid]);
            
            // 延迟初始化表（在 IndexStorageRegistry 设置后）
            if (!table_initialized_) {
                this->table_init();
                table_initialized_ = true;
            }
        }
    }
#endif

    /**
     * @brief 获取当前线程的 threadinfo（懒加载）。
     * @return 当前线程对应的 threadinfo 指针。
     */
    inline table_params::threadinfo_type *get_ti()
    {
        assert(thread_id >= 0 && thread_id <= 64);
        if (unlikely(tis[thread_id] == nullptr))
        {
            tis[thread_id] = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
        }
        return tis[thread_id];
    }

    /**
     * @brief 插入或更新（upsert）一条记录。
     *
     * 若 key 已存在，会通过 `le_helper.old_val` 返回旧值。
     * 新值通过 `le_helper.new_val` 写入。
     *
     * @param int_key 业务侧 KeyType key。
     * @param le_helper 写入辅助信息（新值/旧值等）。
     */
    void insert(KeyType int_key, ValueHelper &le_helper)
    {
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (found)
        {
            le_helper.old_val = lp.value();
        }
        lp.value() = le_helper.new_val;
        fence();
        lp.finish(1, *ti);
    }

    /**
     * @brief 带校验的插入/更新：按 LSN 单调性决定是否覆盖旧值。
     *
     * 仅当新值的 LSN >= 旧值的 LSN 时，才会覆盖。
     *
     * @param int_key 业务侧 KeyType key。
     * @param le_helper 写入辅助信息（new_val 内含 ValuePtr/lsn 语义）。
     */
    void insert_validate(KeyType int_key, ValueHelper &le_helper)
    {
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (unlikely(found))
        {
            // le_helper.old_val = lp.value();
            uint64_t new_lsn = ((ValuePtr*)(&le_helper.new_val))->detail_.lsn;
            uint64_t old_lsn = ((ValuePtr*)(&lp.value()))->detail_.lsn;
            if (new_lsn >= old_lsn)
                lp.value() = le_helper.new_val;
        }
        else
        {
            lp.value() = le_helper.new_val;
        }
        fence();
        lp.finish(1, *ti);
    }

    /**
     * @brief 查询 key。
     * @param int_key 业务侧 KeyType key。
     * @param value 输出：若 found 则写入对应 value。
     * @return 是否找到。
     */
    bool search(KeyType int_key, uint64_t &value)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        bool found = table_.get(key, value, *ti);
        return found;
    }

    /**
     * @brief 从 int_key（含）开始扫描，最多返回 cnt 个 value。
     * @param int_key 起始 KeyType key。
     * @param cnt 最大返回条数。
     * @param vec 输出 value 列表。
     */
    void scan(KeyType int_key, int cnt, std::vector<uint64_t> &vec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        Scanner scanner(cnt, vec);
        table_.scan(key, true, scanner, *ti);
    }

    /**
     * @brief 从 int_key（含）开始扫描，最多返回 cnt 个 key/value。
     * @param int_key 起始 KeyType key。
     * @param cnt 最大返回条数。
     * @param kvec 输出 key 列表（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出 value 列表。
     */
    void scan(KeyType int_key, int cnt, std::vector<KeyType> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        Scanner2 scanner(cnt, kvec, vvec);
        table_.scan(key, true, scanner, *ti);
    }

    /**
     * @brief 扫描范围 [start, end] 内的 key/value（按有序遍历）。
     *
     * 通过从 start 开始 scan，当 key > end 时停止。
     *
     * @param start 起始 KeyType key（包含）。
     * @param end 结束 KeyType key（包含）。
     * @param kvec 输出 key 列表（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出 value 列表。
     */
    void scan(KeyType start, KeyType end, std::vector<KeyType> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t start_buf[16], end_buf[16];
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#else
        uint64_t start_buf, end_buf;
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#endif
        Scanner3 scanner(end_str, kvec, vvec);
        table_.scan(start_str, true, scanner, *ti);
    }

    /**
     * @brief 删除指定 key。
     *
     * 当前实现会执行一次 `find_locked + finish(-1)`。
     * @param int_key 待删除 KeyType key。
     * @return 是否删除成功。
     * @note 目前无论 key 是否存在都返回 true（如需严格语义，可改为返回 found）。
     */
    bool remove(KeyType int_key)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        lp.finish(-1, *ti);
        return true;
    }

private:
    /// Masstree 主表对象（包含 root 指针等）。
    table_type table_;

    /// 简单的 key 生成器（部分 workload/测试可能使用）。
    uint64_t key_gen_;

    /// 控制后台线程/打印的全局标志（具体语义取决于实现文件）。
    static bool stopping;

    /// 打印节流或打印次数控制（具体语义取决于实现文件）。
    static uint32_t printing;

#ifdef HMASSTREE_EXTERNAL_STORAGE
    /// 外存存储管理器
    Masstree::ExternalStorageManager* ext_storage_ = nullptr;
    
    /// 线程本地ScanContext指针数组
    Masstree::ScanContext* scan_contexts_[65] = {nullptr};
    
    /// 线程本地NodeResolver指针数组
    Masstree::NodeResolver<table_params>* node_resolvers_[65] = {nullptr};
    
    /// 表是否已初始化（外存模式下延迟初始化）
    bool table_initialized_ = false;
#endif

    /**
     * @brief 将 uint64 key 编码为 Masstree 使用的 `Str`（8B 版本）。
     *
     * Masstree 的比较是对 `Str` 做字典序（以及内部 ikey 的可比较编码）。
     *
     * @param int_key 输入 key。
     * @param key_buf 输出：保存编码后的字节序列（其地址作为 Str.data()）。
     * @return 指向 key_buf 的 Str 视图。
     */
    static inline Str make_key(uint64_t int_key, uint64_t &key_buf)
    {
		// Becasuse masstree use dictionary order to compare, we don't need to bswap.
        // key_buf = __builtin_bswap64(int_key);
        key_buf = int_key;
        return Str((const char *)&key_buf, sizeof(key_buf));
    }

#if defined(FLOWKV_KEY16)
    /**
     * @brief 将 Key16 编码为 Masstree 使用的 `Str`（16B 版本）。
     *
     * Key16 以大端序编码到 16 字节 buffer，使得 Masstree 的字典序比较等价于数值序。
     *
     * @param int_key 输入 Key16。
     * @param key_buf 输出：16 字节 buffer。
     * @return 指向 key_buf 的 Str 视图。
     */
    static inline Str make_key(const Key16 &int_key, uint8_t *key_buf)
    {
        int_key.ToBigEndianBytes(key_buf);
        return Str((const char *)key_buf, 16);
    }
#endif
};
