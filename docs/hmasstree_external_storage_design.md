# H-Masstree 外存索引改造详细设计方案

## 版本信息

| 版本 | 日期 | 作者 | 描述 |
|------|------|------|------|
| 1.0 | 2026-01-29 | - | 初始设计 |
| 1.1 | 2026-01-29 | - | 添加 Pointer Swizzling 对比与优化设计 |
| 1.2 | 2026-01-29 | - | 添加核心技术挑战与解决方案 |

---

## 1. 设计目标

本方案的核心目标是将 H-Masstree 从纯内存索引改造为支持外存扩展的混合索引结构，同时保持其核心优势。

### 1.1 目标一：保留 Masstree 前缀压缩特性

- **保持分层 Trie + B+-Tree 混合结构**：长 key 按 8 字节分段，共享前缀的 key 复用上层节点
- **节点内部布局不变**：`ikey0_[]`、`keylenx_[]`、`lv_[]`、`permutation_` 等字段保持原有设计
- **Layer 机制完全保留**：支持 key 超过 8 字节时自动创建下层树

### 1.2 目标二：保留乐观并发控制，提供高并发读性能

- **读路径无锁**：缓存命中时，读操作仅依赖 `nodeversion` 的 stable/has_changed 机制
- **写锁粒度在节点级别**：仅锁住目标 leaf/internode，不阻塞其他节点的读写
- **最小化缓存层的锁开销**：使用分片锁 + 快速路径优化

### 1.3 目标三：内存使用可控，索引扩展到外存

- **可配置内存上限**：索引缓存占用内存可设定阈值（如 16GB）
- **自动淘汰冷节点**：基于访问频率的淘汰策略，保持热点数据在内存
- **透明的按需加载**：访问不在缓存中的节点时，自动从 SSD 加载

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          应用层 API                                      │
│         HMasstreeIndex::Get / Put / Delete / Scan                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       HMasstreeWrapper                                   │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  • insert() / search() / remove() / scan()                      │    │
│  │  • 内部使用 tcursor / unlocked_tcursor 执行操作                  │    │
│  │  • threadinfo 管理线程本地状态                                   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     节点寻址层 (Node Addressing Layer)                   │
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────────────┐    │
│  │  NodeHandle   │    │ HandleTable   │    │ handle.resolve()      │    │
│  │  (逻辑地址)   │ ─► │ (映射表)      │ ─► │ (解析为内存指针)      │    │
│  │  8 字节编码   │    │ O(1) 查找     │    │ 触发缓存/加载         │    │
│  └───────────────┘    └───────────────┘    └───────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        NodeCache (节点缓存层)                            │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  核心职责:                                                       │    │
│  │  • 管理内存中的节点缓存                                          │    │
│  │  • 实现 LRU/Clock 淘汰策略                                       │    │
│  │  • 跟踪脏页并触发回写                                            │    │
│  │  • 控制内存使用量在配置阈值内                                    │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │  数据结构:                                                       │    │
│  │  • ShardedHashMap<NodeHandle, CachedNode*>  (分片哈希表)         │    │
│  │  • ClockBuffer / LRUList                     (淘汰结构)          │    │
│  │  • DirtyNodeQueue                            (脏页队列)          │    │
│  │  • MemoryTracker                             (内存计数器)        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   IndexStorageManager (索引存储管理层)                   │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  核心职责:                                                       │    │
│  │  • 管理索引节点在 SSD 上的存储布局                               │    │
│  │  • 分配/回收节点存储空间                                         │    │
│  │  • 执行节点的读写 I/O                                            │    │
│  ├─────────────────────────────────────────────────────────────────┤    │
│  │  存储格式:                                                       │    │
│  │  • 基于 SegmentAllocator 的段式管理                              │    │
│  │  • 每段 4MB，内部按节点大小划分页                                │    │
│  │  • 支持 Direct I/O (可选)                                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              SSD 存储                                    │
│               pool_path/index/segment_*.idx                             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 核心数据结构设计

### 3.1 NodeHandle: 节点逻辑地址

`NodeHandle` 是外存索引的核心抽象，用于替代原有的直接内存指针。

```cpp
/**
 * @brief 节点逻辑句柄，替代原始指针 node_base<P>*
 * 
 * 编码格式 (64 bits):
 *   [63]     : valid bit (0=null handle)
 *   [62:60]  : node_type (0=leaf, 1=internode, 2=layer_root, ...)
 *   [59:40]  : segment_id (20 bits, 支持 ~100万个段)
 *   [39:20]  : page_offset (20 bits, 段内偏移，以节点大小为单位)
 *   [19:0]   : generation (20 bits, 用于检测 ABA 问题)
 */
struct NodeHandle {
    uint64_t data_;
    
    // 位域定义
    static constexpr uint64_t VALID_MASK       = 1ULL << 63;
    static constexpr uint64_t TYPE_SHIFT       = 60;
    static constexpr uint64_t TYPE_MASK        = 0x7ULL << TYPE_SHIFT;
    static constexpr uint64_t SEGMENT_SHIFT    = 40;
    static constexpr uint64_t SEGMENT_MASK     = 0xFFFFFULL << SEGMENT_SHIFT;
    static constexpr uint64_t OFFSET_SHIFT     = 20;
    static constexpr uint64_t OFFSET_MASK      = 0xFFFFFULL << OFFSET_SHIFT;
    static constexpr uint64_t GENERATION_MASK  = 0xFFFFFULL;
    
    // 构造函数
    NodeHandle() : data_(0) {}
    
    NodeHandle(uint32_t segment_id, uint32_t offset, uint8_t type, uint32_t gen)
        : data_(VALID_MASK 
                | ((uint64_t)type << TYPE_SHIFT)
                | ((uint64_t)segment_id << SEGMENT_SHIFT)
                | ((uint64_t)offset << OFFSET_SHIFT)
                | gen) {}
    
    // 属性访问
    bool is_null() const { return (data_ & VALID_MASK) == 0; }
    bool is_valid() const { return !is_null(); }
    uint8_t node_type() const { return (data_ & TYPE_MASK) >> TYPE_SHIFT; }
    uint32_t segment_id() const { return (data_ & SEGMENT_MASK) >> SEGMENT_SHIFT; }
    uint32_t page_offset() const { return (data_ & OFFSET_MASK) >> OFFSET_SHIFT; }
    uint32_t generation() const { return data_ & GENERATION_MASK; }
    
    // 用于哈希表
    uint64_t hash() const { return data_; }
    bool operator==(const NodeHandle& other) const { return data_ == other.data_; }
    
    // 创建空句柄
    static NodeHandle Null() { return NodeHandle(); }
    
    // 节点类型常量
    enum NodeType : uint8_t {
        TYPE_LEAF      = 0,
        TYPE_INTERNODE = 1,
        TYPE_LAYER_ROOT = 2,
    };
};
```

### 3.2 CachedNode: 缓存节点元数据

```cpp
/**
 * @brief 缓存中的节点包装结构
 * 
 * 管理节点的内存、状态、引用计数等元信息
 */
struct CachedNode {
    // 指向实际节点内存的指针
    void* node_ptr;
    
    // 节点句柄 (用于回写时定位)
    NodeHandle handle;
    
    // 引用计数：防止正在使用的节点被驱逐
    std::atomic<uint32_t> pin_count{0};
    
    // 访问计数：用于淘汰决策
    std::atomic<uint64_t> access_count{0};
    
    // 最近访问时间戳
    std::atomic<uint64_t> last_access_time{0};
    
    // 状态标记
    std::atomic<bool> is_dirty{false};      // 是否有未持久化的修改
    std::atomic<bool> is_loading{false};    // 是否正在加载中
    std::atomic<bool> is_evicting{false};   // 是否正在被驱逐
    
    // Clock 算法的引用位
    std::atomic<uint8_t> reference_bit{0};
    
    // 节点大小 (用于内存计数)
    size_t node_size;
    
    // 链表指针 (用于 LRU/Clock)
    CachedNode* lru_prev{nullptr};
    CachedNode* lru_next{nullptr};
    
    // 辅助方法
    template<typename P>
    Masstree::node_base<P>* as_node() const {
        return reinterpret_cast<Masstree::node_base<P>*>(node_ptr);
    }
    
    void touch() {
        access_count.fetch_add(1, std::memory_order_relaxed);
        last_access_time.store(current_timestamp(), std::memory_order_relaxed);
        reference_bit.store(1, std::memory_order_relaxed);
    }
    
    bool try_pin() {
        if (is_evicting.load(std::memory_order_acquire)) {
            return false;
        }
        pin_count.fetch_add(1, std::memory_order_acq_rel);
        return true;
    }
    
    void unpin() {
        pin_count.fetch_sub(1, std::memory_order_acq_rel);
    }
};
```

### 3.3 NodeCache: 节点缓存管理器

```cpp
/**
 * @brief 节点缓存管理器
 * 
 * 核心职责:
 * 1. 管理节点在内存中的缓存
 * 2. 实现按需加载 (demand paging)
 * 3. 实现淘汰策略 (Clock/LRU)
 * 4. 控制内存使用量
 */
class NodeCache {
public:
    // 配置参数
    struct Config {
        size_t max_memory_bytes = 16ULL << 30;  // 默认 16GB
        size_t node_page_size = 512;             // 单个节点分配大小
        size_t num_shards = 256;                 // 分片数 (降低锁竞争)
        size_t evict_batch_size = 64;            // 批量驱逐数量
        double evict_trigger_ratio = 0.9;        // 触发驱逐的内存占比
        double evict_target_ratio = 0.8;         // 驱逐目标内存占比
        bool enable_prefetch = true;             // 启用预取
    };
    
private:
    // ============ 分片哈希表 ============
    struct CacheShard {
        SpinLock lock;
        std::unordered_map<uint64_t, CachedNode*> map;  // handle.data_ -> CachedNode*
    };
    std::vector<CacheShard> shards_;
    
    // ============ 淘汰结构 (Clock) ============
    struct ClockBuffer {
        SpinLock lock;
        std::vector<CachedNode*> buffer;
        size_t hand = 0;  // Clock 指针
    };
    ClockBuffer clock_;
    
    // ============ 脏页管理 ============
    struct DirtyQueue {
        SpinLock lock;
        std::deque<CachedNode*> queue;
    };
    DirtyQueue dirty_queue_;
    
    // ============ 内存统计 ============
    std::atomic<size_t> current_memory_bytes_{0};
    std::atomic<size_t> total_nodes_cached_{0};
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
    
    // ============ 配置与依赖 ============
    Config config_;
    IndexStorageManager* storage_;  // 存储管理器
    
public:
    NodeCache(IndexStorageManager* storage, const Config& config);
    ~NodeCache();
    
    // ============ 核心接口 ============
    
    /**
     * @brief 获取节点 (核心方法)
     * 
     * 快速路径：无锁检查缓存命中
     * 慢速路径：加锁 + I/O 加载
     * 
     * @param handle 节点句柄
     * @param ti 线程上下文
     * @return 节点指针 (调用者需要后续调用 Release)
     */
    template<typename P>
    Masstree::node_base<P>* Get(NodeHandle handle, threadinfo& ti);
    
    /**
     * @brief 释放节点 (解除 pin)
     */
    void Release(NodeHandle handle);
    
    /**
     * @brief 分配新节点
     * 
     * @param node_type 节点类型 (leaf/internode)
     * @param node_size 节点大小
     * @return 新节点的句柄
     */
    NodeHandle Allocate(uint8_t node_type, size_t node_size);
    
    /**
     * @brief 标记节点为脏
     */
    void MarkDirty(NodeHandle handle);
    
    /**
     * @brief 释放节点 (删除操作)
     */
    void Free(NodeHandle handle);
    
    // ============ 后台任务接口 ============
    
    /**
     * @brief 刷盘所有脏页
     */
    void Flush();
    
    /**
     * @brief 触发淘汰 (当内存超过阈值)
     */
    void MaybeEvict();
    
    /**
     * @brief 预取节点 (异步)
     */
    void PrefetchAsync(NodeHandle handle);
    
    // ============ 统计接口 ============
    
    size_t GetMemoryUsage() const { return current_memory_bytes_.load(); }
    size_t GetCachedNodeCount() const { return total_nodes_cached_.load(); }
    double GetHitRate() const;
    void PrintStats() const;
    
private:
    // ============ 内部方法 ============
    
    CacheShard& GetShard(NodeHandle handle) {
        return shards_[handle.hash() % config_.num_shards];
    }
    
    CachedNode* LookupLockFree(NodeHandle handle);
    CachedNode* LoadNodeSlow(NodeHandle handle, threadinfo& ti);
    void EvictNodes(size_t target_count);
    CachedNode* FindVictim();
    void WriteBackNode(CachedNode* node);
    void* AllocateNodeMemory(size_t size);
    void FreeNodeMemory(void* ptr, size_t size);
};
```

### 3.4 IndexStorageManager: 索引存储管理

```cpp
/**
 * @brief 索引节点的 SSD 存储管理器
 * 
 * 负责:
 * 1. 索引节点在 SSD 上的空间分配
 * 2. 节点的读写 I/O
 * 3. 与 SegmentAllocator 集成
 */
class IndexStorageManager {
public:
    // 存储格式常量
    static constexpr size_t NODE_PAGE_SIZE = 512;      // 节点页大小
    static constexpr size_t SEGMENT_SIZE = 4ULL << 20; // 4MB 段大小
    static constexpr size_t NODES_PER_SEGMENT = SEGMENT_SIZE / NODE_PAGE_SIZE;
    
    // 段头格式
    struct SegmentHeader {
        uint64_t magic;           // 魔数 0x48494458 ("HIDX")
        uint32_t version;         // 格式版本
        uint32_t node_count;      // 已分配节点数
        uint64_t checksum;        // 校验和
        uint8_t  bitmap[NODES_PER_SEGMENT / 8];  // 分配位图
    };
    
private:
    std::string index_path_;
    int fd_;
    
    // 段分配管理
    SpinLock alloc_lock_;
    std::vector<uint32_t> free_segments_;           // 空闲段列表
    std::unordered_map<uint32_t, SegmentHeader> segment_headers_;
    
    // 节点到段的映射
    std::atomic<uint32_t> next_segment_id_{0};
    std::atomic<uint32_t> current_generation_{0};
    
    // I/O 统计
    std::atomic<uint64_t> read_bytes_{0};
    std::atomic<uint64_t> write_bytes_{0};
    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> write_count_{0};
    
public:
    IndexStorageManager(const std::string& pool_path, bool recover = false);
    ~IndexStorageManager();
    
    /**
     * @brief 分配一个节点存储位置
     * 
     * @param node_type 节点类型
     * @return 新分配的句柄
     */
    NodeHandle AllocateNode(uint8_t node_type);
    
    /**
     * @brief 释放节点存储位置
     */
    void FreeNode(NodeHandle handle);
    
    /**
     * @brief 读取节点
     * 
     * @param handle 节点句柄
     * @param dest 目标内存缓冲区
     * @param size 节点大小
     */
    void ReadNode(NodeHandle handle, void* dest, size_t size);
    
    /**
     * @brief 写入节点
     * 
     * @param handle 节点句柄
     * @param src 源内存缓冲区
     * @param size 节点大小
     */
    void WriteNode(NodeHandle handle, const void* src, size_t size);
    
    /**
     * @brief 批量读取 (用于预取)
     */
    void ReadNodesBatch(const std::vector<NodeHandle>& handles,
                        std::vector<void*>& dests,
                        size_t node_size);
    
    /**
     * @brief 同步元数据
     */
    void SyncMetadata();
    
    /**
     * @brief 恢复
     */
    void Recover();
    
    // 统计
    void PrintIOStats() const;
    
private:
    size_t HandleToOffset(NodeHandle handle) const;
    uint32_t AllocateSegment();
    void FreeSegment(uint32_t segment_id);
};
```

---

## 4. 节点结构修改

### 4.1 修改 internode 中的指针

原始定义 (masstree_struct.hh):
```cpp
template <typename P>
class internode : public node_base<P> {
    // ... 其他字段 ...
    node_base<P>* child_[width + 1];  // 原始：直接指针
    node_base<P>* parent_;            // 原始：直接指针
};
```

修改后:
```cpp
template <typename P>
class internode : public node_base<P> {
    // ... 其他字段 ...
    NodeHandle child_[width + 1];     // 修改为句柄
    NodeHandle parent_;               // 修改为句柄
    
    // 添加解析方法
    node_base<P>* get_child(int p, NodeCache& cache, threadinfo& ti) const {
        if (child_[p].is_null()) return nullptr;
        return cache.Get<P>(child_[p], ti);
    }
    
    void set_child(int p, NodeHandle h) {
        child_[p] = h;
    }
};
```

### 4.2 修改 leaf 中的指针

原始定义:
```cpp
template <typename P>
class leaf : public node_base<P> {
    // ... 其他字段 ...
    union { leaf<P>* ptr; uintptr_t x; } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
};
```

修改后:
```cpp
template <typename P>
class leaf : public node_base<P> {
    // ... 其他字段 ...
    NodeHandle next_;          // 修改为句柄
    NodeHandle prev_;          // 修改为句柄
    NodeHandle parent_;        // 修改为句柄
    
    // 添加解析方法
    leaf<P>* get_next(NodeCache& cache, threadinfo& ti) const {
        if (next_.is_null()) return nullptr;
        return static_cast<leaf<P>*>(cache.Get<P>(next_, ti));
    }
    
    leaf<P>* get_prev(NodeCache& cache, threadinfo& ti) const {
        if (prev_.is_null()) return nullptr;
        return static_cast<leaf<P>*>(cache.Get<P>(prev_, ti));
    }
};
```

### 4.3 修改 node_base 基类

```cpp
template <typename P>
class node_base : public make_nodeversion<P>::type {
public:
    // ... 原有方法 ...
    
    // 新增：获取 parent 的通用方法
    NodeHandle parent_handle() const {
        if (this->isleaf())
            return static_cast<const leaf<P>*>(this)->parent_;
        else
            return static_cast<const internode<P>*>(this)->parent_;
    }
    
    void set_parent_handle(NodeHandle h) {
        if (this->isleaf())
            static_cast<leaf<P>*>(this)->parent_ = h;
        else
            static_cast<internode<P>*>(this)->parent_ = h;
    }
    
    // 新增：节点自身的句柄
    NodeHandle self_handle_;
    
    NodeHandle self_handle() const { return self_handle_; }
    void set_self_handle(NodeHandle h) { self_handle_ = h; }
};
```

---

## 5. 关键操作流程修改

### 5.1 reach_leaf() 修改

这是最核心的改动之一，影响所有读写操作。

```cpp
template <typename P>
leaf<P>* node_base<P>::reach_leaf(const key_type& ka,
                                   nodeversion_type& version,
                                   threadinfo& ti) const
{
    NodeCache* cache = ti.node_cache();
    const node_base<P>* n[2];
    typename node_base<P>::nodeversion_type v[2];
    unsigned sense;
    
    // RAII guard: 自动管理 pin/unpin
    thread_local std::vector<NodeHandle> pinned_nodes;
    struct PinGuard {
        NodeCache* c;
        ~PinGuard() {
            for (auto& h : pinned_nodes) c->Release(h);
            pinned_nodes.clear();
        }
    } guard{cache};

retry:
    sense = 0;
    n[sense] = this;
    
    // 获取稳定的根节点
    while (true) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (v[sense].is_root()) {
            break;
        }
        ti.mark(tc_root_retry);
        
        // 获取 parent 需要通过 cache 解析
        NodeHandle parent_h = n[sense]->parent_handle();
        if (parent_h.is_null()) break;
        n[sense] = cache->Get<P>(parent_h, ti);
        pinned_nodes.push_back(parent_h);
    }

    // 遍历内部节点
    while (!v[sense].isleaf()) {
        const internode<P>* in = static_cast<const internode<P>*>(n[sense]);
        in->prefetch();
        int kp = internode<P>::bound_type::upper(ka, *in);
        
        // 关键修改：通过句柄获取子节点
        NodeHandle child_h = in->child_[kp];
        if (child_h.is_null()) {
            goto retry;
        }
        
        n[sense ^ 1] = cache->Get<P>(child_h, ti);
        pinned_nodes.push_back(child_h);
        
        v[sense ^ 1] = n[sense ^ 1]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
            sense ^= 1;
            continue;
        }

        // 版本变化，需要重试
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
    return const_cast<leaf<P>*>(static_cast<const leaf<P>*>(n[sense]));
}
```

### 5.2 split 操作修改

分裂时需要正确设置新节点的句柄。

```cpp
template <typename P>
int leaf<P>::split_into(leaf<P>* nr, tcursor<P>* ti_cursor,
                         ikey_type& split_ikey, threadinfo& ti)
{
    NodeCache* cache = ti.node_cache();
    
    // 分配新叶子节点
    NodeHandle new_leaf_h = cache->Allocate(NodeHandle::TYPE_LEAF, sizeof(leaf<P>));
    nr = static_cast<leaf<P>*>(cache->Get<P>(new_leaf_h, ti));
    nr->set_self_handle(new_leaf_h);
    
    // ... 原有的数据迁移逻辑 ...
    
    // 设置链表指针 (使用句柄)
    nr->next_ = this->next_;
    nr->prev_ = this->self_handle();
    
    if (!this->next_.is_null()) {
        leaf<P>* old_next = cache->Get<P>(this->next_, ti);
        old_next->prev_ = new_leaf_h;
        cache->MarkDirty(this->next_);
    }
    
    this->next_ = new_leaf_h;
    
    // 标记两个节点都需要回写
    cache->MarkDirty(this->self_handle());
    cache->MarkDirty(new_leaf_h);
    
    return result;
}
```

### 5.3 scan 操作修改

扫描时需要沿着叶子链表遍历。

```cpp
// 在 scan 循环中
while (more_to_scan) {
    leaf<P>* current_leaf = ...;
    
    // 处理当前叶子节点
    for (int i = 0; i < current_leaf->size(); ++i) {
        // 访问 key/value
        if (!scanner.visit_value(...)) {
            return;  // 提前终止
        }
    }
    
    // 移动到下一个叶子
    NodeHandle next_h = current_leaf->next_;
    if (next_h.is_null()) {
        break;  // 到达末尾
    }
    
    // 释放当前节点的 pin，获取下一个节点
    cache->Release(current_leaf->self_handle());
    current_leaf = cache->Get<P>(next_h, ti);
    pinned_nodes.push_back(next_h);
}
```

---

## 6. 缓存层详细实现

### 6.1 Get() 方法实现

```cpp
template<typename P>
Masstree::node_base<P>* NodeCache::Get(NodeHandle handle, threadinfo& ti) {
    if (handle.is_null()) {
        return nullptr;
    }
    
    // ========== 快速路径：无锁缓存查找 ==========
    CacheShard& shard = GetShard(handle);
    
    // 先尝试无锁查找
    CachedNode* cached = LookupLockFree(handle);
    if (likely(cached != nullptr)) {
        // 检查是否可以 pin
        if (likely(cached->try_pin())) {
            cached->touch();
            cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return cached->as_node<P>();
        }
        // 节点正在被驱逐，走慢速路径
    }
    
    // ========== 慢速路径：加锁 + 可能的 I/O ==========
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    return LoadNodeSlow(handle, ti)->as_node<P>();
}

CachedNode* NodeCache::LookupLockFree(NodeHandle handle) {
    CacheShard& shard = GetShard(handle);
    
    // 使用 acquire 语义读取，确保看到完整的 CachedNode
    auto it = shard.map.find(handle.data_);
    if (it != shard.map.end()) {
        CachedNode* node = it->second;
        // 检查节点是否有效且未被驱逐
        if (!node->is_evicting.load(std::memory_order_acquire)) {
            return node;
        }
    }
    return nullptr;
}

CachedNode* NodeCache::LoadNodeSlow(NodeHandle handle, threadinfo& ti) {
    CacheShard& shard = GetShard(handle);
    
    std::lock_guard<SpinLock> lock(shard.lock);
    
    // Double-check: 可能其他线程已经加载
    auto it = shard.map.find(handle.data_);
    if (it != shard.map.end() && !it->second->is_evicting.load()) {
        CachedNode* cached = it->second;
        cached->pin_count.fetch_add(1);
        cached->touch();
        return cached;
    }
    
    // 检查是否需要驱逐
    if (current_memory_bytes_.load() > config_.max_memory_bytes * config_.evict_trigger_ratio) {
        // 释放锁后执行驱逐 (避免持锁时间过长)
        // 这里简化处理，实际可以用后台线程
    }
    
    // 分配内存
    size_t node_size = config_.node_page_size;
    void* mem = AllocateNodeMemory(node_size);
    
    // 从 SSD 读取
    storage_->ReadNode(handle, mem, node_size);
    
    // 创建 CachedNode
    CachedNode* cached = new CachedNode();
    cached->node_ptr = mem;
    cached->handle = handle;
    cached->node_size = node_size;
    cached->pin_count.store(1);  // 初始 pin
    cached->touch();
    
    // 插入缓存
    shard.map[handle.data_] = cached;
    
    // 添加到 Clock 缓冲区
    {
        std::lock_guard<SpinLock> clock_lock(clock_.lock);
        clock_.buffer.push_back(cached);
    }
    
    // 更新统计
    current_memory_bytes_.fetch_add(node_size);
    total_nodes_cached_.fetch_add(1);
    
    return cached;
}
```

### 6.2 淘汰策略实现 (Clock 算法)

```cpp
void NodeCache::MaybeEvict() {
    size_t current = current_memory_bytes_.load();
    if (current <= config_.max_memory_bytes * config_.evict_trigger_ratio) {
        return;  // 内存充足，无需驱逐
    }
    
    size_t target = config_.max_memory_bytes * config_.evict_target_ratio;
    size_t to_free = current - target;
    size_t freed = 0;
    size_t scanned = 0;
    
    std::lock_guard<SpinLock> lock(clock_.lock);
    
    while (freed < to_free && scanned < clock_.buffer.size() * 2) {
        CachedNode* victim = clock_.buffer[clock_.hand];
        scanned++;
        
        // 移动 clock 指针
        clock_.hand = (clock_.hand + 1) % clock_.buffer.size();
        
        // 检查引用位
        if (victim->reference_bit.load() == 1) {
            // 给第二次机会
            victim->reference_bit.store(0);
            continue;
        }
        
        // 检查是否可以驱逐
        if (victim->pin_count.load() > 0) {
            continue;  // 正在使用，跳过
        }
        
        if (victim->is_dirty.load()) {
            // 脏页需要先回写
            WriteBackNode(victim);
        }
        
        // 标记为正在驱逐
        victim->is_evicting.store(true);
        
        // 从 shard map 中移除
        CacheShard& shard = GetShard(victim->handle);
        {
            std::lock_guard<SpinLock> shard_lock(shard.lock);
            shard.map.erase(victim->handle.data_);
        }
        
        // 释放内存
        freed += victim->node_size;
        FreeNodeMemory(victim->node_ptr, victim->node_size);
        delete victim;
        
        // 从 clock buffer 中移除 (惰性标记，实际实现可更高效)
        clock_.buffer[clock_.hand] = nullptr;
    }
    
    current_memory_bytes_.fetch_sub(freed);
    total_nodes_cached_.fetch_sub(freed / config_.node_page_size);
}
```

### 6.3 脏页回写

```cpp
void NodeCache::MarkDirty(NodeHandle handle) {
    CachedNode* cached = LookupLockFree(handle);
    if (cached && !cached->is_dirty.exchange(true)) {
        // 首次标记为脏，加入脏页队列
        std::lock_guard<SpinLock> lock(dirty_queue_.lock);
        dirty_queue_.queue.push_back(cached);
    }
}

void NodeCache::Flush() {
    std::vector<CachedNode*> to_flush;
    
    {
        std::lock_guard<SpinLock> lock(dirty_queue_.lock);
        to_flush.swap(dirty_queue_.queue);
    }
    
    for (CachedNode* node : to_flush) {
        WriteBackNode(node);
    }
}

void NodeCache::WriteBackNode(CachedNode* node) {
    if (!node->is_dirty.load()) {
        return;
    }
    
    // 写回 SSD
    storage_->WriteNode(node->handle, node->node_ptr, node->node_size);
    
    // 清除脏标记
    node->is_dirty.store(false);
}
```

---

## 7. threadinfo 扩展

需要修改 `threadinfo` 以持有 `NodeCache` 引用。

```cpp
class threadinfo {
public:
    // ... 原有成员 ...
    
    // 新增：节点缓存引用
    NodeCache* node_cache() { return node_cache_; }
    void set_node_cache(NodeCache* cache) { node_cache_ = cache; }
    
private:
    // ... 原有成员 ...
    NodeCache* node_cache_{nullptr};
};
```

初始化时:
```cpp
// 在 HMasstreeWrapper 构造中
HMasstreeWrapper::HMasstreeWrapper(const Config& config) {
    // 创建存储管理器
    storage_ = new IndexStorageManager(config.index_path, config.recover);
    
    // 创建节点缓存
    NodeCache::Config cache_config;
    cache_config.max_memory_bytes = config.max_cache_memory;
    cache_ = new NodeCache(storage_, cache_config);
    
    // 初始化 threadinfo 数组，设置 cache 引用
    for (int i = 0; i < 65; i++) {
        if (tis[i] != nullptr) {
            tis[i]->set_node_cache(cache_);
        }
    }
    
    // 初始化表
    table_init();
}
```

---

## 8. 预取策略

### 8.1 子节点预取

在遍历 internode 时，可以预取即将访问的子节点。

```cpp
void NodeCache::PrefetchChildren(const internode<P>* node, int predicted_kp) {
    if (!config_.enable_prefetch) return;
    
    // 预取可能访问的相邻子节点
    for (int offset = -1; offset <= 1; offset++) {
        int kp = predicted_kp + offset;
        if (kp >= 0 && kp <= node->nkeys_) {
            NodeHandle child_h = node->child_[kp];
            if (!child_h.is_null() && !IsCached(child_h)) {
                PrefetchAsync(child_h);
            }
        }
    }
}

void NodeCache::PrefetchAsync(NodeHandle handle) {
    // 提交到后台线程池异步加载
    prefetch_pool_->Submit([this, handle]() {
        CacheShard& shard = GetShard(handle);
        std::lock_guard<SpinLock> lock(shard.lock);
        
        if (shard.map.find(handle.data_) != shard.map.end()) {
            return;  // 已经在缓存中
        }
        
        // 分配并加载
        void* mem = AllocateNodeMemory(config_.node_page_size);
        storage_->ReadNode(handle, mem, config_.node_page_size);
        
        CachedNode* cached = new CachedNode();
        cached->node_ptr = mem;
        cached->handle = handle;
        cached->node_size = config_.node_page_size;
        
        shard.map[handle.data_] = cached;
        
        {
            std::lock_guard<SpinLock> clock_lock(clock_.lock);
            clock_.buffer.push_back(cached);
        }
        
        current_memory_bytes_.fetch_add(config_.node_page_size);
        total_nodes_cached_.fetch_add(1);
    });
}
```

### 8.2 扫描时连续叶子预取

```cpp
void NodeCache::PrefetchLeafChain(NodeHandle start, int count) {
    std::vector<NodeHandle> to_prefetch;
    NodeHandle current = start;
    
    for (int i = 0; i < count && !current.is_null(); i++) {
        if (!IsCached(current)) {
            to_prefetch.push_back(current);
        }
        // 需要读取当前节点以获取 next
        // 这里简化处理，实际可以批量读取
    }
    
    for (auto& h : to_prefetch) {
        PrefetchAsync(h);
    }
}
```

---

## 9. 崩溃恢复

### 9.1 恢复流程

```cpp
void IndexStorageManager::Recover() {
    // 1. 打开索引文件
    fd_ = open(index_path_.c_str(), O_RDWR);
    if (fd_ < 0) {
        // 首次启动，创建新文件
        fd_ = open(index_path_.c_str(), O_RDWR | O_CREAT, 0666);
        InitializeNewIndex();
        return;
    }
    
    // 2. 读取并验证超级块
    SuperBlock sb;
    pread(fd_, &sb, sizeof(sb), 0);
    if (sb.magic != MAGIC_NUMBER || sb.checksum != ComputeChecksum(sb)) {
        ERROR_EXIT("Index file corrupted");
    }
    
    // 3. 恢复段分配状态
    for (uint32_t i = 0; i < sb.total_segments; i++) {
        SegmentHeader header;
        pread(fd_, &header, sizeof(header), i * SEGMENT_SIZE);
        
        if (header.magic == SEGMENT_MAGIC) {
            segment_headers_[i] = header;
            if (header.node_count < NODES_PER_SEGMENT) {
                // 有空闲空间的段
                partially_full_segments_.push_back(i);
            }
        } else {
            free_segments_.push_back(i);
        }
    }
    
    // 4. 恢复 root 节点句柄
    root_handle_ = NodeHandle(sb.root_segment, sb.root_offset, 
                              NodeHandle::TYPE_LEAF, sb.root_generation);
    
    next_segment_id_.store(sb.next_segment_id);
    current_generation_.store(sb.current_generation);
}
```

### 9.2 Checkpoint 机制

```cpp
void NodeCache::Checkpoint() {
    // 1. 刷盘所有脏页
    Flush();
    
    // 2. 同步存储管理器元数据
    storage_->SyncMetadata();
    
    // 3. 更新超级块
    SuperBlock sb;
    sb.magic = MAGIC_NUMBER;
    sb.root_segment = root_handle_.segment_id();
    sb.root_offset = root_handle_.page_offset();
    sb.root_generation = root_handle_.generation();
    sb.next_segment_id = next_segment_id_.load();
    sb.current_generation = current_generation_.load();
    sb.checksum = ComputeChecksum(sb);
    
    pwrite(storage_->fd(), &sb, sizeof(sb), 0);
    fsync(storage_->fd());
}
```

---

## 10. 配置与调优指南

### 10.1 配置参数

```cpp
struct HMasstreeConfig {
    // ========== 存储配置 ==========
    std::string index_path;           // 索引文件路径
    bool use_direct_io = false;       // 是否使用 Direct I/O
    
    // ========== 缓存配置 ==========
    size_t max_cache_memory = 16ULL << 30;  // 最大缓存内存 (默认 16GB)
    size_t cache_shards = 256;              // 缓存分片数
    double evict_trigger_ratio = 0.9;       // 触发驱逐的内存占比
    double evict_target_ratio = 0.8;        // 驱逐目标占比
    
    // ========== 预取配置 ==========
    bool enable_prefetch = true;            // 启用预取
    int prefetch_depth = 2;                 // 预取深度
    int prefetch_threads = 2;               // 预取线程数
    
    // ========== 回写配置 ==========
    size_t dirty_page_threshold = 1000;     // 触发批量回写的脏页数
    int flush_interval_ms = 1000;           // 后台刷盘间隔
    
    // ========== 恢复配置 ==========
    bool recover = false;                   // 是否恢复模式
};
```

### 10.2 调优建议

| 参数 | 场景 | 建议值 |
|------|------|--------|
| `max_cache_memory` | 读密集 + 热点明显 | 索引总大小的 10-20% |
| `max_cache_memory` | 读密集 + 均匀访问 | 尽可能大 |
| `cache_shards` | 高并发 (>32 线程) | 512 或更高 |
| `enable_prefetch` | 范围扫描为主 | true |
| `enable_prefetch` | 随机点查为主 | false (减少无效 I/O) |
| `flush_interval_ms` | 写密集 | 较小值 (如 500ms) |
| `use_direct_io` | SSD + 大量随机读 | true (避免 page cache 污染) |

---

## 11. 性能预期

### 11.1 延迟模型

| 操作 | 缓存命中 | 缓存未命中 |
|------|----------|------------|
| 点查 (Get) | ~0.5-1 μs | 10-50 μs (1-2 次 I/O) |
| 插入 (Put) | ~1-2 μs | 20-100 μs (可能多次 I/O + 脏页) |
| 范围扫描 | ~0.1 μs/key | 取决于预取效果 |

### 11.2 吞吐预期

假设:
- NVMe SSD: 500K IOPS 随机读
- 缓存命中率: 90%
- 64 线程并发

预期点查吞吐:
- 缓存命中: ~2M ops/s (受 CPU 限制)
- 缓存未命中: ~50K ops/s (受 SSD IOPS 限制)
- 综合 (90% 命中): ~500K ops/s

### 11.3 内存效率

| 索引规模 | 全内存占用 | 16GB 缓存 (10% 命中时) |
|----------|------------|------------------------|
| 1 亿 key | ~3.2 GB | 100% 命中 |
| 10 亿 key | ~32 GB | ~50% 命中 |
| 100 亿 key | ~320 GB | ~5% 命中 (需热点分布) |

---

## 12. 方案局限性分析

本节分析当前方案（基于 NodeHandle + NodeCache 的设计）的性能特征与潜在局限，为后续优化提供依据。

### 12.1 热路径开销分析

在当前方案中，**每次**访问子节点都需要经过缓存层查表：

```
当前方案热路径:
  load child_[kp]  →  NodeHandle  →  cache.Get(handle)  →  hash查表  →  返回指针
  延迟: ~50-100 CPU cycles (即使缓存100%命中)
```

对比原生 Masstree（纯内存）：

```
原生 Masstree 热路径:
  load child_[kp]  →  直接是内存指针  →  访问节点
  延迟: ~1-5 CPU cycles
```

### 12.2 性能开销来源

| 开销项 | 来源 | 估计延迟 |
|--------|------|----------|
| Handle 解析 | 提取 segment_id, offset 等字段 | ~2 cycles |
| 分片定位 | `hash % num_shards` | ~5 cycles |
| 哈希表查找 | `unordered_map::find()` | ~20-40 cycles |
| Pin 操作 | `atomic_fetch_add` | ~10-20 cycles |
| Touch 更新 | 更新访问计数和时间戳 | ~10 cycles |
| **总计** | | **~50-100 cycles** |

### 12.3 与业界方案对比

| 方案 | 代表系统 | 热路径开销 | 冷路径开销 | 实现复杂度 |
|------|----------|------------|------------|------------|
| 纯内存指针 | 原生 Masstree | ~1 cycle | N/A | 低 |
| **NodeHandle + Cache** | **本方案** | **~50-100 cycles** | **~10-50 μs** | **中** |
| Pointer Swizzling | LeanStore, Umbra | ~1-5 cycles | ~10-50 μs | 高 |
| mmap | 传统数据库 | ~1 cycle (TLB命中) | ~1-10 μs (page fault) | 低 |

### 12.4 适用场景评估

**当前方案适合**：
- 索引规模远超内存，缓存命中率 < 80%
- I/O 延迟是主要瓶颈，CPU 开销相对次要
- 追求快速交付，先验证功能正确性

**当前方案不适合**：
- 缓存命中率很高（>95%）且追求极致吞吐
- 对 P99 延迟有严格要求（μs 级别）
- CPU 资源是系统瓶颈

---

## 13. Pointer Swizzling 优化设计（后续阶段）

> **注意**：本章节描述的是**后续优化方向**，不在初始实现范围内。
> 建议在完成 Phase 1-6 并验证功能正确性后，再考虑引入此优化。

### 13.1 技术背景

Pointer Swizzling（指针旋转）是一种在外存数据库中广泛使用的优化技术，代表系统包括 LeanStore (ICDE '18) 和 Umbra。

**核心思想**：
- **内存充足时**：父节点直接存储子节点的内存指针（性能等同纯内存数据库）
- **内存不足时**：将子节点驱逐到磁盘，父节点中的指针被替换为磁盘 ID (PID)
- **再次访问时**：检测到是 PID，透明地从磁盘加载并在原位"旋转"回内存指针

### 13.2 与当前方案的核心差异

| 方面 | 当前方案 (NodeHandle) | Pointer Swizzling |
|------|----------------------|-------------------|
| **热路径表示** | 始终为 `NodeHandle` 逻辑句柄 | 直接内存指针 `node_base<P>*` |
| **冷路径表示** | 同样是 `NodeHandle` | PID（磁盘页 ID） |
| **热路径开销** | 每次访问需 handle→缓存查表 | **零开销**（与纯内存相同） |
| **指针类型** | 统一逻辑句柄 | 联合体：指针 \| PID（通过标志位区分） |
| **状态转换** | 无转换，始终通过缓存层 | 动态"旋转"（swizzle/unswizzle） |

**性能对比**：

```
Pointer Swizzling (已 swizzle 的热路径):
  load child_[kp]  →  检查最高位=1  →  直接是内存指针  →  访问节点
  延迟: ~1-5 CPU cycles

当前方案 (热路径):
  load child_[kp]  →  NodeHandle  →  cache.Get(handle)  →  hash查表  →  返回指针
  延迟: ~50-100 CPU cycles
```

### 13.3 Swizzled Pointer 数据结构

```cpp
/**
 * @brief 可旋转的指针/句柄联合体
 * 
 * 通过最高位区分：
 *   bit 63 = 1: 已 swizzle，低 63 位是内存指针
 *   bit 63 = 0: 未 swizzle，低 63 位是磁盘 PID
 */
union SwizzledPointer {
    // 原始数据
    uint64_t data_;
    
    // ===== Swizzled 状态：直接内存指针 =====
    // 最高位为 1，低 63 位为指针（x86-64 用户空间地址 < 2^47）
    void* swizzled_ptr;
    
    // ===== Unswizzled 状态：磁盘 PID =====
    struct {
        uint64_t is_swizzled : 1;   // = 0 表示 PID
        uint64_t segment_id  : 20;  // 段 ID
        uint64_t offset      : 20;  // 段内偏移
        uint64_t generation  : 23;  // 版本号
    } pid;
    
    // ===== 构造与访问 =====
    
    SwizzledPointer() : data_(0) {}
    
    // 从内存指针构造（设置最高位）
    static SwizzledPointer FromPointer(void* ptr) {
        SwizzledPointer sp;
        sp.data_ = reinterpret_cast<uint64_t>(ptr) | (1ULL << 63);
        return sp;
    }
    
    // 从 PID 构造
    static SwizzledPointer FromPID(uint32_t seg, uint32_t off, uint32_t gen) {
        SwizzledPointer sp;
        sp.pid.is_swizzled = 0;
        sp.pid.segment_id = seg;
        sp.pid.offset = off;
        sp.pid.generation = gen;
        return sp;
    }
    
    // 状态检查
    bool is_swizzled() const { return data_ & (1ULL << 63); }
    bool is_null() const { return data_ == 0; }
    
    // 获取内存指针（仅在 is_swizzled() 时有效）
    template<typename T>
    T* get_pointer() const {
        return reinterpret_cast<T*>(data_ & ~(1ULL << 63));
    }
    
    // 获取 PID（仅在 !is_swizzled() 时有效）
    NodeHandle get_handle() const {
        return NodeHandle(pid.segment_id, pid.offset, 0, pid.generation);
    }
};
```

### 13.4 快速解析路径

```cpp
/**
 * @brief 解析 SwizzledPointer，可能触发 swizzle
 * 
 * 热路径：检查最高位，直接返回指针
 * 冷路径：加载节点并 swizzle
 */
template<typename P>
inline Masstree::node_base<P>* resolve(SwizzledPointer& slot, 
                                        NodeCache& cache, 
                                        threadinfo& ti) {
    // ========== 热路径：已 swizzle ==========
    if (likely(slot.is_swizzled())) {
        // 零查表开销！直接返回指针
        return slot.get_pointer<Masstree::node_base<P>>();
    }
    
    // ========== 冷路径：需要 swizzle ==========
    return cache.LoadAndSwizzle<P>(slot, ti);
}
```

### 13.5 Swizzle 操作

```cpp
template<typename P>
Masstree::node_base<P>* NodeCache::LoadAndSwizzle(SwizzledPointer& slot, 
                                                   threadinfo& ti) {
    NodeHandle handle = slot.get_handle();
    
    // 1. CAS 防止重复加载
    uint64_t old_val = slot.data_;
    if (SwizzledPointer{old_val}.is_swizzled()) {
        // 其他线程已经 swizzle
        return SwizzledPointer{old_val}.get_pointer<Masstree::node_base<P>>();
    }
    
    // 可选：设置 "loading" 标记防止并发
    
    // 2. 从 SSD 加载节点
    void* mem = AllocateNodeMemory();
    storage_->ReadNode(handle, mem, node_size_);
    
    // 3. 创建 CachedNode，记录 back pointer
    CachedNode* cached = new CachedNode();
    cached->node_ptr = mem;
    cached->handle = handle;
    cached->back_pointers.push_back(&slot);  // 关键：记录父节点槽位
    
    InsertToCache(cached);
    
    // 4. 原子地 swizzle：将槽位从 PID 改为内存指针
    SwizzledPointer new_val = SwizzledPointer::FromPointer(mem);
    __atomic_store_n(&slot.data_, new_val.data_, __ATOMIC_RELEASE);
    
    return static_cast<Masstree::node_base<P>*>(mem);
}
```

### 13.6 Unswizzle 操作（驱逐时）

```cpp
void NodeCache::EvictNodeWithSwizzle(CachedNode* node) {
    // 1. 遍历所有 back pointer，将内存指针改回 PID
    for (SwizzledPointer* slot : node->back_pointers) {
        SwizzledPointer new_val = SwizzledPointer::FromPID(
            node->handle.segment_id(),
            node->handle.page_offset(),
            node->handle.generation()
        );
        __atomic_store_n(&slot->data_, new_val.data_, __ATOMIC_RELEASE);
    }
    
    // 2. 如果是脏页，写回磁盘
    if (node->is_dirty.load()) {
        storage_->WriteNode(node->handle, node->node_ptr, node->node_size);
    }
    
    // 3. 释放内存
    FreeNodeMemory(node->node_ptr, node->node_size);
    
    // 4. 从缓存移除
    RemoveFromCache(node);
    delete node;
}
```

### 13.7 Back Pointer 管理

每个缓存节点需要维护"谁指向我"的列表：

```cpp
struct CachedNode {
    // ... 原有字段 ...
    
    // 新增：back pointers（指向父节点中的槽位）
    std::vector<SwizzledPointer*> back_pointers;
    SpinLock bp_lock;  // 保护 back_pointers 的修改
    uint64_t swizzle_epoch;  // swizzle 时的 epoch（用于安全检查）
};
```

**Masstree 中的 back pointer 特点**：
- `child_[]` 槽位：一个子节点只有一个父节点的一个槽位指向它（1:1 关系）
- `parent_` 指针：可选择不 swizzle（保持 NodeHandle），简化实现

### 13.8 实现挑战与解决方案

#### 挑战 1：并发 Swizzle/Unswizzle

多线程同时访问未 swizzle 的槽位：

```
线程 A: 读取 child_[kp]，发现是 PID，开始加载
线程 B: 同时读取同一个 child_[kp]，也发现是 PID
```

**解决方案**：使用 CAS + 加载中标记

```cpp
// 定义特殊的 "loading" 状态
static constexpr uint64_t LOADING_MARKER = 0x8000000000000001ULL;

template<typename P>
Masstree::node_base<P>* NodeCache::LoadAndSwizzle(SwizzledPointer& slot, 
                                                   threadinfo& ti) {
    uint64_t old_val = slot.data_;
    
    // 快速检查：已 swizzle
    if (SwizzledPointer{old_val}.is_swizzled()) {
        // 注意：可能是 LOADING_MARKER
        if (old_val == LOADING_MARKER) {
            return WaitForSwizzle<P>(slot);  // 等待其他线程完成
        }
        return SwizzledPointer{old_val}.get_pointer<Masstree::node_base<P>>();
    }
    
    // 尝试设置 LOADING 标记
    if (!__atomic_compare_exchange_n(&slot.data_, &old_val, LOADING_MARKER,
                                      false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        // CAS 失败，其他线程在处理
        return WaitForSwizzle<P>(slot);
    }
    
    // 获得加载权，执行 I/O 并 swizzle
    // ...
}
```

#### 挑战 2：驱逐时正在被读取

```
线程 A: 正在遍历节点 N
线程 B: 尝试驱逐节点 N
```

如果 B unswizzle 了 N，A 持有的指针变成悬挂指针！

**解决方案**：Epoch-based Protection

```cpp
class NodeCache {
    std::atomic<uint64_t> global_epoch_{0};
    
    // 每个线程有本地 epoch
    struct ThreadState {
        std::atomic<uint64_t> local_epoch{0};
        bool active{false};
    };
    std::vector<ThreadState> thread_states_;
    
    void EnterCriticalSection(threadinfo& ti) {
        auto& ts = thread_states_[ti.index()];
        ts.local_epoch.store(global_epoch_.load(), std::memory_order_acquire);
        ts.active = true;
    }
    
    void ExitCriticalSection(threadinfo& ti) {
        auto& ts = thread_states_[ti.index()];
        ts.active = false;
        ts.local_epoch.store(0, std::memory_order_release);
    }
    
    bool CanSafelyEvict(CachedNode* node) {
        // 检查是否有线程在 node 被 swizzle 之前进入了临界区
        uint64_t node_epoch = node->swizzle_epoch;
        for (auto& ts : thread_states_) {
            if (ts.active && ts.local_epoch.load() <= node_epoch) {
                return false;  // 有线程可能还在使用
            }
        }
        return true;
    }
};
```

#### 挑战 3：与 Masstree 乐观并发集成

```cpp
// 修改后的 reach_leaf()
template <typename P>
leaf<P>* node_base<P>::reach_leaf(const key_type& ka,
                                   nodeversion_type& version,
                                   threadinfo& ti) const {
    NodeCache* cache = ti.node_cache();
    cache->EnterCriticalSection(ti);
    
    // RAII guard
    struct EpochGuard {
        NodeCache* c; threadinfo* t;
        ~EpochGuard() { c->ExitCriticalSection(*t); }
    } guard{cache, &ti};
    
    // ... 遍历逻辑 ...
    
    while (!v[sense].isleaf()) {
        const internode<P>* in = ...;
        int kp = bound_type::upper(ka, *in);
        
        // 使用 resolve 而非 cache->Get()
        SwizzledPointer& child_slot = const_cast<SwizzledPointer&>(in->child_[kp]);
        n[sense ^ 1] = resolve<P>(child_slot, *cache, ti);
        
        // 版本检查保持不变
        v[sense ^ 1] = n[sense ^ 1]->stable_annotated(ti.stable_fence());
        // ... 
    }
}
```

### 13.9 分层 Swizzling 策略

可以只对部分节点类型启用 swizzling：

| 节点类型 | Swizzling | 理由 |
|----------|-----------|------|
| Root | 始终 swizzle | 每次操作都访问 |
| Internode (上层) | 启用 swizzle | 访问频率高，back pointer 少 |
| Internode (下层) | 可选 | 视访问模式决定 |
| Leaf | 不启用 | 数量大，back pointer 开销高 |

```cpp
bool should_swizzle(CachedNode* node) {
    if (node->handle.node_type() == NodeHandle::TYPE_INTERNODE) {
        // 根据树高度或访问频率决定
        return node->tree_level < SWIZZLE_LEVEL_THRESHOLD;
    }
    return false;
}
```

### 13.10 性能预期（启用 Swizzling 后）

| 场景 | 当前方案 | + Swizzling 优化 |
|------|----------|------------------|
| 热路径延迟 | ~50-100 cycles | ~1-5 cycles |
| 热路径吞吐 (单线程) | ~20M ops/s | ~200M ops/s |
| 冷路径延迟 | ~10-50 μs | ~10-50 μs (相同) |
| 内存开销 | handle + 缓存元数据 | handle + back_pointers |
| 实现复杂度 | 中 | 高 |

### 13.11 Swizzling 优化实施路线

> 建议在 Phase 6 完成后，再考虑以下优化阶段。

#### Phase 7: Swizzling 基础设施 (2 周)

1. 定义 `SwizzledPointer` 联合体
2. 修改节点结构使用 `SwizzledPointer`
3. 实现 `resolve()` 快速路径
4. 保持与 `NodeHandle` 的互操作性

#### Phase 8: Swizzle/Unswizzle 实现 (2 周)

1. 实现 `LoadAndSwizzle()`
2. 实现 back pointer 管理
3. 实现 `EvictNodeWithSwizzle()`
4. 单线程功能测试

#### Phase 9: 并发安全 (3 周)

1. 实现 epoch-based protection
2. 实现并发 swizzle 的 CAS 协议
3. 与 Masstree nodeversion 集成
4. 并发压力测试

#### Phase 10: 调优与评估 (1 周)

1. 分层 swizzling 策略
2. 性能对比测试
3. 确定最佳配置

---

## 14. 实现路线图（初始阶段）

以下为初始实现阶段（Phase 1-6），完成后可获得功能完整的外存索引。
Pointer Swizzling 优化（Phase 7-10）请参见第 13 章。

### Phase 1: 基础抽象层 (2 周)

**目标**: 引入 NodeHandle 抽象，但节点仍全部驻留内存

**任务**:
1. 定义 `NodeHandle` 结构
2. 在 `internode` 和 `leaf` 中使用 `NodeHandle` 替代指针
3. 实现内存中的 `HandleTable` (handle → 内存指针)
4. 修改 `reach_leaf()` 使用 handle
5. 确保所有单元测试通过

**验收标准**: 功能与原版一致，性能下降 <5%

### Phase 2: 节点缓存层 (3 周)

**目标**: 实现完整的 NodeCache

**任务**:
1. 实现 `CachedNode` 结构
2. 实现分片哈希表
3. 实现 Get() 的快速路径/慢速路径
4. 实现 pin/unpin 机制
5. 集成到 `threadinfo`

**验收标准**: 缓存命中时性能正确

### Phase 3: 存储管理与 I/O (3 周)

**目标**: 实现节点的 SSD 存储

**任务**:
1. 实现 `IndexStorageManager`
2. 定义节点页格式
3. 实现节点读写 I/O
4. 实现段分配/回收
5. 集成 `SegmentAllocator`

**验收标准**: 可以正确读写节点到 SSD

### Phase 4: 淘汰与回写 (2 周)

**目标**: 实现内存控制

**任务**:
1. 实现 Clock 淘汰算法
2. 实现脏页管理
3. 实现后台刷盘
4. 实现内存阈值控制

**验收标准**: 内存使用不超过配置上限

### Phase 5: 并发优化与恢复 (3 周)

**目标**: 保证正确性与可靠性

**任务**:
1. 压力测试并发正确性
2. 实现 Checkpoint
3. 实现崩溃恢复
4. 性能优化 (预取等)

**验收标准**: 通过并发压力测试，崩溃恢复正确

### Phase 6: 集成测试与调优 (2 周)

**目标**: 生产可用

**任务**:
1. 集成到 FlowKV
2. 端到端性能测试
3. 参数调优
4. 文档完善

---

## 15. 核心技术挑战与解决方案

将 Masstree 从纯内存索引改造为外存索引时，存在三个核心技术挑战。本章详细分析每个问题并给出解决方案。

### 15.1 挑战一：读写放大问题

#### 问题描述

| 因素 | 数值 |
|------|------|
| Masstree 节点大小 | 256B - 512B（适配 Cache Line） |
| SSD 最小读写单元 | 4KB (Page) |
| 读放大倍数 | 8x - 16x |

**风险**：
- **读放大 (Read Amplification)**：读取 256B 数据却需加载 4KB，带宽浪费 8-16 倍
- **内存浪费**：Buffer Pool 中 4KB 的 Frame 只存储 256B-512B 有效数据
- **写放大**：修改小节点也需写回整个 4KB 页

#### 解决方案

**方案 A：节点打包 (Node Packing) - 推荐**

将多个小节点打包到一个 4KB 页中：

```cpp
/**
 * @brief 一个 4KB 磁盘页，包含多个节点
 */
struct PackedPage {
    static constexpr size_t PAGE_SIZE = 4096;
    static constexpr size_t NODE_SLOT_SIZE = 512;
    static constexpr size_t NODES_PER_PAGE = 8;   // 4096 / 512
    
    struct PageHeader {
        uint64_t magic;              // 魔数 0x48494458
        uint64_t page_id;            // 页 ID
        uint8_t  slot_bitmap;        // 哪些槽位已分配 (bit mask)
        uint8_t  slot_types;         // 每个槽位的节点类型
        uint8_t  reserved[6];
    };  // 16 bytes
    
    PageHeader header;
    uint8_t    slots[NODES_PER_PAGE][NODE_SLOT_SIZE];  // 8 * 512 = 4096B
};

/**
 * @brief 修改后的 NodeHandle 编码
 * 
 * 编码格式 (64 bits):
 *   [63]     : valid bit
 *   [62:60]  : node_type
 *   [59:23]  : page_id (37 bits, 支持 ~137B 页)
 *   [22:20]  : slot_index (3 bits, 0-7)
 *   [19:0]   : generation (20 bits)
 */
struct NodeHandle {
    uint64_t data_;
    
    uint64_t page_id() const { return (data_ >> 23) & 0x1FFFFFFFFF; }
    uint8_t  slot_index() const { return (data_ >> 20) & 0x7; }
    // ...
};
```

**I/O 与缓存策略调整**：

```cpp
class NodeCache {
    // 缓存单位改为 4KB Page
    std::unordered_map<uint64_t, CachedPage*> page_cache_;
    
    template<typename P>
    node_base<P>* Get(NodeHandle handle, threadinfo& ti) {
        uint64_t page_id = handle.page_id();
        uint8_t slot = handle.slot_index();
        
        // 以页为单位加载
        CachedPage* page = GetOrLoadPage(page_id, ti);
        
        // 返回页内指定槽位的节点
        return reinterpret_cast<node_base<P>*>(page->slots[slot]);
    }
};
```

**优点**：
- 读取一个 4KB 页可命中最多 8 个节点（空间局部性）
- 相邻节点（如同一父节点的多个子节点）可能在同一页
- 减少元数据开销

**方案 B：扩大节点尺寸**

修改 Masstree 的节点宽度参数：

```cpp
// 原始配置: width = 15, 节点 ~256-512B
struct table_params : public Masstree::nodeparams<15, 15> { ... };

// 可选配置:
// width = 32, 节点 ~1KB
// width = 64, 节点 ~2KB  
// width = 128, 节点 ~4KB（完整页）
struct table_params : public Masstree::nodeparams<64, 64> { ... };
```

| 配置 | 节点大小 | 树高度 (10亿 key) | 点查 I/O 次数 |
|------|----------|-------------------|---------------|
| width=15 | ~512B | 5-6 层 | 5-6 次 |
| width=64 | ~2KB | 3-4 层 | 3-4 次 |
| width=128 | ~4KB | 2-3 层 | 2-3 次 |

**权衡**：更大的节点减少树高和 I/O 次数，但更新时写放大增加。

**方案 C：混合策略 - 推荐**

针对不同节点类型采用不同策略：

| 节点类型 | 策略 | 理由 |
|----------|------|------|
| Internode (内部节点) | 扩大到 2-4KB | 数量少、访问频繁、影响树高 |
| Leaf (叶子节点) | 保持原大小，打包存储 | 数量多、节省空间 |

```cpp
// Internode 使用更大的 width
template<typename P>
class internode_external : public node_base<P> {
    static constexpr int width = 64;  // 扩大！
    // 节点约 2KB
};

// Leaf 保持原大小，打包到页
template<typename P>
class leaf_external : public node_base<P> {
    static constexpr int width = 15;  // 保持原样
    // 节点约 512B，8 个打包到一页
};
```

---

### 15.2 挑战二：OCC Retry 与 I/O 阻塞的冲突

#### 问题描述

Masstree 的乐观并发控制 (OCC) 假设 **retry 代价很低**（几十纳秒）：

```cpp
// 原始 Masstree OCC 模式
retry:
    v1 = node->stable();      // ~10ns
    child = node->child_[kp]; // ~1ns (直接内存访问)
    // ... 读操作 ...
    if (node->has_changed(v1)) 
        goto retry;           // 重试代价低
```

外存场景下，访问子节点可能触发 I/O：

```cpp
retry:
    v1 = node->stable();
    child = resolve(child_handle);  // 可能阻塞 10-100μs!
    // ...
    if (node->has_changed(v1)) 
        goto retry;  // 之前的 I/O 白做了！
```

**更严重的问题**：不能在持有版本保护时做 I/O

```
场景:
1. 线程 A: v1 = parent->stable()
2. 线程 A: 发现 child 在磁盘，发起 I/O（阻塞 50μs）
3. 线程 B: 在 A 阻塞期间删除了 parent
4. 线程 A: I/O 完成，parent 可能已被 RCU 回收！

结果: v1 毫无意义，访问 parent 可能导致 segfault
```

#### 解决方案

**核心原则**：**I/O 必须在无锁/无版本持有状态下进行**

**分阶段遍历 + 缺页时 Restart**：

```cpp
/**
 * @brief 外存感知的 reach_leaf 实现
 * 
 * 关键设计:
 * 1. 第一遍遍历只检查缓存，不做 I/O
 * 2. 发现缺页时，释放所有状态，发起 I/O
 * 3. I/O 完成后从头重新遍历
 */
template <typename P>
leaf<P>* node_base<P>::reach_leaf_external(const key_type& ka,
                                            nodeversion_type& version,
                                            threadinfo& ti) const
{
    NodeCache* cache = ti.node_cache();
    
restart:
    // ========== Phase 1: 乐观遍历，收集缺页 ==========
    std::vector<NodeHandle> missing_pages;
    const node_base<P>* path[MAX_TREE_HEIGHT];
    nodeversion_type versions[MAX_TREE_HEIGHT];
    int depth = 0;
    
    const node_base<P>* n = this;
    nodeversion_type v = n->stable_annotated(ti.stable_fence());
    
    while (!v.isleaf()) {
        path[depth] = n;
        versions[depth] = v;
        depth++;
        
        const internode<P>* in = static_cast<const internode<P>*>(n);
        int kp = bound_type::upper(ka, *in);
        NodeHandle child_h = in->child_[kp];
        
        // 只检查缓存，不加载！
        CachedPage* cached = cache->LookupNoLoad(child_h);
        if (cached == nullptr) {
            // 记录缺页
            missing_pages.push_back(child_h);
            // 尝试继续收集更多可能的缺页（推测性遍历）
            // 但无法确定实际路径，所以这里中断
            break;
        }
        
        n = cached->GetNode<P>(child_h.slot_index());
        v = n->stable_annotated(ti.stable_fence());
    }
    
    // ========== Phase 2: 如果有缺页，释放状态并加载 ==========
    if (!missing_pages.empty()) {
        // 清除所有路径状态！此时不持有任何版本保护
        depth = 0;
        
        // 发起 I/O（完全无锁状态）
        for (auto& h : missing_pages) {
            cache->LoadPageBlocking(h);
        }
        
        // I/O 完成后，必须从头开始！
        // 因为在 I/O 期间树结构可能已变化
        goto restart;
    }
    
    // ========== Phase 3: 所有节点都在缓存中，验证路径 ==========
    // 验证遍历路径是否仍然有效
    for (int i = 0; i < depth; i++) {
        if (path[i]->has_changed(versions[i])) {
            // 路径失效，重新开始
            goto restart;
        }
    }
    
    version = v;
    return const_cast<leaf<P>*>(static_cast<const leaf<P>*>(n));
}
```

**Scan 场景的处理**：

```cpp
/**
 * @brief 外存感知的 scan 实现
 * 
 * 使用预取减少阻塞等待
 */
void scan_external(const key_type& start_key, int count, 
                   std::vector<value_type>& results, threadinfo& ti) {
    NodeCache* cache = ti.node_cache();
    
    // 定位起始叶子
    nodeversion_type v;
    leaf<P>* current = reach_leaf_external(start_key, v, ti);
    
    while (results.size() < count && current != nullptr) {
        // 处理当前叶子
        for (int i = 0; i < current->size() && results.size() < count; i++) {
            results.push_back(current->lv_[i].value());
        }
        
        // 获取下一个叶子的 handle
        NodeHandle next_h = current->next_;
        if (next_h.is_null()) break;
        
        // 预取下下个叶子（非阻塞）
        NodeHandle next_next = /* 需要先加载 next 才能知道 */;
        
        // 移动到下一个叶子
        CachedPage* next_page = cache->GetOrLoadPage(next_h, ti);
        current = next_page->GetNode<P>(next_h.slot_index());
    }
}
```

**优化：批量预取**

```cpp
// 检测到缺页时，尝试预测并批量预取
void NodeCache::PrefetchPredicted(NodeHandle current, const key_type& ka) {
    // 基于 key 范围预测可能访问的页
    // 例如：如果是范围扫描，预取相邻页
    std::vector<NodeHandle> to_prefetch = PredictNextPages(current, ka);
    
    for (auto& h : to_prefetch) {
        // 提交异步预取
        prefetch_pool_->Submit([this, h]() {
            LoadPageBlocking(h);
        });
    }
}
```

---

### 15.3 挑战三：RCU 与 Buffer Pool Eviction 的冲突

#### 问题描述

Masstree 使用 **Epoch-based RCU** 来回收内存节点（代码中的 `phantom_epoch`）。

| 机制 | 特点 |
|------|------|
| RCU 回收 | **滞后的**：节点逻辑删除后，等待所有读者退出 epoch 才物理释放 |
| Buffer Pool 驱逐 | **显式的**：内存不足时主动驱逐页，可能持有活跃读者的指针 |

**冲突场景**：

```
1. 线程 A: ptr = cache->Get(handle)   // 获得节点指针
2. 线程 A: v1 = ptr->stable()         // 开始 OCC 读
3. Buffer Pool: 内存紧张，决定驱逐包含 ptr 的页
4. Buffer Pool: 释放页内存                // ptr 变成悬挂指针！
5. 线程 A: 访问 ptr->child_[kp]        // Segfault!
```

**问题本质**：Buffer Pool 不知道哪些节点正在被读取。

#### 解决方案

**方案：Pin + Epoch 混合保护机制**

**设计原则**：
- **Pin**：细粒度保护，防止当前正在使用的页被驱逐
- **Epoch**：粗粒度保护，确保驱逐发生在安全时间点

```cpp
/**
 * @brief 增强的 CachedPage 结构
 */
struct CachedPage {
    void* page_ptr;                          // 页内存指针
    uint64_t page_id;                        // 页 ID
    
    // ===== Pin 保护 =====
    std::atomic<uint32_t> pin_count{0};      // 引用计数
    std::atomic<bool> evict_requested{false}; // 驱逐请求标记
    
    // ===== Epoch 信息 =====
    uint64_t load_epoch;                     // 加载时的 epoch
    
    // ===== 淘汰相关 =====
    std::atomic<uint8_t> reference_bit{0};
    std::atomic<bool> is_dirty{false};
    
    bool try_pin() {
        // 如果已请求驱逐，拒绝 pin
        if (evict_requested.load(std::memory_order_acquire)) {
            return false;
        }
        pin_count.fetch_add(1, std::memory_order_acq_rel);
        // Double check
        if (evict_requested.load(std::memory_order_acquire)) {
            pin_count.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
        return true;
    }
    
    void unpin() {
        pin_count.fetch_sub(1, std::memory_order_release);
    }
};
```

**Epoch 管理**：

```cpp
class NodeCache {
    // ===== 全局 Epoch =====
    std::atomic<uint64_t> global_epoch_{0};
    
    // ===== 每线程 Epoch 状态 =====
    struct ThreadEpoch {
        std::atomic<uint64_t> local_epoch{0};   // 进入临界区时的 epoch
        std::atomic<bool> in_critical{false};    // 是否在临界区
        char padding[48];                        // 避免 false sharing
    };
    std::vector<ThreadEpoch> thread_epochs_;     // 每线程一个
    
    // ===== 待驱逐队列 =====
    struct PendingEviction {
        CachedPage* page;
        uint64_t request_epoch;  // 请求驱逐时的 epoch
    };
    std::queue<PendingEviction> evict_queue_;
    
public:
    // 读者进入临界区
    void EnterCriticalSection(threadinfo& ti) {
        auto& te = thread_epochs_[ti.index()];
        te.local_epoch.store(global_epoch_.load(std::memory_order_acquire),
                             std::memory_order_release);
        te.in_critical.store(true, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
    
    // 读者退出临界区
    void ExitCriticalSection(threadinfo& ti) {
        auto& te = thread_epochs_[ti.index()];
        te.in_critical.store(false, std::memory_order_release);
    }
    
    // 推进全局 epoch
    void AdvanceEpoch() {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    // 计算最小活跃 epoch
    uint64_t MinActiveEpoch() {
        uint64_t min_epoch = global_epoch_.load();
        for (auto& te : thread_epochs_) {
            if (te.in_critical.load(std::memory_order_acquire)) {
                uint64_t e = te.local_epoch.load(std::memory_order_acquire);
                if (e < min_epoch) min_epoch = e;
            }
        }
        return min_epoch;
    }
};
```

**安全驱逐流程**：

```cpp
void NodeCache::RequestEviction(CachedPage* page) {
    // 1. 标记驱逐请求（阻止新的 pin）
    page->evict_requested.store(true, std::memory_order_release);
    
    // 2. 检查是否有活跃 pin
    if (page->pin_count.load(std::memory_order_acquire) > 0) {
        // 有活跃用户，加入延迟队列
        evict_queue_.push({page, global_epoch_.load()});
        return;
    }
    
    // 3. 无活跃 pin，加入 epoch 保护队列
    evict_queue_.push({page, global_epoch_.load()});
}

void NodeCache::ProcessPendingEvictions() {
    uint64_t safe_epoch = MinActiveEpoch();
    
    while (!evict_queue_.empty()) {
        auto& pending = evict_queue_.front();
        
        // 检查 epoch 安全性
        if (pending.request_epoch >= safe_epoch) {
            // 还有读者可能在使用，稍后重试
            break;
        }
        
        // 再次检查 pin（可能在等待期间有新的 pin）
        if (pending.page->pin_count.load() > 0) {
            // 移到队列尾部，稍后重试
            evict_queue_.push(pending);
            evict_queue_.pop();
            continue;
        }
        
        // 安全！执行物理驱逐
        DoEvictPage(pending.page);
        evict_queue_.pop();
    }
}

void NodeCache::DoEvictPage(CachedPage* page) {
    // 1. 如果脏页，写回磁盘
    if (page->is_dirty.load()) {
        storage_->WritePage(page->page_id, page->page_ptr);
    }
    
    // 2. 从缓存表移除
    RemoveFromCache(page->page_id);
    
    // 3. 释放内存
    FreePageMemory(page->page_ptr);
    delete page;
    
    // 4. 更新统计
    current_memory_bytes_.fetch_sub(PAGE_SIZE);
}
```

**读者端使用 RAII Guard**：

```cpp
/**
 * @brief RAII guard for critical section + pin
 */
class ReadGuard {
    NodeCache* cache_;
    threadinfo* ti_;
    std::vector<CachedPage*> pinned_pages_;
    
public:
    ReadGuard(NodeCache* cache, threadinfo& ti) 
        : cache_(cache), ti_(&ti) {
        cache_->EnterCriticalSection(ti);
    }
    
    ~ReadGuard() {
        // 释放所有 pin
        for (auto* page : pinned_pages_) {
            page->unpin();
        }
        cache_->ExitCriticalSection(*ti_);
    }
    
    // 获取节点并自动 pin
    template<typename P>
    node_base<P>* GetAndPin(NodeHandle handle) {
        CachedPage* page = cache_->GetPage(handle);
        if (page && page->try_pin()) {
            pinned_pages_.push_back(page);
            return page->GetNode<P>(handle.slot_index());
        }
        return nullptr;  // 页正在被驱逐，需要重试
    }
};

// 使用示例
template <typename P>
leaf<P>* node_base<P>::reach_leaf_safe(const key_type& ka,
                                        nodeversion_type& version,
                                        threadinfo& ti) const
{
    NodeCache* cache = ti.node_cache();
    
restart:
    {
        ReadGuard guard(cache, ti);
        
        // ... 遍历逻辑 ...
        // 使用 guard.GetAndPin() 获取节点
        
    }  // guard 析构时自动释放 pin 和退出临界区
    
    // 如果需要 restart，此时已经释放了所有保护
    if (need_restart) goto restart;
}
```

**后台 Epoch 推进任务**：

```cpp
void NodeCache::BackgroundEpochAdvancer() {
    while (!shutdown_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        // 推进 epoch
        AdvanceEpoch();
        
        // 处理待驱逐队列
        ProcessPendingEvictions();
    }
}
```

---

### 15.4 解决方案总结

| 挑战 | 解决方案 | 关键技术 |
|------|----------|----------|
| **读写放大** | 节点打包 + 混合策略 | 4KB 页存多个节点，Internode 扩大 |
| **OCC + I/O 阻塞** | 分阶段遍历 + Restart | 无锁状态下 I/O，I/O 后从头验证 |
| **RCU vs Eviction** | Pin + Epoch 混合保护 | 细粒度 pin + 粗粒度 epoch 屏障 |

**整合到 NodeCache 的完整 Get 流程**：

```cpp
template<typename P>
node_base<P>* NodeCache::Get(NodeHandle handle, threadinfo& ti) {
    // 1. 快速路径：检查缓存
    CachedPage* page = LookupNoLoad(handle);
    if (page != nullptr) {
        // 尝试 pin
        if (page->try_pin()) {
            page->touch();
            cache_hits_.fetch_add(1);
            return page->GetNode<P>(handle.slot_index());
        }
        // 页正在被驱逐，走慢速路径
    }
    
    // 2. 慢速路径：需要加载
    cache_misses_.fetch_add(1);
    
    // 注意：调用者应该在 ReadGuard 保护下
    // 此处返回 nullptr 表示需要 restart
    return nullptr;  
}

// 调用者负责处理 nullptr 和 restart
```

---

## 16. 风险与缓解

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|----------|
| 缓存命中率低 | 中 | 性能差 | 增加缓存/优化淘汰策略 |
| 并发 bug | 中 | 数据损坏 | 充分测试 + 代码审查 |
| I/O 瓶颈 | 中 | 吞吐受限 | 批量 I/O + 预取 |
| 回写延迟高 | 低 | 崩溃丢数据 | 定期 checkpoint |
| 改造范围大 | 高 | 工期延长 | 分阶段验收 |

---

## 17. 附录

### 17.1 文件清单

| 文件 | 状态 | 描述 |
|------|------|------|
| `lib/hmasstree/node_handle.h` | 新增 | NodeHandle 定义 |
| `lib/hmasstree/node_cache.h` | 新增 | NodeCache 声明 |
| `lib/hmasstree/node_cache.cpp` | 新增 | NodeCache 实现 |
| `lib/hmasstree/index_storage.h` | 新增 | IndexStorageManager 声明 |
| `lib/hmasstree/index_storage.cpp` | 新增 | IndexStorageManager 实现 |
| `lib/hmasstree/masstree_struct.hh` | 修改 | 节点指针改为句柄 |
| `lib/hmasstree/masstree_get.hh` | 修改 | reach_leaf 修改 |
| `lib/hmasstree/masstree_insert.hh` | 修改 | 插入/分裂修改 |
| `lib/hmasstree/masstree_remove.hh` | 修改 | 删除修改 |
| `lib/hmasstree/masstree_scan2.hh` | 修改 | 扫描修改 |
| `lib/hmasstree/kvthread.hh` | 修改 | threadinfo 扩展 |
| `lib/hmasstree/hmasstree_wrapper.h` | 修改 | 配置与初始化 |
| `lib/index_hmasstree.h` | 修改 | 接口适配 |
| `lib/hmasstree/swizzled_pointer.h` | 新增 (Phase 7+) | SwizzledPointer 定义 |

### 17.2 参考资料

- Masstree 原始论文: "Cache Craftiness for Fast Multicore Key-Value Storage" (EUROSYS 2012)
- B-Tree 外存实现: "Modern B-Tree Techniques" (Graefe, 2011)
- 缓存淘汰算法: "CLOCK-Pro: An Effective Improvement of the CLOCK Replacement" (USENIX ATC 2005)
- LeanStore: "LeanStore: In-Memory Data Management Beyond Main Memory" (ICDE 2018)
- Umbra: "Umbra: A Disk-Based System with In-Memory Performance" (CIDR 2020)
- Pointer Swizzling: "Pointer Swizzling at Page Fault Time" (SIGMOD 1992)
