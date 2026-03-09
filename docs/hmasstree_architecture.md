# HMasstree 架构文档

**版本**: v1.0  
**更新日期**: 2026-02-10  
**状态**: 开发阶段

---

## 1. 概述与设计目标

### 1.1 什么是 HMasstree

HMasstree（Hybrid Masstree）是 FlowKV 对原版 Masstree 的扩展，添加了**外部存储**（External Storage）支持。核心目标是：

- **冷启动按需加载**: 仅加载 root 节点，其他节点在访问时按需从磁盘加载
- **内存受限运行**: 通过页面缓存 + 淘汰机制，在有限内存下运行大规模索引
- **透明兼容**: 保持与原版 Masstree API 兼容，可通过编译宏切换

### 1.2 设计目标

| 目标 | 描述 | 状态 |
|------|------|------|
| 冷启动时间 | < 10ms 启动 10M+ keys 的索引 | ✅ 0.8ms |
| 内存可控 | 支持配置 cache 大小限制 | ⚠️ 部分实现 |
| 读性能 | 热点数据接近内存版性能 | ⚠️ 需优化 |
| 写性能 | 支持增量写入和持久化 | ✅ 已实现 |
| 正确性 | 100% 数据可查 | ⚠️ 97-100% |

---

## 2. 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         HMasstreeWrapper                                 │
│    ┌──────────────────────────────────────────────────────────────────┐ │
│    │                   basic_table<table_params>                       │ │
│    │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐        │ │
│    │  │  internode  │────▶│  internode  │────▶│    leaf     │        │ │
│    │  │ child_[]    │     │ child_[]    │     │ lv_[]       │        │ │
│    │  │ child_h[]   │     │ child_h[]   │     │ next_h      │        │ │
│    │  └─────────────┘     └─────────────┘     └─────────────┘        │ │
│    └──────────────────────────────────────────────────────────────────┘ │
│                                    │                                     │
│                   ┌────────────────┼────────────────┐                   │
│                   ▼                ▼                ▼                   │
│    ┌─────────────────────┐ ┌──────────────┐ ┌──────────────────┐       │
│    │    NodeResolver     │ │  NodeCache   │ │  IndexStorage    │       │
│    │  handle→node map    │ │  page cache  │ │  file I/O        │       │
│    │  on-demand loading  │ │  Clock evict │ │  segment alloc   │       │
│    └─────────────────────┘ └──────────────┘ └──────────────────┘       │
│                                    │                                     │
│                                    ▼                                     │
│                         ┌──────────────────┐                            │
│                         │   Storage File   │                            │
│                         │  (mmap/pread)    │                            │
│                         └──────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 关键数据结构

### 3.1 NodeHandle - 8字节逻辑地址

NodeHandle 是外存模式的核心，用于替代原始指针进行持久化。

```cpp
/**
 * @brief NodeHandle - 8-byte logical address for external storage nodes
 *
 * Encoding format (64 bits):
 *   [63]     : valid bit (1 = valid handle, 0 = null/invalid)
 *   [62:60]  : node_type (3 bits, NodeType enum)
 *   [59:23]  : page_id (37 bits, supports ~137 billion pages)
 *   [22:20]  : slot_index (3 bits, 0-7, for node packing)
 *   [19:0]   : generation (20 bits, for ABA prevention)
 */
class NodeHandle {
public:
    static constexpr uint64_t VALID_BIT = 63;
    static constexpr uint64_t TYPE_SHIFT = 60;
    static constexpr uint64_t TYPE_MASK = 0x7ULL;  // 3 bits
    static constexpr uint64_t PAGE_ID_SHIFT = 23;
    static constexpr uint64_t PAGE_ID_MASK = 0x1FFFFFFFFFULL;  // 37 bits
    static constexpr uint64_t SLOT_SHIFT = 20;
    static constexpr uint64_t SLOT_MASK = 0x7ULL;  // 3 bits
    static constexpr uint64_t GEN_MASK = 0xFFFFFULL;  // 20 bits

private:
    uint64_t data_;
    
    // ... accessors ...
};
```

**设计要点**:
- 与原始指针同为 8 字节，可无缝替换
- page_id (37位) 支持最大 ~548TB 存储空间
- slot_index (3位) 支持每页打包 8 个节点
- generation (20位) 防止 ABA 问题

### 3.2 PackedPage - 4KB 页面打包

每个存储页面可容纳多个节点，提高空间利用率：

```cpp
/**
 * @brief 4KB page containing multiple packed nodes
 *
 * Layout:
 *   - PageHeader (64 bytes)
 *   - 8 node slots, each 504 bytes (total 4032 bytes)
 */
struct PackedPage {
    static constexpr size_t PAGE_SIZE = 4096;
    static constexpr size_t HEADER_SIZE = 64;
    static constexpr size_t NODE_SLOT_SIZE = 504;
    static constexpr size_t NODES_PER_PAGE = 8;

    struct PageHeader {
        uint64_t magic;           // 0x464C4F574B560001 ("FLOWKV" + version)
        uint64_t page_id;
        uint8_t  slot_bitmap;     // Which slots are allocated (bit mask)
        uint8_t  slot_types[8];   // NodeType for each slot
        uint8_t  reserved[39];
    };

    PageHeader header;
    alignas(8) uint8_t slots[NODES_PER_PAGE][NODE_SLOT_SIZE];
    
    // ... methods ...
};
```

### 3.3 修改后的 Masstree 节点结构

**internode 结构** (内部节点):

```cpp
template <typename P>
class internode : public node_base<P> {
public:
    uint8_t nkeys_;
    uint32_t height_;
    ikey_type ikey0_[width];
    
#ifdef HMASSTREE_EXTERNAL_STORAGE
    node_base<P>* child_[width + 1];        // 内存指针 (遍历用)
    node_base<P>* parent_;
    NodeHandle child_handles_[width + 1];   // 持久化 handle
    NodeHandle parent_handle_;
    NodeHandle self_handle_;                // 自身 handle
#else
    node_base<P>* child_[width + 1];
    node_base<P>* parent_;
#endif
    // ...
};
```

**leaf 结构** (叶子节点):

```cpp
template <typename P>
class leaf : public node_base<P> {
public:
    int8_t extrasize64_;
    uint8_t modstate_;
    uint8_t keylenx_[width];
    typename permuter_type::storage_type permutation_;
    ikey_type ikey0_[width];
    leafvalue_type lv_[width];
    external_ksuf_type* ksuf_;
    
#ifdef HMASSTREE_EXTERNAL_STORAGE
    union { leaf<P>* ptr; uintptr_t x; } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
    NodeHandle self_handle_;
    NodeHandle next_handle_;
    NodeHandle prev_handle_;
    NodeHandle parent_handle_;
#else
    // ... original layout ...
#endif
    // ...
};
```

### 3.4 序列化格式

**ExternalInternodeLayout** (504 bytes):

```cpp
struct ExternalInternodeLayout {
    static constexpr size_t MAX_WIDTH = 15;
    
    // Header (16 bytes)
    uint8_t  nkeys;
    uint8_t  reserved1;
    uint16_t reserved2;
    uint32_t height;
    uint64_t version;           // nodeversion for OCC
    
    // Keys (15 * 8 = 120 bytes)
    uint64_t ikey0[MAX_WIDTH];
    
    // Children handles (16 * 8 = 128 bytes)
    NodeHandle children[MAX_WIDTH + 1];
    
    // Parent (8 bytes)
    NodeHandle parent;
    
    // Padding
    uint8_t padding[232];
};
```

**ExternalLeafLayout** (504 bytes):

```cpp
struct ExternalLeafLayout {
    static constexpr size_t MAX_WIDTH = 15;
    
    // Header (40 bytes)
    uint64_t version;
    uint64_t permutation;
    int8_t   extrasize64;
    uint8_t  modstate;
    uint8_t  keylenx[MAX_WIDTH];
    uint8_t  reserved[5];
    
    // Keys (120 bytes)
    uint64_t ikey0[MAX_WIDTH];
    
    // Values/layer handles (120 bytes)
    union { uint64_t raw; NodeHandle layer_handle; } lv[MAX_WIDTH];
    
    // Navigation (24 bytes)
    NodeHandle next;
    NodeHandle prev;
    NodeHandle parent;
    
    // Phantom epoch (8 bytes)
    uint64_t phantom_epoch;
    
    uint8_t padding[192];
};
```

### 3.5 NodeCache - 页面缓存

```cpp
class NodeCache {
private:
    NodeCacheConfig config_;
    IndexStorageManager* storage_;
    
    // Sharded hash map
    struct Shard {
        std::shared_mutex mutex;
        std::unordered_map<uint64_t, std::unique_ptr<CachedPage>> pages;
        std::vector<uint64_t> clock_list;  // Clock 淘汰算法
        size_t clock_hand = 0;
    };
    std::vector<Shard> shards_;
    
    // Eviction callback for NodeResolver notification
    EvictCallback evict_callback_;

public:
    CachedPage* get_or_load(NodeHandle handle, bool pin = true);
    size_t evict_pages(size_t target_bytes);
    void set_evict_callback(EvictCallback callback);
    // ...
};
```

### 3.6 NodeResolver - 按需加载器

```cpp
template <typename P>
class NodeResolver {
public:
    using node_type = node_base<P>;
    
    // 核心解析函数 - 从 handle 获取内存节点指针
    node_type* resolve(NodeHandle handle, threadinfo_type& ti) {
        if (handle.is_null()) return nullptr;

        // Fast path: 已加载的节点
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            auto it = handle_to_node_.find(handle.raw());
            if (it != handle_to_node_.end()) {
                return it->second;
            }
        }
        
        // Slow path: 从存储加载
        return load_and_register(handle, ti);
    }
    
    // 页面淘汰回调 - 清理失效指针
    size_t on_page_evicted(uint64_t page_id, const PackedPage* page_ptr);

private:
    NodeCache* cache_;
    std::shared_mutex mutex_;
    std::unordered_map<uint64_t, node_type*> handle_to_node_;
};
```

---

## 4. 关键数据处理流程

### 4.1 冷启动流程

```
┌─────────────────────────────────────────────────────────────┐
│                    Cold Restart Flow                         │
└─────────────────────────────────────────────────────────────┘

1. 打开存储文件
   IndexStorageManager::open()
         │
         ▼
2. 读取 Superblock (Page 0)
   - 验证 magic number
   - 获取 root_handle
         │
         ▼
3. 加载 root 节点
   NodeResolver::resolve(root_handle, ti)
   - NodeCache::get_or_load(root_handle)
   - 反序列化到内存节点
         │
         ▼
4. 设置 table root
   table_.set_root(root_node)
         │
         ▼
5. 完成 (仅加载了 root, ~1ms)
```

### 4.2 按需加载流程 (reach_leaf)

```
┌─────────────────────────────────────────────────────────────┐
│                  On-Demand Loading Flow                      │
└─────────────────────────────────────────────────────────────┘

查询 key K:
         │
         ▼
reach_leaf(ka, version, ti):
         │
    ┌────┴────┐
    │ 当前节点 │
    │  node   │
    └────┬────┘
         │
         ▼ (是 internode?)
    ┌─────────────────────────────────────────┐
    │ 查找子节点位置 p = lower_bound(ka)       │
    │                                         │
    │ if (child_[p] == nullptr &&             │
    │     child_handles_[p].is_valid()) {     │
    │                                         │
    │   // 按需加载                           │
    │   child_[p] = resolver->resolve(        │
    │       child_handles_[p], ti);           │
    │                                         │
    │   // 设置父指针                          │
    │   child_[p]->set_parent(node);          │
    │ }                                       │
    │                                         │
    │ node = child_[p];                       │
    └──────────────────────┬──────────────────┘
                           │
                           ▼
         repeat until node->isleaf()
                           │
                           ▼
               return leaf_type*(node)
```

### 4.3 页面淘汰流程 (Eviction Callback)

```
┌─────────────────────────────────────────────────────────────┐
│                   Page Eviction Flow                         │
└─────────────────────────────────────────────────────────────┘

内存超过阈值:
         │
         ▼
NodeCache::evict_pages(target_bytes):
         │
         ▼
for each shard:
    │
    ▼
select_victim_clock():   ──▶  Clock 算法选择牺牲页
    │
    ▼
request_eviction(page):
    │
    ▼
process_pending_evictions():
    │
    ├───▶ 调用 evict_callback_(page_id, page_ptr)
    │           │
    │           ▼
    │     HMasstreeWrapper 遍历所有 NodeResolver
    │           │
    │           ▼
    │     NodeResolver::on_page_evicted(page_id):
    │     - 清除 handle_to_node_ 中该页面的所有条目
    │
    ├───▶ 写回脏页 (如有)
    │
    └───▶ 释放页面内存, 更新统计
```

### 4.4 序列化流程

```
┌─────────────────────────────────────────────────────────────┐
│                   Serialization Flow                         │
└─────────────────────────────────────────────────────────────┘

serialize_all_nodes():
         │
         ▼
深度优先遍历树:
    for each node (internode/leaf):
        │
        ▼
    获取 page from cache:
    page = cache->get_or_load(node->self_handle_)
        │
        ▼
    序列化节点到 slot:
    if (node->isleaf())
        serialize_leaf(node, page->get_slot_ptr(slot))
    else
        serialize_internode(node, page->get_slot_ptr(slot))
        │
        ▼
    标记页面为脏:
    page->mark_dirty()
        │
        ▼
persist_tree_structure():
    storage->set_root_handle(root->self_handle_)
    storage->persist_metadata()
    storage->sync()
```

---

## 5. 与原版 Masstree 的对比

### 5.1 代码层面差异

| 组件 | 原版 Masstree | HMasstree (外存模式) |
|------|--------------|---------------------|
| 子节点引用 | `child_[p]` 指针 | `child_[p]` + `child_handles_[p]` 双重存储 |
| 父节点引用 | `parent_` 指针 | `parent_` + `parent_handle_` |
| 叶链表 | `next_`, `prev_` 指针 | 额外维护 `next_handle_`, `prev_handle_` |
| 节点分配 | `pool_allocate()` | 额外调用 `allocate_node_slot()` 获取 handle |
| 空间占用 | ~320B/leaf, ~280B/internode | 增加 ~128B/node (handles 数组) |

### 5.2 性能对比 (10M keys)

| 指标 | 原版 Masstree | HMasstree (1024MB) | HMasstree (256MB) |
|------|--------------|-------------------|-------------------|
| 启动时间 | ~15s (全加载) | 0.8 ms | 0.86 ms |
| 启动内存 | ~1480 MB | 84 MB → 995 MB | 84 MB → 715 MB |
| 随机读 P50 | ~0.5 µs | 4.7 µs | 202 µs |
| 随机读 P99 | ~2 µs | 329 µs | 545 µs |
| 查找成功率 | 100% | 100% | 99.74% |

### 5.3 优势与劣势

**优势**:
- ✅ 冷启动快 1875x（从 15s 降到 0.8ms）
- ✅ 启动内存减少 94%（从 1480MB 到 84MB）
- ✅ 支持内存受限运行
- ✅ 数据自动持久化

**劣势**:
- ⚠️ 随机读延迟增加 ~10-400x（磁盘 I/O 开销）
- ⚠️ 内存占用超过 cache 限制（反序列化节点内存未释放）
- ⚠️ 部分 key 查找失败（1-3%）
- ⚠️ 每节点额外 ~128 字节空间开销

---

## 6. 测试结果与问题分析

### 6.1 10M Keys 冷启动测试结果

| Cache 大小 | 冷启动时间 | 启动内存 | 读后内存 | Cache 用量 | 顺序读成功 | 随机读成功 |
|-----------|-----------|---------|---------|-----------|-----------|-----------|
| 1024 MB | 0.80 ms | 84.4 MB | 994.8 MB | 403.6 MB | 100.00% | 100.00% |
| 256 MB | 0.86 ms | 84.4 MB | 714.7 MB | 207.9 MB | 99.95% | 99.74% |
| 128 MB | 0.85 ms | 84.4 MB | 442.3 MB | 41.9 MB | 99.36% | 99.66% |
| 64 MB | 0.87 ms | 84.3 MB | 282.6 MB | 17.7 MB | 98.69% | 99.36% |
| 32 MB | 0.81 ms | 84.3 MB | 211.2 MB | 2.4 MB | 97.66% | 99.33% |

### 6.2 读性能详情

```
Cache = 1024 MB (无限制):
  Sequential: P50: 141 µs, P99: 705 µs
  Random:     P50: 4.7 µs, P99: 329 µs

Cache = 256 MB:
  Sequential: P50: 217 µs, P99: 706 µs
  Random:     P50: 202 µs, P99: 545 µs

Cache = 32 MB:
  Sequential: P50: 232 µs, P99: 1024 µs
  Random:     P50: 203 µs, P99: 746 µs
```

### 6.3 问题分析

#### 问题 1: 进程 RSS 超过 Cache 限制

**现象**: 配置 32MB cache，实际进程内存达到 211MB

**原因**:
```
进程 RSS = NodeCache page 内存 (受控) 
         + NodeResolver 反序列化节点内存 (未受控)
         + 其他
```

反序列化节点使用 `ti.pool_allocate()` 分配，属于线程本地内存池，在 `on_page_evicted()` 回调中无法释放（需要对应线程的 threadinfo）。

**影响**: 内存限制效果打折扣

#### 问题 2: 1-3% Key 查找失败

**现象**: 小 cache 配置下，部分 key 无法找到

**可能原因**:
1. Masstree 多层 trie 结构中的 layer 指针解析问题
2. 缓存淘汰后重新加载时的版本不一致
3. `is_unresolved_layer()` 判断的边界情况

#### 问题 3: 读延迟较高

**现象**: 即使 cache 充足，P50 延迟也达到 141µs（原版 <1µs）

**原因**:
1. 每次访问需要经过 NodeResolver 查找
2. 首次访问需要反序列化（内存拷贝）
3. 锁竞争（shared_mutex）

#### 问题 4: Cache 统计不准确

**现象**: Cache Hit Rate 显示 0%

**原因**: 统计 counter 未正确更新

---

## 7. 任务清单

### 7.1 高优先级 - 核心功能修复

| ID | 任务 | 描述 | 预估工作量 |
|----|------|------|-----------|
| P1-1 | 修复 Key 查找 100% 成功率 | 调查并修复 1-3% 查找失败的根本原因，涉及 layer 解析和版本控制 | 2-3 天 |
| P1-2 | 实现节点内存释放 | 在 `on_page_evicted()` 中释放反序列化节点内存，方案: (1) 维护 handle→threadinfo 映射，或 (2) 使用全局内存池 | 3-5 天 |
| P1-3 | 修复 Cache 统计收集 | 正确更新 cache_hits/cache_misses 计数器 | 0.5 天 |

### 7.2 中优先级 - 性能优化

| ID | 任务 | 描述 | 预估工作量 |
|----|------|------|-----------|
| P2-1 | 降低 NodeResolver 开销 | 优化 handle_to_node_ 查找路径，考虑无锁数据结构 | 2-3 天 |
| P2-2 | 批量预加载 | 实现 prefetch 机制，根据访问模式预加载邻近节点 | 3-5 天 |
| P2-3 | 减少反序列化开销 | 考虑直接使用 mmap 数据，避免拷贝 | 5-7 天 |
| P2-4 | 优化 Clock 淘汰算法 | 实现 CLOCK-Pro 或自适应淘汰策略 | 3-5 天 |

### 7.3 低优先级 - 功能增强

| ID | 任务 | 描述 | 预估工作量 |
|----|------|------|-----------|
| P3-1 | 存储压缩 | 压缩存储文件减少磁盘空间 | 3-5 天 |
| P3-2 | 并发写入优化 | 优化多线程写入时的锁竞争 | 5-7 天 |
| P3-3 | 崩溃恢复增强 | 实现 WAL 日志保证一致性 | 7-10 天 |
| P3-4 | 热点页面预加载 | 启动时预加载高频访问路径 | 2-3 天 |

### 7.4 结合设计目标的完善任务

| 设计目标 | 当前状态 | 差距 | 完善任务 |
|---------|---------|------|---------|
| 冷启动 <10ms | ✅ 0.8ms | 已达成 | - |
| 查找 100% 正确 | ⚠️ 97-100% | 1-3% 缺失 | P1-1 |
| 内存受控 | ⚠️ 超限 | 反序列化内存未释放 | P1-2 |
| 热点读性能 | ⚠️ P50=5µs | 比原版慢 10x | P2-1, P2-3 |
| 生产可用 | ❌ | 多个问题 | 完成 P1 全部任务 |

---

## 8. 开发指南

### 8.1 编译配置

```bash
# 启用 HMasstree 外存模式
cmake -DUSE_HMASSTREE=ON -DHMASSTREE_EXTERNAL_STORAGE=ON ..

# 编译选项
-DFLOWKV_KEY16          # 启用 16 字节 key
-DHMASSTREE_EXTERNAL_STORAGE  # 启用外存模式
```

### 8.2 使用示例

```cpp
#include "hmasstree_wrapper.h"

// 初始化
HMasstreeWrapper mt;
Masstree::ExternalIndexConfig config;
config.storage_path = "/data/hmasstree.dat";
config.cache_size_mb = 256;
mt.init_external_storage(config);
mt.thread_init_external(1);

// 恢复数据 (冷启动)
if (mt.has_persisted_tree()) {
    mt.restore_from_storage();
}

// 写入
Key16 key(0, 12345);
ValueHelper vh(100);
mt.insert(key, vh);

// 查询
uint64_t value;
bool found = mt.search(key, value);

// 持久化
mt.serialize_all_nodes();
mt.persist_tree_structure();
```

### 8.3 调试技巧

```cpp
// 打印外存统计
mt.print_external_stats();

// 检查节点内存
std::cout << "Resolver nodes: " << resolver->size() << std::endl;
std::cout << "Cache pages: " << cache->page_count() << std::endl;
```

---

## 附录 A: 文件清单

| 文件 | 功能 |
|------|------|
| `node_handle.h` | NodeHandle 和 PackedPage 定义 |
| `node_cache.h/cpp` | 页面缓存实现 |
| `node_resolver.h` | 按需加载器 |
| `node_serializer.h` | 序列化/反序列化 |
| `index_storage.h/cpp` | 磁盘存储管理 |
| `external_index.h` | 外存管理器集成 |
| `external_node.h` | 外存节点布局定义 |
| `hmasstree_wrapper.h` | 对外 API 封装 |
| `masstree_struct.hh` | 修改后的节点结构 |
| `masstree_split.hh` | 修改后的分裂逻辑 |

---

## 附录 B: 测试报告链接

完整测试报告见: [cold_restart_memory_limited_report.md](cold_restart_memory_limited_report.md)
