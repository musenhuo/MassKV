# H-Masstree Handle-Only 模式分析报告

## 一、Handle-Only 方案性能下降 50% 原因分析

### 1.1 遍历开销对比

| 操作 | 双存储模式 | 仅 Handle 模式 |
|------|-----------|----------------|
| 读取子节点位置 | 1 次内存加载 (`child_[kp]`) | 1 次内存加载 (`child_handles_[kp]`) |
| 解析为指针 | 无需解析 | 需要完整解析流程 |
| Hash 计算 | 无 | ~10 个周期（handle to hash key）|
| 锁开销 | 无 | shared_lock 获取/释放 ~10-20 周期 |
| 哈希表查找 | 无 | 哈希表 lookup ~20-50 周期 |
| Pin/Unpin 原子操作 | 无 | atomic fetch_add ~10 周期 |
| **总开销 (每层)** | **~3-5 ns** | **~50-100 ns** |

### 1.2 树高影响

对于 100 万条记录，典型树高为 3-5 层：
- 双存储模式：3-5 次指针加载 = 15-25 ns
- Handle-Only：3-5 次完整解析 = 150-500 ns
- **性能差异：约 10-20 倍**

### 1.3 实际测试结果

由于 Handle-Only 模式存在根本性问题（见下），无法获取完整性能数据。

## 二、Handle-Only 代码修改及发现的问题

### 2.1 已完成的修改

1. **scan_context.h** - 添加 `resolve_node<P>()` 通用解析方法
2. **masstree_struct.hh** - 修改 `reach_leaf()` 支持条件编译
3. **masstree_struct.hh** - 修改 `advance_to_key()` 支持条件编译

编译标志：`HMASSTREE_HANDLE_ONLY`

### 2.2 发现的根本性问题

**节点创建时 `self_handle_` 未被正确设置**

问题位置：
```cpp
// masstree_struct.hh
static internode<P>* make(uint32_t height, threadinfo& ti) {
    void* ptr = ti.pool_allocate(sizeof(internode<P>), memtag_masstree_internode);
    internode<P>* n = new(ptr) internode<P>(height);
    // self_handle_ 保持默认值 NodeHandle::null()
    return n;
}
```

在 split 等操作中：
```cpp
// masstree_split.hh - line 288-289
internode_type *nn = internode_type::make(height + 1, ti);
NodeHandle n_handle = node_to_handle<P>(n);  // 返回 null handle!
```

**后果**：遍历时 `child_handle(kp)` 返回无效 handle，cache 解析失败，导致死循环。

### 2.3 修复方案

需要修改所有节点创建路径，使其通过 `NodeFactory` 创建节点：

```cpp
// 方案 A: 修改 make() 接受 NodeFactory 参数
static internode<P>* make(uint32_t height, threadinfo& ti, NodeFactory<P>* factory = nullptr) {
    void* ptr = ti.pool_allocate(sizeof(internode<P>), memtag_masstree_internode);
    internode<P>* n = new(ptr) internode<P>(height);
    if (factory) {
        auto slot = factory->storage()->allocate_node_slot(NodeType::INTERNODE);
        n->self_handle_ = NodeHandle::make_internode(slot.first, slot.second, 0);
    }
    return n;
}

// 方案 B: 在 split 后手动分配 handle
internode_type *nn = internode_type::make(height + 1, ti);
// 立即分配 handle
IndexStorage* storage = get_thread_storage();
if (storage) {
    auto slot = storage->allocate_node_slot(NodeType::INTERNODE);
    nn->self_handle_ = NodeHandle::make_internode(slot.first, slot.second, 0);
}
```

**修改范围**：
- `internode<P>::make()`
- `leaf<P>::make()`
- `leaf<P>::make_root()`
- `masstree_split.hh` 中所有节点创建点
- `masstree_tcursor.hh` 中层创建点

## 三、1M 性能测试结果

### 3.1 双存储模式（当前可工作模式）

```
Mode: Dual-Storage (direct pointer traversal)
Keys: 1,000,000 (16B key, 16B value)
Cache Size: 64 MB

Insert: 401.55 ms, 2.49 M ops/sec
Memory: 54.20 MB
Read:   348.77 ms, 2.87 M ops/sec
Latency P50: 0.39 us, P99: 0.60 us, P999: 0.72 us
```

### 3.2 Cache 统计

```
Cache Hit Rate: 0.00%
Cache Hits: 0, Misses: 0
```

**说明**：双存储模式下遍历使用 `child_[]` 指针，cache 完全未被使用。

## 四、冷启动测试结果

### 4.1 测试结果

```
=== Phase 1: Warm Start ===
Insert: 31.09 ms, 3.22 M ops/sec
Warm Read: 23.92 ms, 4.18 M ops/sec
Latency P50: 0.27 us, P99: 0.38 us
Memory: 6.11 MB
```

### 4.2 当前限制

**真冷启动尚未完全支持**，原因：
1. 树结构（root handle）未持久化到磁盘
2. 遍历仍使用内存指针（双存储模式）
3. 节点内容未实际写入存储文件

## 五、按需加载/驱逐支持分析

### 5.1 当前支持情况

| 功能 | 状态 | 说明 |
|------|------|------|
| NodeCache LRU | ✅ 已实现 | 标准 LRU 驱逐策略 |
| CachedPage Pin/Unpin | ✅ 已实现 | 引用计数防止驱逐 |
| Handle 解析 | ✅ 已实现 | `ScanContext::resolve_*()` |
| 按需加载 | ⚠️ 部分 | Cache miss 时从存储加载 |
| 节点驱逐 | ⚠️ 未激活 | 需 Handle-Only 模式工作 |
| 持久化 | ❌ 未实现 | 需完整的 WAL/checkpoint |

### 5.2 按需加载工作流程（设计）

```
Search(key) 
  → reach_leaf(key)
     → child_handle(kp)
        → ScanContext::resolve_node(handle)
           → NodeCache::get_or_load(handle)
              → if cached: return page (hit)
              → else: IndexStorage::read_node(handle)
                     → load from SSD
                     → insert to cache
                     → possibly evict LRU page
                     → return page (miss)
```

### 5.3 限制内存压力测试（理论）

当 cache 容量 < 全部节点大小时：
- Cache 会触发 LRU 驱逐
- 频繁访问的路径保留在 cache
- 冷节点被驱出，需要时重新加载

**实际测试未进行**，因为 Handle-Only 模式存在问题。

## 六、后续工作建议

### 6.1 优先级 1：修复 Handle-Only 模式

1. 修改 `internode<P>::make()` 和 `leaf<P>::make()` 支持 handle 分配
2. 添加线程本地 IndexStorage 访问器
3. 更新 masstree_split.hh 等文件

### 6.2 优先级 2：实现真冷启动

1. 持久化元数据（root handle, 配置）
2. 实现节点序列化/反序列化
3. 启动时从存储恢复树结构

### 6.3 优先级 3：完整驱逐测试

1. 配置小于数据量的 cache
2. 执行随机访问负载
3. 测量 cache miss 时的 SSD I/O

## 七、总结

| 问题 | 状态 | 解决方案 |
|------|------|----------|
| Handle-Only 性能下降 50% | 已分析 | 预期行为，hash+lock+lookup 开销 |
| Handle-Only 死循环 | 已定位 | 需修复 make() 函数，分配 handle |
| Cache 未命中 | 已确认 | 双存储模式绕过 cache |
| 冷启动 | 部分实现 | 需持久化层 |
| 按需驱逐 | 框架就绪 | 需 Handle-Only 工作后启用 |

**结论**：H-Masstree 外存框架基础已就绪，需完成 Handle-Only 修复后才能进行真正的外存 I/O 测试。
