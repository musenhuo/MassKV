# H-Masstree 外存方案内存分析

## 1. 当前测试问题诊断

### 1.1 为什么 P99 延迟低于 1 微秒？

**根本原因**: 当前的"双存储策略"完全绕过了缓存层，所有操作都在内存中完成。

```cpp
// 当前 internode 结构（masstree_struct.hh 第 175-184 行）
#ifdef HMASSTREE_EXTERNAL_STORAGE
    node_base<P>* child_[width + 1];        // 原始指针 → 用于遍历
    node_base<P>* parent_;                  
    NodeHandle child_handles_[width + 1];   // Handle → 仅用于持久化
    NodeHandle parent_handle_;              
    NodeHandle self_handle_;                
#endif
```

**关键问题**: 
- `reach_leaf()`, `advance_to_key()` 等所有遍历函数使用 `child_[]` 指针
- Handle 从未被用于查询，Cache Hit/Miss 统计始终为 0
- 节点永远不会被驱逐，因为遍历不经过缓存层

### 1.2 如何让外存模式真正工作

有两种方案：

**方案A: 实现 Pointer Swizzling (推荐)**
- 使用联合体 `SwizzledPointer` 代替双数组
- 允许驱逐时将指针转换为 PID
- 访问时透明地从 SSD 加载

**方案B: 模拟冷启动**
- 先将数据持久化到磁盘
- 销毁内存中的树结构
- 重新从磁盘加载（每次访问触发 I/O）

---

## 2. Pointer Swizzling 为何能节省 30% 内存？

### 2.1 您的疑问

> "Pointer Swizzling 需要子节点维护一个指向父节点的指针，理论上内存占用应该更高？"

这是一个很好的问题。让我澄清设计中的细节：

### 2.2 Back Pointer 的存储位置

**关键区别**: Back pointer 存储在**缓存元数据 (CachedNode)** 中，而不是树节点本身。

```cpp
// CachedNode（缓存层元数据，不在树节点内）
struct CachedNode {
    void* node_ptr;                          // 节点内存
    NodeHandle handle;                       // 节点 handle
    std::vector<SwizzledPointer*> back_pointers;  // 父节点槽位指针
    SpinLock bp_lock;
    uint64_t swizzle_epoch;
};
```

这意味着：
- **树节点内存**: 减少（不再需要 `child_handles_[]`）
- **缓存元数据**: 增加（需要维护 back_pointers）
- **净效果**: 节省内存（因为缓存元数据开销远小于每个节点的 handle 数组）

### 2.3 详细内存计算

**当前双存储方案（每个 internode）**:
```
child_[16]:          16 × 8 = 128 bytes  (指针数组)
child_handles_[16]:  16 × 8 = 128 bytes  (handle 数组) ← 额外开销
parent_:             8 bytes
parent_handle_:      8 bytes              ← 额外开销  
self_handle_:        8 bytes              ← 额外开销
---------------------------------------
额外开销:            144 bytes / internode
```

**Pointer Swizzling 方案（每个 internode）**:
```
children_[16]:       16 × 8 = 128 bytes  (SwizzledPointer 联合体)
parent_:             8 bytes
---------------------------------------
额外开销:            0 bytes / internode
```

**缓存层 back_pointer 开销（每个被缓存的节点）**:
```
std::vector 开销:     24 bytes (capacity, size, data ptr)
1 个 back_pointer:    8 bytes
---------------------------------------
缓存元数据开销:       ~32 bytes / 缓存节点
```

### 2.4 1M Keys 场景计算

假设 1M keys，约 60K 个 internode：

| 方案 | 节点内额外开销 | 缓存元数据开销 | 总额外开销 |
|------|---------------|---------------|-----------|
| 双存储 | 60K × 144 = 8.6 MB | 0 | **8.6 MB** |
| Pointer Swizzling | 0 | 60K × 32 = 1.9 MB | **1.9 MB** |

**节省内存**: 8.6 - 1.9 = **6.7 MB** (~30% 的外存模式额外开销)

### 2.5 为什么 Pointer Swizzling 更高效？

1. **联合体复用**: 同一个 64 位字段根据最高位区分是指针还是 PID，不需要两个数组
2. **Back pointer 是稀疏的**: 只有当前在缓存中的节点需要 back_pointer
3. **热路径零开销**: 已 swizzle 的节点访问和纯内存模式完全相同

---

## 3. 其他减少内存占用的方案

### 3.1 方案一：仅保留 Handle（牺牲性能）

完全移除 `child_[]` 指针，所有访问都通过 handle 解析。

**实现**:
```cpp
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // 仅保留 handle
    NodeHandle child_handles_[width + 1];
    NodeHandle parent_handle_;
#endif
```

**优点**:
- 内存与纯内存模式相同
- 实现简单

**缺点**:
- **每次访问都需要缓存查询**（~50-100 cycles overhead）
- 吞吐率可能下降 2-3 倍

**适用场景**: 内存极度受限，愿意牺牲性能

### 3.2 方案二：仅 Leaf 节点使用 Handle

Internode 保持内存指针，仅叶节点使用 handle。

**理由**:
- Internode 数量少（~5% 的总节点），但访问频繁
- Leaf 节点数量多（~95%），但访问相对集中

**实现**:
```cpp
struct internode {
    node_base<P>* child_[width + 1];  // 保持指针（热节点）
    // 无 child_handles_[]
};

struct leaf {
    NodeHandle lv_handles_[width];    // 子树 layer 使用 handle
    NodeHandle next_handle_;          // 链表 next 使用 handle
};
```

**优点**:
- Internode 遍历零开销
- 叶节点可按需加载

**缺点**:
- 实现复杂度增加
- 跨 layer 访问仍需 handle 解析

### 3.3 方案三：延迟 Handle 分配

仅在节点首次被驱逐时分配 handle。

**理由**: 如果内存充足，很多节点永远不会被驱逐，不需要 handle。

**实现**:
```cpp
struct internode {
    node_base<P>* child_[width + 1];
    
    // 延迟分配：仅在需要持久化时创建
    NodeHandle* deferred_handles_;  // 初始为 nullptr
    
    void ensure_handles_allocated() {
        if (!deferred_handles_) {
            deferred_handles_ = new NodeHandle[width + 1];
        }
    }
};
```

**优点**:
- 内存充足时零额外开销
- 按需分配，自适应

**缺点**:
- 增加条件检查开销
- 持久化时需要遍历分配

### 3.4 方案对比

| 方案 | 内存开销 | 遍历性能 | 实现复杂度 |
|------|---------|---------|-----------|
| 当前双存储 | +144 bytes/node | 零 | 低 |
| Pointer Swizzling | +32 bytes/cached | 零(热) | 高 |
| 仅 Handle | 0 | -50% | 低 |
| Leaf-only Handle | +16 bytes/internode | 接近零 | 中 |
| 延迟分配 | 0~144 bytes | 零 | 中 |

---

## 4. 推荐路径

### 短期（1-2 周）
1. **修复当前测试**: 实现真正的冷启动测试（从磁盘加载后测试）
2. **验证外存加载路径**: 确保 handle→缓存→加载 路径正常工作

### 中期（1 个月）
3. **实现 Pointer Swizzling 原型**: 从 internode 开始
4. **性能对比**: 热路径 vs 冷路径延迟

### 长期（2-3 个月）
5. **完整 Pointer Swizzling**: 包括并发控制、epoch 保护
6. **自适应驱逐策略**: 基于内存压力触发驱逐

---

## 5. 测试修改建议

要正确测试外存模式，需要：

### 5.1 真正的冷启动测试

```cpp
// 1. 插入数据并持久化
HMasstreeWrapper mt;
mt.init_external_storage(config);
for (size_t i = 0; i < N; i++) {
    mt.insert(keys[i], values[i]);
}
mt.flush_external_storage();
mt.shutdown();

// 2. 清空页面缓存（需要 root）
system("sync && echo 3 > /proc/sys/vm/drop_caches");

// 3. 重新加载并测试
HMasstreeWrapper mt2;
mt2.init_external_storage(config);  // 从磁盘恢复
mt2.recover_from_storage();         // 需要实现恢复逻辑

// 4. 现在的读操作会触发实际的 SSD 加载
for (size_t i = 0; i < N; i++) {
    mt2.search(keys[i], val);  // 每次都需要加载
}
```

### 5.2 强制缓存限制测试

```cpp
// 极小的缓存大小，强制驱逐
Masstree::ExternalIndexConfig config;
config.cache_size_mb = 1;  // 仅 1MB 缓存

// 1M keys 约需要 50MB，缓存命中率会很低
```

---

*分析时间: 2026-02-03*
