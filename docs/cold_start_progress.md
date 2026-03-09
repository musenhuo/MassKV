# H-Masstree 冷启动支持进展

## 概述

本文档记录了 H-Masstree 外存模式冷启动支持的实现进展。

## 已完成的工作

### 1. 节点 Handle 自动分配

在外存模式下，节点创建时自动分配 `self_handle_`：

- **修改位置**：`masstree_struct.hh` 中的 `leaf<P>::make()` 和 `internode<P>::make()`
- **机制**：使用 `IndexStorageRegistry::get()` 获取线程本地的 `IndexStorageManager`
- **分配**：调用 `allocate_node_slot()` 获取 `(page_id, slot_index)` 对

```cpp
#ifdef HMASSTREE_EXTERNAL_STORAGE
IndexStorageManager* storage = IndexStorageRegistry::get();
if (storage && storage->is_open()) {
    auto slot = storage->allocate_node_slot(NodeType::LEAF);
    if (slot.first != 0) {
        n->self_handle_ = NodeHandle::make_leaf(slot.first, slot.second, 0);
    }
}
#endif
```

### 2. IndexStorageRegistry 线程本地存储访问

新增 `IndexStorageRegistry` 类提供对 `IndexStorageManager` 的线程安全访问：

- **位置**：`index_storage.h`
- **用途**：让节点创建代码（在 `masstree_struct.hh` 中）能够访问外存管理器

```cpp
class IndexStorageRegistry {
public:
    static IndexStorageManager* get() { return tls_storage_; }
    static void set(IndexStorageManager* storage) { tls_storage_ = storage; }
private:
    static inline thread_local IndexStorageManager* tls_storage_ = nullptr;
};
```

### 3. Root Handle 持久化

实现了将根节点 handle 持久化到存储的 superblock：

**Superblock 结构变更**：
```cpp
struct Superblock {
    // ...existing fields...
    uint64_t root_handle_raw;  // 持久化的根节点 handle
    // ...
};
```

**新增方法**（`IndexStorageManager`）：
- `set_root_handle(NodeHandle h)` - 设置 root handle
- `get_root_handle()` - 获取 root handle
- `has_root_handle()` - 检查是否有持久化的 root handle

**Wrapper 层方法**（`HMasstreeWrapper`）：
- `persist_tree_structure()` - 持久化树结构
- `has_persisted_tree()` - 检查是否有持久化的树
- `get_persisted_root_handle()` - 获取持久化的 root handle

### 4. 延迟表初始化

在外存模式下，`table_init()` 延迟到 `thread_init_external()` 之后执行：

```cpp
HMasstreeWrapper() {
#ifndef HMASSTREE_EXTERNAL_STORAGE
    this->table_init();  // 非外存模式立即初始化
#endif
    // 外存模式在 thread_init_external() 后初始化
}

void thread_init_external(int tid) {
    // 设置 IndexStorageRegistry
    IndexStorageRegistry::set(ext_storage_->storage());
    
    // 延迟初始化
    if (!table_initialized_) {
        this->table_init();
        table_initialized_ = true;
    }
}
```

### 5. Page ID 0 保留

修复了 page_id=0 被当作无效值的问题：

- **问题**：segment 0, page 0 的 page_id 计算结果为 0，与"无效"值冲突
- **解决**：在 segment 0 中跳过 page 0，从 page 1 开始分配

```cpp
IndexSegment(uint64_t id) : segment_id(id), ... {
    uint16_t start_page = (id == 0) ? 1 : 0;
    if (id == 0) {
        page_bitmap[0] = true;  // 保留 page 0
        allocated_count = 1;
    }
    for (uint16_t i = start_page; i < PAGES_PER_SEGMENT; ++i) {
        free_pages.push(i);
    }
}
```

## 测试结果

### 冷启动测试 v2

| 规模 | Keys | Cache | Insert 性能 | Read 性能 | 内存使用 |
|------|------|-------|-------------|-----------|----------|
| 小规模 | 100K | 32MB | 3.21 M ops/sec | 4.00 M ops/sec | 6.04 MB |
| 大规模 | 1M | 64MB | 2.35 M ops/sec | 2.71 M ops/sec | 40.18 MB |

### 关键验证点

- ✅ Phase 1: Root handle 成功持久化 (`valid=1, page_id=1`)
- ✅ Phase 2: Root handle 成功从存储恢复 (`valid=1, page_id=1`)
- ✅ 存储文件创建成功（8KB superblock）

## 当前状态

### 支持的场景
- **热重启**（同进程内）：可以持久化和恢复 root handle

### 未支持的场景
- **冷重启**（新进程）：需要节点内容序列化

## 后续工作

要实现真正的冷启动（进程重启后恢复数据），需要：

### 1. 节点内容序列化
- 在 `flush_external_storage()` 时将内存中的 Masstree 节点序列化到存储页面
- 每个节点需要按照 `PackedNodeLayout` 格式写入其分配的页面

### 2. Handle-Only 遍历模式
- 启用 `HMASSTREE_HANDLE_ONLY` 宏
- 修改 `child()` 方法通过 cache 解析子节点 handle
- 实现 `resolve_child_handle()` 方法

### 3. 按需节点加载
- 当 cache miss 时从 SSD 读取节点数据
- 反序列化节点结构
- 更新缓存

### 4. 恢复流程
```
1. 打开存储文件（recover=true）
2. 读取 superblock 获取 root_handle
3. 创建空的 masstree 表
4. 设置 root_handle 到表
5. 首次访问时按需加载根节点
6. 后续访问按需加载其他节点
```

## 文件修改清单

| 文件 | 修改内容 |
|------|----------|
| `index_storage.h` | 添加 `IndexStorageRegistry`，root handle 方法 |
| `node_handle.h` | 添加 `from_raw()` 方法 |
| `masstree_struct.hh` | 节点创建时自动分配 handle |
| `masstree.hh` | 添加 `root_handle()`, `set_root_handle()` 等方法 |
| `hmasstree_wrapper.h` | 添加持久化方法，延迟初始化 |
| `external_index.h` | 默认启用恢复模式 |

---
*更新时间：2024-02*
