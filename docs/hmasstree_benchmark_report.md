# H-Masstree 性能对比测试报告

## 一、测试配置

| 参数 | 值 |
|------|-----|
| 数据规模 | 1,000,000 keys |
| 操作次数 | 1,000,000 ops |
| 读写比例 | 80% 读 / 20% 写 |
| Key 大小 | 16 bytes (Key16) |
| Value 大小 | 16 bytes |
| 缓存大小 | 4 MB (外存模式) |
| CPU | 单线程测试 |

## 二、测试结果汇总 (v2 - 2026-02-03)

### 2.1 三种实现对比

| 指标 | 原版 Masstree | H-Masstree (内存) | H-Masstree (外存) |
|------|--------------|------------------|------------------|
| 内存增量 | 42.4 MB | 42.4 MB | **54.5 MB** (+28%) |
| 插入吞吐 | 2,843 K/s | 2,927 K/s | 5,233 K/s |
| 读取吞吐 | 2,788 K/s | **5,570 K/s** (+100%) | 5,640 K/s |
| 混合吞吐 | 2,756 K/s | **5,659 K/s** (+105%) | 5,673 K/s |

### 2.2 关键发现

**1. H-Masstree 读性能显著优于原版 Masstree**
- 读吞吐提升约 **2 倍** (2788 → 5570 K ops/sec)
- 可能原因：H-Masstree 的节点结构优化或预取策略

**2. 外存模式内存开销增加约 28%**
- 原因：双存储策略 (child_[] + child_handles_[])
- 每个 internode 额外开销约 144 bytes

**3. 外存模式未触发实际 SSD I/O**
- P99 延迟仍低于 1 微秒（全部命中内存）
- 原因：双存储策略使所有遍历通过原始指针完成
- 需要实现 Pointer Swizzling 才能真正测试外存性能

## 四、优化建议

### 4.1 短期优化（高 ROI）

#### 4.1.1 消除双存储开销 - Pointer Swizzling

**当前问题**: 每个节点同时存储指针和 handle，浪费内存
**解决方案**: 使用 Pointer Swizzling，复用同一 64 位字段

```cpp
// 当前实现（浪费内存）
node_base* child_[16];      // 8 bytes × 16 = 128 bytes
NodeHandle child_handles_[16];  // 8 bytes × 16 = 128 bytes

// Pointer Swizzling 优化
union SwizzledPtr {
    node_base* ptr;     // 当 LSB = 0: 已解析的指针
    uint64_t handle;    // 当 LSB = 1: 未解析的 handle
} children_[16];        // 只需 128 bytes
```

**预期收益**:
- 内存占用减少 ~30%（恢复到内存模式水平）
- 缓存行利用率提升

#### 4.1.2 延迟加载叶节点

**当前问题**: 所有节点在内存中，未利用外存模式的内存节省潜力
**解决方案**: 仅保留 internode 在内存，叶节点按需加载

```cpp
leaf<P>* resolve_leaf(SwizzledPtr ptr) {
    if (is_swizzled(ptr)) {
        return reinterpret_cast<leaf<P>*>(ptr.ptr);
    }
    // 从外存加载叶节点
    return cache_->get_or_load<leaf<P>>(ptr.handle);
}
```

**预期收益**:
- 内存占用可降低 50-70%（叶节点占总节点的 ~60%）
- 读放大换取内存节省

### 4.2 中期优化（中等 ROI）

#### 4.2.1 批量预取

在 range scan 时预取后续叶节点：
```cpp
void prefetch_next_leaves(leaf<P>* current, int depth = 2) {
    for (int i = 0; i < depth; i++) {
        NodeHandle h = current->next_handle();
        if (h.is_valid()) {
            cache_->prefetch_async(h);
            current = current->next_ptr();
        }
    }
}
```

#### 4.2.2 冷热分层

- **热节点**: 频繁访问的 internode 和叶节点，保持在内存
- **冷节点**: 低频访问的叶节点，允许驱逐
- 基于 Clock 算法或 LRU 实现

### 4.3 长期优化（架构级别）

#### 4.3.1 完整 Pointer Swizzling 实现

参考设计文档 Phase 7-10：
1. Phase 7: 引入 swizzling 标记位
2. Phase 8: SwizzledPtr 类型安全封装
3. Phase 9: 自动 swizzle/unswizzle 逻辑
4. Phase 10: 并发控制和内存压力驱逐

#### 4.3.2 异步 I/O 集成

使用 io_uring 实现真正的异步加载：
```cpp
// 提交异步加载请求
cache_->submit_async_load(handle, [this](CachedNode* node) {
    // 加载完成后的回调
    process_node(node);
});
```

## 五、当前状态总结

### 5.1 已完成功能

| 阶段 | 功能 | 状态 |
|------|------|------|
| Phase 1-3 | 基础架构 (NodeHandle, NodeCache, Storage) | ✅ |
| Phase 4 | 写路径 (node_factory, split) | ✅ |
| Phase 5 | 扫描路径 (scan_context) | ✅ |
| Phase 6 | FlowKV 集成 (external_index) | ✅ |
| Phase 6.5 | 核心解析 (reach_leaf, advance_to_key) | ✅ |
| Phase 7 | 删除路径 (locked_parent, gc_layer) | ✅ |
| Phase 8 | 索引适配层 (ScanContextGuard) | ✅ |
| Phase 9 | 持久化 (Superblock, metadata, recovery) | ✅ |
| Phase 10 | 后台刷盘、销毁流程 | ✅ |

### 5.2 待完成功能（建议优先级）

| 优先级 | 功能 | 描述 | 预期收益 |
|--------|------|------|---------|
| P0 | Pointer Swizzling | 消除双存储开销 | 内存 -30% |
| P1 | 延迟叶节点加载 | 按需加载叶节点 | 内存 -50% |
| P2 | 批量预取 | Scan 性能优化 | 延迟 -20% |
| P2 | 冷热分层 | 智能缓存管理 | 内存效率提升 |
| P3 | 异步 I/O | io_uring 集成 | 吞吐率提升 |

## 六、测试复现

```bash
# 运行对比测试
cd /home/zwt/yjy/FlowKV/lib/hmasstree
./run_benchmark_comparison.sh 1000000 64 1000000

# 单独运行内存模式
./../../build_hmasstree/benchmark_memory -n 1000000 -o 1000000

# 单独运行外存模式
./../../build_hmasstree/benchmark_external -n 1000000 -o 1000000 -c 64
```

---

*报告生成时间: $(date)*
*测试环境: Linux / GCC 13 / O3 优化*
