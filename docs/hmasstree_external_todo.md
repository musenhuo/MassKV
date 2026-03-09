# H-Masstree External Storage TODO List

## 已完成阶段 (Phase 1-11) ✅

### Phase 1-3: 基础架构
- [x] NodeHandle 64 位编码设计与实现
- [x] CachedNode 封装（Pin/Unpin 引用计数）
- [x] NodeCache 分片哈希表 + Clock 驱逐
- [x] IndexStorageManager 段式存储
- [x] 条件编译框架 (`HMASSTREE_EXTERNAL_STORAGE`)

### Phase 4: 写路径
- [x] node_factory.h 节点创建与持久化
- [x] internode::split_into() 句柄分配
- [x] leaf::split_into() 句柄分配
- [x] link_split() 双存储更新

### Phase 5: 扫描路径
- [x] ScanContext 线程本地上下文
- [x] ScanContextRegistry 全局注册
- [x] forward_scan_helper 指针遍历
- [x] reverse_scan_helper 指针遍历

### Phase 6: FlowKV 集成
- [x] ExternalStorageManager 封装
- [x] ExternalStorageGuard RAII 守卫
- [x] HMasstreeWrapper 外存 API

### Phase 7: 删除路径
- [x] locked_parent() 指针遍历
- [x] gc_layer() 节点释放
- [x] finish_remove() 清理逻辑

### Phase 8: 索引适配层
- [x] ScanContextGuard 自动管理
- [x] thread_init_external() 初始化

### Phase 9: 持久化
- [x] Superblock 格式设计
- [x] 元数据持久化
- [x] 启动恢复流程

### Phase 10: 后台任务
- [x] 后台刷盘线程
- [x] 销毁流程优化
- [x] 内存统计

### Phase 11: 性能测试
- [x] benchmark_comparison.cpp 对比测试程序
- [x] 1M 规模性能测试
- [x] 性能分析报告
- [x] 优化建议文档

---

## 待完成阶段 (Phase 12+) ⏳

### Phase 12: Pointer Swizzling (P0 - 高优先级)

**目标**: 消除双存储开销，内存降低 30%

- [ ] 设计 SwizzledPtr 联合类型
- [ ] 引入 swizzling 标记位 (LSB = 1 表示 handle)
- [ ] 修改 child_[]/next_ 为 SwizzledPtr 类型
- [ ] 实现自动 swizzle 逻辑 (加载时 handle → ptr)
- [ ] 实现自动 unswizzle 逻辑 (驱逐时 ptr → handle)
- [ ] 并发控制 (atomic swizzle)
- [ ] 压力测试验证

### Phase 13: 延迟叶节点加载 (P1)

**目标**: 按需加载叶节点，内存降低 50%

- [ ] 叶节点初始不加载 (仅存 handle)
- [ ] reach_leaf() 延迟解析
- [ ] 热叶节点缓存策略
- [ ] LRU/Clock 驱逐叶节点

### Phase 14: 批量预取 (P2)

**目标**: 优化 range scan 延迟

- [ ] prefetch_next_leaves() 实现
- [ ] scan 时异步预取后续叶节点
- [ ] 预取深度配置

### Phase 15: 冷热分层 (P2)

**目标**: 智能缓存管理

- [ ] 热度统计 (访问计数/LRU 时间戳)
- [ ] 冷热节点识别
- [ ] 冷节点优先驱逐
- [ ] 热节点保护

### Phase 16: 异步 I/O (P3)

**目标**: io_uring 集成，提升吞吐率

- [ ] io_uring 初始化
- [ ] 异步加载 API
- [ ] 异步刷盘 API
- [ ] 回调机制

---

## 当前状态

| 阶段 | 状态 | 完成度 |
|------|------|--------|
| Phase 1-11 | ✅ 已完成 | 100% |
| Phase 12 | ⏳ 待开始 | 0% |
| Phase 13-16 | ⏳ 规划中 | 0% |

## 下一步行动

1. **短期 (1-2 周)**: 实现 Pointer Swizzling 原型
2. **中期 (1 个月)**: 完成延迟加载和预取优化
3. **长期 (2-3 个月)**: 异步 I/O 和生产级优化

---

*最后更新: 2025-01-16*
