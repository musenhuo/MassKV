# H-Masstree 外存方案功能分析与待完成清单

## 文档信息

| 项目 | 内容 |
|------|------|
| 创建日期 | 2026-02-02 |
| 基于 | hmasstree_external_storage_design.md, hmasstree_external_dev_log.md |

---

## 1. 功能规划与实现状态对比

### 1.1 核心抽象层 (设计 Phase 1-2)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| NodeHandle 逻辑地址 | 8字节编码 (valid + type + page_id + slot + generation) | ✅ 已完成 | node_handle.h |
| AtomicNodeHandle | CAS操作支持 | ✅ 已完成 | node_handle.h |
| PackedPage | 4KB 页面，8个节点槽位 | ✅ 已完成 | node_handle.h |
| node_base 句柄替换 | child_[], parent_, next_, prev_ 改为 NodeHandle | ✅ 已完成 | masstree_struct.hh |
| self_handle 成员 | 节点自引用 | ✅ 已完成 | masstree_struct.hh |
| NodeType 枚举 | leaf/internode/layer_root | ✅ 已完成 | node_handle.h |

### 1.2 缓存层 (设计 Phase 2)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| CachedPage 结构 | pin_count, epoch, dirty, reference_bit | ✅ 已完成 | node_cache.h |
| 分片哈希表 | 减少锁竞争 | ✅ 已完成 | 64分片 |
| Pin/Unpin 机制 | 防止正在使用的页被驱逐 | ✅ 已完成 | node_cache.h/cpp |
| get_or_load() | 快速路径/慢速路径 | ✅ 已完成 | node_cache.cpp |
| allocate_node() | 节点分配 | ✅ 已完成 | node_cache.cpp |
| free_node() | 节点释放 | ✅ 已完成 | node_cache.cpp |
| mark_dirty() | 脏页标记 | ✅ 已完成 | node_cache.cpp |
| Clock 淘汰算法 | 内存控制 | ✅ 已完成 | evict_pages() |
| Epoch protection | 临界区保护 | ✅ 已完成 | enter/exit_critical_section |
| ReadGuard | RAII 上下文 | ✅ 已完成 | node_cache.h |

### 1.3 存储层 (设计 Phase 3)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| IndexSegment | 4MB 段，1024 页 | ✅ 已完成 | index_storage.h |
| allocate_page() | 页面分配 | ✅ 已完成 | index_storage.cpp |
| free_page() | 页面释放 | ✅ 已完成 | index_storage.cpp |
| read_page() | 页面读取 | ✅ 已完成 | index_storage.cpp |
| write_page() | 页面写入 | ✅ 已完成 | index_storage.cpp |
| allocate_node_slot() | 槽位分配 | ✅ 已完成 | index_storage.cpp |
| free_node_slot() | 槽位释放 | ✅ 已完成 | index_storage.cpp |
| open()/close() | 文件管理 | ✅ 已完成 | index_storage.cpp |
| sync() | 数据同步 | ✅ 已完成 | index_storage.cpp |
| recover() | 从文件恢复 | ⚠️ 基础实现 | 需完善元数据恢复 |
| persist_metadata() | 元数据持久化 | ❌ 待实现 | TODO in code |
| Direct I/O | 绕过 page cache | ❌ 未实现 | 可选优化 |

### 1.4 读写路径改造 (设计 Phase 4-5)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| reach_leaf() | 使用 ScanContext 解析子节点 | ✅ 已完成 | masstree_struct.hh |
| advance_to_key() | 使用 ScanContext | ✅ 已完成 | masstree_struct.hh |
| locked_parent() | 外存模式实现 | ✅ 已完成 | masstree_struct.hh |
| split_into() handle 版本 | 分裂时设置 handle | ✅ 已完成 | masstree_split.hh |
| make_split() | 节点创建和链接 | ✅ 已完成 | masstree_split.hh |
| btree_leaflink | 外存链表操作 | ✅ 已完成 | btree_leaflink.hh |
| gc_layer() | child 解析 | ✅ 已完成 | masstree_remove.hh |
| finish_remove() | prev 解析 | ✅ 已完成 | masstree_remove.hh |
| destroy_rcu_callback | child handle 解析 | ✅ 已完成 | masstree_remove.hh |

### 1.5 扫描路径改造 (设计 Phase 5)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| ScanContext | cache 引用 + pin 追踪 | ✅ 已完成 | scan_context.h |
| ScanContextRegistry | 线程本地存储 | ✅ 已完成 | scan_context.h |
| ScanContextGuard | RAII 管理 | ✅ 已完成 | scan_context.h |
| resolve_leaf() | leaf handle 解析 | ✅ 已完成 | scan_context.h |
| resolve_internode() | internode handle 解析 | ✅ 已完成 | scan_context.h |
| forward_scan_helper | 使用 ScanContext | ✅ 已完成 | masstree_scan.hh/scan2.hh |
| reverse_scan_helper | 使用 ScanContext | ✅ 已完成 | masstree_scan.hh/scan2.hh |

### 1.6 FlowKV 集成 (设计 Phase 6)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| ExternalIndexConfig | 配置类 | ✅ 已完成 | external_index.h |
| ExternalIndexStats | 统计类 | ✅ 已完成 | external_index.h |
| ExternalStorageManager | cache + storage 管理 | ✅ 已完成 | external_index.h |
| HMasstreeExternalIndex | Index 适配器 | ✅ 已完成 | index_hmasstree_external.h |
| ThreadInit 上下文 | ScanContext 初始化 | ✅ 已完成 | index_hmasstree_external.h |
| Get/Put/Delete/Scan | ScanContextGuard | ✅ 已完成 | index_hmasstree_external.h |
| 后台刷盘线程 | 定期 flush | ❌ 未实现 | 设计中提到 |
| 崩溃恢复 | checkpoint + recovery | ❌ 未实现 | 需补充 |

### 1.7 预取与优化 (设计中提到)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| PrefetchChildren() | 子节点预取 | ❌ 未实现 | 可选优化 |
| PrefetchLeafChain() | 扫描预取 | ❌ 未实现 | 可选优化 |
| 批量 I/O | ReadNodesBatch | ❌ 未实现 | 可选优化 |

### 1.8 Pointer Swizzling (设计 Phase 7-10)

| 功能 | 设计要求 | 实现状态 | 说明 |
|------|----------|----------|------|
| SwizzledPointer | 指针/PID联合体 | ❌ 未实现 | 后续优化阶段 |
| LoadAndSwizzle | 加载并旋转 | ❌ 未实现 | 后续优化阶段 |
| Back pointer | 反向指针管理 | ❌ 未实现 | 后续优化阶段 |
| Unswizzle | 驱逐时解旋转 | ❌ 未实现 | 后续优化阶段 |

---

## 2. 代码中的待完成项 (TODO 扫描结果)

| 文件 | 位置 | 内容 | 优先级 |
|------|------|------|--------|
| **index_storage.cpp** | L240 | `Read actual page bitmap from metadata region` | P1 |
| **index_storage.cpp** | L254 | `Implement metadata persistence` | P1 |
| **external_index.h** | L227 | `Add sync parameter support` | P2 |
| **masstree_remove.hh** | L390 | `需要重新设计destroy流程` | P2 |
| **masstree_print.hh** | L190 | `外部存储模式下需要通过cache解析handle才能递归打印` | P3 |
| **hmasstree_wrapper.h** | L162,202,250 | `skip the val which points to hybrid index block` | P3 |
| **masstree_scan.hh** | L352 | `need another visit_value function` | P3 |
| **masstree_scan2.hh** | L353 | `need another visit_value function` | P3 |
| **kvthread.cc** | L272-273 | `record start addr/reuse init` | P3 |

---

## 3. 分阶段待完成清单

### 阶段 A: 存储层完善 (P1 优先级)

**目标**: 完成可靠的持久化能力

| 任务 | 文件 | 描述 |
|------|------|------|
| A1 | index_storage.cpp | 实现 `persist_metadata()` - 段分配状态持久化 |
| A2 | index_storage.cpp | 完善 `recover()` - 读取页面位图元数据 |
| A3 | index_storage.h/cpp | 添加 superblock 结构和持久化 |
| A4 | external_index.h | 实现 flush 的 sync 参数支持 |

**验收标准**:
- 重启后能正确恢复已分配的页面状态
- 持久化后数据在意外断电后不丢失

### 阶段 B: 端到端测试 (P1 优先级)

**目标**: 验证外存模式功能正确性

| 任务 | 文件 | 描述 |
|------|------|------|
| B1 | test_external_mode.cpp | 创建外存模式端到端测试 |
| B2 | - | Get/Put/Delete 基本操作测试 |
| B3 | - | Scan 扫描测试 |
| B4 | - | 重启恢复测试 |

**验收标准**:
- 外存模式下所有基础操作正确
- 数据重启后可恢复

### 阶段 C: 生产化完善 (P2 优先级)

**目标**: 提升可靠性和可维护性

| 任务 | 文件 | 描述 |
|------|------|------|
| C1 | external_index.h | 实现后台刷盘线程 |
| C2 | masstree_remove.hh | 重新设计 destroy 流程，确保外存模式正确释放 |
| C3 | masstree_print.hh | 实现外存模式的递归打印 (调试用) |

### 阶段 D: 性能优化 (P2+ 优先级)

**目标**: 提升外存模式性能

| 任务 | 文件 | 描述 |
|------|------|------|
| D1 | node_cache.cpp | 实现子节点预取 |
| D2 | node_cache.cpp | 实现扫描时的连续叶子预取 |
| D3 | index_storage.cpp | 添加 Direct I/O 支持 |
| D4 | - | Pointer Swizzling 优化 (大型优化) |

---

## 4. 当前阶段建议

根据以上分析，建议按以下顺序推进：

### 立即执行 (阶段 A1-A3)

1. **实现 persist_metadata()** - 确保段分配状态可持久化
2. **完善 recover()** - 能正确读取已持久化的元数据
3. **添加 superblock** - 存储根节点 handle 等关键信息

### 验证阶段 (阶段 B)

4. **创建端到端测试** - 验证外存模式完整流程

---

## 5. 技术债务

| 项目 | 描述 | 风险 |
|------|------|------|
| 元数据恢复 | recover() 当前假设所有页面已分配 (保守策略) | 空间浪费 |
| ~~destroy 流程~~ | ~~需要适配外存模式~~ | ✅ 已修复 (P2 完成) |
| hybrid index block | 多处 TODO 提到需要处理 | 功能不完整 |

---

## 6. L5 压力测试可行性分析

### 6.1 当前状态评估

| 组件 | 状态 | L5 就绪度 |
|------|------|----------|
| IndexStorageManager | ✅ 完成 | 就绪 |
| NodeCache | ✅ 完成 | 就绪 |
| NodeHandle | ✅ 完成 | 就绪 |
| 后台刷盘线程 | ✅ 完成 (P2) | 就绪 |
| destroy 流程 | ✅ 完成 (P2) | 就绪 |
| HMasstreeWrapper 外存集成 | ❌ 未完成 | **阻塞** |

### 6.2 L5 压力测试阻塞项

**主要阻塞**: HMasstreeWrapper 尚未集成 ExternalStorageManager

当前状态：
- `hmasstree_test.cpp` 中的 L5 测试 (`test_stress_mixed`) 使用 HMasstreeWrapper
- HMasstreeWrapper 内部使用 `Masstree::default_table` 和 `threadinfo`
- 外存模式需要在 HMasstreeWrapper 初始化时创建 ExternalStorageManager
- 每个线程需要设置 ScanContext 才能解析 NodeHandle

### 6.3 L5 就绪所需修改

```cpp
// 需要添加到 HMasstreeWrapper
#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
class HMasstreeWrapper {
    ExternalStorageManager* ext_storage_ = nullptr;
    
    bool init_external_storage(const ExternalIndexConfig& cfg) {
        ext_storage_ = new ExternalStorageManager(cfg);
        return ext_storage_->initialize();
    }
    
    static void thread_init_external(int tid) {
        thread_init(tid);
        // 设置线程的 ScanContext
        if (instance_->ext_storage_) {
            ScanContext* ctx = new ScanContext(instance_->ext_storage_->cache());
            ScanContextRegistry::set(ctx);
        }
    }
};
#endif
```

### 6.4 结论

**L5 压力测试当前不可行**，需要先完成 HMasstreeWrapper 与 ExternalStorageManager 的集成。

预估工作量: 0.5-1 天

---

## 7. FlowKV 集成能力分析

### 7.1 集成架构

```
FlowKV DB
    ↓
HMasstreeIndex (lib/index_hmasstree.h)
    ↓
HMasstreeWrapper (lib/hmasstree/hmasstree_wrapper.h)
    ↓
Masstree basic_table
    ↓ (需要外存集成)
ExternalStorageManager → NodeCache → IndexStorageManager
```

### 7.2 集成状态

| 层次 | 组件 | 外存支持 |
|------|------|----------|
| DB 层 | MYDB/MYDBClient | 无需修改 |
| 索引适配层 | HMasstreeIndex | 需要添加初始化配置 |
| Wrapper 层 | HMasstreeWrapper | **需要外存集成** |
| 核心层 | Masstree | ✅ 已完成 |
| 存储层 | ExternalStorageManager | ✅ 已完成 |

### 7.3 FlowKV Benchmark 集成所需修改

1. **HMasstreeWrapper 扩展** (同 L5 所需修改)

2. **HMasstreeIndex 配置传递**:
```cpp
class HMasstreeIndex : public Index {
public:
#ifdef HMASSTREE_EXTERNAL_STORAGE
    HMasstreeIndex(const ExternalIndexConfig& cfg) {
        mt_ = new HMasstreeWrapper();
        mt_->init_external_storage(cfg);
    }
#endif
};
```

3. **simple_benchmark.cpp 修改**:
```cpp
// 添加外存配置选项
DEFINE_string(index_storage_path, "/tmp/index.dat", "Index storage path");
DEFINE_uint64(index_cache_mb, 256, "Index cache size in MB");
```

### 7.4 结论

**FlowKV 集成当前不可行**，原因与 L5 相同 - HMasstreeWrapper 未集成外存模式。

集成完成后，可通过 `simple_benchmark` 的以下命令测试:
```bash
./benchmark --benchmarks=write,read --num=1000000 \
    --index_storage_path=/data/index.dat --index_cache_mb=512
```

预估工作量: 1-2 天 (含 HMasstreeWrapper 修改)

---

## 8. 建议开发顺序

| 优先级 | 任务 | 预估时间 |
|--------|------|----------|
| P1 | HMasstreeWrapper 外存集成 | 0.5-1 天 |
| P2 | L5 压力测试验证 | 0.5 天 |
| P3 | HMasstreeIndex 配置扩展 | 0.5 天 |
| P4 | FlowKV Benchmark 验证 | 0.5 天 |
| P5 | 性能调优 (预取等) | 持续 |
