# H-Masstree 测试方案与使用指南

## 概述

本文档描述了 H-Masstree 的独立单元测试方案，用于验证 H-Masstree 作为独立索引结构的正确性。测试覆盖基本操作、扫描、大规模数据、并发以及压力测试五个层次。

## 测试程序清单

| 文件 | 路径 | 说明 |
|------|------|------|
| `hmasstree_test.cpp` | `lib/hmasstree/hmasstree_test.cpp` | 主测试程序 (~730 行) |
| `run_test.sh` | `lib/hmasstree/run_test.sh` | 编译运行脚本 |

## 测试层次结构

### Level 1: 基本操作 (4 个测试)

| 测试名称 | 说明 |
|----------|------|
| `test_basic_insert_get` | 基本插入和查找 |
| `test_update` | 更新已存在的 key |
| `test_delete` | 删除操作及验证 |
| `test_boundary_keys` | 边界值 key (0, UINT64_MAX 等) |

### Level 2: 扫描操作 (4 个测试)

| 测试名称 | 说明 |
|----------|------|
| `test_forward_scan` | 正向扫描，验证结果有序 |
| `test_scan_with_keys` | 同时返回 key 和 value 的扫描 |
| `test_range_scan` | 范围扫描 [start, end] |
| `test_empty_scan` | 空范围扫描 |

### Level 3: 大规模测试 (3 个测试)

| 测试名称 | 说明 |
|----------|------|
| `test_large_sequential` | 顺序插入 N 个 key 并验证 |
| `test_large_random` | 随机插入 N 个 key 并验证 |
| `test_insert_delete_verify` | 插入-删除-验证循环 |

### Level 4: 并发操作 (3 个测试)

| 测试名称 | 说明 |
|----------|------|
| `test_concurrent_read` | 多线程只读 |
| `test_concurrent_read_write` | 读写混合并发 |
| `test_concurrent_scan` | 多线程并发扫描 |

### Level 5: 压力测试 (1 个测试)

| 测试名称 | 说明 |
|----------|------|
| `test_stress_mixed` | 混合操作压力测试 (40% insert, 10% delete, 40% read, 10% scan) |

## 使用方法

### 方法一：使用脚本 (推荐)

```bash
cd lib/hmasstree
chmod +x run_test.sh

# 默认配置运行 (100,000 keys, 4 threads)
./run_test.sh

# 自定义 key 数量
./run_test.sh -n 50000

# 自定义线程数
./run_test.sh -n 100000 -t 8
```

### 方法二：手动编译运行

```bash
cd /path/to/FlowKV

# 编译
g++ -std=c++17 -O2 -fpermissive \
    -I lib/hmasstree -I include -I lib \
    -include lib/hmasstree/config.h \
    -o build_hmasstree/hmasstree_test \
    lib/hmasstree/hmasstree_test.cpp \
    lib/hmasstree/hmasstree_wrapper.cc \
    lib/hmasstree/straccum.cc \
    lib/hmasstree/string.cc \
    lib/hmasstree/str.cc \
    lib/hmasstree/string_slice.cc \
    lib/hmasstree/kvthread.cc \
    lib/hmasstree/misc.cc \
    lib/hmasstree/compiler.cc \
    lib/hmasstree/memdebug.cc \
    lib/hmasstree/clp.c \
    -lpthread

# 运行
./build_hmasstree/hmasstree_test -n 10000 -t 4
```

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-n NUM` | 测试 key 数量 | 100000 |
| `-t NUM` | 并发线程数 | 4 |
| `-v` 或 `--verbose` | 详细输出 | 关闭 |

## 预期输出示例

```
========================================
H-Masstree Standalone Unit Tests
========================================
Config: num_keys=50000, threads=4

=== Level 1: Basic Operations ===
[TEST] Basic Insert/Get
[PASS] test_basic_insert_get
[TEST] Update
[PASS] test_update
...

=== Level 5: Stress Test ===
[TEST] Mixed Stress
  Operations: Insert=19890, Delete=4992, Read=20200, Scan=4918
  Throughput: 4.90683e+06 ops/sec
[PASS] test_stress_mixed

========================================
Results: 15 passed, 0 failed
========================================
```

## 测试通过标准

- **全部通过**: 返回码 0，显示 "15 passed, 0 failed"
- **部分失败**: 返回码 1，显示具体失败的测试和原因

## 依赖项

- C++17 编译器 (GCC 7+ 或 Clang 5+)
- pthread 库
- 无需 pmem 库（测试仅验证内存模式）

## 测试覆盖的 API

```cpp
// 构造与初始化
HMasstreeWrapper();
static void thread_init(int tid);

// 核心操作
void insert(KeyType key, ValueHelper& vh);
bool search(KeyType key, uint64_t& value);
bool remove(KeyType key);

// 扫描操作
void scan(KeyType start, int cnt, std::vector<uint64_t>& values);
void scan(KeyType start, int cnt, std::vector<KeyType>& keys, std::vector<uint64_t>& values);
void scan(KeyType start, KeyType end, std::vector<KeyType>& keys, std::vector<uint64_t>& values);

// 外存模式专用
bool init_external_storage(const ExternalIndexConfig& config);
void thread_init_external(int tid);
void flush_external_storage();
```

## Key16 模式说明

当启用 `FLOWKV_KEY16` 时，`KeyType` 为 `Key16`（128 位 key）。`Key16` 支持从 `uint64_t` 的隐式转换：

```cpp
// 隐式转换：uint64_t -> Key16(0, val)
mt.insert(123, vh);         // 等价于 mt.insert(Key16(0, 123), vh)
mt.search(456, result);     // 等价于 mt.search(Key16(0, 456), result)
mt.scan(100, 50, values);   // 从 Key16(0, 100) 开始扫描 50 个
```

这使得测试代码可以直接使用 `uint64_t` 常量，无需显式构造 `Key16`。

## 注意事项

1. **线程 ID 要求**: `thread_init(tid)` 的 tid 必须在 [1, 64] 范围内
2. **范围扫描语义**: `scan(start, end, ...)` 是闭区间 [start, end]
3. **并发测试**: 需要足够的系统资源才能稳定通过

## 相关文档

- [H-Masstree 外存设计文档](hmasstree_external_storage_design.md)
- [H-Masstree 开发日志](hmasstree_external_dev_log.md)
- [Masstree 架构文档](masstree_architecture.md)

---

# 外存模式测试方案

## 概述

外存模式测试验证 H-Masstree 在 `HMASSTREE_EXTERNAL_STORAGE` 宏开启时的功能正确性。测试分为 5 个级别，覆盖编译验证、组件单元测试、集成测试、端到端测试和压力测试。

## 测试级别总览

| 级别 | 测试内容 | 优先级 | 当前状态 |
|------|----------|--------|----------|
| L1 | 基础编译验证 | P0 | ✅ 通过 |
| L2 | 组件单元测试 | P0 | ✅ 通过 |
| L3 | 集成测试 (内存模式功能) | P1 | ✅ 通过 |
| L4 | 端到端测试 (外存模式) | P1 | ✅ 通过 |
| L5 | 压力测试 & 性能测试 | P2 | ✅ 通过 |

## 实现架构

### 双存储策略

外存模式采用**双存储策略**：节点同时维护指针（用于内存遍历）和句柄（用于持久化）。

```cpp
// 在 internode 和 leaf 结构中
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // 指针用于内存遍历（与内存模式相同）
    node_base<P>* child_[width + 1];
    node_base<P>* parent_;
    
    // 句柄用于序列化/外存持久化
    NodeHandle child_handles_[width + 1];
    NodeHandle parent_handle_;
#endif
```

### 核心修改

1. **树遍历**: `reach_leaf()`, `locked_parent()`, `maybe_parent()` 使用指针遍历
2. **叶子链接**: `link_split()` 同时更新 `next_.ptr`/`prev_` 和 `next_handle_`/`prev_handle_`
3. **节点分裂**: `internode::split_into()` 接受指针和句柄两个参数
4. **扫描操作**: `advance_to_key()` 和 `forward_scan_helper::advance()` 使用 `safe_next()` 指针

---

## L1: 基础编译验证

### 测试目标
验证外存模式和非外存模式都能正确编译。

### 执行命令

```bash
# 外存模式编译
mkdir -p build_external && cd build_external
cmake .. -DUSE_HMASSTREE=ON -DCMAKE_CXX_FLAGS="-DHMASSTREE_EXTERNAL_STORAGE"
make -j$(nproc)

# 非外存模式编译 (确保不破坏现有功能)
mkdir -p build_hmasstree && cd build_hmasstree
cmake .. -DUSE_HMASSTREE=ON
make -j$(nproc)
```

### 验收标准
- 两种模式都编译成功，无编译错误
- 只有 deprecated warning 可接受

---

## L2: 组件单元测试

### 2.1 NodeHandle 测试

```cpp
#include "node_handle.h"

void test_node_handle() {
    using namespace Masstree;
    
    // 1. 创建与解析
    NodeHandle h = NodeHandle::make_leaf(12345, 3, 100);
    assert(h.is_valid());
    assert(h.is_leaf());
    assert(h.page_id() == 12345);
    assert(h.slot_index() == 3);
    assert(h.generation() == 100);
    
    // 2. 空 handle
    NodeHandle null_h;
    assert(null_h.is_null());
    
    // 3. 比较
    NodeHandle h2 = NodeHandle::make_leaf(12345, 3, 100);
    assert(h == h2);
    
    // 4. 序列化/反序列化
    uint64_t raw = h.raw();
    NodeHandle h3(raw);
    assert(h == h3);
    
    printf("[PASS] test_node_handle\n");
}
```

### 2.2 PackedPage 测试

```cpp
#include "node_handle.h"

void test_packed_page() {
    using namespace Masstree;
    
    PackedPage page;
    page.init();
    
    // 1. 槽位分配
    for (int i = 0; i < 8; i++) {
        void* slot = page.slot(i);
        assert(slot != nullptr);
        // 验证槽位在页内
        assert((char*)slot >= (char*)&page);
        assert((char*)slot < (char*)&page + PackedPage::PAGE_SIZE);
    }
    
    // 2. 槽位独立性
    memset(page.slot(0), 0xAA, 64);
    memset(page.slot(1), 0xBB, 64);
    assert(((uint8_t*)page.slot(0))[0] == 0xAA);
    assert(((uint8_t*)page.slot(1))[0] == 0xBB);
    
    printf("[PASS] test_packed_page\n");
}
```

### 2.3 NodeCache 测试 (需要完整 I/O 集成后)

```cpp
#include "node_cache.h"

void test_node_cache() {
    using namespace Masstree;
    
    NodeCache::Config config;
    config.max_memory_bytes = 64 * 1024 * 1024;  // 64MB
    NodeCache cache(config);
    
    // 1. 页分配
    CachedPage* page = cache.allocate_page();
    assert(page != nullptr);
    
    // 2. Pin/Unpin
    page->pin();
    assert(page->pin_count() > 0);
    page->unpin();
    
    // 3. 驱逐 (不应驱逐 pinned 页)
    page->pin();
    cache.maybe_evict();
    assert(page->page_ptr != nullptr);
    page->unpin();
    
    printf("[PASS] test_node_cache\n");
}
```

---

## L3: 集成测试 - 内存模式功能

使用现有的 `hmasstree_test` 测试程序验证非外存模式功能正确性。

### 执行命令

```bash
cd build_hmasstree

# 运行所有测试
./hmasstree_test --level=3

# 或使用脚本
cd lib/hmasstree && ./run_test.sh
```

### 验收标准
- 15/15 测试全部通过

---

## L4: 端到端测试 - 外存模式

### 4.1 测试文件: `hmasstree_external_test.cpp`

```cpp
#include "index_hmasstree_external.h"
#include <cassert>
#include <vector>

using namespace Masstree;

void test_external_init() {
    ExternalIndexConfig config;
    config.storage_path = "/tmp/flowkv_ext_test.dat";
    config.cache_size_mb = 64;
    config.storage_size_mb = 512;
    
    HMasstreeExternalIndex index(config);
    assert(index.Initialize());
    
    printf("[PASS] test_external_init\n");
    
    index.Shutdown();
}

void test_external_basic_operations() {
    ExternalIndexConfig config;
    config.storage_path = "/tmp/flowkv_basic_test.dat";
    config.cache_size_mb = 64;
    config.storage_size_mb = 512;
    
    HMasstreeExternalIndex index(config);
    assert(index.Initialize());
    
    index.ThreadInit(0);
    
    // Insert 10000 keys
    for (uint64_t i = 0; i < 10000; i++) {
        ValueHelper vh(i * 100);
        index.Put(i, vh);
    }
    
    // Get and verify
    for (uint64_t i = 0; i < 10000; i++) {
        auto val = index.Get(i);
        assert(val == i * 100);
    }
    
    // Scan
    std::vector<uint64_t> results;
    index.Scan(100, 50, results);
    assert(results.size() == 50);
    
    // Delete
    for (uint64_t i = 0; i < 5000; i++) {
        index.Delete(i);
    }
    
    // Verify deletion
    for (uint64_t i = 0; i < 5000; i++) {
        auto val = index.Get(i);
        assert(val == 0);  // Deleted
    }
    
    printf("[PASS] test_external_basic_operations\n");
    
    index.Flush();
    index.Shutdown();
}

void test_external_cache_eviction() {
    ExternalIndexConfig config;
    config.storage_path = "/tmp/flowkv_evict_test.dat";
    config.cache_size_mb = 16;  // 小缓存强制驱逐
    config.storage_size_mb = 512;
    
    HMasstreeExternalIndex index(config);
    assert(index.Initialize());
    
    index.ThreadInit(0);
    
    // 插入足够多的数据触发驱逐
    const int N = 100000;
    for (int i = 0; i < N; i++) {
        ValueHelper vh(i);
        index.Put(i, vh);
    }
    
    // 验证所有数据可读 (触发 cache miss 和加载)
    int found = 0;
    for (int i = 0; i < N; i++) {
        if (index.Get(i) == (uint64_t)i) {
            found++;
        }
    }
    assert(found == N);
    
    index.PrintStats();  // 查看 cache 命中率
    
    printf("[PASS] test_external_cache_eviction\n");
    
    index.Shutdown();
}

int main() {
    printf("========================================\n");
    printf("H-Masstree External Mode Tests\n");
    printf("========================================\n");
    
    test_external_init();
    test_external_basic_operations();
    test_external_cache_eviction();
    
    printf("\nAll external mode tests passed!\n");
    return 0;
}
```

### 4.2 编译命令

```bash
cd /path/to/FlowKV

g++ -std=c++17 -O2 -DHMASSTREE_EXTERNAL_STORAGE \
    -I include -I lib -I lib/hmasstree \
    -o build_external/hmasstree_external_test \
    tests/hmasstree_external_test.cpp \
    -Lbuild_external -lflowkv -lhmasstree -lmasstree \
    -lpthread
```

### 4.3 运行

```bash
./build_external/hmasstree_external_test
```

---

## L5: 压力测试 & 性能测试

### 5.1 并发压力测试

```cpp
void test_concurrent_stress() {
    ExternalIndexConfig config;
    config.storage_path = "/tmp/stress_test.dat";
    config.cache_size_mb = 256;
    
    HMasstreeExternalIndex index(config);
    index.Initialize();
    
    const int NUM_THREADS = 8;
    const int OPS_PER_THREAD = 100000;
    std::atomic<int> total_ops{0};
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            index.ThreadInit(t);
            
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                int op = rand() % 100;
                uint64_t key_id = rand() % (OPS_PER_THREAD * NUM_THREADS);
                
                if (op < 40) {  // 40% insert
                    ValueHelper vh(key_id);
                    index.Put(key_id, vh);
                } else if (op < 80) {  // 40% read
                    index.Get(key_id);
                } else if (op < 90) {  // 10% delete
                    index.Delete(key_id);
                } else {  // 10% scan
                    std::vector<uint64_t> results;
                    index.Scan(key_id, 10, results);
                }
                total_ops++;
            }
        });
    }
    
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    
    printf("Total ops: %d\n", total_ops.load());
    printf("Throughput: %.2f ops/sec\n", total_ops.load() / seconds);
    
    index.PrintStats();
    index.Shutdown();
}
```

### 5.2 性能基准测试

```cpp
void benchmark_comparison() {
    const int N = 1000000;
    
    // 1. 纯内存模式基准
    {
        HMasstreeWrapper mem_index;
        mem_index.thread_init(1);
        
        auto start = now();
        for (int i = 0; i < N; i++) {
            ValueHelper vh(i);
            mem_index.insert(i, vh);
        }
        auto insert_time = now() - start;
        
        start = now();
        for (int i = 0; i < N; i++) {
            uint64_t val;
            mem_index.search(i, val);
        }
        auto read_time = now() - start;
        
        printf("Memory mode:\n");
        printf("  Insert: %.2f ops/sec\n", N / insert_time);
        printf("  Read:   %.2f ops/sec\n", N / read_time);
    }
    
    // 2. 外存模式 (大缓存 - 接近 100% 命中)
    {
        ExternalIndexConfig config;
        config.cache_size_mb = 1024;  // 1GB 缓存
        HMasstreeExternalIndex ext_index(config);
        ext_index.Initialize();
        ext_index.ThreadInit(0);
        
        // ... 类似测试 ...
        
        printf("External mode (large cache):\n");
        printf("  Insert: %.2f ops/sec\n", N / insert_time);
        printf("  Read:   %.2f ops/sec\n", N / read_time);
        printf("  Cache hit rate: %.2f%%\n", hit_rate * 100);
    }
    
    // 3. 外存模式 (小缓存 - 触发 I/O)
    {
        ExternalIndexConfig config;
        config.cache_size_mb = 32;  // 32MB 缓存
        // ... 类似测试 ...
    }
}
```

---

## 测试环境准备

```bash
# 1. 创建测试目录
mkdir -p /tmp/flowkv_test

# 2. 确保有足够磁盘空间 (至少 2GB)
df -h /tmp

# 3. 清理旧测试文件
rm -f /tmp/flowkv_*.dat

# 4. 设置环境变量 (可选)
export FLOWKV_TEST_PATH=/tmp/flowkv_test
```

## 验收标准

| 测试级别 | 通过条件 |
|----------|----------|
| L1 | 两种模式编译成功，无错误 |
| L2 | 所有组件单元测试通过 |
| L3 | 非外存模式 15/15 测试通过 |
| L4 | 外存模式基本操作测试通过 |
| L5 | 并发测试无死锁/崩溃，性能符合预期 |

## 当前可执行测试

截至 2026-02-02，所有测试均可执行：

| 测试 | 命令 | 状态 | 吞吐量 |
|------|------|------|--------|
| L1 编译验证 | `make -j4` (两个 build 目录) | ✅ | - |
| L3 内存模式测试 | `./hmasstree_test --test-level 3` | ✅ 15/15 通过 | ~3.6M ops/sec |
| L5 内存压力测试 | `./hmasstree_test --test-level 5` | ✅ 15/15 通过 | ~3.6M ops/sec |
| L5 外存正确性 | `./test_l5_external -n 10000` | ✅ 10000/10000 正确 | - |
| L5 外存压力 | `./test_l5_external -n 100000 -t 8` | ✅ 通过 | ~3.5M ops/sec |

## 快速开始

### 方法一：使用 CMake 构建目录

```bash
# 内存模式测试
cd /path/to/FlowKV/build_hmasstree
./hmasstree_test --test-level 5

# 外存模式测试（需要手动编译）
cd /path/to/FlowKV/lib/hmasstree
g++ -std=c++17 -O2 -DHMASSTREE_EXTERNAL_STORAGE -DFLOWKV_KEY16 \
    -I. -I.. -I../../include \
    -include config.h test_l5_stress.cpp hmasstree_wrapper.cc \
    index_storage.cpp node_cache.cpp straccum.cc string.cc str.cc \
    string_slice.cc kvthread.cc misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o ../../build_hmasstree/test_l5_external

cd ../../build_hmasstree
./test_l5_external -n 100000 -t 8 --storage-path /tmp/test.dat
```

### 方法二：使用测试脚本

```bash
cd lib/hmasstree
chmod +x run_test.sh
./run_test.sh -n 100000 -t 4
```

## 外存模式测试输出示例

```
========================================
H-Masstree L5 Stress Test
========================================
Mode: External Storage
Config: keys=100000, threads=8

=== External Storage Correctness Test ===
  Inserting 10000 keys...
  Verifying...
  Found: 10000/10000
  Correct: 10000/10000
  Scan returned: 50 items
[PASS] test_external_correctness

=== L5 Stress Test (External Storage Mode) ===
  Storage path: /tmp/hmasstree_test.dat
  Cache size:   64 MB
  Storage size: 512 MB
  External storage initialized successfully
Statistics:
  Insert: 40017
  Read:   39857
  Delete: 10068
  Scan:   10058
  Total:  100000
  Time:       0.028388 sec
  Throughput: 3522.62 Kops/sec
[PASS] test_stress_external

========================================
Results: 2 passed, 0 failed
========================================
```
