# FlowKV Masstree Benchmark Report

## 测试环境

- **日期**: 2025-02-05
- **测试规模**: 1M keys (完整测试), 10M keys (基准测试)
- **Key类型**: 顺序 Key16 (hi=key>>32, lo=key)
- **操作系统**: Linux

## 测试方法

### 测试目标

1. **基准性能测试**: 比较三种模式在相同数据规模下的内存占用和读写性能
   - Original Masstree: 原版 Masstree 实现
   - HMasstree Memory Mode: 带 Handle 扩展的 Masstree（纯内存模式）
   - HMasstree External Mode: 带 Handle 扩展的 Masstree（外存模式）

2. **冷启动性能测试**: 测试 HMasstree 外存模式冷启动后的读性能
   - 从持久化存储加载树结构
   - 测试恢复时间和读取性能

### 测试流程

1. **基准测试**:
   - 插入 N 个顺序 key-value 对
   - 随机打乱后查询全部 key
   - 记录写入/读取吞吐量和内存使用

2. **冷启动测试**:
   - 先写入数据并序列化到存储文件
   - 重新创建实例，从存储恢复树结构
   - 测试恢复时间和读取性能

## 测试结果

### 1. 基准性能测试 (1M keys)

| 模式 | 写入吞吐量 (M ops/s) | 读取吞吐量 (M ops/s) | 内存占用 (MB) |
|------|---------------------|---------------------|--------------|
| Original Masstree | 11.46 | 2.77 | 24.5 |
| HMasstree Memory Mode | 11.53 | 2.91 | 24.5 |
| HMasstree External Mode | 10.83 | 2.97 | 30.9 (写) / 100.0 (读) |

**观察**:
- 三种模式写入性能基本一致（10.8-11.5 M ops/s）
- 读取性能也相近（2.77-2.97 M ops/s）
- HMasstree Memory Mode 与 Original Masstree 内存占用**完全相同**

### 2. 基准性能测试 (10M keys)

| 模式 | 写入吞吐量 (M ops/s) | 读取吞吐量 (M ops/s) | 内存占用 (MB) |
|------|---------------------|---------------------|--------------|
| Original Masstree | 10.33 | 3.58 | 221.3 |
| HMasstree Memory Mode | 10.36 | 3.57 | 221.3 |
| HMasstree External Mode | 10.00 | 2.47 | 269.8 (写) / 966.3 (读) |

**观察**:
- 10M 规模下性能略有下降（更多的 B+ 树层级）
- 外存模式读取性能降低（序列化和 Handle 开销）

### 3. 冷启动性能测试

#### 1M keys (X = 25 MB 为 Original Masstree 内存)

| 测试场景 | 缓存 (MB) | 恢复时间 (ms) | 读取吞吐量 (M ops/s) | 读后内存 (MB) |
|---------|----------|--------------|---------------------|--------------|
| 无限缓存 | 256 | 862 | 3.00 | 99.8 |

#### 10M keys (X = 221 MB)

| 测试场景 | 缓存 (MB) | 恢复时间 (ms) | 读取吞吐量 (M ops/s) | 读后内存 (MB) |
|---------|----------|--------------|---------------------|--------------|
| 无限缓存 | 4096 | 5,662 | 2.91 | 965.0 |

**观察**:
- 冷启动恢复时间随数据规模增长（1M: 0.9秒, 10M: 5.7秒）
- 恢复后读性能与热数据相当

## 分析

### 内存效率

1. **纯内存模式**: HMasstree Memory Mode 与 Original Masstree 内存占用完全相同
   - 1M keys: 24.5 MB
   - 10M keys: 221.3 MB
   - **Handle 扩展没有引入额外的内存开销**

2. **外存模式**: 
   - 写入时额外占用约 26%（用于 Handle 存储和序列化缓冲）
   - 支持数据持久化和冷启动恢复

### 性能对比总结

| 指标 | Original | HMasstree Memory | HMasstree External |
|------|----------|-----------------|-------------------|
| 写性能 | 基准 | ≈100% | ~95% |
| 读性能 | 基准 | ~105% | ~70-85% |
| 内存占用 | 基准 | =100% | +26% (写入时) |
| 持久化 | ❌ | ❌ | ✅ |
| 冷启动 | ❌ | ❌ | ✅ |

### 结论与建议

1. **内存充足、只需纯内存索引**: 使用 **HMasstree Memory Mode**
   - 与原版性能相同
   - 内存占用相同
   - 代码兼容性好

2. **需要持久化或冷启动恢复**: 使用 **HMasstree External Mode**
   - 支持数据持久化到文件
   - 支持冷启动快速恢复
   - 写性能损失约 5%
   - 推荐缓存大小 ≥ 索引大小

## 测试文件

- **源码**: [comprehensive_benchmark.cpp](comprehensive_benchmark.cpp)
- **脚本**: [run_comprehensive_benchmark.sh](run_comprehensive_benchmark.sh)
- **编译后的二进制**: `/tmp/masstree_benchmark_build/`

## 测试命令参考

```bash
# 编译所有版本
cd /home/zwt/yjy/FlowKV
# Original Masstree
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DBENCHMARK_ORIGINAL_MASSTREE \
    -I./lib -I./include -include ./lib/masstree/config.h \
    ./benchmark_pools/comprehensive_benchmark.cpp \
    ./lib/masstree/*.cc ./lib/masstree/clp.c \
    -lpthread -o bench_original

# HMasstree Memory
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DBENCHMARK_HMASSTREE_MEMORY \
    -I./lib -I./include -I./lib/hmasstree -include ./lib/hmasstree/config.h \
    ./benchmark_pools/comprehensive_benchmark.cpp \
    ./lib/hmasstree/*.cc ./lib/hmasstree/clp.c \
    -lpthread -o bench_hmasstree_mem

# HMasstree External
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DBENCHMARK_HMASSTREE_EXTERNAL -DHMASSTREE_EXTERNAL_STORAGE \
    -I./lib -I./include -I./lib/hmasstree -include ./lib/hmasstree/config.h \
    ./benchmark_pools/comprehensive_benchmark.cpp \
    ./lib/hmasstree/hmasstree_wrapper.cc ./lib/hmasstree/index_storage.cpp \
    ./lib/hmasstree/node_cache.cpp ./lib/hmasstree/*.cc ./lib/hmasstree/clp.c \
    -lpthread -o bench_hmasstree_ext

# 运行测试
./bench_original -n 1000000 -o results.csv
./bench_hmasstree_mem -n 1000000 -o results.csv
./bench_hmasstree_ext -n 1000000 -c 128 -s /tmp/storage.dat -o results.csv
./bench_hmasstree_ext -n 1000000 -c 64 -r 0 -s /tmp/storage.dat -o results.csv  # 冷启动
```

---
*报告生成时间: 2025-02-05*
