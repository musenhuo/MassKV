#!/bin/bash
# H-Masstree v2 Benchmark Comparison Script
# 对比三种索引：原版Masstree、H-Masstree内存、H-Masstree外存

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_KEYS=${1:-1000000}
CACHE_SIZE_MB=${2:-4}
NUM_OPS=${3:-1000000}

echo "============================================"
echo "H-Masstree Benchmark Comparison v2"
echo "============================================"
echo "Keys:       $NUM_KEYS"
echo "Cache Size: $CACHE_SIZE_MB MB (External Mode)"
echo "Operations: $NUM_OPS"
echo "Key Size:   16 bytes"
echo "Value Size: 16 bytes"
echo ""

BUILD_DIR="../../build_hmasstree"
mkdir -p $BUILD_DIR

# Common source files for H-Masstree
HMASSTREE_SOURCES="hmasstree_wrapper.cc straccum.cc string.cc str.cc string_slice.cc kvthread.cc misc.cc compiler.cc memdebug.cc clp.c"

# Common source files for original Masstree
MASSTREE_SOURCES="../masstree/masstree_wrapper.cc ../masstree/straccum.cc ../masstree/string.cc ../masstree/str.cc ../masstree/string_slice.cc ../masstree/kvthread.cc ../masstree/misc.cc ../masstree/compiler.cc ../masstree/memdebug.cc ../masstree/clp.c"

# ============================================================================
# Compile Memory Mode (Original Masstree + H-Masstree Memory)
# ============================================================================
echo "Compiling Memory Mode benchmark..."

# 创建一个临时的单文件测试，避免复杂的链接问题
cat > /tmp/benchmark_mem_test.cpp << 'EOF'
/* Memory Mode Benchmark - Original Masstree vs H-Masstree */
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <random>
#include <fstream>
#include <iomanip>

// 避免符号冲突，只测试一种
#ifndef TEST_HMASSTREE
#define TEST_ORIGINAL
#endif

#ifdef TEST_ORIGINAL
#include "masstree_wrapper.h"
typedef MasstreeWrapper TreeWrapper;
#else
#include "hmasstree_wrapper.h"
typedef HMasstreeWrapper TreeWrapper;
#endif

size_t get_memory_usage_kb() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.find("VmRSS:") == 0) {
            size_t kb = 0;
            sscanf(line.c_str(), "VmRSS: %zu kB", &kb);
            return kb;
        }
    }
    return 0;
}

int main(int argc, char* argv[]) {
    size_t N = 1000000;
    if (argc > 1) N = std::stoull(argv[1]);
    
#ifdef TEST_ORIGINAL
    std::cout << "Testing: Original Masstree" << std::endl;
#else
    std::cout << "Testing: H-Masstree Memory Mode" << std::endl;
#endif
    std::cout << "Keys: " << N << " (16B key, 16B value)" << std::endl;
    
    size_t mem_before = get_memory_usage_kb();
    
    TreeWrapper mt;
    TreeWrapper::thread_init(1);
    
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) keys[i] = i;
    
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // Insert
    auto t1 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < N; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i] * 100);
        mt.insert(k16, vh);
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    
    size_t mem_after = get_memory_usage_kb();
    double insert_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    
    std::cout << "Insert: " << insert_ms << " ms, " 
              << (N / insert_ms * 1000 / 1000) << " K ops/sec" << std::endl;
    std::cout << "Memory: " << (mem_after - mem_before) / 1024.0 << " MB" << std::endl;
    
    // Read
    std::shuffle(keys.begin(), keys.end(), rng);
    t1 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < N; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
    }
    t2 = std::chrono::high_resolution_clock::now();
    
    double read_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "Read: " << read_ms << " ms, " 
              << (N / read_ms * 1000 / 1000) << " K ops/sec" << std::endl;
    
    // Mixed (80% read, 20% write)
    t1 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < N; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        if ((rng() % 100) < 80) {
            uint64_t val;
            mt.search(k16, val);
        } else {
            ValueHelper vh(keys[i] * 100 + 1);
            mt.insert(k16, vh);
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    
    double mixed_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "Mixed: " << mixed_ms << " ms, " 
              << (N / mixed_ms * 1000 / 1000) << " K ops/sec" << std::endl;
    
    return 0;
}
EOF

# Compile original Masstree test (from masstree directory)
echo "  Compiling Original Masstree test..."
pushd ../masstree > /dev/null
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DTEST_ORIGINAL \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/benchmark_mem_test.cpp \
    masstree_wrapper.cc straccum.cc string.cc \
    str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o ../../build_hmasstree/benchmark_original 2>&1 | grep -v "warning:" | head -10 || {
    echo "Warning: Original Masstree compile may have issues, continuing..."
}
popd > /dev/null

# Compile H-Masstree memory test
echo "  Compiling H-Masstree Memory test..."
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DTEST_HMASSTREE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/benchmark_mem_test.cpp \
    hmasstree_wrapper.cc straccum.cc string.cc str.cc string_slice.cc \
    kvthread.cc misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o $BUILD_DIR/benchmark_hmasstree_mem 2>&1 | grep -v "warning:" | head -10 || {
    echo "Warning: H-Masstree Memory compile may have issues, continuing..."
}

# ============================================================================
# Compile External Storage Mode
# ============================================================================
echo "  Compiling H-Masstree External test..."
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE -DTEST_HMASSTREE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/benchmark_mem_test.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o $BUILD_DIR/benchmark_hmasstree_ext 2>&1 | grep -v "warning:" | head -10 || {
    echo "Warning: H-Masstree External compile may have issues, continuing..."
}

echo ""
echo "============================================"
echo "Running Benchmarks"
echo "============================================"

# Run Original Masstree
if [ -f "$BUILD_DIR/benchmark_original" ]; then
    echo ""
    echo "--- Original Masstree ---"
    $BUILD_DIR/benchmark_original $NUM_KEYS 2>&1
else
    echo "Skipping Original Masstree (compile failed)"
fi

# Run H-Masstree Memory
if [ -f "$BUILD_DIR/benchmark_hmasstree_mem" ]; then
    echo ""
    echo "--- H-Masstree Memory ---"
    $BUILD_DIR/benchmark_hmasstree_mem $NUM_KEYS 2>&1
else
    echo "Skipping H-Masstree Memory (compile failed)"
fi

# Run H-Masstree External
if [ -f "$BUILD_DIR/benchmark_hmasstree_ext" ]; then
    echo ""
    echo "--- H-Masstree External (Cache: ${CACHE_SIZE_MB}MB) ---"
    $BUILD_DIR/benchmark_hmasstree_ext $NUM_KEYS 2>&1
else
    echo "Skipping H-Masstree External (compile failed)"
fi

echo ""
echo "============================================"
echo "Benchmark Complete"
echo "============================================"
