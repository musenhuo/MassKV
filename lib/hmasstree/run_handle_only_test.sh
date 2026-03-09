#!/bin/bash
# H-Masstree Handle-Only vs Dual-Storage Benchmark
# 对比 Handle-Only 模式（通过 handle 解析遍历）和双存储模式（直接指针遍历）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_KEYS=${1:-1000000}
CACHE_SIZE_MB=${2:-64}

echo "============================================"
echo "H-Masstree Handle-Only vs Dual-Storage Test"
echo "============================================"
echo "Keys:       $NUM_KEYS"
echo "Cache Size: $CACHE_SIZE_MB MB"
echo ""

BUILD_DIR="../../build_hmasstree"
mkdir -p $BUILD_DIR

# Create test source (embedded to avoid include path issues)
cat > /tmp/hmasstree_handle_test.cpp << 'EOFCPP'
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <random>
#include <fstream>
#include <iomanip>

#include "hmasstree_wrapper.h"

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
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
    size_t cache_mb = 64;
    if (argc > 1) N = std::stoull(argv[1]);
    if (argc > 2) cache_mb = std::stoull(argv[2]);
    
#ifdef HMASSTREE_HANDLE_ONLY
    std::cout << "Mode: Handle-Only (resolve child via ScanContext)" << std::endl;
#elif defined(HMASSTREE_EXTERNAL_STORAGE)
    std::cout << "Mode: Dual-Storage (direct pointer traversal)" << std::endl;
#else
    std::cout << "Mode: Memory-Only (no external storage)" << std::endl;
#endif
    std::cout << "Keys: " << N << " (16B key, 16B value)" << std::endl;
    
    size_t mem_before = get_memory_usage_kb();
    
    HMasstreeWrapper mt;
    
#ifdef HMASSTREE_EXTERNAL_STORAGE
    Masstree::ExternalIndexConfig ext_config;
    ext_config.storage_path = "/tmp/hmasstree_handle_test.dat";
    ext_config.cache_size_mb = cache_mb;
    ext_config.storage_size_mb = 1024;
    
    if (!mt.init_external_storage(ext_config)) {
        std::cerr << "Failed to init external storage!" << std::endl;
        return 1;
    }
    mt.thread_init_external(1);
    std::cout << "Cache Size: " << cache_mb << " MB" << std::endl;
#else
    HMasstreeWrapper::thread_init(1);
#endif
    
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
    
    std::cout << "Insert: " << std::fixed << std::setprecision(2) 
              << insert_ms << " ms, " 
              << (N / insert_ms * 1000 / 1000) << " K ops/sec" << std::endl;
    std::cout << "Memory: " << (mem_after - mem_before) / 1024.0 << " MB" << std::endl;
    
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // Flush before read
    mt.flush_external_storage();
#endif
    
    // Read (random order)
    std::shuffle(keys.begin(), keys.end(), rng);
    t1 = std::chrono::high_resolution_clock::now();
    size_t found = 0;
    for (size_t i = 0; i < N; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        if (mt.search(k16, val)) found++;
    }
    t2 = std::chrono::high_resolution_clock::now();
    
    double read_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "Read: " << read_ms << " ms, " 
              << (N / read_ms * 1000 / 1000) << " K ops/sec"
              << " (found: " << found << "/" << N << ")" << std::endl;
    
    // Latency sampling
    std::vector<uint64_t> latencies;
    latencies.reserve(10000);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (size_t i = 0; i < 10000; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        auto lt1 = std::chrono::high_resolution_clock::now();
        uint64_t val;
        mt.search(k16, val);
        auto lt2 = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(lt2 - lt1).count());
    }
    std::sort(latencies.begin(), latencies.end());
    std::cout << "Latency P50: " << latencies[5000] / 1000.0 << " us, "
              << "P99: " << latencies[9900] / 1000.0 << " us, "
              << "P999: " << latencies[9990] / 1000.0 << " us" << std::endl;
    
#ifdef HMASSTREE_EXTERNAL_STORAGE
    mt.print_external_stats();
    // Cleanup
    unlink("/tmp/hmasstree_handle_test.dat");
#endif
    
    return 0;
}
EOFCPP

echo "--- Compiling Dual-Storage Mode ---"
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/hmasstree_handle_test.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o $BUILD_DIR/benchmark_dual_storage 2>&1 | head -10 || true
echo ""

echo "--- Compiling Handle-Only Mode ---"
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE -DHMASSTREE_HANDLE_ONLY \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/hmasstree_handle_test.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o $BUILD_DIR/benchmark_handle_only 2>&1 | head -10 || true
echo ""

echo "============================================"
echo "Running Dual-Storage Mode"
echo "============================================"
if [ -f "$BUILD_DIR/benchmark_dual_storage" ]; then
    $BUILD_DIR/benchmark_dual_storage $NUM_KEYS $CACHE_SIZE_MB
else
    echo "Build failed!"
fi
echo ""

echo "============================================"
echo "Running Handle-Only Mode"
echo "============================================"
if [ -f "$BUILD_DIR/benchmark_handle_only" ]; then
    $BUILD_DIR/benchmark_handle_only $NUM_KEYS $CACHE_SIZE_MB
else
    echo "Build failed!"
fi

echo ""
echo "Benchmark complete."
