#!/bin/bash
# H-Masstree Cold Start Test
# 测试外存模式的冷启动 - 持久化后清空缓存再加载

set -e

echo "============================================"
echo "H-Masstree Cold Start Test"
echo "============================================"
echo ""

# Build test program
cat > /tmp/cold_start_test.cpp << 'EOFCPP'
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <random>
#include <fstream>
#include <iomanip>
#include <unistd.h>
#include <sys/stat.h>

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

bool file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

int main(int argc, char* argv[]) {
    size_t N = 100000;
    size_t cache_mb = 32;
    const char* storage_path = "/tmp/hmasstree_cold_start.dat";
    
    if (argc > 1) N = std::stoull(argv[1]);
    if (argc > 2) cache_mb = std::stoull(argv[2]);
    
#ifndef HMASSTREE_EXTERNAL_STORAGE
    std::cerr << "ERROR: This test requires HMASSTREE_EXTERNAL_STORAGE" << std::endl;
    return 1;
#else
    
    std::cout << "Keys: " << N << std::endl;
    std::cout << "Cache Size: " << cache_mb << " MB" << std::endl;
    std::cout << "Storage Path: " << storage_path << std::endl;
    std::cout << std::endl;
    
    // Prepare keys
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) keys[i] = i;
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // Clean up any existing file
    unlink(storage_path);
    
    //===========================================
    // Phase 1: Initial Load - Warm Start
    //===========================================
    std::cout << "=== Phase 1: Warm Start (Initial Load) ===" << std::endl;
    
    {
        HMasstreeWrapper mt;
        
        Masstree::ExternalIndexConfig config;
        config.storage_path = storage_path;
        config.cache_size_mb = cache_mb;
        config.storage_size_mb = 1024;
        
        if (!mt.init_external_storage(config)) {
            std::cerr << "Failed to init external storage!" << std::endl;
            return 1;
        }
        mt.thread_init_external(1);
        
        size_t mem_before = get_memory_usage_kb();
        
        // Insert all keys
        auto t1 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < N; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            ValueHelper vh(keys[i] * 100);
            mt.insert(k16, vh);
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        
        double insert_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Insert: " << std::fixed << std::setprecision(2)
                  << insert_ms << " ms, "
                  << (N / insert_ms * 1000 / 1000) << " K ops/sec" << std::endl;
        
        // Flush to storage
        mt.flush_external_storage();
        std::cout << "Flushed to storage." << std::endl;
        
        // Warm read (after insert, data is in memory)
        std::shuffle(keys.begin(), keys.end(), rng);
        t1 = std::chrono::high_resolution_clock::now();
        size_t found = 0;
        for (size_t i = 0; i < N; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            uint64_t val;
            if (mt.search(k16, val)) found++;
        }
        t2 = std::chrono::high_resolution_clock::now();
        
        double warm_read_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Warm Read: " << warm_read_ms << " ms, "
                  << (N / warm_read_ms * 1000 / 1000) << " K ops/sec"
                  << " (found: " << found << "/" << N << ")" << std::endl;
        
        // Latency samples
        std::vector<uint64_t> latencies;
        latencies.reserve(std::min(N, (size_t)10000));
        for (size_t i = 0; i < std::min(N, (size_t)10000); i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            auto lt1 = std::chrono::high_resolution_clock::now();
            uint64_t val;
            mt.search(k16, val);
            auto lt2 = std::chrono::high_resolution_clock::now();
            latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(lt2 - lt1).count());
        }
        std::sort(latencies.begin(), latencies.end());
        std::cout << "Warm Latency P50: " << latencies[latencies.size()/2] / 1000.0 << " us, "
                  << "P99: " << latencies[(size_t)(latencies.size() * 0.99)] / 1000.0 << " us" << std::endl;
        
        size_t mem_after = get_memory_usage_kb();
        std::cout << "Memory: " << (mem_after - mem_before) / 1024.0 << " MB" << std::endl;
        
        mt.print_external_stats();
        std::cout << std::endl;
    }  // mt destroyed here, but storage file remains
    
    //===========================================
    // Phase 2: Cold Start - New instance loading
    //===========================================
    std::cout << "=== Phase 2: Cold Start (New Instance) ===" << std::endl;
    
    if (!file_exists(storage_path)) {
        std::cerr << "ERROR: Storage file not found after Phase 1!" << std::endl;
        return 1;
    }
    
    std::cout << "(Note: Current implementation doesn't persist tree structure to disk." << std::endl;
    std::cout << " True cold start would need additional persistence layer.)" << std::endl;
    std::cout << std::endl;
    
    // For now, just demonstrate cache behavior with limited cache
    {
        HMasstreeWrapper mt;
        
        Masstree::ExternalIndexConfig config;
        config.storage_path = storage_path;
        config.cache_size_mb = cache_mb;
        config.storage_size_mb = 1024;
        
        if (!mt.init_external_storage(config)) {
            std::cerr << "Failed to init external storage!" << std::endl;
            return 1;
        }
        mt.thread_init_external(1);
        
        // Re-insert to simulate a fresh tree with same data
        size_t mem_before = get_memory_usage_kb();
        
        std::shuffle(keys.begin(), keys.end(), rng);
        auto t1 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < N; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            ValueHelper vh(keys[i] * 100);
            mt.insert(k16, vh);
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        
        double insert_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Re-Insert: " << insert_ms << " ms, "
                  << (N / insert_ms * 1000 / 1000) << " K ops/sec" << std::endl;
        
        size_t mem_after = get_memory_usage_kb();
        std::cout << "Memory: " << (mem_after - mem_before) / 1024.0 << " MB" << std::endl;
        
        mt.print_external_stats();
        std::cout << std::endl;
    }
    
    //===========================================
    // Summary
    //===========================================
    std::cout << "=== Cold Start Test Summary ===" << std::endl;
    std::cout << "" << std::endl;
    std::cout << "Current Status:" << std::endl;
    std::cout << "- External storage infrastructure is in place" << std::endl;
    std::cout << "- Cache/eviction framework ready" << std::endl;
    std::cout << "- For true cold start, need to:" << std::endl;
    std::cout << "  1. Persist tree structure (root handle, node handles)" << std::endl;
    std::cout << "  2. Use Handle-only traversal (requires node_to_handle fix)" << std::endl;
    std::cout << "  3. Implement on-demand node loading" << std::endl;
    std::cout << std::endl;
    
    // Cleanup
    unlink(storage_path);
    
    return 0;
#endif
}
EOFCPP

echo "Compiling cold start test..."
cd /home/zwt/yjy/FlowKV/lib/hmasstree
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/cold_start_test.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o /tmp/cold_start_test 2>/dev/null

echo ""
/tmp/cold_start_test ${1:-100000} ${2:-32}
