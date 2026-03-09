#!/bin/bash
# H-Masstree Cold Start Test (Complete Version)
# 测试外存模式的冷启动功能

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_KEYS=${1:-100000}
CACHE_SIZE_MB=${2:-32}
STORAGE_PATH="/tmp/hmasstree_cold_start_v2.dat"

echo "============================================"
echo "H-Masstree Cold Start Test (v2)"
echo "============================================"
echo "Keys:       $NUM_KEYS"
echo "Cache Size: $CACHE_SIZE_MB MB"
echo "Storage:    $STORAGE_PATH"
echo ""

# Remove old storage file
rm -f "$STORAGE_PATH"

# Create test program
cat > /tmp/cold_start_test_v2.cpp << 'EOFCPP'
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
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <mode> <storage_path> [num_keys] [cache_mb]" << std::endl;
        std::cerr << "  mode: 1=warm_start, 2=cold_start_verify" << std::endl;
        return 1;
    }
    
    int mode = atoi(argv[1]);
    const char* storage_path = argv[2];
    size_t N = (argc > 3) ? std::stoull(argv[3]) : 100000;
    size_t cache_mb = (argc > 4) ? std::stoull(argv[4]) : 32;
    
#ifndef HMASSTREE_EXTERNAL_STORAGE
    std::cerr << "ERROR: This test requires HMASSTREE_EXTERNAL_STORAGE" << std::endl;
    return 1;
#else
    
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) keys[i] = i;
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    if (mode == 1) {
        //===========================================
        // Mode 1: Warm Start - Create and Persist
        //===========================================
        std::cout << "=== Phase 1: Warm Start (Create & Persist) ===" << std::endl;
        std::cout << "Keys: " << N << ", Cache: " << cache_mb << " MB" << std::endl;
        std::cout << std::endl;
        
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
        
        // Get root handle before persisting
        Masstree::NodeHandle root_h = mt.get_current_root_handle();
        std::cout << "Root handle (current): valid=" << root_h.is_valid() 
                  << ", page_id=" << root_h.page_id()
                  << ", slot=" << (int)root_h.slot_index() << std::endl;
        
        // Persist tree structure
        if (mt.persist_tree_structure()) {
            std::cout << "Tree structure persisted successfully." << std::endl;
        } else {
            std::cerr << "Failed to persist tree structure!" << std::endl;
        }
        
        // Flush all dirty pages
        mt.flush_external_storage();
        std::cout << "Cache flushed to storage." << std::endl;
        
        // Warm read test
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
        
        size_t mem_after = get_memory_usage_kb();
        std::cout << "Memory: " << (mem_after - mem_before) / 1024.0 << " MB" << std::endl;
        
        mt.print_external_stats();
        
        std::cout << std::endl;
        std::cout << "Phase 1 complete. Storage file saved." << std::endl;
        
    } else if (mode == 2) {
        //===========================================
        // Mode 2: Cold Start - Recover and Verify
        //===========================================
        std::cout << "=== Phase 2: Cold Start (Recover & Verify) ===" << std::endl;
        std::cout << "Keys (expected): " << N << std::endl;
        std::cout << std::endl;
        
        if (!file_exists(storage_path)) {
            std::cerr << "ERROR: Storage file not found: " << storage_path << std::endl;
            return 1;
        }
        
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
        
        // Check if we have persisted tree
        if (mt.has_persisted_tree()) {
            Masstree::NodeHandle root_h = mt.get_persisted_root_handle();
            std::cout << "Found persisted root handle: valid=" << root_h.is_valid()
                      << ", page_id=" << root_h.page_id()
                      << ", slot=" << (int)root_h.slot_index() << std::endl;
            
            std::cout << std::endl;
            std::cout << "NOTE: True cold start recovery requires:" << std::endl;
            std::cout << "  1. Node serialization to storage pages" << std::endl;
            std::cout << "  2. Handle-only traversal mode (HMASSTREE_HANDLE_ONLY)" << std::endl;
            std::cout << "  3. On-demand node loading from storage" << std::endl;
            std::cout << std::endl;
            std::cout << "Current status: Root handle is persisted, but nodes are in-memory only." << std::endl;
            std::cout << "After process restart, data would need to be re-inserted." << std::endl;
        } else {
            std::cout << "No persisted tree found - this is expected after restart." << std::endl;
        }
        
        mt.print_external_stats();
    }
    
    return 0;
#endif
}
EOFCPP

# Compile
echo "Compiling cold start test..."
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/cold_start_test_v2.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o /tmp/cold_start_test_v2 2>/dev/null

if [ ! -f /tmp/cold_start_test_v2 ]; then
    echo "Compilation failed!"
    exit 1
fi

echo ""
echo "=== Running Phase 1: Warm Start ==="
/tmp/cold_start_test_v2 1 "$STORAGE_PATH" $NUM_KEYS $CACHE_SIZE_MB

echo ""
echo "=== Storage file info ==="
ls -lh "$STORAGE_PATH" 2>/dev/null || echo "Storage file not found"

echo ""
echo "=== Running Phase 2: Cold Start Verify ==="
/tmp/cold_start_test_v2 2 "$STORAGE_PATH" $NUM_KEYS $CACHE_SIZE_MB

echo ""
echo "============================================"
echo "Cold Start Test Summary"
echo "============================================"
echo ""
echo "What works now:"
echo "  ✅ Nodes get self_handle_ during creation"
echo "  ✅ Root handle can be persisted to superblock"
echo "  ✅ IndexStorageRegistry provides thread-local storage access"
echo ""
echo "What's needed for true cold start:"
echo "  ⚠️  Node content serialization to storage pages"
echo "  ⚠️  Handle-only traversal (resolve child via cache)"
echo "  ⚠️  On-demand node loading from SSD"
echo ""
echo "Current architecture supports hot restart (same process),"
echo "but not cold restart (new process loading persisted data)."

# Cleanup
rm -f "$STORAGE_PATH"
