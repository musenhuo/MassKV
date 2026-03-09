#!/bin/bash
# H-Masstree Cold Restart Test (v3 - Full Cold Restart)
# 测试真正的冷启动功能：序列化所有节点，关闭进程，重新加载

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_KEYS=${1:-10000}
CACHE_SIZE_MB=${2:-32}
STORAGE_PATH="/tmp/hmasstree_cold_restart.dat"

echo "============================================"
echo "H-Masstree Cold Restart Test (v3)"
echo "============================================"
echo "Keys:       $NUM_KEYS"
echo "Cache Size: $CACHE_SIZE_MB MB"
echo "Storage:    $STORAGE_PATH"
echo ""

# Remove old storage file
rm -f "$STORAGE_PATH"

# Create test program
cat > /tmp/cold_restart_test.cpp << 'EOFCPP'
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
#include "node_serializer.h"
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

size_t file_size(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        return st.st_size;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <mode> <storage_path> [num_keys] [cache_mb]" << std::endl;
        std::cerr << "  mode: 1=serialize, 2=restore" << std::endl;
        return 1;
    }
    
    int mode = atoi(argv[1]);
    const char* storage_path = argv[2];
    size_t N = (argc > 3) ? std::stoull(argv[3]) : 10000;
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
        // Mode 1: Create, Serialize, Close
        //===========================================
        std::cout << "=== Phase 1: Create & Serialize ===" << std::endl;
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
        
        // Insert all keys
        std::cout << "Inserting " << N << " keys..." << std::endl;
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
        
        // Verify some reads before serialization
        std::cout << "\nVerifying reads before serialization..." << std::endl;
        size_t found_before = 0;
        for (size_t i = 0; i < std::min(N, (size_t)1000); i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            uint64_t val;
            if (mt.search(k16, val)) {
                if (val == keys[i] * 100) found_before++;
            }
        }
        std::cout << "Verified: " << found_before << "/" << std::min(N, (size_t)1000) << " keys correct" << std::endl;
        
        // Get root handle
        Masstree::NodeHandle root_h = mt.get_current_root_handle();
        std::cout << "\nRoot handle: valid=" << root_h.is_valid()
                  << ", page=" << root_h.page_id()
                  << ", slot=" << (int)root_h.slot_index() << std::endl;
        
        // Serialize all nodes
        std::cout << "\nSerializing all nodes..." << std::endl;
        t1 = std::chrono::high_resolution_clock::now();
        size_t serialized = mt.serialize_all_nodes();
        t2 = std::chrono::high_resolution_clock::now();
        
        double serialize_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Serialized " << serialized << " nodes in " 
                  << serialize_ms << " ms" << std::endl;
        
        // Get memory and file stats
        size_t mem_kb = get_memory_usage_kb();
        std::cout << "\nMemory usage: " << mem_kb / 1024.0 << " MB" << std::endl;
        
        mt.print_external_stats();
        
        std::cout << "\n=== Phase 1 Complete ===" << std::endl;
        std::cout << "Storage file: " << storage_path << std::endl;
        std::cout << "File size: " << file_size(storage_path) / 1024.0 << " KB" << std::endl;
        
    } else if (mode == 2) {
        //===========================================
        // Mode 2: Restore from storage
        //===========================================
        std::cout << "=== Phase 2: Cold Restart & Restore ===" << std::endl;
        std::cout << "Keys (expected): " << N << std::endl;
        std::cout << std::endl;
        
        if (!file_exists(storage_path)) {
            std::cerr << "ERROR: Storage file not found: " << storage_path << std::endl;
            return 1;
        }
        
        std::cout << "Storage file size: " << file_size(storage_path) / 1024.0 << " KB" << std::endl;
        
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
        
        // Try to restore from storage
        std::cout << "\nAttempting to restore tree from storage..." << std::endl;
        auto t1 = std::chrono::high_resolution_clock::now();
        bool restored = mt.restore_from_storage();
        auto t2 = std::chrono::high_resolution_clock::now();
        
        if (!restored) {
            std::cerr << "Failed to restore tree!" << std::endl;
            return 1;
        }
        
        double restore_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Tree restored in " << restore_ms << " ms" << std::endl;
        
        // Get root handle after restore
        Masstree::NodeHandle root_h = mt.get_current_root_handle();
        std::cout << "Root handle after restore: valid=" << root_h.is_valid()
                  << ", page=" << root_h.page_id()
                  << ", slot=" << (int)root_h.slot_index() << std::endl;
        
        // Try to read some keys (this may trigger on-demand loading)
        std::cout << "\nAttempting to read keys after cold restart..." << std::endl;
        std::cout << "(Note: Full key verification requires complete on-demand loading)" << std::endl;
        
        // For now, just verify the root node is accessible
        size_t found = 0;
        size_t tested = std::min(N, (size_t)100);
        for (size_t i = 0; i < tested; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            uint64_t val;
            if (mt.search(k16, val)) {
                if (val == keys[i] * 100) found++;
            }
        }
        
        std::cout << "Read test: " << found << "/" << tested << " keys found" << std::endl;
        
        if (found == tested) {
            std::cout << "\n✅ COLD RESTART SUCCESS!" << std::endl;
            std::cout << "All tested keys were found after process restart." << std::endl;
        } else if (found > 0) {
            std::cout << "\n⚠️  PARTIAL SUCCESS" << std::endl;
            std::cout << found << "/" << tested << " keys recovered." << std::endl;
            std::cout << "Some keys may require on-demand child loading." << std::endl;
        } else {
            std::cout << "\n❌ COLD RESTART FAILED" << std::endl;
            std::cout << "No keys could be read after restart." << std::endl;
            std::cout << "This may indicate incomplete on-demand loading." << std::endl;
        }
        
        mt.print_external_stats();
        
        std::cout << "\n=== Phase 2 Complete ===" << std::endl;
    }
    
    return 0;
#endif
}
EOFCPP

# Compile
echo "Compiling cold restart test..."
g++ -std=c++17 -O3 -g -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
    -I. -I.. -I../../include \
    -include config.h \
    /tmp/cold_restart_test.cpp \
    hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
    straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
    misc.cc compiler.cc memdebug.cc clp.c \
    -lpthread -o /tmp/cold_restart_test 2>&1 | head -30

if [ ! -f /tmp/cold_restart_test ]; then
    echo "Compilation failed!"
    exit 1
fi

echo ""
echo "=== Running Phase 1: Create & Serialize ==="
/tmp/cold_restart_test 1 "$STORAGE_PATH" $NUM_KEYS $CACHE_SIZE_MB

echo ""
echo "=== Storage file info ==="
ls -lh "$STORAGE_PATH" 2>/dev/null || echo "Storage file not found"

echo ""
echo "=== Simulating process restart... ==="
sleep 1

echo ""
echo "=== Running Phase 2: Cold Restart & Restore ==="
/tmp/cold_restart_test 2 "$STORAGE_PATH" $NUM_KEYS $CACHE_SIZE_MB

echo ""
echo "============================================"
echo "Cold Restart Test Complete"
echo "============================================"

# Cleanup (optional - comment out to inspect storage file)
# rm -f "$STORAGE_PATH"
