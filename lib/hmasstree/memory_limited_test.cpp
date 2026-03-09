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

double measure_percentile(std::vector<double>& data, double p) {
    if (data.empty()) return 0;
    std::sort(data.begin(), data.end());
    size_t idx = (size_t)(p * data.size());
    if (idx >= data.size()) idx = data.size() - 1;
    return data[idx];
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <mode> <storage_path> [num_keys] [cache_mb]" << std::endl;
        std::cerr << "  mode: 1=create&serialize, 2=cold_restart&benchmark" << std::endl;
        return 1;
    }
    
    int mode = atoi(argv[1]);
    const char* storage_path = argv[2];
    size_t N = (argc > 3) ? std::stoull(argv[3]) : 1000000;
    size_t cache_mb = (argc > 4) ? std::stoull(argv[4]) : 16;
    
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
        // Mode 1: Create & Serialize
        //===========================================
        std::cout << "\n=== Phase 1: Create & Serialize ===" << std::endl;
        std::cout << "Keys: " << N << ", Cache: " << cache_mb << " MB" << std::endl;
        
        size_t mem_before = get_memory_usage_kb();
        
        HMasstreeWrapper mt;
        
        Masstree::ExternalIndexConfig config;
        config.storage_path = storage_path;
        config.cache_size_mb = cache_mb;
        config.storage_size_mb = 4096;  // 4GB storage
        
        if (!mt.init_external_storage(config)) {
            std::cerr << "Failed to init external storage!" << std::endl;
            return 1;
        }
        mt.thread_init_external(1);
        
        // Insert all keys
        std::cout << "\nInserting " << N << " keys..." << std::endl;
        auto t1 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < N; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            ValueHelper vh(keys[i] * 100);
            mt.insert(k16, vh);
            if ((i + 1) % 1000000 == 0) {
                std::cout << "  Inserted: " << (i + 1) / 1000000 << "M" << std::endl;
            }
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        
        double insert_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Insert completed: " << std::fixed << std::setprecision(2)
                  << insert_ms << " ms, "
                  << (N / insert_ms * 1000 / 1000) << " K ops/sec" << std::endl;
        
        size_t mem_after = get_memory_usage_kb();
        std::cout << "Memory after insert: " << mem_after / 1024.0 << " MB" << std::endl;
        
        // Serialize all nodes
        std::cout << "\nSerializing all nodes..." << std::endl;
        t1 = std::chrono::high_resolution_clock::now();
        size_t serialized = mt.serialize_all_nodes();
        t2 = std::chrono::high_resolution_clock::now();
        
        double serialize_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::cout << "Serialized: " << serialized << " nodes in " 
                  << serialize_ms << " ms" << std::endl;
        
        std::cout << "\nPhase 1 Result:" << std::endl;
        std::cout << "- Storage file: " << storage_path << std::endl;
        std::cout << "- File size: " << file_size(storage_path) / 1024.0 / 1024.0 << " MB" << std::endl;
        std::cout << "- Memory used: " << mem_after / 1024.0 << " MB" << std::endl;
        
    } else if (mode == 2) {
        //===========================================
        // Mode 2: Cold Restart & Benchmark
        //===========================================
        std::cout << "\n=== Phase 2: Cold Restart & Benchmark ===" << std::endl;
        std::cout << "Keys (expected): " << N << ", Cache: " << cache_mb << " MB" << std::endl;
        
        if (!file_exists(storage_path)) {
            std::cerr << "ERROR: Storage file not found: " << storage_path << std::endl;
            return 1;
        }
        
        std::cout << "Storage file size: " << file_size(storage_path) / 1024.0 / 1024.0 << " MB" << std::endl;
        
        size_t mem_before = get_memory_usage_kb();
        
        HMasstreeWrapper mt;
        
        Masstree::ExternalIndexConfig config;
        config.storage_path = storage_path;
        config.cache_size_mb = cache_mb;
        config.storage_size_mb = 4096;
        
        if (!mt.init_external_storage(config)) {
            std::cerr << "Failed to init external storage!" << std::endl;
            return 1;
        }
        mt.thread_init_external(1);
        
        // Restore from storage - measure time and memory
        std::cout << "\nRestoring tree (on-demand loading enabled)..." << std::endl;
        auto t1 = std::chrono::high_resolution_clock::now();
        bool restored = mt.restore_from_storage();
        auto t2 = std::chrono::high_resolution_clock::now();
        
        if (!restored) {
            std::cerr << "Failed to restore tree!" << std::endl;
            return 1;
        }
        
        double restore_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
        size_t mem_after_restore = get_memory_usage_kb();
        
        std::cout << "Restore completed: " << restore_ms << " ms" << std::endl;
        std::cout << "Memory after restore: " << mem_after_restore / 1024.0 << " MB" << std::endl;
        
        // Benchmark reads with various patterns
        std::cout << "\n--- Read Benchmark ---" << std::endl;
        
        // Sequential read test
        std::cout << "\n1. Sequential Read (first 100K keys):" << std::endl;
        size_t test_count = std::min(N, (size_t)100000);
        std::vector<double> seq_latencies;
        seq_latencies.reserve(test_count);
        
        size_t found = 0;
        for (size_t i = 0; i < test_count; i++) {
            Key16 k16(keys[i] >> 32, keys[i]);
            auto start = std::chrono::high_resolution_clock::now();
            uint64_t val;
            if (mt.search(k16, val)) {
                if (val == keys[i] * 100) found++;
            }
            auto end = std::chrono::high_resolution_clock::now();
            seq_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }
        
        std::cout << "  Found: " << found << "/" << test_count << " keys" << std::endl;
        std::cout << "  P50:  " << measure_percentile(seq_latencies, 0.50) << " us" << std::endl;
        std::cout << "  P99:  " << measure_percentile(seq_latencies, 0.99) << " us" << std::endl;
        std::cout << "  P999: " << measure_percentile(seq_latencies, 0.999) << " us" << std::endl;
        
        size_t mem_after_seq = get_memory_usage_kb();
        std::cout << "  Memory after seq read: " << mem_after_seq / 1024.0 << " MB" << std::endl;
        
        // Random read test (warm cache)
        std::cout << "\n2. Random Read (100K ops, warm cache):" << std::endl;
        std::vector<double> rand_latencies;
        rand_latencies.reserve(100000);
        
        std::mt19937_64 test_rng(123);
        found = 0;
        for (size_t i = 0; i < 100000; i++) {
            size_t idx = test_rng() % N;
            Key16 k16(keys[idx] >> 32, keys[idx]);
            auto start = std::chrono::high_resolution_clock::now();
            uint64_t val;
            if (mt.search(k16, val)) {
                if (val == keys[idx] * 100) found++;
            }
            auto end = std::chrono::high_resolution_clock::now();
            rand_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }
        
        std::cout << "  Found: " << found << "/100000 keys" << std::endl;
        std::cout << "  P50:  " << measure_percentile(rand_latencies, 0.50) << " us" << std::endl;
        std::cout << "  P99:  " << measure_percentile(rand_latencies, 0.99) << " us" << std::endl;
        std::cout << "  P999: " << measure_percentile(rand_latencies, 0.999) << " us" << std::endl;
        
        size_t mem_after_rand = get_memory_usage_kb();
        std::cout << "  Memory after random read: " << mem_after_rand / 1024.0 << " MB" << std::endl;
        
        // Statistics
        std::cout << "\n--- Final Statistics ---" << std::endl;
        mt.print_external_stats();
        
        std::cout << "\nPhase 2 Summary:" << std::endl;
        std::cout << "- Cold restart time: " << restore_ms << " ms" << std::endl;
        std::cout << "- Memory after restore: " << mem_after_restore / 1024.0 << " MB" << std::endl;
        std::cout << "- Memory after reads: " << mem_after_rand / 1024.0 << " MB" << std::endl;
        std::cout << "- Cache efficiency: On-demand loading enabled" << std::endl;
    }
    
    return 0;
#endif
}
