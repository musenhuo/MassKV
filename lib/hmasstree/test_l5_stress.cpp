/* H-Masstree L5 Stress Test
 * Tests both memory mode and external storage mode
 * 
 * Build:
 *   # Memory mode (default):
 *   g++ -std=c++17 -O2 -DTEST_MEMORY_MODE -I. -I.. -I../../include \
 *       -include config.h test_l5_stress.cpp hmasstree_wrapper.cc \
 *       straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
 *       misc.cc compiler.cc memdebug.cc clp.c -lpthread -o test_l5_memory
 * 
 *   # External storage mode:
 *   g++ -std=c++17 -O2 -DHMASSTREE_EXTERNAL_STORAGE -I. -I.. -I../../include \
 *       -include config.h test_l5_stress.cpp hmasstree_wrapper.cc \
 *       index_storage.cpp node_cache.cpp straccum.cc string.cc str.cc \
 *       string_slice.cc kvthread.cc misc.cc compiler.cc memdebug.cc clp.c \
 *       -lpthread -o test_l5_external
 * 
 * Usage:
 *   ./test_l5_memory -n 100000 -t 4
 *   ./test_l5_external -n 100000 -t 4 --cache-mb 64 --storage-path /tmp/test.dat
 */

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <cassert>
#include <cstring>
#include <unistd.h>

#include "hmasstree_wrapper.h"

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
#endif

// Test configuration
struct TestConfig {
    size_t num_keys = 100000;
    size_t num_threads = 4;
    size_t duration_sec = 10;  // 0 = use num_keys
    uint32_t seed = 12345;
    
    // External storage specific
    std::string storage_path = "/tmp/hmasstree_l5_test.dat";
    size_t cache_mb = 64;
    size_t storage_mb = 512;
    bool enable_external = false;
    
    bool verbose = false;
};

// Test statistics
struct TestStats {
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> deletes{0};
    std::atomic<uint64_t> scans{0};
    std::atomic<uint64_t> errors{0};
    
    std::atomic<bool> stop{false};
    
    void reset() {
        inserts = 0;
        reads = 0;
        deletes = 0;
        scans = 0;
        errors = 0;
        stop = false;
    }
    
    uint64_t total() const {
        return inserts + reads + deletes + scans;
    }
    
    void print() const {
        std::cout << "  Insert: " << inserts << std::endl;
        std::cout << "  Read:   " << reads << std::endl;
        std::cout << "  Delete: " << deletes << std::endl;
        std::cout << "  Scan:   " << scans << std::endl;
        std::cout << "  Total:  " << total() << std::endl;
        if (errors > 0) {
            std::cout << "  ERRORS: " << errors << std::endl;
        }
    }
};

// Global test objects
TestConfig g_config;
TestStats g_stats;

// Helper: create ValueHelper
inline ValueHelper make_vh(uint64_t new_val) {
    return ValueHelper(new_val);
}

// ============================================================================
// Test: Memory Mode Stress
// ============================================================================

void stress_worker_memory(HMasstreeWrapper* mt, int tid) {
    HMasstreeWrapper::thread_init(tid);
    
    std::mt19937_64 rng(g_config.seed + tid);
    const size_t key_space = g_config.num_keys * 10;
    const size_t ops_per_thread = g_config.num_keys / g_config.num_threads;
    
    for (size_t i = 0; i < ops_per_thread && !g_stats.stop; i++) {
        uint64_t key = rng() % key_space;
        int op = rng() % 100;
        
        if (op < 40) {
            // 40% insert
            ValueHelper vh = make_vh(key * 100);
            mt->insert(key, vh);
            g_stats.inserts++;
        } else if (op < 50) {
            // 10% delete
            mt->remove(key);
            g_stats.deletes++;
        } else if (op < 90) {
            // 40% read
            uint64_t result;
            mt->search(key, result);
            g_stats.reads++;
        } else {
            // 10% scan
            std::vector<uint64_t> values;
            mt->scan(key, 10, values);
            g_stats.scans++;
        }
    }
}

bool test_stress_memory() {
    std::cout << "\n=== L5 Stress Test (Memory Mode) ===" << std::endl;
    
    HMasstreeWrapper mt;
    g_stats.reset();
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i < g_config.num_threads; i++) {
        threads.emplace_back(stress_worker_memory, &mt, i + 1);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    
    std::cout << "Statistics:" << std::endl;
    g_stats.print();
    std::cout << "  Time:       " << elapsed << " sec" << std::endl;
    std::cout << "  Throughput: " << g_stats.total() / elapsed / 1000 << " Kops/sec" << std::endl;
    
    if (g_stats.errors > 0) {
        std::cout << "[FAIL] test_stress_memory: errors detected" << std::endl;
        return false;
    }
    
    std::cout << "[PASS] test_stress_memory" << std::endl;
    return true;
}

// ============================================================================
// Test: External Storage Mode Stress
// ============================================================================

#ifdef HMASSTREE_EXTERNAL_STORAGE

void stress_worker_external(HMasstreeWrapper* mt, int tid) {
    mt->thread_init_external(tid);
    
    std::mt19937_64 rng(g_config.seed + tid);
    const size_t key_space = g_config.num_keys * 10;
    const size_t ops_per_thread = g_config.num_keys / g_config.num_threads;
    
    for (size_t i = 0; i < ops_per_thread && !g_stats.stop; i++) {
        uint64_t key = rng() % key_space;
        int op = rng() % 100;
        
        if (op < 40) {
            // 40% insert
            ValueHelper vh = make_vh(key * 100);
            mt->insert(key, vh);
            g_stats.inserts++;
        } else if (op < 50) {
            // 10% delete
            mt->remove(key);
            g_stats.deletes++;
        } else if (op < 90) {
            // 40% read
            uint64_t result;
            mt->search(key, result);
            g_stats.reads++;
        } else {
            // 10% scan
            std::vector<uint64_t> values;
            mt->scan(key, 10, values);
            g_stats.scans++;
        }
    }
}

bool test_stress_external() {
    std::cout << "\n=== L5 Stress Test (External Storage Mode) ===" << std::endl;
    
    // Clean up old test file
    unlink(g_config.storage_path.c_str());
    
    // Create wrapper and initialize external storage
    HMasstreeWrapper mt;
    
    Masstree::ExternalIndexConfig ext_config;
    ext_config.storage_path = g_config.storage_path;
    ext_config.cache_size_mb = g_config.cache_mb;
    ext_config.storage_size_mb = g_config.storage_mb;
    ext_config.enable_background_flush = true;
    ext_config.flush_interval_ms = 500;
    
    std::cout << "  Storage path: " << ext_config.storage_path << std::endl;
    std::cout << "  Cache size:   " << ext_config.cache_size_mb << " MB" << std::endl;
    std::cout << "  Storage size: " << ext_config.storage_size_mb << " MB" << std::endl;
    
    if (!mt.init_external_storage(ext_config)) {
        std::cout << "[FAIL] Failed to initialize external storage" << std::endl;
        return false;
    }
    
    std::cout << "  External storage initialized successfully" << std::endl;
    
    g_stats.reset();
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i < g_config.num_threads; i++) {
        threads.emplace_back(stress_worker_external, &mt, i + 1);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    
    // Flush and print stats
    mt.flush_external_storage();
    
    std::cout << "Statistics:" << std::endl;
    g_stats.print();
    std::cout << "  Time:       " << elapsed << " sec" << std::endl;
    std::cout << "  Throughput: " << g_stats.total() / elapsed / 1000 << " Kops/sec" << std::endl;
    
    std::cout << "\nExternal Storage Stats:" << std::endl;
    mt.print_external_stats();
    
    if (g_stats.errors > 0) {
        std::cout << "[FAIL] test_stress_external: errors detected" << std::endl;
        return false;
    }
    
    std::cout << "[PASS] test_stress_external" << std::endl;
    
    // Cleanup
    unlink(g_config.storage_path.c_str());
    
    return true;
}

bool test_external_correctness() {
    std::cout << "\n=== External Storage Correctness Test ===" << std::endl;
    
    // Clean up old test file
    unlink(g_config.storage_path.c_str());
    
    HMasstreeWrapper mt;
    
    Masstree::ExternalIndexConfig ext_config;
    ext_config.storage_path = g_config.storage_path;
    ext_config.cache_size_mb = 32;
    ext_config.storage_size_mb = 256;
    
    if (!mt.init_external_storage(ext_config)) {
        std::cout << "[FAIL] Failed to initialize external storage" << std::endl;
        return false;
    }
    
    mt.thread_init_external(1);
    
    const int N = 10000;
    
    // Insert
    std::cout << "  Inserting " << N << " keys..." << std::endl;
    for (int i = 0; i < N; i++) {
        ValueHelper vh = make_vh(i * 100);
        mt.insert(i, vh);
    }
    
    // Verify
    std::cout << "  Verifying..." << std::endl;
    int found = 0;
    int correct = 0;
    for (int i = 0; i < N; i++) {
        uint64_t val = 0;
        if (mt.search(i, val)) {
            found++;
            if (val == (uint64_t)(i * 100)) {
                correct++;
            }
        }
    }
    
    std::cout << "  Found: " << found << "/" << N << std::endl;
    std::cout << "  Correct: " << correct << "/" << found << std::endl;
    
    // Scan test - scan keys from 100 to 199 using count-based scan
    std::vector<uint64_t> scan_results;
    mt.scan(100, 50, scan_results);  // Start at 100, get 50 items
    std::cout << "  Scan returned: " << scan_results.size() << " items" << std::endl;
    
    mt.flush_external_storage();
    
    // Cleanup
    unlink(g_config.storage_path.c_str());
    
    if (found == N && correct == N && scan_results.size() == 50) {
        std::cout << "[PASS] test_external_correctness" << std::endl;
        return true;
    } else {
        std::cout << "[FAIL] test_external_correctness" << std::endl;
        return false;
    }
}

#endif  // HMASSTREE_EXTERNAL_STORAGE

// ============================================================================
// Main
// ============================================================================

void print_usage(const char* progname) {
    std::cout << "Usage: " << progname << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  -n NUM          Number of keys (default: 100000)" << std::endl;
    std::cout << "  -t NUM          Number of threads (default: 4)" << std::endl;
    std::cout << "  -v, --verbose   Verbose output" << std::endl;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    std::cout << "  --cache-mb NUM  Cache size in MB (default: 64)" << std::endl;
    std::cout << "  --storage-mb NUM Storage size in MB (default: 512)" << std::endl;
    std::cout << "  --storage-path PATH Storage file path (default: /tmp/hmasstree_l5_test.dat)" << std::endl;
#endif
    std::cout << "  -h, --help      Show this help" << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) {
            g_config.num_keys = std::stoull(argv[++i]);
        } else if (arg == "-t" && i + 1 < argc) {
            g_config.num_threads = std::stoull(argv[++i]);
        } else if (arg == "-v" || arg == "--verbose") {
            g_config.verbose = true;
        } else if (arg == "--cache-mb" && i + 1 < argc) {
            g_config.cache_mb = std::stoull(argv[++i]);
        } else if (arg == "--storage-mb" && i + 1 < argc) {
            g_config.storage_mb = std::stoull(argv[++i]);
        } else if (arg == "--storage-path" && i + 1 < argc) {
            g_config.storage_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    std::cout << "========================================" << std::endl;
    std::cout << "H-Masstree L5 Stress Test" << std::endl;
    std::cout << "========================================" << std::endl;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    std::cout << "Mode: External Storage" << std::endl;
#else
    std::cout << "Mode: Memory Only" << std::endl;
#endif
    std::cout << "Config: keys=" << g_config.num_keys 
              << ", threads=" << g_config.num_threads << std::endl;
    
    int passed = 0, failed = 0;
    
    // Run tests
#ifdef HMASSTREE_EXTERNAL_STORAGE
    // In external storage build, node structures are different
    // Must use external storage mode tests only
    if (test_external_correctness()) passed++; else failed++;
    if (test_stress_external()) passed++; else failed++;
#else
    // Memory-only mode
    if (test_stress_memory()) passed++; else failed++;
#endif
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return failed > 0 ? 1 : 0;
}
