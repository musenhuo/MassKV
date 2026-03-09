/* H-Masstree vs Masstree Benchmark Comparison
 *
 * 对比测试三种索引实现：
 * 1. 原版 Masstree（内存模式）
 * 2. H-Masstree 内存模式 
 * 3. H-Masstree 外存模式
 *
 * 测试指标：
 * - 内存占用量
 * - 读写延迟（P50/P99/P999）
 * - 吞吐率（ops/sec）
 *
 * 编译命令：
 * # 使用 CMake 构建目录中的库
 * g++ -std=c++17 -O2 -DFLOWKV_KEY16 \
 *     -I. -I.. -I../../include \
 *     -include config.h benchmark_comparison.cpp \
 *     hmasstree_wrapper.cc straccum.cc string.cc str.cc \
 *     string_slice.cc kvthread.cc misc.cc compiler.cc memdebug.cc clp.c \
 *     -lpthread -o ../../build_hmasstree/benchmark_comparison
 *
 * # 外存模式
 * g++ -std=c++17 -O2 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
 *     -I. -I.. -I../../include \
 *     -include config.h benchmark_comparison.cpp \
 *     hmasstree_wrapper.cc index_storage.cpp node_cache.cpp \
 *     straccum.cc string.cc str.cc string_slice.cc kvthread.cc \
 *     misc.cc compiler.cc memdebug.cc clp.c \
 *     -lpthread -o ../../build_hmasstree/benchmark_comparison_external
 */

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sys/resource.h>
#include <unistd.h>

#include "hmasstree_wrapper.h"

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
#endif

// ============================================================================
// Configuration
// ============================================================================

struct BenchmarkConfig {
    size_t num_keys = 1000000;      // 1M keys
    size_t num_threads = 4;
    size_t warmup_keys = 10000;
    double read_ratio = 0.8;         // 80% read, 20% write
    size_t num_operations = 1000000;
    bool verbose = false;
    std::string storage_path = "/tmp/hmasstree_bench.dat";
    size_t cache_size_mb = 64;
};

BenchmarkConfig g_config;

// ============================================================================
// Utilities
// ============================================================================

// Get current memory usage in KB
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

// Timer class
class BenchTimer {
public:
    void start() { start_ = std::chrono::high_resolution_clock::now(); }
    void stop() { end_ = std::chrono::high_resolution_clock::now(); }
    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(end_ - start_).count();
    }
    double elapsed_sec() const {
        return std::chrono::duration<double>(end_ - start_).count();
    }
    
    // For single operation timing
    uint64_t elapsed_ns() const {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - start_).count();
    }
    
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_, end_;
};

// Latency histogram
class LatencyHistogram {
public:
    void add(uint64_t latency_ns) {
        latencies_.push_back(latency_ns);
    }
    
    void sort_for_percentiles() {
        std::sort(latencies_.begin(), latencies_.end());
    }
    
    double p50() const {
        if (latencies_.empty()) return 0;
        return latencies_[latencies_.size() * 50 / 100] / 1000.0;  // us
    }
    
    double p99() const {
        if (latencies_.empty()) return 0;
        return latencies_[latencies_.size() * 99 / 100] / 1000.0;  // us
    }
    
    double p999() const {
        if (latencies_.empty()) return 0;
        return latencies_[latencies_.size() * 999 / 1000] / 1000.0;  // us
    }
    
    double avg() const {
        if (latencies_.empty()) return 0;
        uint64_t sum = 0;
        for (auto l : latencies_) sum += l;
        return (double)sum / latencies_.size() / 1000.0;  // us
    }
    
    size_t count() const { return latencies_.size(); }
    
private:
    std::vector<uint64_t> latencies_;
};

// ============================================================================
// Benchmark Results
// ============================================================================

struct BenchmarkResult {
    std::string name;
    
    // Memory
    size_t memory_before_kb;
    size_t memory_after_insert_kb;
    size_t memory_peak_kb;
    
    // Insert performance
    double insert_time_sec;
    double insert_throughput;  // ops/sec
    LatencyHistogram insert_latency;
    
    // Read performance
    double read_throughput;  // ops/sec
    LatencyHistogram read_latency;
    
    // Mixed workload
    double mixed_throughput;  // ops/sec
    LatencyHistogram mixed_latency;
    
    void print() const {
        std::cout << "\n===== " << name << " =====" << std::endl;
        
        std::cout << "\n[Memory Usage]" << std::endl;
        std::cout << "  Before:       " << std::fixed << std::setprecision(2) 
                  << memory_before_kb / 1024.0 << " MB" << std::endl;
        std::cout << "  After Insert: " << memory_after_insert_kb / 1024.0 << " MB" << std::endl;
        std::cout << "  Delta:        " << (memory_after_insert_kb - memory_before_kb) / 1024.0 << " MB" << std::endl;
        
        std::cout << "\n[Insert Performance]" << std::endl;
        std::cout << "  Throughput:   " << std::fixed << std::setprecision(2) 
                  << insert_throughput / 1000.0 << " K ops/sec" << std::endl;
        std::cout << "  Latency P50:  " << insert_latency.p50() << " us" << std::endl;
        std::cout << "  Latency P99:  " << insert_latency.p99() << " us" << std::endl;
        std::cout << "  Latency P999: " << insert_latency.p999() << " us" << std::endl;
        
        std::cout << "\n[Read Performance]" << std::endl;
        std::cout << "  Throughput:   " << read_throughput / 1000.0 << " K ops/sec" << std::endl;
        std::cout << "  Latency P50:  " << read_latency.p50() << " us" << std::endl;
        std::cout << "  Latency P99:  " << read_latency.p99() << " us" << std::endl;
        std::cout << "  Latency P999: " << read_latency.p999() << " us" << std::endl;
        
        std::cout << "\n[Mixed Workload (80% Read, 20% Write)]" << std::endl;
        std::cout << "  Throughput:   " << mixed_throughput / 1000.0 << " K ops/sec" << std::endl;
        std::cout << "  Latency P50:  " << mixed_latency.p50() << " us" << std::endl;
        std::cout << "  Latency P99:  " << mixed_latency.p99() << " us" << std::endl;
        std::cout << "  Latency P999: " << mixed_latency.p999() << " us" << std::endl;
    }
};

// ============================================================================
// H-Masstree Memory Mode Benchmark
// ============================================================================

BenchmarkResult benchmark_hmasstree_memory() {
    BenchmarkResult result;
    result.name = "H-Masstree (Memory Mode)";
    
    std::cout << "\n----- Running " << result.name << " -----" << std::endl;
    
    result.memory_before_kb = get_memory_usage_kb();
    
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    const size_t N = g_config.num_keys;
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) {
        keys[i] = i;
    }
    
    // Shuffle for random insert order
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // ===== Insert Benchmark =====
    std::cout << "  Inserting " << N << " keys..." << std::flush;
    BenchTimer timer;
    timer.start();
    
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        ValueHelper vh(keys[i] * 100);
        mt.insert(keys[i], vh);
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {  // Sample 1% of insertions
            result.insert_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    
    timer.stop();
    result.insert_time_sec = timer.elapsed_sec();
    result.insert_throughput = N / result.insert_time_sec;
    result.insert_latency.sort_for_percentiles();
    result.memory_after_insert_kb = get_memory_usage_kb();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    // ===== Read Benchmark =====
    std::cout << "  Reading " << N << " keys..." << std::flush;
    std::shuffle(keys.begin(), keys.end(), rng);
    
    timer.start();
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t val;
        mt.search(keys[i], val);
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {
            result.read_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    timer.stop();
    result.read_throughput = N / timer.elapsed_sec();
    result.read_latency.sort_for_percentiles();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    // ===== Mixed Workload Benchmark =====
    std::cout << "  Mixed workload (" << g_config.num_operations << " ops)..." << std::flush;
    const size_t OPS = g_config.num_operations;
    
    timer.start();
    for (size_t i = 0; i < OPS; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t key = rng() % N;
        if ((rng() % 100) < (g_config.read_ratio * 100)) {
            // Read
            uint64_t val;
            mt.search(key, val);
        } else {
            // Write
            ValueHelper vh(key * 100 + 1);
            mt.insert(key, vh);
        }
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {
            result.mixed_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    timer.stop();
    result.mixed_throughput = OPS / timer.elapsed_sec();
    result.mixed_latency.sort_for_percentiles();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    return result;
}

// ============================================================================
// H-Masstree External Storage Mode Benchmark
// ============================================================================

#ifdef HMASSTREE_EXTERNAL_STORAGE

BenchmarkResult benchmark_hmasstree_external() {
    BenchmarkResult result;
    result.name = "H-Masstree (External Storage Mode)";
    
    std::cout << "\n----- Running " << result.name << " -----" << std::endl;
    
    // Clean up old test file
    unlink(g_config.storage_path.c_str());
    
    result.memory_before_kb = get_memory_usage_kb();
    
    HMasstreeWrapper mt;
    
    // Initialize external storage
    Masstree::ExternalIndexConfig ext_config;
    ext_config.storage_path = g_config.storage_path;
    ext_config.cache_size_mb = g_config.cache_size_mb;
    ext_config.storage_size_mb = 512;
    
    if (!mt.init_external_storage(ext_config)) {
        std::cerr << "Failed to initialize external storage!" << std::endl;
        return result;
    }
    
    mt.thread_init_external(1);
    
    const size_t N = g_config.num_keys;
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) {
        keys[i] = i;
    }
    
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // ===== Insert Benchmark =====
    std::cout << "  Inserting " << N << " keys..." << std::flush;
    BenchTimer timer;
    timer.start();
    
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        ValueHelper vh(keys[i] * 100);
        mt.insert(keys[i], vh);
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {
            result.insert_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    
    timer.stop();
    result.insert_time_sec = timer.elapsed_sec();
    result.insert_throughput = N / result.insert_time_sec;
    result.insert_latency.sort_for_percentiles();
    result.memory_after_insert_kb = get_memory_usage_kb();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    // ===== Read Benchmark =====
    std::cout << "  Reading " << N << " keys..." << std::flush;
    std::shuffle(keys.begin(), keys.end(), rng);
    
    timer.start();
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t val;
        mt.search(keys[i], val);
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {
            result.read_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    timer.stop();
    result.read_throughput = N / timer.elapsed_sec();
    result.read_latency.sort_for_percentiles();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    // ===== Mixed Workload Benchmark =====
    std::cout << "  Mixed workload (" << g_config.num_operations << " ops)..." << std::flush;
    const size_t OPS = g_config.num_operations;
    
    timer.start();
    for (size_t i = 0; i < OPS; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t key = rng() % N;
        if ((rng() % 100) < (g_config.read_ratio * 100)) {
            uint64_t val;
            mt.search(key, val);
        } else {
            ValueHelper vh(key * 100 + 1);
            mt.insert(key, vh);
        }
        
        auto t2 = std::chrono::high_resolution_clock::now();
        if (i % 100 == 0) {
            result.mixed_latency.add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        }
    }
    timer.stop();
    result.mixed_throughput = OPS / timer.elapsed_sec();
    result.mixed_latency.sort_for_percentiles();
    
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    // Print external storage stats
    mt.print_external_stats();
    
    // Cleanup
    mt.flush_external_storage();
    unlink(g_config.storage_path.c_str());
    
    return result;
}

#endif

// ============================================================================
// Comparison Summary
// ============================================================================

void print_comparison_summary(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "COMPARISON SUMMARY" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    
    // Header
    std::cout << std::left << std::setw(35) << "Metric";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << r.name.substr(0, 20);
    }
    std::cout << std::endl;
    std::cout << std::string(80, '-') << std::endl;
    
    // Memory
    std::cout << std::left << std::setw(35) << "Memory Delta (MB)";
    for (const auto& r : results) {
        double delta = (r.memory_after_insert_kb - r.memory_before_kb) / 1024.0;
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) << delta;
    }
    std::cout << std::endl;
    
    // Insert throughput
    std::cout << std::left << std::setw(35) << "Insert Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.insert_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Insert P99 latency
    std::cout << std::left << std::setw(35) << "Insert Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.insert_latency.p99();
    }
    std::cout << std::endl;
    
    // Read throughput
    std::cout << std::left << std::setw(35) << "Read Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.read_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Read P99 latency
    std::cout << std::left << std::setw(35) << "Read Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.read_latency.p99();
    }
    std::cout << std::endl;
    
    // Mixed throughput
    std::cout << std::left << std::setw(35) << "Mixed Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.mixed_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Mixed P99 latency
    std::cout << std::left << std::setw(35) << "Mixed Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(22) << std::fixed << std::setprecision(2) 
                  << r.mixed_latency.p99();
    }
    std::cout << std::endl;
    
    std::cout << std::string(80, '=') << std::endl;
}

// ============================================================================
// Main
// ============================================================================

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  -n NUM      Number of keys (default: 1000000)" << std::endl;
    std::cout << "  -t NUM      Number of threads (default: 4)" << std::endl;
    std::cout << "  -o NUM      Number of operations for mixed workload (default: 1000000)" << std::endl;
    std::cout << "  -c NUM      Cache size in MB for external mode (default: 64)" << std::endl;
    std::cout << "  -v          Verbose output" << std::endl;
    std::cout << "  -h          Show this help" << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) {
            g_config.num_keys = std::stoull(argv[++i]);
        } else if (arg == "-t" && i + 1 < argc) {
            g_config.num_threads = std::stoull(argv[++i]);
        } else if (arg == "-o" && i + 1 < argc) {
            g_config.num_operations = std::stoull(argv[++i]);
        } else if (arg == "-c" && i + 1 < argc) {
            g_config.cache_size_mb = std::stoull(argv[++i]);
        } else if (arg == "-v") {
            g_config.verbose = true;
        } else if (arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    std::cout << "========================================" << std::endl;
    std::cout << "H-Masstree Benchmark Comparison" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Config:" << std::endl;
    std::cout << "  Keys:        " << g_config.num_keys << std::endl;
    std::cout << "  Threads:     " << g_config.num_threads << std::endl;
    std::cout << "  Operations:  " << g_config.num_operations << std::endl;
    std::cout << "  Read Ratio:  " << (g_config.read_ratio * 100) << "%" << std::endl;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    std::cout << "  Mode:        External Storage" << std::endl;
    std::cout << "  Cache Size:  " << g_config.cache_size_mb << " MB" << std::endl;
#else
    std::cout << "  Mode:        Memory Only" << std::endl;
#endif
    
    std::vector<BenchmarkResult> results;
    
    // Run H-Masstree memory mode benchmark
#ifndef HMASSTREE_EXTERNAL_STORAGE
    results.push_back(benchmark_hmasstree_memory());
#endif

    // Run H-Masstree external mode benchmark
#ifdef HMASSTREE_EXTERNAL_STORAGE
    results.push_back(benchmark_hmasstree_external());
#endif
    
    // Print individual results
    for (const auto& r : results) {
        r.print();
    }
    
    // Print comparison if multiple results
    if (results.size() > 1) {
        print_comparison_summary(results);
    }
    
    std::cout << "\nBenchmark completed." << std::endl;
    
    return 0;
}
