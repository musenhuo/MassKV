/* H-Masstree vs Masstree Benchmark Comparison v2
 *
 * 对比测试三种索引实现：
 * 1. 原版 Masstree（内存模式）
 * 2. H-Masstree 内存模式 
 * 3. H-Masstree 外存模式（限制缓存大小以触发实际外存访问）
 *
 * Key/Value 配置：
 * - Key: 16 bytes (Key16 类型)
 * - Value: 16 bytes (存储两个 uint64_t)
 *
 * 测试指标：
 * - 内存占用量
 * - 读写延迟（P50/P99/P999）
 * - 吞吐率（ops/sec）
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

// Include H-Masstree wrapper
#include "hmasstree_wrapper.h"

// Include original Masstree wrapper
#include "../masstree/masstree_wrapper.h"

#ifdef HMASSTREE_EXTERNAL_STORAGE
#include "external_index.h"
#endif

// ============================================================================
// Configuration
// ============================================================================

struct BenchmarkConfig {
    size_t num_keys = 1000000;      // 1M keys
    size_t num_threads = 1;
    double read_ratio = 0.8;         // 80% read, 20% write
    size_t num_operations = 1000000;
    bool verbose = false;
    std::string storage_path = "/tmp/hmasstree_bench_v2.dat";
    size_t cache_size_mb = 4;        // 仅4MB缓存，强制触发外存访问
    bool cold_start = false;         // 是否冷启动测试
    bool test_original_masstree = true;
    bool test_hmasstree_memory = true;
    bool test_hmasstree_external = true;
};

BenchmarkConfig g_config;

// ============================================================================
// Utilities
// ============================================================================

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
    uint64_t elapsed_ns() const {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - start_).count();
    }
    
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_, end_;
};

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
        return latencies_[latencies_.size() * 50 / 100] / 1000.0;
    }
    
    double p99() const {
        if (latencies_.empty()) return 0;
        return latencies_[latencies_.size() * 99 / 100] / 1000.0;
    }
    
    double p999() const {
        if (latencies_.empty()) return 0;
        return latencies_[latencies_.size() * 999 / 1000] / 1000.0;
    }
    
    double avg() const {
        if (latencies_.empty()) return 0;
        uint64_t sum = 0;
        for (auto l : latencies_) sum += l;
        return (double)sum / latencies_.size() / 1000.0;
    }
    
    size_t count() const { return latencies_.size(); }
    void clear() { latencies_.clear(); }
    
private:
    std::vector<uint64_t> latencies_;
};

// ============================================================================
// Key16 Generation (16 bytes key)
// ============================================================================

struct TestKey16 {
    uint64_t prefix;
    uint64_t suffix;
    
    TestKey16() : prefix(0), suffix(0) {}
    TestKey16(uint64_t p, uint64_t s) : prefix(p), suffix(s) {}
    
    // 从单个 uint64_t 生成，高8位做前缀的一部分
    static TestKey16 from_id(uint64_t id) {
        return TestKey16(id >> 32, id);
    }
    
    uint64_t to_uint64() const {
        return suffix;  // 用于兼容性
    }
};

// ============================================================================
// Benchmark Results
// ============================================================================

struct BenchmarkResult {
    std::string name;
    
    size_t memory_before_kb = 0;
    size_t memory_after_insert_kb = 0;
    
    double insert_time_sec = 0;
    double insert_throughput = 0;
    LatencyHistogram insert_latency;
    
    double read_throughput = 0;
    LatencyHistogram read_latency;
    
    double mixed_throughput = 0;
    LatencyHistogram mixed_latency;
    
    // External storage specific
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
    
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
        
        if (cache_hits > 0 || cache_misses > 0) {
            double hit_rate = (double)cache_hits / (cache_hits + cache_misses) * 100.0;
            std::cout << "\n[External Storage]" << std::endl;
            std::cout << "  Cache Hits:   " << cache_hits << std::endl;
            std::cout << "  Cache Misses: " << cache_misses << std::endl;
            std::cout << "  Hit Rate:     " << hit_rate << "%" << std::endl;
        }
    }
};

// ============================================================================
// Original Masstree Benchmark (Memory Only)
// ============================================================================

BenchmarkResult benchmark_original_masstree() {
    BenchmarkResult result;
    result.name = "Original Masstree (Memory)";
    
    std::cout << "\n----- Running " << result.name << " -----" << std::endl;
    
    result.memory_before_kb = get_memory_usage_kb();
    
    MasstreeWrapper mt;
    MasstreeWrapper::thread_init(1);
    
    const size_t N = g_config.num_keys;
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; i++) {
        keys[i] = i;
    }
    
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // ===== Insert Benchmark =====
    std::cout << "  Inserting " << N << " keys (16B key, 16B value)..." << std::flush;
    BenchTimer timer;
    timer.start();
    
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        // Key16: use both prefix and suffix
        Key16 k16(keys[i] >> 32, keys[i]);
        // Value: use ValueHelper which stores 16 bytes effectively
        ValueHelper vh(keys[i] * 100);
        mt.insert(k16, vh);
        
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
        
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
        
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
    
    // ===== Mixed Workload =====
    std::cout << "  Mixed workload (" << g_config.num_operations << " ops)..." << std::flush;
    const size_t OPS = g_config.num_operations;
    
    timer.start();
    for (size_t i = 0; i < OPS; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t key_id = rng() % N;
        Key16 k16(key_id >> 32, key_id);
        
        if ((rng() % 100) < (g_config.read_ratio * 100)) {
            uint64_t val;
            mt.search(k16, val);
        } else {
            ValueHelper vh(key_id * 100 + 1);
            mt.insert(k16, vh);
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
    
    std::mt19937_64 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    // ===== Insert =====
    std::cout << "  Inserting " << N << " keys (16B key, 16B value)..." << std::flush;
    BenchTimer timer;
    timer.start();
    
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i] * 100);
        mt.insert(k16, vh);
        
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
    
    // ===== Read =====
    std::cout << "  Reading " << N << " keys..." << std::flush;
    std::shuffle(keys.begin(), keys.end(), rng);
    
    timer.start();
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
        
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
    
    // ===== Mixed =====
    std::cout << "  Mixed workload (" << g_config.num_operations << " ops)..." << std::flush;
    const size_t OPS = g_config.num_operations;
    
    timer.start();
    for (size_t i = 0; i < OPS; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t key_id = rng() % N;
        Key16 k16(key_id >> 32, key_id);
        
        if ((rng() % 100) < (g_config.read_ratio * 100)) {
            uint64_t val;
            mt.search(k16, val);
        } else {
            ValueHelper vh(key_id * 100 + 1);
            mt.insert(k16, vh);
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
// H-Masstree External Storage Mode Benchmark (with cache limit)
// ============================================================================

#ifdef HMASSTREE_EXTERNAL_STORAGE

BenchmarkResult benchmark_hmasstree_external() {
    BenchmarkResult result;
    result.name = "H-Masstree (External, Cache=" + std::to_string(g_config.cache_size_mb) + "MB)";
    
    std::cout << "\n----- Running " << result.name << " -----" << std::endl;
    
    // Clean up old test file
    unlink(g_config.storage_path.c_str());
    
    result.memory_before_kb = get_memory_usage_kb();
    
    HMasstreeWrapper mt;
    
    // Initialize external storage with LIMITED cache size
    Masstree::ExternalIndexConfig ext_config;
    ext_config.storage_path = g_config.storage_path;
    ext_config.cache_size_mb = g_config.cache_size_mb;  // 小缓存以触发外存访问
    ext_config.storage_size_mb = 1024;  // 1GB storage
    ext_config.enable_background_flush = true;
    
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
    
    // ===== Insert =====
    std::cout << "  Inserting " << N << " keys (16B key, 16B value)..." << std::flush;
    BenchTimer timer;
    timer.start();
    
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i] * 100);
        mt.insert(k16, vh);
        
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
    
    // Flush all to disk before read test
    std::cout << "  Flushing to disk..." << std::flush;
    mt.flush_external_storage();
    std::cout << " done" << std::endl;
    
    // ===== Cold Start Read (if enabled) =====
    if (g_config.cold_start) {
        std::cout << "  [Cold Start] Clearing cache..." << std::flush;
        // 清空系统页面缓存
        sync();
        // Note: 需要root权限: echo 3 > /proc/sys/vm/drop_caches
        std::cout << " done" << std::endl;
    }
    
    // ===== Read =====
    std::cout << "  Reading " << N << " keys (random order to stress cache)..." << std::flush;
    std::shuffle(keys.begin(), keys.end(), rng);
    
    timer.start();
    for (size_t i = 0; i < N; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
        
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
    
    // ===== Mixed =====
    std::cout << "  Mixed workload (" << g_config.num_operations << " ops)..." << std::flush;
    const size_t OPS = g_config.num_operations;
    
    timer.start();
    for (size_t i = 0; i < OPS; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        
        uint64_t key_id = rng() % N;
        Key16 k16(key_id >> 32, key_id);
        
        if ((rng() % 100) < (g_config.read_ratio * 100)) {
            uint64_t val;
            mt.search(k16, val);
        } else {
            ValueHelper vh(key_id * 100 + 1);
            mt.insert(k16, vh);
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
    
    // Get cache stats
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
    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "COMPARISON SUMMARY (Key: 16B, Value: 16B)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;
    
    // Header
    std::cout << std::left << std::setw(40) << "Metric";
    for (const auto& r : results) {
        std::string short_name = r.name.length() > 18 ? r.name.substr(0, 18) + ".." : r.name;
        std::cout << std::right << std::setw(20) << short_name;
    }
    std::cout << std::endl;
    std::cout << std::string(100, '-') << std::endl;
    
    // Memory Delta
    std::cout << std::left << std::setw(40) << "Memory Delta (MB)";
    for (const auto& r : results) {
        double delta = (r.memory_after_insert_kb - r.memory_before_kb) / 1024.0;
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) << delta;
    }
    std::cout << std::endl;
    
    // Insert Throughput
    std::cout << std::left << std::setw(40) << "Insert Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.insert_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Insert P99
    std::cout << std::left << std::setw(40) << "Insert Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.insert_latency.p99();
    }
    std::cout << std::endl;
    
    // Read Throughput
    std::cout << std::left << std::setw(40) << "Read Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.read_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Read P99
    std::cout << std::left << std::setw(40) << "Read Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.read_latency.p99();
    }
    std::cout << std::endl;
    
    // Mixed Throughput
    std::cout << std::left << std::setw(40) << "Mixed Throughput (K ops/sec)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.mixed_throughput / 1000.0;
    }
    std::cout << std::endl;
    
    // Mixed P99
    std::cout << std::left << std::setw(40) << "Mixed Latency P99 (us)";
    for (const auto& r : results) {
        std::cout << std::right << std::setw(20) << std::fixed << std::setprecision(2) 
                  << r.mixed_latency.p99();
    }
    std::cout << std::endl;
    
    std::cout << std::string(100, '=') << std::endl;
}

// ============================================================================
// Main
// ============================================================================

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  -n NUM      Number of keys (default: 1000000)" << std::endl;
    std::cout << "  -o NUM      Number of operations for mixed workload" << std::endl;
    std::cout << "  -c NUM      Cache size in MB for external mode (default: 4)" << std::endl;
    std::cout << "  --cold      Enable cold start test (clear cache before read)" << std::endl;
    std::cout << "  --only-ext  Only run external storage test" << std::endl;
    std::cout << "  --only-mem  Only run memory tests (original + H-Masstree)" << std::endl;
    std::cout << "  -v          Verbose output" << std::endl;
    std::cout << "  -h          Show this help" << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) {
            g_config.num_keys = std::stoull(argv[++i]);
        } else if (arg == "-o" && i + 1 < argc) {
            g_config.num_operations = std::stoull(argv[++i]);
        } else if (arg == "-c" && i + 1 < argc) {
            g_config.cache_size_mb = std::stoull(argv[++i]);
        } else if (arg == "--cold") {
            g_config.cold_start = true;
        } else if (arg == "--only-ext") {
            g_config.test_original_masstree = false;
            g_config.test_hmasstree_memory = false;
        } else if (arg == "--only-mem") {
            g_config.test_hmasstree_external = false;
        } else if (arg == "-v") {
            g_config.verbose = true;
        } else if (arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    std::cout << "============================================" << std::endl;
    std::cout << "H-Masstree Benchmark Comparison v2" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "Config:" << std::endl;
    std::cout << "  Keys:        " << g_config.num_keys << std::endl;
    std::cout << "  Operations:  " << g_config.num_operations << std::endl;
    std::cout << "  Key Size:    16 bytes (Key16)" << std::endl;
    std::cout << "  Value Size:  16 bytes" << std::endl;
    std::cout << "  Read Ratio:  " << (g_config.read_ratio * 100) << "%" << std::endl;
#ifdef HMASSTREE_EXTERNAL_STORAGE
    std::cout << "  External:    Enabled (Cache: " << g_config.cache_size_mb << " MB)" << std::endl;
#else
    std::cout << "  External:    Disabled (memory-only build)" << std::endl;
#endif
    std::cout << "  Cold Start:  " << (g_config.cold_start ? "Yes" : "No") << std::endl;
    
    std::vector<BenchmarkResult> results;
    
    // Run tests
#ifndef HMASSTREE_EXTERNAL_STORAGE
    if (g_config.test_original_masstree) {
        results.push_back(benchmark_original_masstree());
    }
    
    if (g_config.test_hmasstree_memory) {
        results.push_back(benchmark_hmasstree_memory());
    }
#endif

#ifdef HMASSTREE_EXTERNAL_STORAGE
    if (g_config.test_hmasstree_external) {
        results.push_back(benchmark_hmasstree_external());
    }
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
