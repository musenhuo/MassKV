/**
 * FlowKV Comprehensive Masstree Benchmark
 * 
 * 测试三种模式：
 * 1. 原版 Masstree (BENCHMARK_ORIGINAL_MASSTREE)
 * 2. HMasstree 内存模式 (BENCHMARK_HMASSTREE_MEMORY)
 * 3. HMasstree 外存模式 (BENCHMARK_HMASSTREE_EXTERNAL)
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <random>
#include <fstream>
#include <iomanip>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <thread>
#include <atomic>

// 根据编译选项选择wrapper
#if defined(BENCHMARK_ORIGINAL_MASSTREE)
    #include "masstree/masstree_wrapper.h"
    #define BENCHMARK_MODE "Original Masstree"
#elif defined(BENCHMARK_HMASSTREE_MEMORY)
    #include "hmasstree/hmasstree_wrapper.h"
    #define BENCHMARK_MODE "HMasstree Memory Mode"
#elif defined(BENCHMARK_HMASSTREE_EXTERNAL)
    #include "hmasstree/hmasstree_wrapper.h"
    #define BENCHMARK_MODE "HMasstree External Mode"
#else
    #error "Must define one of: BENCHMARK_ORIGINAL_MASSTREE, BENCHMARK_HMASSTREE_MEMORY, BENCHMARK_HMASSTREE_EXTERNAL"
#endif

// 配置
static size_t NUM_KEYS = 10000000;  // 10M keys
static size_t NUM_THREADS = 1;      // 单线程测试
static size_t CACHE_SIZE_MB = 256;  // 外存模式默认缓存
static std::string STORAGE_PATH = "/tmp/hmasstree_benchmark.dat";
static size_t WARMUP_OPS = 100000;  // 预热操作数

// 获取内存使用量 (KB)
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

// 获取峰值内存 (KB)
size_t get_peak_memory_kb() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.find("VmHWM:") == 0) {
            size_t kb = 0;
            sscanf(line.c_str(), "VmHWM: %zu kB", &kb);
            return kb;
        }
    }
    return 0;
}

// 简单哈希函数 (仅用于基准测试中的非冷启动模式)
inline uint64_t hash_key(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

// 生成测试key (使用顺序key以支持冷启动恢复)
void generate_keys(std::vector<uint64_t>& keys, size_t n) {
    keys.resize(n);
    for (size_t i = 0; i < n; i++) {
        keys[i] = i;  // 顺序key: 0, 1, 2, ...
    }
}

// 随机打乱keys用于读测试
void shuffle_keys(std::vector<uint64_t>& keys, unsigned seed = 42) {
    std::mt19937_64 rng(seed);
    std::shuffle(keys.begin(), keys.end(), rng);
}

// 结果结构
struct BenchmarkResult {
    std::string mode;
    size_t num_keys;
    size_t cache_mb;
    
    // 写测试
    double write_time_ms;
    double write_ops_per_sec;
    size_t write_mem_kb;
    
    // 读测试
    double read_time_ms;
    double read_ops_per_sec;
    size_t read_mem_kb;
    
    // 峰值内存
    size_t peak_mem_kb;
    
    void print() const {
        std::cout << "\n========== Benchmark Result ==========\n";
        std::cout << "Mode: " << mode << "\n";
        std::cout << "Keys: " << num_keys << " (" << (num_keys / 1000000.0) << "M)\n";
        std::cout << "Cache: " << cache_mb << " MB\n";
        std::cout << "\nWrite Performance:\n";
        std::cout << "  Time: " << std::fixed << std::setprecision(2) << write_time_ms << " ms\n";
        std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
                  << (write_ops_per_sec / 1000000.0) << " M ops/sec\n";
        std::cout << "  Memory: " << std::fixed << std::setprecision(2) 
                  << (write_mem_kb / 1024.0) << " MB\n";
        std::cout << "\nRead Performance:\n";
        std::cout << "  Time: " << std::fixed << std::setprecision(2) << read_time_ms << " ms\n";
        std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
                  << (read_ops_per_sec / 1000000.0) << " M ops/sec\n";
        std::cout << "  Memory: " << std::fixed << std::setprecision(2) 
                  << (read_mem_kb / 1024.0) << " MB\n";
        std::cout << "\nPeak Memory: " << std::fixed << std::setprecision(2)
                  << (peak_mem_kb / 1024.0) << " MB\n";
        std::cout << "=======================================\n";
    }
    
    void write_csv(const std::string& filename) const {
        bool exists = (access(filename.c_str(), F_OK) == 0);
        std::ofstream f(filename, std::ios::app);
        if (!exists) {
            f << "mode,num_keys,cache_mb,write_time_ms,write_ops_sec,write_mem_mb,"
              << "read_time_ms,read_ops_sec,read_mem_mb,peak_mem_mb\n";
        }
        f << mode << "," << num_keys << "," << cache_mb << ","
          << write_time_ms << "," << write_ops_per_sec << "," << (write_mem_kb / 1024.0) << ","
          << read_time_ms << "," << read_ops_per_sec << "," << (read_mem_kb / 1024.0) << ","
          << (peak_mem_kb / 1024.0) << "\n";
    }
};

// 原版Masstree测试
#if defined(BENCHMARK_ORIGINAL_MASSTREE)
BenchmarkResult run_benchmark() {
    BenchmarkResult result;
    result.mode = BENCHMARK_MODE;
    result.num_keys = NUM_KEYS;
    result.cache_mb = 0;  // 不适用
    
    std::cout << "=== " << BENCHMARK_MODE << " ===\n";
    std::cout << "Keys: " << NUM_KEYS << "\n\n";
    
    // 生成keys
    std::vector<uint64_t> keys;
    generate_keys(keys, NUM_KEYS);
    
    // 记录初始内存
    size_t initial_mem = get_memory_usage_kb();
    
    // 创建Masstree
    MasstreeWrapper mt;
    mt.thread_init(1);  // 线程ID必须 > 0
    
    // 写测试
    std::cout << "Inserting " << NUM_KEYS << " keys...\n";
    auto t1 = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i]);
        mt.insert(k16, vh);
    }
    
    auto t2 = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.write_ops_per_sec = NUM_KEYS / (result.write_time_ms / 1000.0);
    result.write_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Write done: " << std::fixed << std::setprecision(2) 
              << result.write_time_ms << " ms, "
              << (result.write_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Memory after write: " << (result.write_mem_kb / 1024.0) << " MB\n\n";
    
    // 预热读
    std::cout << "Warming up...\n";
    shuffle_keys(keys, 123);
    for (size_t i = 0; i < std::min(WARMUP_OPS, NUM_KEYS); i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
    }
    
    // 读测试
    std::cout << "Reading " << NUM_KEYS << " keys...\n";
    shuffle_keys(keys, 456);
    
    t1 = std::chrono::high_resolution_clock::now();
    
    size_t found = 0;
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        if (mt.search(k16, val)) {
            found++;
        }
    }
    
    t2 = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.read_ops_per_sec = NUM_KEYS / (result.read_time_ms / 1000.0);
    result.read_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Read done: " << std::fixed << std::setprecision(2)
              << result.read_time_ms << " ms, "
              << (result.read_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Found: " << found << "/" << NUM_KEYS << "\n";
    
    result.peak_mem_kb = get_peak_memory_kb() - initial_mem;
    
    return result;
}
#endif

// HMasstree内存模式测试
#if defined(BENCHMARK_HMASSTREE_MEMORY)
BenchmarkResult run_benchmark() {
    BenchmarkResult result;
    result.mode = BENCHMARK_MODE;
    result.num_keys = NUM_KEYS;
    result.cache_mb = 0;  // 不适用
    
    std::cout << "=== " << BENCHMARK_MODE << " ===\n";
    std::cout << "Keys: " << NUM_KEYS << "\n\n";
    
    // 生成keys
    std::vector<uint64_t> keys;
    generate_keys(keys, NUM_KEYS);
    
    // 记录初始内存
    size_t initial_mem = get_memory_usage_kb();
    
    // 创建HMasstree
    HMasstreeWrapper mt;
    mt.thread_init(1);  // 线程ID必须 > 0
    
    // 写测试
    std::cout << "Inserting " << NUM_KEYS << " keys...\n";
    auto t1 = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i]);
        mt.insert(k16, vh);
    }
    
    auto t2 = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.write_ops_per_sec = NUM_KEYS / (result.write_time_ms / 1000.0);
    result.write_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Write done: " << std::fixed << std::setprecision(2) 
              << result.write_time_ms << " ms, "
              << (result.write_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Memory after write: " << (result.write_mem_kb / 1024.0) << " MB\n\n";
    
    // 预热读
    std::cout << "Warming up...\n";
    shuffle_keys(keys, 123);
    for (size_t i = 0; i < std::min(WARMUP_OPS, NUM_KEYS); i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
    }
    
    // 读测试
    std::cout << "Reading " << NUM_KEYS << " keys...\n";
    shuffle_keys(keys, 456);
    
    t1 = std::chrono::high_resolution_clock::now();
    
    size_t found = 0;
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        if (mt.search(k16, val)) {
            found++;
        }
    }
    
    t2 = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.read_ops_per_sec = NUM_KEYS / (result.read_time_ms / 1000.0);
    result.read_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Read done: " << std::fixed << std::setprecision(2)
              << result.read_time_ms << " ms, "
              << (result.read_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Found: " << found << "/" << NUM_KEYS << "\n";
    
    result.peak_mem_kb = get_peak_memory_kb() - initial_mem;
    
    return result;
}
#endif

// HMasstree外存模式测试
#if defined(BENCHMARK_HMASSTREE_EXTERNAL)
BenchmarkResult run_benchmark() {
    BenchmarkResult result;
    result.mode = BENCHMARK_MODE;
    result.num_keys = NUM_KEYS;
    result.cache_mb = CACHE_SIZE_MB;
    
    std::cout << "=== " << BENCHMARK_MODE << " ===\n";
    std::cout << "Keys: " << NUM_KEYS << ", Cache: " << CACHE_SIZE_MB << " MB\n\n";
    
    // 删除旧存储文件
    unlink(STORAGE_PATH.c_str());
    
    // 生成keys
    std::vector<uint64_t> keys;
    generate_keys(keys, NUM_KEYS);
    
    // 记录初始内存
    size_t initial_mem = get_memory_usage_kb();
    
    // 创建HMasstree
    HMasstreeWrapper mt;
    
    // 初始化外存
    Masstree::ExternalIndexConfig config;
    config.storage_path = STORAGE_PATH;
    config.cache_size_mb = CACHE_SIZE_MB;
    config.storage_size_mb = 4096;  // 4GB
    
    if (!mt.init_external_storage(config)) {
        std::cerr << "Failed to init external storage!\n";
        exit(1);
    }
    mt.thread_init_external(1);  // 线程ID必须 > 0
    
    // 写测试
    std::cout << "Inserting " << NUM_KEYS << " keys...\n";
    auto t1 = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        ValueHelper vh(keys[i]);
        mt.insert(k16, vh);
    }
    
    auto t2 = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.write_ops_per_sec = NUM_KEYS / (result.write_time_ms / 1000.0);
    result.write_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Write done: " << std::fixed << std::setprecision(2) 
              << result.write_time_ms << " ms, "
              << (result.write_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Memory after write: " << (result.write_mem_kb / 1024.0) << " MB\n\n";
    
    // 序列化
    std::cout << "Serializing tree...\n";
    size_t serialized = mt.serialize_all_nodes();
    mt.persist_tree_structure();
    mt.flush_external_storage();
    std::cout << "Serialized " << serialized << " nodes\n\n";
    
    // 预热读
    std::cout << "Warming up...\n";
    shuffle_keys(keys, 123);
    for (size_t i = 0; i < std::min(WARMUP_OPS, NUM_KEYS); i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        mt.search(k16, val);
    }
    
    // 读测试
    std::cout << "Reading " << NUM_KEYS << " keys...\n";
    shuffle_keys(keys, 456);
    
    t1 = std::chrono::high_resolution_clock::now();
    
    size_t found = 0;
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        if (mt.search(k16, val)) {
            found++;
        }
    }
    
    t2 = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.read_ops_per_sec = NUM_KEYS / (result.read_time_ms / 1000.0);
    result.read_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Read done: " << std::fixed << std::setprecision(2)
              << result.read_time_ms << " ms, "
              << (result.read_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Found: " << found << "/" << NUM_KEYS << "\n";
    
    result.peak_mem_kb = get_peak_memory_kb() - initial_mem;
    
    // 打印存储文件大小
    struct stat st;
    if (stat(STORAGE_PATH.c_str(), &st) == 0) {
        std::cout << "Storage file size: " << (st.st_size / 1024.0 / 1024.0) << " MB\n";
    }
    
    return result;
}

// 冷启动读测试
BenchmarkResult run_cold_restart_benchmark(size_t cache_mb) {
    BenchmarkResult result;
    result.mode = "HMasstree Cold Restart";
    result.num_keys = NUM_KEYS;
    result.cache_mb = cache_mb;
    
    std::cout << "\n=== Cold Restart Test (Cache: " << cache_mb << " MB) ===\n";
    
    // 生成keys (必须与写入时相同)
    std::vector<uint64_t> keys;
    generate_keys(keys, NUM_KEYS);
    
    // 记录初始内存
    size_t initial_mem = get_memory_usage_kb();
    
    // 创建新实例
    HMasstreeWrapper mt;
    
    // 初始化外存 (使用指定缓存大小)
    Masstree::ExternalIndexConfig config;
    config.storage_path = STORAGE_PATH;
    config.cache_size_mb = cache_mb;
    config.storage_size_mb = 4096;
    
    if (!mt.init_external_storage(config)) {
        std::cerr << "Failed to init external storage!\n";
        exit(1);
    }
    mt.thread_init_external(1);  // 线程ID必须 > 0
    
    // 从存储恢复
    std::cout << "Restoring from storage...\n";
    auto t1 = std::chrono::high_resolution_clock::now();
    
    if (!mt.restore_from_storage()) {
        std::cerr << "Failed to restore from storage!\n";
        exit(1);
    }
    
    auto t2 = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.write_ops_per_sec = 0;  // 恢复时用来记录恢复时间
    result.write_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Restore done: " << std::fixed << std::setprecision(2)
              << result.write_time_ms << " ms\n";
    std::cout << "Memory after restore: " << (result.write_mem_kb / 1024.0) << " MB\n\n";
    
    // 读测试
    std::cout << "Reading " << NUM_KEYS << " keys...\n";
    shuffle_keys(keys, 456);
    
    t1 = std::chrono::high_resolution_clock::now();
    
    size_t found = 0;
    for (size_t i = 0; i < NUM_KEYS; i++) {
        Key16 k16(keys[i] >> 32, keys[i]);
        uint64_t val;
        if (mt.search(k16, val)) {
            found++;
        }
    }
    
    t2 = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.read_ops_per_sec = NUM_KEYS / (result.read_time_ms / 1000.0);
    result.read_mem_kb = get_memory_usage_kb() - initial_mem;
    
    std::cout << "Read done: " << std::fixed << std::setprecision(2)
              << result.read_time_ms << " ms, "
              << (result.read_ops_per_sec / 1000000.0) << " M ops/sec\n";
    std::cout << "Found: " << found << "/" << NUM_KEYS << "\n";
    
    result.peak_mem_kb = get_peak_memory_kb() - initial_mem;
    
    return result;
}
#endif

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n";
    std::cerr << "Options:\n";
    std::cerr << "  -n <num>     Number of keys (default: 10000000)\n";
    std::cerr << "  -c <size>    Cache size in MB (external mode only, default: 256)\n";
    std::cerr << "  -s <path>    Storage path (external mode only)\n";
    std::cerr << "  -o <file>    Output CSV file\n";
#if defined(BENCHMARK_HMASSTREE_EXTERNAL)
    std::cerr << "  -r <cache>   Run cold restart test with specified cache MB\n";
    std::cerr << "               Use -r 0 to skip initial write and only test cold restart\n";
#endif
}

int main(int argc, char* argv[]) {
    std::string output_csv;
    size_t cold_restart_cache = 0;
    bool cold_restart_only = false;
    
    int opt;
    while ((opt = getopt(argc, argv, "n:c:s:o:r:h")) != -1) {
        switch (opt) {
            case 'n':
                NUM_KEYS = std::stoull(optarg);
                break;
            case 'c':
                CACHE_SIZE_MB = std::stoull(optarg);
                break;
            case 's':
                STORAGE_PATH = optarg;
                break;
            case 'o':
                output_csv = optarg;
                break;
#if defined(BENCHMARK_HMASSTREE_EXTERNAL)
            case 'r':
                cold_restart_cache = std::stoull(optarg);
                if (cold_restart_cache == 0) {
                    cold_restart_only = true;
                    cold_restart_cache = CACHE_SIZE_MB;
                }
                break;
#endif
            case 'h':
            default:
                print_usage(argv[0]);
                return 1;
        }
    }
    
    std::cout << "========================================\n";
    std::cout << "FlowKV Masstree Benchmark\n";
    std::cout << "Mode: " << BENCHMARK_MODE << "\n";
    std::cout << "========================================\n\n";
    
#if defined(BENCHMARK_HMASSTREE_EXTERNAL)
    if (cold_restart_only) {
        // 只运行冷启动测试
        auto result = run_cold_restart_benchmark(cold_restart_cache);
        result.print();
        if (!output_csv.empty()) {
            result.write_csv(output_csv);
        }
    } else if (cold_restart_cache > 0) {
        // 先运行正常测试，然后冷启动测试
        auto result = run_benchmark();
        result.print();
        if (!output_csv.empty()) {
            result.write_csv(output_csv);
        }
        
        // 冷启动测试
        auto cold_result = run_cold_restart_benchmark(cold_restart_cache);
        cold_result.print();
        if (!output_csv.empty()) {
            cold_result.write_csv(output_csv);
        }
    } else {
        auto result = run_benchmark();
        result.print();
        if (!output_csv.empty()) {
            result.write_csv(output_csv);
        }
    }
#else
    auto result = run_benchmark();
    result.print();
    if (!output_csv.empty()) {
        result.write_csv(output_csv);
    }
#endif
    
    return 0;
}
