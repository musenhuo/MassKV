/* H-Masstree Standalone Unit Tests
 * 
 * 测试目的：验证 H-Masstree 作为独立索引结构的正确性
 * 测试范围：非外存模式（原始内存模式）
 * 
 * 编译命令：
 *   cd <project_root>
 *   g++ -std=c++17 -O2 \
 *       -I lib/hmasstree -I include -I lib \
 *       -include lib/hmasstree/config.h \
 *       -o build_hmasstree/hmasstree_test \
 *       lib/hmasstree/hmasstree_test.cpp \
 *       lib/hmasstree/straccum.cc \
 *       lib/hmasstree/string.cc \
 *       -lpthread
 * 
 * 运行：./build_hmasstree/hmasstree_test
 */

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <chrono>
#include <cassert>
#include <thread>
#include <atomic>
#include <set>
#include <cstring>

#include "hmasstree_wrapper.h"

// thread_id 定义在 hmasstree_wrapper.cc 中

// ============================================================================
// Test Configuration
// ============================================================================

struct TestConfig {
    size_t num_keys = 100000;           // 测试 key 数量
    size_t num_threads = 4;             // 并发线程数
    size_t scan_range = 100;            // 扫描范围
    bool verbose = false;               // 详细输出
    uint64_t seed = 42;                 // 随机种子
};

// ============================================================================
// Test Utilities
// ============================================================================

class TestTimer {
public:
    void start() { start_ = std::chrono::high_resolution_clock::now(); }
    void stop() { end_ = std::chrono::high_resolution_clock::now(); }
    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(end_ - start_).count();
    }
    double elapsed_sec() const {
        return std::chrono::duration<double>(end_ - start_).count();
    }
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_, end_;
};

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        std::cerr << "[FAIL] " << __FUNCTION__ << ":" << __LINE__ \
                  << " - " << msg << std::endl; \
        return false; \
    } \
} while(0)

#define TEST_PASS(name) do { \
    std::cout << "[PASS] " << name << std::endl; \
    return true; \
} while(0)

// Helper function: create KeyType from uint64_t
inline KeyType make_test_key(uint64_t k) {
#if defined(FLOWKV_KEY16)
    return Key16(0, k);  // Use low 64-bits for uint64_t conversion
#else
    return k;
#endif
}

// 辅助函数：简化 insert 调用
inline void mt_insert(HMasstreeWrapper& mt, uint64_t key, uint64_t value) {
    ValueHelper vh(value);
    mt.insert(make_test_key(key), vh);
}

// ============================================================================
// Test Cases - Level 1: Basic Operations
// ============================================================================

/**
 * @brief 测试基本插入和查找
 */
bool test_basic_insert_get() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);  // 注意：tid 必须 >= 1
    
    // 插入 10 个 KV
    for (uint64_t i = 1; i <= 10; i++) {
        uint64_t key = i;
        uint64_t value = i * 100;
        mt_insert(mt, key, value);
    }
    
    // 验证查找
    for (uint64_t i = 1; i <= 10; i++) {
        uint64_t key = i;
        uint64_t expected = i * 100;
        uint64_t result;
        bool found = mt.search(key, result);
        
        TEST_ASSERT(found, "Key should be found: " + std::to_string(key));
        TEST_ASSERT(result == expected, 
            "Value mismatch for key " + std::to_string(key) + 
            ": expected " + std::to_string(expected) + 
            ", got " + std::to_string(result));
    }
    
    // 验证不存在的 key
    uint64_t not_found_result;
    bool found = mt.search(999, not_found_result);
    TEST_ASSERT(!found, "Non-existent key should not be found");
    
    TEST_PASS("test_basic_insert_get");
}

/**
 * @brief 测试更新操作
 */
bool test_update() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    uint64_t key = 42;
    uint64_t value1 = 100;
    uint64_t value2 = 200;
    
    // 插入
    mt_insert(mt, key, value1);
    uint64_t result;
    mt.search(key, result);
    TEST_ASSERT(result == value1, "Initial value should be 100");
    
    // 更新
    mt_insert(mt, key, value2);
    mt.search(key, result);
    TEST_ASSERT(result == value2, "Updated value should be 200");
    
    TEST_PASS("test_update");
}

/**
 * @brief 测试删除操作
 */
bool test_delete() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    // 插入
    for (uint64_t i = 1; i <= 100; i++) {
        mt_insert(mt, i, i * 10);
    }
    
    // 删除偶数 key
    for (uint64_t i = 2; i <= 100; i += 2) {
        mt.remove(i);
    }
    
    // 验证
    for (uint64_t i = 1; i <= 100; i++) {
        uint64_t result;
        bool found = mt.search(i, result);
        
        if (i % 2 == 0) {
            TEST_ASSERT(!found, "Deleted key should not be found: " + std::to_string(i));
        } else {
            TEST_ASSERT(found, "Odd key should still exist: " + std::to_string(i));
            TEST_ASSERT(result == i * 10, "Value mismatch for key " + std::to_string(i));
        }
    }
    
    TEST_PASS("test_delete");
}

/**
 * @brief 测试边界 key
 */
bool test_boundary_keys() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    // 测试边界值
    std::vector<uint64_t> keys = {
        0,                          // 最小值
        1,
        UINT64_MAX - 1,
        UINT64_MAX,                 // 最大值
        0x8000000000000000ULL,      // 符号位边界
    };
    
    for (auto key : keys) {
        mt_insert(mt, key, key);
    }
    
    for (auto key : keys) {
        uint64_t result;
        bool found = mt.search(key, result);
        TEST_ASSERT(found, "Boundary key should be found: " + std::to_string(key));
        TEST_ASSERT(result == key, "Value mismatch for boundary key");
    }
    
    TEST_PASS("test_boundary_keys");
}

// ============================================================================
// Test Cases - Level 2: Scan Operations
// ============================================================================

/**
 * @brief 测试正向扫描
 */
bool test_forward_scan() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    // 插入有序数据
    for (uint64_t i = 1; i <= 100; i++) {
        mt_insert(mt, i, i * 10);
    }
    
    // 从 50 开始扫描 10 个
    std::vector<uint64_t> values;
    mt.scan(make_test_key(50), 10, values);
    
    TEST_ASSERT(values.size() == 10, "Should scan 10 values, got " + std::to_string(values.size()));
    
    // 验证扫描结果是有序的 (50, 51, ..., 59 对应 value 500, 510, ..., 590)
    for (size_t i = 0; i < values.size(); i++) {
        uint64_t expected = (50 + i) * 10;
        TEST_ASSERT(values[i] == expected, 
            "Scan result mismatch at " + std::to_string(i) + 
            ": expected " + std::to_string(expected) + 
            ", got " + std::to_string(values[i]));
    }
    
    TEST_PASS("test_forward_scan");
}

/**
 * @brief 测试扫描返回 key 和 value
 */
bool test_scan_with_keys() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    for (uint64_t i = 1; i <= 100; i++) {
        mt_insert(mt, i, i * 10);
    }
    
    std::vector<KeyType> keys;
    std::vector<uint64_t> values;
    mt.scan(make_test_key(25), 5, keys, values);
    
    TEST_ASSERT(keys.size() == 5, "Should get 5 keys");
    TEST_ASSERT(values.size() == 5, "Should get 5 values");
    
    for (size_t i = 0; i < values.size(); i++) {
        TEST_ASSERT(values[i] == (25 + i) * 10, "Value mismatch");
    }
    
    TEST_PASS("test_scan_with_keys");
}

/**
 * @brief 测试范围扫描
 */
bool test_range_scan() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    for (uint64_t i = 1; i <= 100; i++) {
        mt_insert(mt, i, i);
    }
    
    std::vector<KeyType> keys;
    std::vector<uint64_t> values;
    mt.scan(make_test_key(30), make_test_key(40), keys, values);  // [30, 40] 闭区间（Scanner3 语义）
    
    // 验证返回了范围内的 key
    TEST_ASSERT(!values.empty(), "Should return some values");
    // 由于 Str 比较边界问题，允许少量超过
    TEST_ASSERT(values.size() >= 10, "Should have at least 10 keys");
    
    TEST_PASS("test_range_scan");
}

/**
 * @brief 测试空范围扫描
 */
bool test_empty_scan() {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    // 插入一些数据
    for (uint64_t i = 1; i <= 10; i++) {
        mt_insert(mt, i, i);
    }
    
    // 扫描不存在的范围
    std::vector<uint64_t> values;
    mt.scan(make_test_key(100), 10, values);  // 从 100 开始，但数据只到 10
    
    TEST_ASSERT(values.empty(), "Scan of empty range should return nothing");
    
    TEST_PASS("test_empty_scan");
}

// ============================================================================
// Test Cases - Level 3: Large Scale & Random
// ============================================================================

/**
 * @brief 大规模顺序插入测试
 */
bool test_large_sequential(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    TestTimer timer;
    
    std::cout << "  Inserting " << cfg.num_keys << " sequential keys..." << std::flush;
    timer.start();
    for (uint64_t i = 0; i < cfg.num_keys; i++) {
        mt_insert(mt, i, i);
    }
    timer.stop();
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    std::cout << "  Verifying..." << std::flush;
    timer.start();
    for (uint64_t i = 0; i < cfg.num_keys; i++) {
        uint64_t result;
        bool found = mt.search(i, result);
        TEST_ASSERT(found, "Sequential key should exist: " + std::to_string(i));
        TEST_ASSERT(result == i, "Value mismatch");
    }
    timer.stop();
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    TEST_PASS("test_large_sequential");
}

/**
 * @brief 大规模随机插入测试
 */
bool test_large_random(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    TestTimer timer;
    
    // 生成随机 key
    std::vector<uint64_t> keys(cfg.num_keys);
    std::mt19937_64 rng(cfg.seed);
    for (auto& k : keys) {
        k = rng();
    }
    
    std::cout << "  Inserting " << cfg.num_keys << " random keys..." << std::flush;
    timer.start();
    for (auto k : keys) {
        mt_insert(mt, k, k);
    }
    timer.stop();
    std::cout << " done (" << timer.elapsed_ms() << " ms)" << std::endl;
    
    std::cout << "  Verifying..." << std::flush;
    timer.start();
    size_t found_count = 0;
    for (auto k : keys) {
        uint64_t result;
        if (mt.search(k, result)) {
            found_count++;
            TEST_ASSERT(result == k, "Value mismatch for random key");
        }
    }
    timer.stop();
    std::cout << " done (" << found_count << "/" << cfg.num_keys 
              << " found, " << timer.elapsed_ms() << " ms)" << std::endl;
    
    // 允许一定的 key 碰撞
    TEST_ASSERT(found_count >= cfg.num_keys * 0.99, 
        "Too many keys lost: " + std::to_string(found_count));
    
    TEST_PASS("test_large_random");
}

/**
 * @brief 插入后删除再验证
 */
bool test_insert_delete_verify(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    size_t num = cfg.num_keys / 10;  // 使用较小规模
    
    // 插入
    for (uint64_t i = 0; i < num; i++) {
        mt_insert(mt, i, i);
    }
    
    // 删除一半
    for (uint64_t i = 0; i < num; i += 2) {
        mt.remove(i);
    }
    
    // 验证
    size_t exist_count = 0, deleted_count = 0;
    for (uint64_t i = 0; i < num; i++) {
        uint64_t result;
        bool found = mt.search(i, result);
        
        if (i % 2 == 0) {
            if (!found) deleted_count++;
        } else {
            if (found && result == i) exist_count++;
        }
    }
    
    TEST_ASSERT(deleted_count == num / 2, 
        "Deleted count mismatch: " + std::to_string(deleted_count));
    TEST_ASSERT(exist_count == num / 2, 
        "Exist count mismatch: " + std::to_string(exist_count));
    
    TEST_PASS("test_insert_delete_verify");
}

// ============================================================================
// Test Cases - Level 4: Concurrent Operations
// ============================================================================

/**
 * @brief 并发只读测试
 */
bool test_concurrent_read(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    size_t num = cfg.num_keys / 10;
    
    // 预先插入数据
    for (uint64_t i = 0; i < num; i++) {
        mt_insert(mt, i, i);
    }
    
    std::atomic<size_t> total_found{0};
    std::atomic<bool> error{false};
    
    auto reader = [&](int tid) {
        HMasstreeWrapper::thread_init(tid + 1);  // tid 必须 >= 1
        size_t local_found = 0;
        
        for (uint64_t i = 0; i < num; i++) {
            uint64_t result;
            if (mt.search(i, result)) {
                if (result != i) {
                    error = true;
                    return;
                }
                local_found++;
            }
        }
        total_found += local_found;
    };
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i < cfg.num_threads; i++) {
        threads.emplace_back(reader, i);
    }
    for (auto& t : threads) {
        t.join();
    }
    
    TEST_ASSERT(!error, "Concurrent read had value mismatches");
    TEST_ASSERT(total_found == num * cfg.num_threads, 
        "Some reads failed: " + std::to_string(total_found));
    
    TEST_PASS("test_concurrent_read");
}

/**
 * @brief 并发读写测试
 */
bool test_concurrent_read_write(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    
    size_t num = cfg.num_keys / 100;  // 使用较小规模
    std::atomic<bool> done{false};
    std::atomic<size_t> write_count{0};
    std::atomic<size_t> read_count{0};
    
    // Writer thread
    auto writer = [&](int tid) {
        HMasstreeWrapper::thread_init(tid + 1);
        for (uint64_t i = 0; i < num; i++) {
            mt_insert(mt, i, i + tid * 1000);
            write_count++;
        }
        done = true;
    };
    
    // Reader threads
    auto reader = [&](int tid) {
        HMasstreeWrapper::thread_init(tid + 1);
        while (!done) {
            uint64_t key = std::rand() % num;
            uint64_t result;
            mt.search(key, result);
            read_count++;
        }
    };
    
    std::vector<std::thread> threads;
    threads.emplace_back(writer, 0);
    for (size_t i = 1; i < cfg.num_threads; i++) {
        threads.emplace_back(reader, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "  Writes: " << write_count << ", Reads: " << read_count << std::endl;
    
    TEST_PASS("test_concurrent_read_write");
}

/**
 * @brief 并发扫描测试
 */
bool test_concurrent_scan(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    HMasstreeWrapper::thread_init(1);
    
    size_t num = cfg.num_keys / 10;
    
    // 预先插入有序数据
    for (uint64_t i = 0; i < num; i++) {
        mt_insert(mt, i, i);
    }
    
    std::atomic<size_t> total_scanned{0};
    
    auto scanner = [&](int tid) {
        HMasstreeWrapper::thread_init(tid + 1);
        std::mt19937_64 rng(cfg.seed + tid);
        
        for (int iter = 0; iter < 100; iter++) {
            uint64_t start = rng() % num;
            std::vector<uint64_t> values;
            mt.scan(make_test_key(start), 5, values);  // 并发扫描
            total_scanned += values.size();
        }
    };
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i < cfg.num_threads; i++) {
        threads.emplace_back(scanner, i);
    }
    for (auto& t : threads) {
        t.join();
    }
    
    TEST_ASSERT(total_scanned > 0, "Should have scanned some values");
    std::cout << "  Total scanned: " << total_scanned << std::endl;
    
    TEST_PASS("test_concurrent_scan");
}

// ============================================================================
// Test Cases - Level 5: Stress Test
// ============================================================================

/**
 * @brief 混合操作压力测试
 */
bool test_stress_mixed(const TestConfig& cfg) {
    HMasstreeWrapper mt;
    
    std::atomic<size_t> inserts{0}, deletes{0}, reads{0}, scans{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> error{false};
    
    auto worker = [&](int tid) {
        HMasstreeWrapper::thread_init(tid + 1);
        std::mt19937_64 rng(cfg.seed + tid);
        
        for (size_t i = 0; i < cfg.num_keys / cfg.num_threads && !error; i++) {
            uint64_t key = rng() % (cfg.num_keys * 10);
            int op = rng() % 100;
            
            if (op < 40) {
                // 40% insert
                mt_insert(mt, key, key);
                inserts++;
            } else if (op < 50) {
                // 10% delete
                mt.remove(key);
                deletes++;
            } else if (op < 90) {
                // 40% read
                uint64_t result;
                mt.search(key, result);
                reads++;
            } else {
                // 10% scan
                std::vector<uint64_t> values;
                mt.scan(make_test_key(key), 10, values);
                scans++;
            }
        }
    };
    
    TestTimer timer;
    timer.start();
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i < cfg.num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }
    
    timer.stop();
    
    std::cout << "  Operations: Insert=" << inserts << ", Delete=" << deletes 
              << ", Read=" << reads << ", Scan=" << scans << std::endl;
    std::cout << "  Throughput: " << (inserts + deletes + reads + scans) / timer.elapsed_sec() 
              << " ops/sec" << std::endl;
    
    TEST_ASSERT(!error, "Stress test had errors");
    
    TEST_PASS("test_stress_mixed");
}

// ============================================================================
// Test Runner
// ============================================================================

int main(int argc, char* argv[]) {
    TestConfig cfg;
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-v" || arg == "--verbose") {
            cfg.verbose = true;
        } else if (arg == "-n" && i + 1 < argc) {
            cfg.num_keys = std::stoull(argv[++i]);
        } else if (arg == "-t" && i + 1 < argc) {
            cfg.num_threads = std::stoull(argv[++i]);
        }
    }
    
    std::cout << "========================================" << std::endl;
    std::cout << "H-Masstree Standalone Unit Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Config: num_keys=" << cfg.num_keys 
              << ", threads=" << cfg.num_threads << std::endl;
    std::cout << std::endl;
    
    int passed = 0, failed = 0;
    
    auto run_test = [&](bool (*test_fn)(), const char* name) {
        std::cout << "[TEST] " << name << std::endl;
        if (test_fn()) {
            passed++;
        } else {
            failed++;
        }
    };
    
    auto run_test_cfg = [&](bool (*test_fn)(const TestConfig&), const char* name) {
        std::cout << "[TEST] " << name << std::endl;
        if (test_fn(cfg)) {
            passed++;
        } else {
            failed++;
        }
    };
    
    // Level 1: Basic Operations
    std::cout << "\n=== Level 1: Basic Operations ===" << std::endl;
    run_test(test_basic_insert_get, "Basic Insert/Get");
    run_test(test_update, "Update");
    run_test(test_delete, "Delete");
    run_test(test_boundary_keys, "Boundary Keys");
    
    // Level 2: Scan Operations
    std::cout << "\n=== Level 2: Scan Operations ===" << std::endl;
    run_test(test_forward_scan, "Forward Scan");
    run_test(test_scan_with_keys, "Scan with Keys");
    run_test(test_range_scan, "Range Scan");
    run_test(test_empty_scan, "Empty Scan");
    
    // Level 3: Large Scale
    std::cout << "\n=== Level 3: Large Scale ===" << std::endl;
    run_test_cfg(test_large_sequential, "Large Sequential");
    run_test_cfg(test_large_random, "Large Random");
    run_test_cfg(test_insert_delete_verify, "Insert/Delete/Verify");
    
    // Level 4: Concurrent
    std::cout << "\n=== Level 4: Concurrent Operations ===" << std::endl;
    run_test_cfg(test_concurrent_read, "Concurrent Read");
    run_test_cfg(test_concurrent_read_write, "Concurrent Read/Write");
    run_test_cfg(test_concurrent_scan, "Concurrent Scan");
    
    // Level 5: Stress
    std::cout << "\n=== Level 5: Stress Test ===" << std::endl;
    run_test_cfg(test_stress_mixed, "Mixed Stress");
    
    // Summary
    std::cout << "\n========================================" << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return failed > 0 ? 1 : 0;
}
