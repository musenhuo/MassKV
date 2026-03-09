/* H-Masstree External Storage End-to-End Tests
 * Test Plan Level: L4 - E2E Functionality
 */

#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// Force external storage mode for this test
#ifndef HMASSTREE_EXTERNAL_STORAGE
#define HMASSTREE_EXTERNAL_STORAGE
#endif
#include "index_storage.h"
#include "node_cache.h"
#include "node_handle.h"

using namespace Masstree;

int tests_passed = 0;
int tests_failed = 0;

#define TEST(name) \
    std::cout << "[TEST] " << #name << std::endl; \
    try { test_##name(); tests_passed++; std::cout << "[PASS] " << #name << std::endl; } \
    catch (const std::exception& e) { tests_failed++; std::cout << "[FAIL] " << #name << ": " << e.what() << std::endl; } \
    catch (...) { tests_failed++; std::cout << "[FAIL] " << #name << ": unknown exception" << std::endl; }

const char* TEST_FILE = "/tmp/hmasstree_test_storage.dat";

void cleanup_test_file() {
    unlink(TEST_FILE);
}

// ========== Storage Manager Tests ==========

void test_storage_open_close() {
    cleanup_test_file();
    
    IndexStorageConfig config;
    config.storage_path = TEST_FILE;
    config.use_direct_io = false;  // Disable for testing
    config.recover = false;
    
    IndexStorageManager storage(config);
    
    // Open
    assert(storage.open());
    assert(storage.is_open());
    
    // Close
    storage.close();
    assert(!storage.is_open());
    
    // Check file was created
    struct stat st;
    assert(stat(TEST_FILE, &st) == 0);
    
    cleanup_test_file();
}

void test_storage_page_allocation() {
    cleanup_test_file();
    
    IndexStorageConfig config;
    config.storage_path = TEST_FILE;
    config.use_direct_io = false;
    
    IndexStorageManager storage(config);
    assert(storage.open());
    
    // Allocate some pages
    uint64_t page1 = storage.allocate_page();
    uint64_t page2 = storage.allocate_page();
    uint64_t page3 = storage.allocate_page();
    
    assert(page1 != 0);
    assert(page2 != 0);
    assert(page3 != 0);
    assert(page1 != page2);
    assert(page2 != page3);
    
    // Check stats
    assert(storage.pages_allocated() >= 3);
    assert(storage.pages_in_use() >= 3);
    
    // Free a page
    storage.free_page(page2);
    assert(storage.pages_freed() >= 1);
    
    storage.close();
    cleanup_test_file();
}

void test_storage_node_slot_allocation() {
    cleanup_test_file();
    
    IndexStorageConfig config;
    config.storage_path = TEST_FILE;
    config.use_direct_io = false;
    
    IndexStorageManager storage(config);
    assert(storage.open());
    
    // Allocate node slots
    auto [page1, slot1] = storage.allocate_node_slot(NodeType::LEAF);
    assert(page1 != 0);
    assert(slot1 == 0);  // First slot in new page
    
    auto [page2, slot2] = storage.allocate_node_slot(NodeType::LEAF);
    assert(page2 == page1);  // Same page, different slot
    assert(slot2 == 1);
    
    // Allocate all remaining slots in page
    for (int i = 2; i < 8; i++) {
        auto [p, s] = storage.allocate_node_slot(NodeType::LEAF);
        assert(p == page1);
        assert(s == i);
    }
    
    // Next allocation should be in new page
    auto [page_new, slot_new] = storage.allocate_node_slot(NodeType::LEAF);
    assert(page_new != page1);
    assert(slot_new == 0);
    
    storage.close();
    cleanup_test_file();
}

void test_storage_page_io() {
    cleanup_test_file();
    
    IndexStorageConfig config;
    config.storage_path = TEST_FILE;
    config.use_direct_io = false;
    
    IndexStorageManager storage(config);
    assert(storage.open());
    
    // Allocate a page
    uint64_t page_id = storage.allocate_page();
    assert(page_id != 0);
    
    // Prepare test data
    alignas(4096) uint8_t write_buffer[4096];
    alignas(4096) uint8_t read_buffer[4096];
    
    memset(write_buffer, 0, sizeof(write_buffer));
    strcpy(reinterpret_cast<char*>(write_buffer), "Hello, H-Masstree External Storage!");
    write_buffer[100] = 0xAB;
    write_buffer[1000] = 0xCD;
    
    // Write
    assert(storage.write_page(page_id, write_buffer));
    
    // Read back
    memset(read_buffer, 0, sizeof(read_buffer));
    assert(storage.read_page(page_id, read_buffer));
    
    // Verify
    assert(memcmp(write_buffer, read_buffer, sizeof(write_buffer)) == 0);
    
    storage.close();
    cleanup_test_file();
}

void test_storage_persistence() {
    cleanup_test_file();
    
    uint64_t page_id;
    
    // Phase 1: Create and write
    {
        IndexStorageConfig config;
        config.storage_path = TEST_FILE;
        config.use_direct_io = false;
        
        IndexStorageManager storage(config);
        assert(storage.open());
        
        page_id = storage.allocate_page();
        assert(page_id != 0);
        
        alignas(4096) uint8_t buffer[4096];
        memset(buffer, 0, sizeof(buffer));
        strcpy(reinterpret_cast<char*>(buffer), "Persistence Test Data");
        
        assert(storage.write_page(page_id, buffer));
        storage.close();  // This calls persist_metadata()
    }
    
    // Phase 2: Recover and verify
    {
        IndexStorageConfig config;
        config.storage_path = TEST_FILE;
        config.use_direct_io = false;
        config.recover = true;  // Recovery mode
        
        IndexStorageManager storage(config);
        assert(storage.open());
        
        // Read back
        alignas(4096) uint8_t buffer[4096];
        assert(storage.read_page(page_id, buffer));
        
        // Verify content
        assert(strcmp(reinterpret_cast<char*>(buffer), "Persistence Test Data") == 0);
        
        storage.close();
    }
    
    cleanup_test_file();
}

void test_storage_superblock() {
    cleanup_test_file();
    
    // Phase 1: Create storage
    {
        IndexStorageConfig config;
        config.storage_path = TEST_FILE;
        config.use_direct_io = false;
        
        IndexStorageManager storage(config);
        assert(storage.open());
        
        // Allocate some pages
        for (int i = 0; i < 10; i++) {
            storage.allocate_page();
        }
        
        storage.close();
    }
    
    // Phase 2: Verify superblock
    {
        // Read raw superblock
        int fd = open(TEST_FILE, O_RDONLY);
        assert(fd >= 0);
        
        alignas(4096) uint8_t buffer[4096];
        ssize_t bytes = ::pread(fd, buffer, 4096, 0);
        assert(bytes == 4096);
        
        IndexSuperblock* sb = reinterpret_cast<IndexSuperblock*>(buffer);
        
        // Verify magic
        assert(sb->is_valid());
        
        // Verify checksum
        assert(sb->verify_checksum());
        
        // Verify some data
        assert(sb->total_segments > 0);
        assert(sb->next_page_id > 0);
        
        close(fd);
    }
    
    cleanup_test_file();
}

// ========== Node Cache Tests ==========

void test_node_cache_basic() {
    cleanup_test_file();
    
    IndexStorageConfig storage_config;
    storage_config.storage_path = TEST_FILE;
    storage_config.use_direct_io = false;
    
    IndexStorageManager storage(storage_config);
    assert(storage.open());
    
    NodeCacheConfig cache_config;
    cache_config.max_memory_bytes = 64 * 1024 * 1024;  // 64MB
    cache_config.num_shards = 16;
    
    NodeCache cache(&storage, cache_config);
    
    // Allocate a node
    auto [handle, ptr] = cache.allocate_node(NodeType::LEAF);
    assert(!handle.is_null());
    assert(ptr != nullptr);
    
    // Write some data to node
    memset(ptr, 0xAB, 64);
    cache.mark_dirty(handle);
    
    // Get the page and verify using handle
    CachedPage* page = cache.get_or_load(handle, true);
    assert(page != nullptr);
    
    uint8_t* node_data = static_cast<uint8_t*>(page->page_ptr->get_slot_ptr(handle.slot_index()));
    assert(node_data[0] == 0xAB);
    assert(node_data[63] == 0xAB);
    
    page->unpin();
    
    storage.close();
    cleanup_test_file();
}

void test_node_cache_multiple_nodes() {
    cleanup_test_file();
    
    IndexStorageConfig storage_config;
    storage_config.storage_path = TEST_FILE;
    storage_config.use_direct_io = false;
    
    IndexStorageManager storage(storage_config);
    assert(storage.open());
    
    NodeCacheConfig cache_config;
    cache_config.max_memory_bytes = 64 * 1024 * 1024;
    
    NodeCache cache(&storage, cache_config);
    
    const int NUM_NODES = 100;
    std::vector<NodeHandle> handles;
    
    // Allocate many nodes
    for (int i = 0; i < NUM_NODES; i++) {
        auto [handle, ptr] = cache.allocate_node(NodeType::LEAF);
        assert(!handle.is_null());
        
        // Write unique data
        uint8_t* data = static_cast<uint8_t*>(ptr);
        data[0] = static_cast<uint8_t>(i);
        data[1] = static_cast<uint8_t>(i >> 8);
        
        cache.mark_dirty(handle);
        handles.push_back(handle);
    }
    
    // Flush to disk
    cache.flush_all();
    
    // Verify all nodes
    for (int i = 0; i < NUM_NODES; i++) {
        CachedPage* page = cache.get_or_load(handles[i], true);
        assert(page != nullptr);
        
        uint8_t* data = static_cast<uint8_t*>(page->page_ptr->get_slot_ptr(handles[i].slot_index()));
        assert(data[0] == static_cast<uint8_t>(i));
        assert(data[1] == static_cast<uint8_t>(i >> 8));
        
        page->unpin();
    }
    
    storage.close();
    cleanup_test_file();
}

void test_node_cache_persistence() {
    cleanup_test_file();
    
    const int NUM_NODES = 50;
    std::vector<NodeHandle> handles;
    
    // Phase 1: Create nodes
    {
        IndexStorageConfig storage_config;
        storage_config.storage_path = TEST_FILE;
        storage_config.use_direct_io = false;
        
        IndexStorageManager storage(storage_config);
        assert(storage.open());
        
        NodeCacheConfig cache_config;
        cache_config.max_memory_bytes = 64 * 1024 * 1024;
        
        NodeCache cache(&storage, cache_config);
        
        for (int i = 0; i < NUM_NODES; i++) {
            auto [handle, ptr] = cache.allocate_node(NodeType::LEAF);
            
            // Write unique data
            uint8_t* data = static_cast<uint8_t*>(ptr);
            for (int j = 0; j < 64; j++) {
                data[j] = static_cast<uint8_t>(i + j);
            }
            
            cache.mark_dirty(handle);
            handles.push_back(handle);
        }
        
        cache.flush_all();
        storage.close();
    }
    
    // Phase 2: Recover and verify
    {
        IndexStorageConfig storage_config;
        storage_config.storage_path = TEST_FILE;
        storage_config.use_direct_io = false;
        storage_config.recover = true;
        
        IndexStorageManager storage(storage_config);
        assert(storage.open());
        
        NodeCacheConfig cache_config;
        cache_config.max_memory_bytes = 64 * 1024 * 1024;
        
        NodeCache cache(&storage, cache_config);
        
        // Verify all nodes
        for (int i = 0; i < NUM_NODES; i++) {
            CachedPage* page = cache.get_or_load(handles[i], true);
            assert(page != nullptr);
            
            uint8_t* data = static_cast<uint8_t*>(page->page_ptr->get_slot_ptr(handles[i].slot_index()));
            for (int j = 0; j < 64; j++) {
                assert(data[j] == static_cast<uint8_t>(i + j));
            }
            
            page->unpin();
        }
        
        storage.close();
    }
    
    cleanup_test_file();
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "H-Masstree External Storage L4 Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Storage Manager Tests
    std::cout << "\n--- Storage Manager Tests ---" << std::endl;
    TEST(storage_open_close);
    TEST(storage_page_allocation);
    TEST(storage_node_slot_allocation);
    TEST(storage_page_io);
    TEST(storage_persistence);
    TEST(storage_superblock);
    
    // Node Cache Tests
    std::cout << "\n--- Node Cache Tests ---" << std::endl;
    TEST(node_cache_basic);
    TEST(node_cache_multiple_nodes);
    TEST(node_cache_persistence);
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " 
              << tests_failed << " failed" << std::endl;
    std::cout << "========================================" << std::endl;
    
    cleanup_test_file();
    
    return tests_failed > 0 ? 1 : 0;
}
