/* H-Masstree External Storage Component Tests
 * L2 Level: Unit tests for NodeHandle, PackedPage
 */

#include <iostream>
#include <cassert>
#include <cstring>

// Force external storage mode for this test
#define HMASSTREE_EXTERNAL_STORAGE
#include "node_handle.h"

using namespace Masstree;

int tests_passed = 0;
int tests_failed = 0;

#define TEST(name) \
    std::cout << "[TEST] " << #name << std::endl; \
    try { test_##name(); tests_passed++; std::cout << "[PASS] " << #name << std::endl; } \
    catch (const std::exception& e) { tests_failed++; std::cout << "[FAIL] " << #name << ": " << e.what() << std::endl; }

void test_node_handle_creation() {
    // Test null handle
    NodeHandle null_h;
    assert(null_h.is_null());
    assert(!null_h.is_valid());
    
    // Test leaf handle creation
    NodeHandle leaf_h = NodeHandle::make_leaf(12345, 3, 100);
    assert(leaf_h.is_valid());
    assert(!leaf_h.is_null());
    assert(leaf_h.is_leaf());
    assert(!leaf_h.is_internode());
    assert(leaf_h.page_id() == 12345);
    assert(leaf_h.slot_index() == 3);
    assert(leaf_h.generation() == 100);
    
    // Test internode handle creation
    NodeHandle in_h = NodeHandle::make_internode(99999, 7, 500);
    assert(in_h.is_valid());
    assert(in_h.is_internode());
    assert(!in_h.is_leaf());
    assert(in_h.page_id() == 99999);
    assert(in_h.slot_index() == 7);
    assert(in_h.generation() == 500);
}

void test_node_handle_comparison() {
    NodeHandle h1 = NodeHandle::make_leaf(100, 2, 50);
    NodeHandle h2 = NodeHandle::make_leaf(100, 2, 50);
    NodeHandle h3 = NodeHandle::make_leaf(100, 2, 51);  // Different generation
    NodeHandle h4 = NodeHandle::make_internode(100, 2, 50);  // Different type
    
    assert(h1 == h2);
    assert(!(h1 == h3));  // Different generation
    assert(!(h1 == h4));  // Different type
    
    NodeHandle null1, null2;
    assert(null1 == null2);
}

void test_node_handle_serialization() {
    NodeHandle original = NodeHandle::make_leaf(0x1FFFFF, 5, 0xFFFFF);  // Max values
    uint64_t raw = original.raw();
    
    NodeHandle restored(raw);
    assert(restored.is_valid());
    assert(restored.is_leaf());
    assert(restored.page_id() == 0x1FFFFF);
    assert(restored.slot_index() == 5);
    assert(restored.generation() == 0xFFFFF);
    assert(original == restored);
}

void test_node_handle_boundary() {
    // Test max page_id (37 bits)
    NodeHandle max_page = NodeHandle::make_leaf(NodeHandle::MAX_PAGE_ID, 0, 0);
    assert(max_page.page_id() == NodeHandle::MAX_PAGE_ID);
    
    // Test all slots (0-7)
    for (uint8_t slot = 0; slot < 8; slot++) {
        NodeHandle h = NodeHandle::make_leaf(0, slot, 0);
        assert(h.slot_index() == slot);
    }
    
    // Test max generation
    NodeHandle max_gen = NodeHandle::make_leaf(0, 0, NodeHandle::MAX_GENERATION);
    assert(max_gen.generation() == NodeHandle::MAX_GENERATION);
}

void test_packed_page_structure() {
    PackedPage page;
    page.init(0);  // page_id = 0
    
    // Test header
    assert(page.header.magic == PackedPage::MAGIC);
    assert(page.header.page_id == 0);
    
    // Test slot access
    for (uint8_t i = 0; i < PackedPage::NODES_PER_PAGE; i++) {
        void* slot = page.get_slot_ptr(i);
        assert(slot != nullptr);
        
        // Verify slot is within page bounds
        char* slot_ptr = static_cast<char*>(slot);
        char* page_start = reinterpret_cast<char*>(&page);
        char* page_end = page_start + sizeof(page);
        assert(slot_ptr >= page_start && slot_ptr < page_end);
    }
}

void test_packed_page_slot_independence() {
    PackedPage page;
    page.init(0);
    
    // Write different patterns to each slot
    for (uint8_t i = 0; i < PackedPage::NODES_PER_PAGE; i++) {
        memset(page.get_slot_ptr(i), 0xA0 + i, 64);
    }
    
    // Verify each slot has its own pattern
    for (uint8_t i = 0; i < PackedPage::NODES_PER_PAGE; i++) {
        uint8_t* slot = static_cast<uint8_t*>(page.get_slot_ptr(i));
        assert(slot[0] == 0xA0 + i);
        assert(slot[63] == 0xA0 + i);
    }
}

void test_packed_page_size() {
    // Verify page fits in 4KB
    static_assert(sizeof(PackedPage) == PackedPage::PAGE_SIZE, 
                  "PackedPage must be exactly 4KB");
    
    PackedPage page;
    assert(sizeof(page) == 4096);
}

void test_atomic_node_handle() {
    AtomicNodeHandle atomic_h;
    
    // Initially null
    assert(atomic_h.load().is_null());
    
    // Store and load
    NodeHandle h = NodeHandle::make_leaf(1234, 5, 100);
    atomic_h.store(h);
    NodeHandle loaded = atomic_h.load();
    assert(loaded == h);
    
    // Compare exchange weak
    NodeHandle expected = h;
    NodeHandle new_val = NodeHandle::make_internode(5678, 2, 200);
    assert(atomic_h.compare_exchange_weak(expected, new_val));
    assert(atomic_h.load() == new_val);
    
    // Failed compare exchange
    expected = h;  // Wrong expected value
    NodeHandle newer = NodeHandle::make_leaf(9999, 1, 300);
    assert(!atomic_h.compare_exchange_weak(expected, newer));
    assert(atomic_h.load() == new_val);  // Unchanged
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "H-Masstree L2 Component Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    
    TEST(node_handle_creation);
    TEST(node_handle_comparison);
    TEST(node_handle_serialization);
    TEST(node_handle_boundary);
    TEST(packed_page_structure);
    TEST(packed_page_slot_independence);
    TEST(packed_page_size);
    TEST(atomic_node_handle);
    
    std::cout << "========================================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " 
              << tests_failed << " failed" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return tests_failed > 0 ? 1 : 0;
}
