#!/bin/bash
# H-Masstree Benchmark Comparison Script
# 运行内存模式和外存模式的对比测试

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_KEYS=${1:-1000000}
CACHE_SIZE_MB=${2:-64}
NUM_OPS=${3:-1000000}

echo "========================================"
echo "H-Masstree Benchmark Comparison"
echo "========================================"
echo "Keys:       $NUM_KEYS"
echo "Cache Size: $CACHE_SIZE_MB MB"
echo "Operations: $NUM_OPS"
echo ""

# Source files
SOURCES="benchmark_comparison.cpp hmasstree_wrapper.cc straccum.cc string.cc str.cc string_slice.cc kvthread.cc misc.cc compiler.cc memdebug.cc clp.c"

BUILD_DIR="../../build_hmasstree"
mkdir -p $BUILD_DIR

# ============================================================================
# Compile Memory Mode
# ============================================================================
echo "Compiling H-Masstree Memory Mode..."
g++ -std=c++17 -O3 -DFLOWKV_KEY16 \
    -I. -I.. -I../../include \
    -include config.h \
    $SOURCES \
    -lpthread -o $BUILD_DIR/benchmark_memory 2>&1 || {
    echo "Failed to compile memory mode!"
    exit 1
}
echo "  -> $BUILD_DIR/benchmark_memory"

# ============================================================================
# Compile External Storage Mode
# ============================================================================
echo "Compiling H-Masstree External Storage Mode..."
g++ -std=c++17 -O3 -DFLOWKV_KEY16 -DHMASSTREE_EXTERNAL_STORAGE \
    -I. -I.. -I../../include \
    -include config.h \
    $SOURCES index_storage.cpp node_cache.cpp \
    -lpthread -o $BUILD_DIR/benchmark_external 2>&1 || {
    echo "Failed to compile external mode!"
    exit 1
}
echo "  -> $BUILD_DIR/benchmark_external"

echo ""
echo "========================================"
echo "Running Memory Mode Benchmark"
echo "========================================"
$BUILD_DIR/benchmark_memory -n $NUM_KEYS -o $NUM_OPS 2>&1 | tee $BUILD_DIR/memory_results.txt

echo ""
echo "========================================"
echo "Running External Storage Mode Benchmark"  
echo "========================================"
$BUILD_DIR/benchmark_external -n $NUM_KEYS -o $NUM_OPS -c $CACHE_SIZE_MB 2>&1 | tee $BUILD_DIR/external_results.txt

echo ""
echo "========================================"
echo "Results saved to:"
echo "  $BUILD_DIR/memory_results.txt"
echo "  $BUILD_DIR/external_results.txt"
echo "========================================"
