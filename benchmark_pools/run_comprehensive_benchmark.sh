#!/bin/bash
#
# FlowKV Masstree Comprehensive Benchmark Script
# 测试三种模式的性能对比以及冷启动性能
#

set -e

# === 配置 ===
NUM_KEYS=${NUM_KEYS:-10000000}  # 10M keys
STORAGE_PATH="/tmp/hmasstree_benchmark.dat"
RESULT_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
CSV_FILE="$RESULT_DIR/results.csv"
REPORT_FILE="$RESULT_DIR/BENCHMARK_REPORT.md"

# 编译目录
BUILD_DIR="/tmp/masstree_benchmark_build"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLOWKV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# === 编译函数 ===
compile_benchmark() {
    local mode=$1
    local define=$2
    local output=$3
    local extra_flags=${4:-""}
    
    log_info "Compiling $mode..."
    
    mkdir -p "$BUILD_DIR"
    
    local COMMON_FLAGS="-std=c++17 -O3 -g -DFLOWKV_KEY16 $define $extra_flags"
    local INCLUDES="-I$FLOWKV_DIR/lib -I$FLOWKV_DIR/include"
    
    if [[ "$mode" == "Original Masstree" ]]; then
        # 原版Masstree - 使用masstree自己的config.h
        g++ $COMMON_FLAGS $INCLUDES \
            -include "$FLOWKV_DIR/lib/masstree/config.h" \
            "$SCRIPT_DIR/comprehensive_benchmark.cpp" \
            "$FLOWKV_DIR/lib/masstree/masstree_wrapper.cc" \
            "$FLOWKV_DIR/lib/masstree/straccum.cc" \
            "$FLOWKV_DIR/lib/masstree/string.cc" \
            "$FLOWKV_DIR/lib/masstree/str.cc" \
            "$FLOWKV_DIR/lib/masstree/string_slice.cc" \
            "$FLOWKV_DIR/lib/masstree/kvthread.cc" \
            "$FLOWKV_DIR/lib/masstree/misc.cc" \
            "$FLOWKV_DIR/lib/masstree/compiler.cc" \
            "$FLOWKV_DIR/lib/masstree/memdebug.cc" \
            "$FLOWKV_DIR/lib/masstree/clp.c" \
            -lpthread -o "$output"
    elif [[ "$mode" == "HMasstree Memory" ]]; then
        # HMasstree内存模式 - 使用hmasstree自己的config.h
        g++ $COMMON_FLAGS $INCLUDES \
            -I"$FLOWKV_DIR/lib/hmasstree" \
            -include "$FLOWKV_DIR/lib/hmasstree/config.h" \
            "$SCRIPT_DIR/comprehensive_benchmark.cpp" \
            "$FLOWKV_DIR/lib/hmasstree/hmasstree_wrapper.cc" \
            "$FLOWKV_DIR/lib/hmasstree/straccum.cc" \
            "$FLOWKV_DIR/lib/hmasstree/string.cc" \
            "$FLOWKV_DIR/lib/hmasstree/str.cc" \
            "$FLOWKV_DIR/lib/hmasstree/string_slice.cc" \
            "$FLOWKV_DIR/lib/hmasstree/kvthread.cc" \
            "$FLOWKV_DIR/lib/hmasstree/misc.cc" \
            "$FLOWKV_DIR/lib/hmasstree/compiler.cc" \
            "$FLOWKV_DIR/lib/hmasstree/memdebug.cc" \
            "$FLOWKV_DIR/lib/hmasstree/clp.c" \
            -lpthread -o "$output"
    else
        # HMasstree外存模式 - 使用hmasstree自己的config.h
        g++ $COMMON_FLAGS $INCLUDES \
            -I"$FLOWKV_DIR/lib/hmasstree" \
            -include "$FLOWKV_DIR/lib/hmasstree/config.h" \
            "$SCRIPT_DIR/comprehensive_benchmark.cpp" \
            "$FLOWKV_DIR/lib/hmasstree/hmasstree_wrapper.cc" \
            "$FLOWKV_DIR/lib/hmasstree/index_storage.cpp" \
            "$FLOWKV_DIR/lib/hmasstree/node_cache.cpp" \
            "$FLOWKV_DIR/lib/hmasstree/straccum.cc" \
            "$FLOWKV_DIR/lib/hmasstree/string.cc" \
            "$FLOWKV_DIR/lib/hmasstree/str.cc" \
            "$FLOWKV_DIR/lib/hmasstree/string_slice.cc" \
            "$FLOWKV_DIR/lib/hmasstree/kvthread.cc" \
            "$FLOWKV_DIR/lib/hmasstree/misc.cc" \
            "$FLOWKV_DIR/lib/hmasstree/compiler.cc" \
            "$FLOWKV_DIR/lib/hmasstree/memdebug.cc" \
            "$FLOWKV_DIR/lib/hmasstree/clp.c" \
            -lpthread -o "$output"
    fi
    
    log_info "Compiled: $output"
}

# === 运行测试 ===
run_test() {
    local binary=$1
    local name=$2
    local extra_args=${3:-""}
    
    log_info "Running: $name"
    echo "========================================"
    
    $binary -n $NUM_KEYS -o "$CSV_FILE" $extra_args
    
    echo ""
}

# === 主程序 ===
main() {
    log_info "FlowKV Masstree Comprehensive Benchmark"
    log_info "Keys: $NUM_KEYS ($(echo "scale=1; $NUM_KEYS/1000000" | bc)M)"
    log_info "Results: $RESULT_DIR"
    
    # 创建结果目录
    mkdir -p "$RESULT_DIR"
    
    # 清理旧存储
    rm -f "$STORAGE_PATH"
    
    # 编译三个版本
    log_info "Compiling all versions..."
    compile_benchmark "Original Masstree" "-DBENCHMARK_ORIGINAL_MASSTREE" "$BUILD_DIR/bench_original"
    compile_benchmark "HMasstree Memory" "-DBENCHMARK_HMASSTREE_MEMORY" "$BUILD_DIR/bench_hmasstree_mem"
    compile_benchmark "HMasstree External" "-DBENCHMARK_HMASSTREE_EXTERNAL -DHMASSTREE_EXTERNAL_STORAGE" "$BUILD_DIR/bench_hmasstree_ext"
    
    log_info "All compilations successful!"
    echo ""
    
    # === Phase 1: 基准测试 ===
    log_info "=== Phase 1: Baseline Performance Tests ==="
    
    # 1. 原版Masstree
    run_test "$BUILD_DIR/bench_original" "Original Masstree"
    ORIGINAL_MEM=$(tail -1 "$CSV_FILE" | cut -d',' -f6)
    log_info "Original Masstree memory: ${ORIGINAL_MEM} MB"
    
    # 2. HMasstree内存模式
    run_test "$BUILD_DIR/bench_hmasstree_mem" "HMasstree Memory Mode"
    
    # 3. HMasstree外存模式 (不限制缓存)
    run_test "$BUILD_DIR/bench_hmasstree_ext" "HMasstree External Mode (Unlimited Cache)" "-c 4096 -s $STORAGE_PATH"
    
    # === Phase 2: 冷启动测试 ===
    log_info "=== Phase 2: Cold Restart Performance Tests ==="
    
    # 计算不同缓存大小 (基于原版Masstree内存占用)
    X=$(echo "$ORIGINAL_MEM" | cut -d'.' -f1)  # 取整数部分
    X08=$(echo "scale=0; $X * 8 / 10" | bc)
    X05=$(echo "scale=0; $X * 5 / 10" | bc)
    
    log_info "Original Masstree memory (X) = $X MB"
    log_info "Testing cache sizes: Unlimited, ${X}MB (X), ${X08}MB (0.8X), ${X05}MB (0.5X)"
    
    # 先写入数据 (如果还没有)
    if [ ! -f "$STORAGE_PATH" ]; then
        log_info "Creating storage file..."
        run_test "$BUILD_DIR/bench_hmasstree_ext" "Creating storage" "-c 4096 -s $STORAGE_PATH"
    fi
    
    # 冷启动测试 - 不限制
    log_info "Cold restart: Unlimited cache"
    "$BUILD_DIR/bench_hmasstree_ext" -n $NUM_KEYS -s "$STORAGE_PATH" -c 4096 -r 4096 -o "$CSV_FILE"
    
    # 冷启动测试 - X
    log_info "Cold restart: ${X}MB cache (X)"  
    "$BUILD_DIR/bench_hmasstree_ext" -n $NUM_KEYS -s "$STORAGE_PATH" -c $X -r $X -o "$CSV_FILE"
    
    # 冷启动测试 - 0.8X
    log_info "Cold restart: ${X08}MB cache (0.8X)"
    "$BUILD_DIR/bench_hmasstree_ext" -n $NUM_KEYS -s "$STORAGE_PATH" -c $X08 -r $X08 -o "$CSV_FILE"
    
    # 冷启动测试 - 0.5X
    log_info "Cold restart: ${X05}MB cache (0.5X)"
    "$BUILD_DIR/bench_hmasstree_ext" -n $NUM_KEYS -s "$STORAGE_PATH" -c $X05 -r $X05 -o "$CSV_FILE"
    
    # === 生成报告 ===
    log_info "Generating report..."
    generate_report
    
    log_info "Benchmark complete!"
    log_info "Results: $RESULT_DIR"
    log_info "CSV: $CSV_FILE"
    log_info "Report: $REPORT_FILE"
}

# === 生成报告 ===
generate_report() {
    cat > "$REPORT_FILE" << 'EOF'
# FlowKV Masstree Benchmark Report

## Test Environment

EOF
    
    echo "- **Date**: $(date)" >> "$REPORT_FILE"
    echo "- **Keys**: $NUM_KEYS ($(echo "scale=1; $NUM_KEYS/1000000" | bc)M)" >> "$REPORT_FILE"
    echo "- **CPU**: $(grep "model name" /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)" >> "$REPORT_FILE"
    echo "- **Memory**: $(free -h | grep Mem | awk '{print $2}')" >> "$REPORT_FILE"
    echo "- **OS**: $(uname -r)" >> "$REPORT_FILE"
    
    cat >> "$REPORT_FILE" << 'EOF'

## Test Methodology

### Phase 1: Baseline Performance

测试三种模式在相同数据规模下的性能：

1. **Original Masstree**: 原版Masstree实现
2. **HMasstree Memory Mode**: 带Handle扩展的Masstree（纯内存模式）
3. **HMasstree External Mode**: 带Handle扩展的Masstree（外存模式）

测试指标：
- **Write Performance**: 随机插入N个key-value对的吞吐量
- **Read Performance**: 随机查询N个key的吞吐量
- **Memory Usage**: RSS内存占用

### Phase 2: Cold Restart Performance

测试HMasstree外存模式在不同缓存限制下的冷启动读性能：

设原版Masstree内存占用为X，测试：
- **Unlimited**: 不限制缓存
- **X**: 缓存大小等于原版内存占用
- **0.8X**: 缓存为原版内存的80%
- **0.5X**: 缓存为原版内存的50%

## Results

### Baseline Performance Comparison

| Mode | Write (M ops/s) | Read (M ops/s) | Memory (MB) |
|------|-----------------|----------------|-------------|
EOF

    # 解析CSV添加基准测试结果
    if [ -f "$CSV_FILE" ]; then
        # Original Masstree
        line=$(grep "Original Masstree" "$CSV_FILE" | head -1)
        if [ -n "$line" ]; then
            write_ops=$(echo "$line" | cut -d',' -f5 | xargs printf "%.2f")
            read_ops=$(echo "$line" | cut -d',' -f8 | xargs printf "%.2f")
            mem=$(echo "$line" | cut -d',' -f10)
            write_m=$(echo "scale=2; $write_ops/1000000" | bc)
            read_m=$(echo "scale=2; $read_ops/1000000" | bc)
            echo "| Original Masstree | $write_m | $read_m | $mem |" >> "$REPORT_FILE"
        fi
        
        # HMasstree Memory
        line=$(grep "HMasstree Memory" "$CSV_FILE" | head -1)
        if [ -n "$line" ]; then
            write_ops=$(echo "$line" | cut -d',' -f5 | xargs printf "%.2f")
            read_ops=$(echo "$line" | cut -d',' -f8 | xargs printf "%.2f")
            mem=$(echo "$line" | cut -d',' -f10)
            write_m=$(echo "scale=2; $write_ops/1000000" | bc)
            read_m=$(echo "scale=2; $read_ops/1000000" | bc)
            echo "| HMasstree Memory | $write_m | $read_m | $mem |" >> "$REPORT_FILE"
        fi
        
        # HMasstree External (first entry - unlimited cache)
        line=$(grep "HMasstree External" "$CSV_FILE" | head -1)
        if [ -n "$line" ]; then
            write_ops=$(echo "$line" | cut -d',' -f5 | xargs printf "%.2f")
            read_ops=$(echo "$line" | cut -d',' -f8 | xargs printf "%.2f")
            mem=$(echo "$line" | cut -d',' -f10)
            write_m=$(echo "scale=2; $write_ops/1000000" | bc)
            read_m=$(echo "scale=2; $read_ops/1000000" | bc)
            echo "| HMasstree External | $write_m | $read_m | $mem |" >> "$REPORT_FILE"
        fi
    fi

    cat >> "$REPORT_FILE" << 'EOF'

### Cold Restart Performance

| Cache Size | Cache (MB) | Restore (ms) | Read (M ops/s) | Memory (MB) |
|------------|------------|--------------|----------------|-------------|
EOF

    # 解析冷启动结果
    if [ -f "$CSV_FILE" ]; then
        grep "Cold Restart" "$CSV_FILE" | while read line; do
            cache=$(echo "$line" | cut -d',' -f3)
            restore_time=$(echo "$line" | cut -d',' -f4 | xargs printf "%.2f")
            read_ops=$(echo "$line" | cut -d',' -f8 | xargs printf "%.2f")
            mem=$(echo "$line" | cut -d',' -f10)
            read_m=$(echo "scale=2; $read_ops/1000000" | bc)
            
            # 判断缓存类型
            if [ "$cache" -ge 4000 ]; then
                cache_type="Unlimited"
            elif [ "$cache" -ge "$X" ]; then
                cache_type="X (100%)"
            elif [ "$cache" -ge "$X08" ]; then
                cache_type="0.8X (80%)"
            else
                cache_type="0.5X (50%)"
            fi
            
            echo "| $cache_type | $cache | $restore_time | $read_m | $mem |" >> "$REPORT_FILE"
        done
    fi

    cat >> "$REPORT_FILE" << 'EOF'

## Analysis

### Memory Efficiency

HMasstree外存模式通过将索引结构持久化到存储，可以显著降低内存占用。
冷启动时，节点按需加载，内存占用取决于缓存大小配置。

### Performance Tradeoffs

- **写性能**: 外存模式由于额外的Handle维护开销，写性能可能略低于纯内存模式
- **读性能**: 在缓存充足时，外存模式读性能接近内存模式
- **冷启动**: 缓存限制越小，需要更多的磁盘I/O，读性能下降

### Recommendations

1. 对于内存充足的场景，使用纯内存模式获得最佳性能
2. 对于内存受限或需要持久化的场景，使用外存模式
3. 缓存大小建议至少设置为数据集索引大小的50%以保证基本性能

## Raw Data

详见 `results.csv` 文件。
EOF

    log_info "Report generated: $REPORT_FILE"
}

# 运行主程序
main "$@"
