#!/bin/bash
# FlowKV 多规模 benchmark 测试脚本
# 测试 100万、1000万、1亿、10亿 四个规模

cd /home/zwt/yjy/FlowKV

BENCHMARK="./build_no_kvsep/benchmarks/benchmark"
POOL_DIR="benchmark_pools"
DURATION=300    # 5分钟
THREADS=1       # 单线程测试

# 测试配置: 规模 pool_size
declare -A TESTS
TESTS["1M"]="1000000 1"
TESTS["10M"]="10000000 2"
TESTS["100M"]="100000000 20"
TESTS["1B"]="1000000000 200"

echo "============================================="
echo "FlowKV 多规模 Benchmark 测试"
echo "测试时间: $(date)"
echo "每个规模读取测试时长: ${DURATION}秒"
echo "============================================="

for scale in 1M 10M 100M 1B; do
    read num pool_size <<< "${TESTS[$scale]}"
    pool_path="${POOL_DIR}/pool_${scale}"
    
    echo ""
    echo "============================================="
    echo "测试规模: ${scale} (${num} KV pairs)"
    echo "Pool 路径: ${pool_path}"
    echo "Pool 大小: ${pool_size} GB"
    echo "============================================="
    
    # 1. 加载数据
    echo "[$(date +%H:%M:%S)] 开始加载数据..."
    rm -f ${pool_path} ${pool_path}.manifest
    $BENCHMARK \
        --pool_path=${pool_path} \
        --pool_size_GB=${pool_size} \
        --num=${num} \
        --num_ops=${num} \
        --threads=16 \
        --load_type=C \
        2>&1 | tee ${POOL_DIR}/load_${scale}.log | grep -E "(thpt=|flush end|compaction end|L0 tree)"
    
    # 2. 定时随机读测试 (O_DIRECT)
    echo "[$(date +%H:%M:%S)] 开始定时随机读测试 (${DURATION}秒)..."
    $BENCHMARK \
        --pool_path=${pool_path} \
        --pool_size_GB=${pool_size} \
        --num=${num} \
        --threads=${THREADS} \
        --skip_load=true \
        --load_type=H \
        --duration=${DURATION} \
        --use_direct_io=true \
        --recover=true \
        2>&1 | tee ${POOL_DIR}/read_${scale}.log | grep -E "(DRAM|Total operations|Throughput|Avg latency)"
    
    echo "[$(date +%H:%M:%S)] ${scale} 测试完成"
    
    # 显示 pool 文件大小
    echo "Pool 文件大小:"
    du -h ${pool_path}
done

echo ""
echo "============================================="
echo "所有测试完成！"
echo "============================================="

# 生成汇总报告
echo ""
echo "测试结果汇总:"
echo "规模 | 吞吐率(MOPS) | 平均延迟(us) | 内存占用(GB)"
echo "---- | ------------ | ------------ | ------------"
for scale in 1M 10M 100M 1B; do
    log="${POOL_DIR}/read_${scale}.log"
    if [ -f "$log" ]; then
        throughput=$(grep "Throughput:" $log | awk '{print $2}')
        latency=$(grep "Avg latency:" $log | awk '{print $3}')
        dram=$(grep "DRAM consumption:" $log | tail -1 | awk '{print $3}')
        echo "${scale} | ${throughput} | ${latency} | ${dram}"
    fi
done
