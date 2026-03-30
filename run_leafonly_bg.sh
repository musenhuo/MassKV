#!/usr/bin/env bash
set -euo pipefail

export FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES=67108864
export FLOWKV_L1_ROUTE_SWAP_ALL_ON_OVERFLOW=1

python3 experiments/performance_evaluation/01_point_lookup/run_point_lookup_batch.py \
  --build-dir build_hybrid_check \
  --db-root /mnt/nvme0/flowkv_exp/performance_evaluation/01_point_lookup/dbfiles \
  --results-root experiments/performance_evaluation/01_point_lookup/results \
  --key-count 100000000 \
  --query-count 1000000 \
  --threads 1 \
  --distribution uniform \
  --variant direction_b_full \
  --build-mode fast_bulk_l1 \
  --pool-size-bytes 1099511627776 \
  --flush-batch 250000 \
  --hit-percent 80 \
  --prefix-ratios 0.1,0.05,0.01 \
  --use-direct-io 1 \
  --warmup-queries 0 \
  --enable-subtree-cache 1 \
  --subtree-cache-capacity 256 \
  --subtree-cache-max-bytes 268435456 \
  --bitmap-persist-every 1024 \
  --pst-nowait-poll 0 \
  --keep-db-files 0 \
  --run-id 20260321_100m_swap64m_leafonly_bg
