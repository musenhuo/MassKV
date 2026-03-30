# Route Swap 阈值体系

最后更新：2026-03-21

## 背景

`FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES` 控制 route layer 的内存预算。超过阈值时，
`swap_all_on_overflow=true` 会把所有 route leaf 条目 spill 到 SSD，内存中只保留
directory index（一棵小 Masstree，每个 leaf page 一条记录）。

## 内存估算公式

```
P = prefix 数量 = N × prefix_ratio

热内存（不 swap）= P × 64B
  （两棵 Masstree：route_index + route_descriptor_index，各 32B/entry）

swap 后 directory 内存 = ceil(P / 170) × 32B
  （leaf_page_size=4096B，header=16B，entry=24B → 170 entries/page；
   directory Masstree 每页一条 32B 记录）
```

## 各规模热内存 vs swap 后 directory 内存

### 热内存（不 swap）

| Scale  |   0.10N  |   0.05N  |   0.01N  |
|--------|----------|----------|----------|
| 1M     |   6.1MB  |   3.1MB  | 625KB    |
| 10M    |  61.0MB  |  30.5MB  |   6.1MB  |
| 100M   | 610.4MB  | 305.2MB  |  61.0MB  |
| 1B     |   5.96GB |   2.98GB | 610.4MB  |
| 10B    |  59.6GB  |  29.8GB  |   5.96GB |
| 100B   | 596GB    | 298GB    |  59.6GB  |
| 1T     |   5.82TB |   2.91TB | 596GB    |

### swap 后 directory 内存（极小）

| Scale  |   0.10N  |   0.05N  |   0.01N  |
|--------|----------|----------|----------|
| 1M     |  18.4KB  |   9.2KB  |   1.8KB  |
| 10M    | 183.8KB  |  91.9KB  |  18.4KB  |
| 100M   |   1.8MB  | 919.1KB  | 183.8KB  |
| 1B     |  18.0MB  |   9.0MB  |   1.8MB  |
| 10B    | 179.5MB  |  89.8MB  |  18.0MB  |
| 100B   |   1.75GB | 897.6MB  | 179.5MB  |
| 1T     |  17.53GB |   8.77GB |   1.75GB |

## 推荐阈值（`FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES`）

设计原则：
1. **0.01N 尽量保持 hot**（最稀疏 prefix，内存最小，swap 收益低）
2. **0.10N 在大规模下必须 swap**（否则内存不可接受）
3. 阈值设在"0.01N 刚好 hot，0.05N/0.10N 触发 swap"的临界点附近
4. swap 后 directory 内存极小（KB~MB 级），可忽略

| Scale | 推荐阈值 | 0.10N | 0.05N | 0.01N | 备注 |
|-------|---------|-------|-------|-------|------|
| 1M    | 0（禁用）| hot   | hot   | hot   | 全部 hot，无需 swap |
| 10M   | 32MB    | swap  | hot   | hot   | 0.10N=61MB > 32MB |
| 100M  | 64MB    | swap  | swap  | hot   | 当前实验配置 |
| 1B    | 512MB   | swap  | swap  | swap  | 0.01N=610MB > 512MB |
| 10B   | 2GB     | swap  | swap  | swap  | |
| 100B  | 8GB     | swap  | swap  | swap  | |
| 1T    | 32GB    | swap  | swap  | swap  | |

## 自动化配置方案

在 benchmark 脚本中按 key_count 自动推算阈值，无需手动改：

```python
def auto_swap_threshold(key_count: int) -> int:
    """
    Returns FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES in bytes.
    0 means disabled (keep all hot).
    """
    if key_count <= 1_000_000:
        return 0
    if key_count <= 10_000_000:
        return 32 * 1024 * 1024        # 32MB
    if key_count <= 100_000_000:
        return 64 * 1024 * 1024        # 64MB
    if key_count <= 1_000_000_000:
        return 512 * 1024 * 1024       # 512MB
    if key_count <= 10_000_000_000:
        return 2 * 1024 * 1024 * 1024  # 2GB
    if key_count <= 100_000_000_000:
        return 8 * 1024 * 1024 * 1024  # 8GB
    return 32 * 1024 * 1024 * 1024     # 32GB (1T+)
```

使用方式（在 run_point_lookup_batch.py / run_write_online_batch.py 中）：

```python
threshold = auto_swap_threshold(key_count)
env = os.environ.copy()
if threshold > 0:
    env['FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES'] = str(threshold)
subprocess.run([binary, ...], env=env)
```

## 注意事项

1. **swap 触发是 BulkLoad/ApplyDeltaBatch 时一次性决策**，不是运行时动态的。
   每次 compaction 后重建 route layout 时重新判断。

2. **swap 后读路径多一次 SSD 读**（leaf page，4KB），但 directory 极小可常驻 cache，
   实测 100M/0.10N 下 l1_pages/query 从 ~1.0 降到 ~0.063（大量 TinyDirect 命中）。

3. **1B+ 规模下 0.01N 也必须 swap**（610MB > 512MB 阈值），directory 仅 1.8MB，
   读路径代价可接受。

4. **1T 规模的 0.10N directory = 17.5GB**，仍需大内存机器。若内存极度受限，
   可考虑二级 directory（当前未实现）。

5. 当前 `swap_all_on_overflow=true` 是全量 spill，没有部分 spill。
   阈值设置偏保守（宁可多 swap）比偏激进（OOM）更安全。
