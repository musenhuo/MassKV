#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import shutil
import subprocess
import sys


ALLOWED_KEYS = {
    "variant",
    "build_mode",
    "distribution",
    "key_count",
    "prefix_count",
    "query_count",
    "hit_percent",
    "threads",
    "avg_latency_ns",
    "p99_latency_ns",
    "throughput_ops",
    "avg_io_total_per_query",
    "avg_io_total_top1pct_latency",
    "avg_io_l1_pages_per_query",
    "avg_io_l1_pages_top1pct_latency",
    "avg_io_pst_reads_per_query",
    "avg_io_pst_reads_top1pct_latency",
    "rss_bytes",
    "l1_index_bytes_estimated",
    "l1_route_partition_bytes",
    "l1_route_index_estimated_bytes",
    "l1_subtree_bytes",
    "l1_subtree_cache_bytes",
    "l1_governance_bytes",
    "l1_subtree_cache_requests",
    "l1_subtree_cache_hits",
    "l1_subtree_cache_misses",
    "l1_subtree_cache_hit_rate",
    "l1_cow_persist_calls",
    "l1_cow_reused_pages",
    "l1_cow_written_pages",
    "l1_cow_reused_bytes",
    "l1_cow_written_bytes",
    "l1_cow_page_reuse_ratio",
    "pst_direct_unaligned_fallbacks",
    "pst_short_reads",
    "pst_nowait_eagain_retries",
    "pst_nowait_unsupported_fallbacks",
    "use_direct_io",
    "warmup_queries",
    "enable_subtree_cache",
    "subtree_cache_capacity",
    "subtree_cache_max_bytes",
    "bitmap_persist_every",
    "pst_nowait_poll",
}


def parse_kv_output(text: str) -> dict:
    result = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.strip().split("=", 1)
        if key in ALLOWED_KEYS:
            result[key] = value
    return result


def as_number(value: str):
    try:
        if any(ch in value for ch in ".eE"):
            return float(value)
        return int(value)
    except ValueError:
        return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", default="build_hybrid_check")
    parser.add_argument("--db-root", required=True)
    parser.add_argument("--results-root", required=True)
    parser.add_argument("--key-count", type=int, default=1_000_000)
    parser.add_argument("--query-count", type=int, default=1_000_000)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--distribution", default="uniform")
    parser.add_argument("--variant", default="direction_b_full")
    parser.add_argument("--build-mode", default="fast_bulk_l1")
    parser.add_argument("--pool-size-bytes", type=int, default=8 << 30)
    parser.add_argument("--flush-batch", type=int, default=250_000)
    parser.add_argument("--hit-percent", type=int, default=80)
    parser.add_argument("--prefix-ratios", default="0.1,0.05,0.01")
    parser.add_argument("--use-direct-io", type=int, default=1)
    parser.add_argument("--warmup-queries", type=int, default=0)
    parser.add_argument("--enable-subtree-cache", type=int, default=1)
    parser.add_argument("--subtree-cache-capacity", type=int, default=256)
    parser.add_argument("--subtree-cache-max-bytes", type=int, default=256 << 20)
    parser.add_argument("--bitmap-persist-every", type=int, default=1024)
    parser.add_argument("--pst-nowait-poll", type=int, default=0)
    parser.add_argument("--keep-db-files", type=int, default=0)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    benchmark = pathlib.Path(args.build_dir) / "experiments" / "performance_evaluation" / "01_point_lookup" / "point_lookup_benchmark"
    if not benchmark.exists():
        raise SystemExit(f"benchmark not found: {benchmark}")

    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_id = args.run_id if args.run_id else timestamp
    run_dir = pathlib.Path(args.results_root) / run_id
    if run_dir.exists():
        shutil.rmtree(run_dir)
    raw_dir = run_dir / "raw"
    plot_dir = run_dir / "plots"
    run_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.db_root).mkdir(parents=True, exist_ok=True)

    ratios = [float(item) for item in args.prefix_ratios.split(",") if item]
    rows = []
    for ratio in ratios:
        prefix_count = max(1, int(args.key_count * ratio))
        db_dir = pathlib.Path(args.db_root) / f"{timestamp}_{args.variant}_{args.distribution}_{args.key_count}_{prefix_count}_{args.threads}t"
        db_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(benchmark),
            f"--variant={args.variant}",
            f"--build-mode={args.build_mode}",
            f"--db-dir={db_dir}",
            f"--distribution={args.distribution}",
            f"--key-count={args.key_count}",
            f"--prefix-count={prefix_count}",
            f"--query-count={args.query_count}",
            f"--hit-percent={args.hit_percent}",
            f"--threads={args.threads}",
            f"--flush-batch={args.flush_batch}",
            f"--pool-size-bytes={args.pool_size_bytes}",
            f"--use-direct-io={args.use_direct_io}",
            f"--warmup-queries={args.warmup_queries}",
            f"--enable-subtree-cache={args.enable_subtree_cache}",
            f"--subtree-cache-capacity={args.subtree_cache_capacity}",
            f"--subtree-cache-max-bytes={args.subtree_cache_max_bytes}",
            f"--bitmap-persist-every={args.bitmap_persist_every}",
            f"--pst-nowait-poll={args.pst_nowait_poll}",
            f"--keep-db-files={args.keep_db_files}",
        ]
        try:
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            fail_path = raw_dir / f"point_lookup_{args.key_count}_{prefix_count}_{args.distribution}_{args.threads}t.failed.txt"
            fail_path.write_text(
                "CMD:\n" + " ".join(cmd) + "\n\nSTDOUT:\n" + (exc.stdout or "") + "\n\nSTDERR:\n" + (exc.stderr or "")
            )
            raise SystemExit(
                f"benchmark failed for prefix_count={prefix_count}, see {fail_path}"
            ) from exc
        raw_path = raw_dir / f"point_lookup_{args.key_count}_{prefix_count}_{args.distribution}_{args.threads}t.txt"
        raw_path.write_text(completed.stdout)
        parsed = {k: as_number(v) for k, v in parse_kv_output(completed.stdout).items()}
        parsed["prefix_ratio"] = ratio
        rows.append(parsed)

    csv_path = run_dir / "results.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    meta = {
        "timestamp_utc": timestamp,
        "variant": args.variant,
        "distribution": args.distribution,
        "build_mode": args.build_mode,
        "key_count": args.key_count,
        "query_count": args.query_count,
        "threads": args.threads,
        "hit_percent": args.hit_percent,
        "pool_size_bytes": args.pool_size_bytes,
        "flush_batch": args.flush_batch,
        "prefix_ratios": ratios,
        "db_root": args.db_root,
        "run_id": run_id,
        "use_direct_io": args.use_direct_io,
        "warmup_queries": args.warmup_queries,
        "enable_subtree_cache": args.enable_subtree_cache,
        "subtree_cache_capacity": args.subtree_cache_capacity,
        "subtree_cache_max_bytes": args.subtree_cache_max_bytes,
        "bitmap_persist_every": args.bitmap_persist_every,
        "pst_nowait_poll": args.pst_nowait_poll,
        "keep_db_files": args.keep_db_files,
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print(run_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
