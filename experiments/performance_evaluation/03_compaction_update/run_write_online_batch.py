#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
import pathlib
import shutil
import subprocess
import sys


ALLOWED_KEYS = {
    "variant",
    "build_mode",
    "maintenance_mode",
    "distribution",
    "write_ops",
    "threads",
    "flush_threads_effective",
    "compaction_threads_effective",
    "prefix_count",
    "flush_batch",
    "l0_compaction_trigger",
    "l0_write_stall_threshold",
    "use_direct_io",
    "avg_put_latency_ns",
    "p99_put_latency_ns",
    "put_path_throughput_ops",
    "foreground_put_throughput_ops",
    "foreground_put_phase_time_ms",
    "drain_wait_time_ms",
    "ingest_throughput_ops",
    "end_to_end_throughput_ops",
    "total_ingest_time_ms",
    "flush_count",
    "flush_total_time_ms",
    "flush_avg_time_ms",
    "flush_max_time_ms",
    "compaction_count",
    "compaction_total_time_ms",
    "compaction_avg_time_ms",
    "compaction_max_time_ms",
    "compaction_time_ratio",
    "rss_bytes",
    "rss_before_drain_wait_bytes",
    "rss_after_drain_wait_bytes",
    "rss_drain_wait_delta_bytes",
    "l1_route_index_measured_bytes",
    "l1_subtree_cache_bytes",
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
    "delta_prefix_count",
    "delta_ops_count",
    "effective_delta_prefix_count",
    "effective_delta_ops_count",
    "index_update_total_ms",
    "index_update_cow_ms",
    "index_update_bulk_ms",
    "cow_prefix_count",
    "bulk_prefix_count",
    "leaf_stream_merge_ms",
    "rebuild_fallback_count",
    "tiny_descriptor_count",
    "normal_pack_count",
    "tiny_hit_ratio",
    "dirty_pack_pages",
    "pack_write_bytes",
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
    parser.add_argument("--write-ops", type=int, default=1_000_000)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--flush-threads", type=int, default=0)
    parser.add_argument("--compaction-threads", type=int, default=0)
    parser.add_argument("--distribution", default="uniform")
    parser.add_argument("--variant", default="direction_b_full")
    parser.add_argument("--build-mode", default="online")
    # Official write benchmark path follows FlowKV native style:
    # foreground writers run continuously while background trigger handles maintenance.
    parser.add_argument("--maintenance-mode", default="background")
    parser.add_argument("--pool-size-bytes", type=int, default=64 << 30)
    parser.add_argument("--flush-batch", type=int, default=250_000)
    parser.add_argument("--l0-compaction-trigger", type=int, default=4)
    parser.add_argument("--l0-write-stall-threshold", type=int, default=31)
    parser.add_argument("--prefix-ratios", default="0.1,0.05,0.01")
    parser.add_argument("--use-direct-io", type=int, default=1)
    parser.add_argument("--keep-db-files", type=int, default=0)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    benchmark = (
        pathlib.Path(args.build_dir)
        / "experiments"
        / "performance_evaluation"
        / "03_compaction_update"
        / "write_online_benchmark"
    )
    if not benchmark.exists():
        raise SystemExit(f"benchmark not found: {benchmark}")

    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
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
        prefix_count = max(1, int(args.write_ops * ratio))
        db_dir = (
            pathlib.Path(args.db_root)
            / f"{run_id}_{args.variant}_{args.distribution}_{args.write_ops}_{prefix_count}_online"
        )
        db_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(benchmark),
            f"--variant={args.variant}",
            f"--build-mode={args.build_mode}",
            f"--maintenance-mode={args.maintenance_mode}",
            f"--db-dir={db_dir}",
            f"--distribution={args.distribution}",
            f"--write-ops={args.write_ops}",
            f"--threads={args.threads}",
            f"--flush-threads={args.flush_threads}",
            f"--compaction-threads={args.compaction_threads}",
            f"--prefix-count={prefix_count}",
            f"--flush-batch={args.flush_batch}",
            f"--l0-compaction-trigger={args.l0_compaction_trigger}",
            f"--l0-write-stall-threshold={args.l0_write_stall_threshold}",
            f"--pool-size-bytes={args.pool_size_bytes}",
            f"--use-direct-io={args.use_direct_io}",
            f"--keep-db-files={args.keep_db_files}",
        ]
        try:
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            fail_path = raw_dir / (
                f"write_online_{args.write_ops}_{prefix_count}_{args.distribution}.failed.txt"
            )
            fail_path.write_text(
                "CMD:\n" + " ".join(cmd) + "\n\nSTDOUT:\n" + (exc.stdout or "") + "\n\nSTDERR:\n" + (exc.stderr or "")
            )
            raise SystemExit(
                f"benchmark failed for prefix_count={prefix_count}, see {fail_path}"
            ) from exc

        raw_path = raw_dir / f"write_online_{args.write_ops}_{prefix_count}_{args.distribution}.txt"
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
        "maintenance_mode": args.maintenance_mode,
        "write_ops": args.write_ops,
        "threads": args.threads,
        "flush_threads": args.flush_threads,
        "compaction_threads": args.compaction_threads,
        "pool_size_bytes": args.pool_size_bytes,
        "flush_batch": args.flush_batch,
        "l0_compaction_trigger": args.l0_compaction_trigger,
        "l0_write_stall_threshold": args.l0_write_stall_threshold,
        "prefix_ratios": ratios,
        "db_root": args.db_root,
        "run_id": run_id,
        "use_direct_io": args.use_direct_io,
        "keep_db_files": args.keep_db_files,
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    report_script = pathlib.Path(__file__).resolve().parent / "generate_write_online_report.py"
    subprocess.run(
        [
            sys.executable,
            str(report_script),
            "--results-dir",
            str(run_dir),
        ],
        check=True,
    )

    print(run_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
