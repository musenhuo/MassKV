#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_rows(csv_path: Path):
    with csv_path.open() as f:
        return list(csv.DictReader(f))


def make_plot(rows, key, ylabel, output_path: Path):
    xs = [float(r["prefix_ratio"]) for r in rows]
    ys = [float(r[key]) for r in rows]
    plt.figure(figsize=(8, 5))
    plt.plot(xs, ys, marker="o")
    plt.xlabel("Prefix Ratio")
    plt.ylabel(ylabel)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--plot-dir", required=True)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    plot_dir = Path(args.plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    rows = load_rows(csv_path)
    if not rows:
        return 0

    throughput_key = "foreground_put_throughput_ops"
    if throughput_key not in rows[0]:
        throughput_key = "put_path_throughput_ops"
    make_plot(rows, throughput_key, "Foreground Throughput (ops/s)", plot_dir / "foreground_put_throughput_ops.png")

    if "end_to_end_throughput_ops" in rows[0]:
        make_plot(rows, "end_to_end_throughput_ops", "End-to-End Throughput (ops/s)", plot_dir / "end_to_end_throughput_ops.png")
    else:
        make_plot(rows, "ingest_throughput_ops", "End-to-End Throughput (ops/s)", plot_dir / "end_to_end_throughput_ops.png")

    if "drain_wait_time_ms" in rows[0]:
        make_plot(rows, "drain_wait_time_ms", "Drain Wait Time (ms)", plot_dir / "drain_wait_time_ms.png")

    make_plot(rows, "avg_put_latency_ns", "Average Put Latency (ns)", plot_dir / "avg_put_latency_ns.png")
    make_plot(rows, "p99_put_latency_ns", "P99 Put Latency (ns)", plot_dir / "p99_put_latency_ns.png")
    make_plot(rows, "compaction_total_time_ms", "Compaction Total Time (ms)", plot_dir / "compaction_total_time_ms.png")
    make_plot(rows, "compaction_time_ratio", "Compaction Time Ratio", plot_dir / "compaction_time_ratio.png")
    make_plot(rows, "l1_route_index_measured_bytes", "L1 Route Index Measured (bytes)", plot_dir / "l1_route_index_measured_bytes.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
