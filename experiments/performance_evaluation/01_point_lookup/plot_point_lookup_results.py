#!/usr/bin/env python3

import argparse
import csv
import pathlib

import matplotlib.pyplot as plt


def load_rows(csv_path: pathlib.Path):
    with csv_path.open() as f:
        return list(csv.DictReader(f))


def to_float(row, key):
    return float(row[key])


def make_plot(rows, key, ylabel, out_path):
    rows = sorted(rows, key=lambda r: float(r["prefix_ratio"]), reverse=True)
    labels = [f"{float(r['prefix_ratio']):.2f}N" for r in rows]
    values = [to_float(r, key) for r in rows]

    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(labels, values, color="#2f6db3")
    plt.ylabel(ylabel)
    plt.xlabel("Distinct Prefix Cardinality")
    plt.title(ylabel)
    for bar, value in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{value:.2f}" if value < 10000 else f"{value:.0f}",
                 ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    args = parser.parse_args()

    results_dir = pathlib.Path(args.results_dir)
    csv_path = results_dir / "results.csv"
    plot_dir = results_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(csv_path)
    make_plot(rows, "avg_latency_ns", "Average Latency (ns)", plot_dir / "avg_latency_ns.png")
    make_plot(rows, "p99_latency_ns", "P99 Latency (ns)", plot_dir / "p99_latency_ns.png")
    make_plot(rows, "l1_index_bytes_estimated", "L1 Index Memory (bytes)", plot_dir / "l1_index_bytes_estimated.png")
    if rows and "l1_index_bytes_measured" in rows[0]:
        make_plot(rows, "l1_index_bytes_measured", "L1 Index Memory Measured (bytes)", plot_dir / "l1_index_bytes_measured.png")


if __name__ == "__main__":
    main()
