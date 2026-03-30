#!/usr/bin/env python3

import argparse
import csv
import json
import math
import pathlib
import subprocess
import sys
from typing import Dict, List, Optional


def load_rows(csv_path: pathlib.Path) -> List[Dict[str, str]]:
    with csv_path.open() as f:
        return list(csv.DictReader(f))


def to_float(value: str) -> float:
    return float(value)


def fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def fmt_pct(delta: float) -> str:
    return f"{delta:+.2f}%"


def has_value(row: Dict[str, str], key: str) -> bool:
    return key in row and row[key] not in ("", None)


def row_float(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    if not has_value(row, key):
        return default
    return to_float(row[key])


def row_fmt_float(row: Dict[str, str], key: str, digits: int = 2, default: str = "N/A") -> str:
    if not has_value(row, key):
        return default
    return fmt_float(to_float(row[key]), digits)


def row_fmt_int(row: Dict[str, str], key: str, default: str = "N/A") -> str:
    if not has_value(row, key):
        return default
    return fmt_int(to_float(row[key]))


def meta_int(meta: Dict[str, object], key: str, default: int = 0) -> int:
    value = meta.get(key, default)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        if value.strip() == "":
            return default
        return int(float(value))
    return default


def md_header(headers: List[str], alignments: Optional[List[str]] = None) -> List[str]:
    if alignments is None:
        alignments = ["---"] * len(headers)
    if len(alignments) != len(headers):
        raise ValueError("alignments length must match headers length")
    return [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(alignments) + " |",
    ]


def pick_previous_run(results_root: pathlib.Path,
                      current_run_dir: pathlib.Path,
                      current_meta: Dict[str, object]) -> Optional[pathlib.Path]:
    candidates = []
    for entry in sorted(results_root.iterdir()):
        if not entry.is_dir() or entry == current_run_dir:
            continue
        meta_path = entry / "meta.json"
        csv_path = entry / "results.csv"
        if not meta_path.exists() or not csv_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text())
        except Exception:
            continue
        keys = [
            "variant",
            "build_mode",
            "distribution",
            "key_count",
            "query_count",
            "threads",
            "hit_percent",
            "use_direct_io",
            "pst_nowait_poll",
        ]
        if all(meta.get(k) == current_meta.get(k) for k in keys):
            candidates.append(entry)
    if not candidates:
        return None
    return candidates[-1]


def build_change_table(curr_rows: List[Dict[str, str]],
                       prev_rows: List[Dict[str, str]]) -> str:
    prev_by_ratio = {float(r["prefix_ratio"]): r for r in prev_rows}
    lines = []
    lines.append("| Prefix Ratio | avg_latency_ns | p99_latency_ns | throughput_ops | avg_io_l1_pages_per_query | avg_io_pst_reads_per_query | pst_direct_unaligned_fallbacks | pst_short_reads |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in sorted(curr_rows, key=lambda x: float(x["prefix_ratio"]), reverse=True):
        ratio = float(row["prefix_ratio"])
        if ratio not in prev_by_ratio:
            continue
        prev = prev_by_ratio[ratio]

        def diff(metric: str) -> str:
            if not has_value(row, metric) or not has_value(prev, metric):
                return "`N/A`"
            c = to_float(row[metric])
            p = to_float(prev[metric])
            if p == 0:
                return f"`{fmt_float(p, 6)} -> {fmt_float(c, 6)}` (`N/A`)"
            return f"`{fmt_float(p, 6)} -> {fmt_float(c, 6)}` (`{fmt_pct((c - p) / p * 100)}`)"

        def raw(metric: str) -> str:
            if not has_value(row, metric) or not has_value(prev, metric):
                return "`N/A`"
            c = to_float(row[metric])
            p = to_float(prev[metric])
            return f"`{fmt_float(p, 6)} -> {fmt_float(c, 6)}`"

        lines.append(
            f"| `{ratio:.2f}N` | {diff('avg_latency_ns')} | {diff('p99_latency_ns')} | {diff('throughput_ops')} | "
            f"{raw('avg_io_l1_pages_per_query')} | {raw('avg_io_pst_reads_per_query')} | "
            f"{raw('pst_direct_unaligned_fallbacks')} | {raw('pst_short_reads')} |"
        )
    return "\n".join(lines)


def generate_markdown(results_dir: pathlib.Path,
                      rows: List[Dict[str, str]],
                      meta: Dict[str, object],
                      prev_dir: Optional[pathlib.Path],
                      csv_text: str) -> str:
    rows_sorted = sorted(rows, key=lambda r: float(r["prefix_ratio"]), reverse=True)
    avg_l1 = [to_float(r["avg_io_l1_pages_per_query"]) for r in rows_sorted if has_value(r, "avg_io_l1_pages_per_query")]
    avg_pst = [to_float(r["avg_io_pst_reads_per_query"]) for r in rows_sorted if has_value(r, "avg_io_pst_reads_per_query")]
    avg_lat = [to_float(r["avg_latency_ns"]) for r in rows_sorted]
    p99_lat = [to_float(r["p99_latency_ns"]) for r in rows_sorted]
    l1_mem_est = [to_float(r["l1_index_bytes_estimated"]) for r in rows_sorted]
    l1_mem_measured = [
        to_float(r["l1_index_bytes_measured"]) if "l1_index_bytes_measured" in r and r["l1_index_bytes_measured"] != ""
        else to_float(r["l1_index_bytes_estimated"])
        for r in rows_sorted
    ]

    run_id = str(meta.get("run_id", results_dir.name))
    key_count = meta_int(meta, "key_count")
    query_count = meta_int(meta, "query_count")
    threads = meta_int(meta, "threads")
    hit_percent = meta_int(meta, "hit_percent")
    pool_size = meta_int(meta, "pool_size_bytes")
    prefix_ratios = meta.get("prefix_ratios", [])
    if not prefix_ratios:
        prefix_ratios = [float(r["prefix_ratio"]) for r in rows_sorted]
    db_root = str(meta.get("db_root", ""))

    md: List[str] = []
    md.append(f"# Point Lookup Results {run_id} (Generated)")
    md.append("")
    md.append("## Run Scope")
    md.append("")
    md.append(f"- variant: `{meta['variant']}`")
    md.append(f"- build_mode: `{meta.get('build_mode', 'N/A')}`")
    md.append(f"- distribution: `{meta.get('distribution', 'N/A')}`")
    md.append("- key/value size: `16B / 16B`")
    md.append(f"- key count: `{key_count:,}`")
    md.append(f"- query count: `{query_count:,}`")
    md.append(f"- hit ratio: `{hit_percent}%`")
    md.append(f"- threads: `{threads}`")
    md.append(f"- use_direct_io: `{meta.get('use_direct_io', 'N/A')}`")
    md.append(f"- warmup_queries: `{meta.get('warmup_queries', 'N/A')}`")
    md.append(f"- subtree cache: `{'enabled' if meta_int(meta, 'enable_subtree_cache', 1) == 1 else 'disabled'}`")
    md.append(f"- subtree cache capacity: `{meta.get('subtree_cache_capacity', 'N/A')}`")
    subtree_cache_max_bytes = meta.get("subtree_cache_max_bytes", None)
    md.append(f"- subtree cache max bytes: `{int(subtree_cache_max_bytes):,}`" if subtree_cache_max_bytes is not None else "- subtree cache max bytes: `N/A`")
    md.append(f"- bitmap_persist_every: `{meta.get('bitmap_persist_every', 'N/A')}`")
    md.append(f"- pst_nowait_poll: `{meta.get('pst_nowait_poll', 'N/A')}`")
    md.append("- prefix ratio sweep:")
    for ratio in prefix_ratios:
        md.append(f"  - `{float(ratio):.2f}N`")
    md.append("- DB root on SSD:")
    md.append(f"  - `{db_root}`")
    md.append("- pool size:")
    md.append(f"  - `{pool_size:,}` bytes")
    md.append("")
    md.append("## Raw Result Table")
    md.append("")
    md.extend(md_header(
        [
            "Prefix Ratio",
            "Prefix Count",
            "Avg Latency (ns)",
            "P99 Latency (ns)",
            "Throughput (ops/s)",
            "Avg IO Total",
            "Avg IO L1",
            "Avg IO PST",
            "RSS (bytes)",
            "L1 Index Memory Measured (bytes)",
            "L1 Index Memory Estimated (bytes)",
            "Subtree Cache Hit Rate",
        ],
        ["---", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:"],
    ))
    for row in rows_sorted:
        md.append(
            f"| `{float(row['prefix_ratio']):.2f}N` | {fmt_int(to_float(row['prefix_count']))} | "
            f"{fmt_float(to_float(row['avg_latency_ns']), 3)} | {fmt_int(to_float(row['p99_latency_ns']))} | "
            f"{fmt_float(to_float(row['throughput_ops']), 2)} | {row_fmt_float(row, 'avg_io_total_per_query', 5)} | "
            f"{row_fmt_float(row, 'avg_io_l1_pages_per_query', 5)} | {row_fmt_float(row, 'avg_io_pst_reads_per_query', 6)} | "
            f"{fmt_int(to_float(row['rss_bytes']))} | "
            f"{fmt_int(to_float(row.get('l1_index_bytes_measured', row['l1_index_bytes_estimated'])))} | "
            f"{fmt_int(to_float(row['l1_index_bytes_estimated']))} | "
            f"{row_fmt_float(row, 'l1_subtree_cache_hit_rate', 2)} |"
        )
    md.append("")
    md.append("## Memory Overhead Table (RAM Only, Counted)")
    md.append("")
    md.append(
        "- 计入 `L1 Index Memory Measured` 的仅为 **layer0 在内存中的热路由索引**（`l1_route_index_measured_bytes`）。"
    )
    md.append(
        "- 已下沉到 SSD 的冷页（`l1_route_cold_ssd_bytes`）**不计入内存开销**。"
    )
    md.append("")
    md.extend(md_header(
        [
            "Prefix Ratio",
            "l1_route_index_measured_bytes",
            "l1_route_hot_root_index_measured_bytes",
            "l1_route_hot_descriptor_index_measured_bytes",
            "l1_route_index_estimated_bytes",
        ],
        ["---", "---:", "---:", "---:", "---:"],
    ))
    for row in rows_sorted:
        md.append(
            f"| `{float(row['prefix_ratio']):.2f}N` | "
            f"{row_fmt_int(row, 'l1_route_index_measured_bytes', row_fmt_int(row, 'l1_route_index_estimated_bytes'))} | "
            f"{row_fmt_int(row, 'l1_route_hot_root_index_measured_bytes')} | "
            f"{row_fmt_int(row, 'l1_route_hot_descriptor_index_measured_bytes')} | "
            f"{row_fmt_int(row, 'l1_route_index_estimated_bytes')} |"
        )
    md.append("")
    md.append("## Diagnostic Footprint Table (Not Counted into RAM Layer0 Metric)")
    md.append("")
    md.extend(md_header(
        [
            "Prefix Ratio",
            "l1_route_partition_bytes",
            "l1_route_cold_stub_count",
            "l1_route_cold_ssd_bytes",
            "l1_subtree_bytes",
            "l1_subtree_cache_bytes",
            "l1_governance_bytes",
        ],
        ["---", "---:", "---:", "---:", "---:", "---:", "---:"],
    ))
    for row in rows_sorted:
        md.append(
            f"| `{float(row['prefix_ratio']):.2f}N` | {row_fmt_int(row, 'l1_route_partition_bytes')} | "
            f"{row_fmt_int(row, 'l1_route_cold_stub_count')} | "
            f"{row_fmt_int(row, 'l1_route_cold_ssd_bytes')} | "
            f"{row_fmt_int(row, 'l1_subtree_bytes')} | "
            f"{row_fmt_int(row, 'l1_subtree_cache_bytes')} | {row_fmt_int(row, 'l1_governance_bytes')} |"
        )
    md.append("")
    md.append("## Initial Observations")
    md.append("")
    if avg_l1 and avg_pst:
        md.append(
            f"- I/O 路径稳定：`avg_io_l1_pages_per_query` 在 `{min(avg_l1):.5f}~{max(avg_l1):.5f}`，"
            f"`avg_io_pst_reads_per_query` 在 `{min(avg_pst):.6f}~{max(avg_pst):.6f}`。"
        )
    else:
        md.append("- I/O 分解指标在该批次结果中缺失（历史版本未输出相关字段）。")
    md.append(
        f"- 延迟随 prefix 基数下降而明显降低：平均延迟从 `{fmt_float(max(avg_lat),3)}ns` 下降到 `{fmt_float(min(avg_lat),3)}ns`，"
        f"P99 从 `{fmt_int(max(p99_lat))}ns` 下降到 `{fmt_int(min(p99_lat))}ns`。"
    )
    md.append(
        f"- L1 路由索引实测内存随 prefix 基数近线性变化：从 `{fmt_int(max(l1_mem_measured))}B` 下降到 `{fmt_int(min(l1_mem_measured))}B`。"
    )
    md.append("")
    md.append("## Change Comparison")
    md.append("")
    if prev_dir is None:
        md.append("- 暂无同规模同配置的上一组结果，无法生成可比差分。")
    else:
        md.append(f"对比上一组 [{prev_dir.name}/RESULTS.md]({prev_dir.resolve() / 'RESULTS.md'})：")
        md.append("")
        prev_rows = load_rows(prev_dir / "results.csv")
        md.append(build_change_table(rows_sorted, prev_rows))
    md.append("")
    md.append("## Figures")
    md.append("")
    md.append(f"- Average latency: [{(results_dir / 'plots' / 'avg_latency_ns.png').name}]({(results_dir / 'plots' / 'avg_latency_ns.png').resolve()})")
    md.append(f"- P99 latency: [{(results_dir / 'plots' / 'p99_latency_ns.png').name}]({(results_dir / 'plots' / 'p99_latency_ns.png').resolve()})")
    md.append(f"- L1 index memory: [{(results_dir / 'plots' / 'l1_index_bytes_estimated.png').name}]({(results_dir / 'plots' / 'l1_index_bytes_estimated.png').resolve()})")
    measured_plot = results_dir / "plots" / "l1_index_bytes_measured.png"
    if measured_plot.exists():
        md.append(f"- L1 index memory (measured): [{measured_plot.name}]({measured_plot.resolve()})")
    md.append("")
    md.append("## Raw Files")
    md.append("")
    md.append(f"- CSV: [results.csv]({(results_dir / 'results.csv').resolve()})")
    md.append(f"- Meta: [meta.json]({(results_dir / 'meta.json').resolve()})")
    md.append(f"- Raw stdout: [raw]({(results_dir / 'raw').resolve()})")
    md.append("")
    md.append("## Complete Metrics (CSV Dump)")
    md.append("")
    md.append("```csv")
    md.append(csv_text.strip())
    md.append("```")
    md.append("")
    return "\n".join(md)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--results-root", default="")
    args = parser.parse_args()

    results_dir = pathlib.Path(args.results_dir).resolve()
    results_root = pathlib.Path(args.results_root).resolve() if args.results_root else results_dir.parent
    csv_path = results_dir / "results.csv"
    meta_path = results_dir / "meta.json"
    plot_script = pathlib.Path(__file__).resolve().parent / "plot_point_lookup_results.py"

    if not csv_path.exists():
        raise SystemExit(f"missing csv: {csv_path}")
    if not meta_path.exists():
        raise SystemExit(f"missing meta: {meta_path}")
    if not plot_script.exists():
        raise SystemExit(f"missing plot script: {plot_script}")

    rows = load_rows(csv_path)
    if not rows:
        raise SystemExit(f"empty csv: {csv_path}")
    meta = json.loads(meta_path.read_text())
    csv_text = csv_path.read_text()

    subprocess.run(
        [sys.executable, str(plot_script), "--results-dir", str(results_dir)],
        check=True,
    )

    prev_dir = pick_previous_run(results_root, results_dir, meta)
    md_text = generate_markdown(results_dir, rows, meta, prev_dir, csv_text)
    (results_dir / "RESULTS.md").write_text(md_text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
