#!/usr/bin/env python3

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


def load_csv(path: Path):
    with path.open() as f:
        return list(csv.DictReader(f))


def fmt_int(v):
    return f"{int(round(float(v))):,}"


def fmt_float(v, digits=3):
    return f"{float(v):.{digits}f}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    csv_path = results_dir / "results.csv"
    meta_path = results_dir / "meta.json"
    rows = load_csv(csv_path)
    if not rows:
        raise SystemExit("empty csv")
    meta = json.loads(meta_path.read_text())

    plot_script = Path(__file__).resolve().parent / "plot_write_online_results.py"
    subprocess.run(
        [
            sys.executable,
            str(plot_script),
            "--csv",
            str(csv_path),
            "--plot-dir",
            str(results_dir / "plots"),
        ],
        check=True,
    )

    md = []
    md.append(f"# Online Write Results {meta['run_id']} (Generated)")
    md.append("")
    md.append("## Run Scope")
    md.append("")
    md.append(f"- variant: `{meta['variant']}`")
    md.append(f"- build_mode: `{meta['build_mode']}`")
    md.append(f"- distribution: `{meta['distribution']}`")
    md.append(f"- threads: `{fmt_int(meta.get('threads', 1))}`")
    md.append(f"- flush_threads: `{fmt_int(meta.get('flush_threads', 0))}` (0 means engine default)")
    md.append(f"- compaction_threads: `{fmt_int(meta.get('compaction_threads', 0))}` (0 means auto)")
    md.append("- key/value size: `16B / 16B`")
    md.append(f"- write ops: `{fmt_int(meta['write_ops'])}`")
    md.append(f"- flush_batch: `{fmt_int(meta['flush_batch'])}`")
    md.append(f"- l0_compaction_trigger: `{fmt_int(meta['l0_compaction_trigger'])}`")
    md.append(f"- l0_write_stall_threshold: `{fmt_int(meta.get('l0_write_stall_threshold', 0))}`")
    md.append(f"- use_direct_io: `{meta['use_direct_io']}`")
    md.append("- prefix ratio sweep:")
    for ratio in meta["prefix_ratios"]:
        md.append(f"  - `{ratio:.2f}N`")
    md.append("- DB root on SSD:")
    md.append(f"  - `{meta['db_root']}`")
    md.append(f"- pool size: `{fmt_int(meta['pool_size_bytes'])}` bytes")
    md.append("")

    md.append("## Raw Result Table")
    md.append("")
    md.append("| Prefix Ratio | Prefix Count | Avg Put Latency (ns) | P99 Put Latency (ns) | Foreground Throughput (ops/s) | Foreground Put Phase (ms) | Drain Wait (ms) | End-to-End Throughput (ops/s) | Flush Count | Compaction Count | Compaction Total (ms) | Compaction Ratio | RSS Before Drain (bytes) | RSS After Drain (bytes) | RSS Delta After-Before (bytes) | L1 Route Index Measured (bytes) | Index Update Total (ms) |")
    md.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for r in rows:
        fg_thpt = r.get("foreground_put_throughput_ops", r.get("put_path_throughput_ops", 0))
        e2e_thpt = r.get("end_to_end_throughput_ops", r.get("ingest_throughput_ops", 0))
        md.append(
            f"| `{float(r['prefix_ratio']):.2f}N` | {fmt_int(r['prefix_count'])} | "
            f"{fmt_float(r['avg_put_latency_ns'], 3)} | {fmt_int(r['p99_put_latency_ns'])} | "
            f"{fmt_float(fg_thpt, 2)} | {fmt_int(r.get('foreground_put_phase_time_ms', 0))} | "
            f"{fmt_float(r.get('drain_wait_time_ms', 0), 3)} | "
            f"{fmt_float(e2e_thpt, 2)} | {fmt_int(r['flush_count'])} | "
            f"{fmt_int(r['compaction_count'])} | {fmt_float(r['compaction_total_time_ms'], 3)} | "
            f"{fmt_float(r['compaction_time_ratio'], 4)} | "
            f"{fmt_int(r.get('rss_before_drain_wait_bytes', r.get('rss_bytes', 0)))} | "
            f"{fmt_int(r.get('rss_after_drain_wait_bytes', r.get('rss_bytes', 0)))} | "
            f"{fmt_int(r.get('rss_drain_wait_delta_bytes', 0))} | "
            f"{fmt_int(r['l1_route_index_measured_bytes'])} | "
            f"{fmt_float(r.get('index_update_total_ms', 0), 3)} |"
        )
    md.append("")

    md.append("## V7 Index-Update Detail Table")
    md.append("")
    md.append("| Prefix Ratio | effective_delta_prefix_count | effective_delta_ops_count | index_update_total_ms | index_update_cow_ms | index_update_bulk_ms | cow_prefix_count | bulk_prefix_count | tiny_descriptor_count | normal_pack_count | tiny_hit_ratio | dirty_pack_pages | pack_write_bytes | leaf_stream_merge_ms | rebuild_fallback_count |")
    md.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for r in rows:
        md.append(
            f"| `{float(r['prefix_ratio']):.2f}N` | "
            f"{fmt_int(r.get('effective_delta_prefix_count', r.get('delta_prefix_count', 0)))} | "
            f"{fmt_int(r.get('effective_delta_ops_count', r.get('delta_ops_count', 0)))} | "
            f"{fmt_float(r.get('index_update_total_ms', 0), 3)} | "
            f"{fmt_float(r.get('index_update_cow_ms', 0), 3)} | "
            f"{fmt_float(r.get('index_update_bulk_ms', 0), 3)} | "
            f"{fmt_int(r.get('cow_prefix_count', 0))} | "
            f"{fmt_int(r.get('bulk_prefix_count', 0))} | "
            f"{fmt_int(r.get('tiny_descriptor_count', 0))} | "
            f"{fmt_int(r.get('normal_pack_count', 0))} | "
            f"{fmt_float(r.get('tiny_hit_ratio', 0), 4)} | "
            f"{fmt_int(r.get('dirty_pack_pages', 0))} | "
            f"{fmt_int(r.get('pack_write_bytes', 0))} | "
            f"{fmt_float(r.get('leaf_stream_merge_ms', 0), 3)} | "
            f"{fmt_int(r.get('rebuild_fallback_count', 0))} |"
        )
    md.append("")

    md.append("## Compaction Detail Table")
    md.append("")
    md.append("| Prefix Ratio | flush_total_time_ms | flush_avg_time_ms | flush_max_time_ms | compaction_total_time_ms | compaction_avg_time_ms | compaction_max_time_ms |")
    md.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for r in rows:
        md.append(
            f"| `{float(r['prefix_ratio']):.2f}N` | "
            f"{fmt_float(r['flush_total_time_ms'], 3)} | "
            f"{fmt_float(r['flush_avg_time_ms'], 3)} | "
            f"{fmt_float(r['flush_max_time_ms'], 3)} | "
            f"{fmt_float(r['compaction_total_time_ms'], 3)} | "
            f"{fmt_float(r['compaction_avg_time_ms'], 3)} | "
            f"{fmt_float(r['compaction_max_time_ms'], 3)} |"
        )
    md.append("")

    md.append("## Initial Observations")
    md.append("")
    md.append("- 所有核心指标均来自在线运行时实测计时/计数，不使用理论估算。")
    md.append("- `Foreground Throughput` 为纯前台 Put 阶段墙钟吞吐，是本实验的主吞吐口径。")
    md.append("- `Drain Wait` 表示前台写完后等待后台清空（flush/compaction）的时间。")
    md.append("- background 模式下 compaction 细分计数可能低估，因此优先看前台吞吐与 drain wait。")
    md.append("- 通过 `prefix_ratio` 对照可以观察不同前缀基数下 compaction 代价变化。")
    md.append("")

    md.append("## Change Comparison")
    md.append("")
    md.append("- 当前为 03 写性能模块首版结果，无上一版可比对。")
    md.append("")

    md.append("## Figures")
    md.append("")
    md.append(f"- Foreground throughput: [foreground_put_throughput_ops.png]({(results_dir / 'plots' / 'foreground_put_throughput_ops.png').as_posix()})")
    md.append(f"- End-to-end throughput: [end_to_end_throughput_ops.png]({(results_dir / 'plots' / 'end_to_end_throughput_ops.png').as_posix()})")
    md.append(f"- Drain wait time: [drain_wait_time_ms.png]({(results_dir / 'plots' / 'drain_wait_time_ms.png').as_posix()})")
    md.append(f"- Avg put latency: [avg_put_latency_ns.png]({(results_dir / 'plots' / 'avg_put_latency_ns.png').as_posix()})")
    md.append(f"- P99 put latency: [p99_put_latency_ns.png]({(results_dir / 'plots' / 'p99_put_latency_ns.png').as_posix()})")
    md.append(f"- Compaction total time: [compaction_total_time_ms.png]({(results_dir / 'plots' / 'compaction_total_time_ms.png').as_posix()})")
    md.append(f"- Compaction ratio: [compaction_time_ratio.png]({(results_dir / 'plots' / 'compaction_time_ratio.png').as_posix()})")
    md.append(f"- L1 route index measured memory: [l1_route_index_measured_bytes.png]({(results_dir / 'plots' / 'l1_route_index_measured_bytes.png').as_posix()})")
    md.append("")

    md.append("## Raw Files")
    md.append("")
    md.append(f"- CSV: [{csv_path.name}]({csv_path.as_posix()})")
    md.append(f"- Meta: [{meta_path.name}]({meta_path.as_posix()})")
    md.append(f"- Raw stdout: [raw]({(results_dir / 'raw').as_posix()})")
    md.append("")

    md.append("## Complete Metrics (CSV Dump)")
    md.append("")
    md.append("```csv")
    md.append(csv_path.read_text().strip())
    md.append("```")
    md.append("")

    (results_dir / "RESULTS.md").write_text("\n".join(md))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
