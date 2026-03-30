#!/usr/bin/env bash
set -euo pipefail

cd /home/zwt/compare_flow_massKV/MassKV

RUN_ID=20260328_200m_memtable40m_single_flush_uniform_p01_periodic_v2
OUT_DIR=experiments/performance_evaluation/03_compaction_update/results/$RUN_ID
RAW_DIR=$OUT_DIR/raw
PLOT_DIR=$OUT_DIR/plots
RAW_FILE=$RAW_DIR/write_online_200000000_2000000_uniform.txt
SAMPLE_CSV=$OUT_DIR/rss_periodic_samples.csv
OVERHEAD_CSV=$OUT_DIR/rss_periodic_overhead.csv
PLOT_PNG=$PLOT_DIR/rss_periodic_overhead_line.png

mkdir -p "$RAW_DIR" "$PLOT_DIR"

BIN=build_mem40m_singleflush/experiments/performance_evaluation/03_compaction_update/write_online_benchmark
DB_DIR=/mnt/nvme0/flowkv_exp/performance_evaluation/03_compaction_update/dbfiles/${RUN_ID}_direction_b_full_uniform_200000000_2000000_online

start_ns=$(date +%s%N)
echo "elapsed_sec,rss_bytes" > "$SAMPLE_CSV"

"$BIN" \
  --variant=direction_b_full \
  --build-mode=online \
  --maintenance-mode=manual \
  --db-dir="$DB_DIR" \
  --distribution=uniform \
  --write-ops=200000000 \
  --threads=1 \
  --compaction-threads=16 \
  --prefix-count=2000000 \
  --flush-batch=200000000 \
  --l0-compaction-trigger=4 \
  --l0-write-stall-threshold=31 \
  --pool-size-bytes=274877906944 \
  --use-direct-io=1 \
  --keep-db-files=0 > "$RAW_FILE" 2>&1 &
pid=$!

while [ -r "/proc/$pid/status" ]; do
  now_ns=$(date +%s%N)
  elapsed_ms=$(( (now_ns - start_ns) / 1000000 ))
  rss_kb=$(awk '/VmRSS:/ {print $2; found=1; exit} END {if (!found) print 0}' "/proc/$pid/status")
  [ -z "$rss_kb" ] && rss_kb=0
  printf "%d.%03d,%d\n" $((elapsed_ms/1000)) $((elapsed_ms%1000)) $((rss_kb*1024)) >> "$SAMPLE_CSV"
  sleep 1
done

wait "$pid"
run_exit=$?

python3 - <<'PY'
import csv
import pathlib
import statistics

run_id = "20260328_200m_memtable40m_single_flush_uniform_p01_periodic_v2"
out_dir = pathlib.Path("experiments/performance_evaluation/03_compaction_update/results") / run_id
sample_csv = out_dir / "rss_periodic_samples.csv"
overhead_csv = out_dir / "rss_periodic_overhead.csv"
plot_png = out_dir / "plots" / "rss_periodic_overhead_line.png"
plot_err = out_dir / "plots" / "rss_periodic_overhead_line.error.txt"

rows = []
with sample_csv.open() as f:
  reader = csv.DictReader(f)
  for r in reader:
    try:
      t = float(r["elapsed_sec"])
      b = int(float(r["rss_bytes"]))
    except Exception:
      continue
    rows.append((t, b))

if not rows:
  raise SystemExit("NO_SAMPLES_COLLECTED")

baseline = rows[0][1]
overheads = []
with overhead_csv.open("w", newline="") as f:
  w = csv.writer(f)
  w.writerow(["elapsed_sec", "rss_bytes", "overhead_bytes", "overhead_mib"])
  for t, b in rows:
    ob = b - baseline
    om = ob / (1024.0 * 1024.0)
    overheads.append(om)
    w.writerow([f"{t:.3f}", b, ob, f"{om:.6f}"])

plot_ok = False
plot_error = ""
try:
  import matplotlib
  matplotlib.use("Agg")
  import matplotlib.pyplot as plt

  xs = [t for t, _ in rows]
  ys = overheads
  plt.figure(figsize=(10, 5.5))
  plt.plot(xs, ys, linewidth=1.6)
  plt.title("200M Periodic RSS Overhead (memtable_40m, single_flush, uniform_p01)")
  plt.xlabel("Elapsed Time (s)")
  plt.ylabel("RSS Overhead (MiB)")
  plt.grid(True, alpha=0.3)
  plt.tight_layout()
  plt.savefig(plot_png, dpi=160)
  plt.close()
  plot_ok = True
except Exception as e:
  plot_error = str(e)
  plot_err.write_text(plot_error)

print(f"SAMPLE_COUNT={len(rows)}")
print(f"OVERHEAD_MIN_MIB={min(overheads):.6f}")
print(f"OVERHEAD_AVG_MIB={statistics.fmean(overheads):.6f}")
print(f"OVERHEAD_MAX_MIB={max(overheads):.6f}")
print(f"PLOT_OK={'yes' if plot_ok else 'no'}")
if not plot_ok:
  print(f"PLOT_ERROR={plot_error}")
PY

echo "RUN_ID=$RUN_ID"
echo "RAW_FILE=$RAW_FILE"
echo "SAMPLE_CSV=$SAMPLE_CSV"
echo "OVERHEAD_CSV=$OVERHEAD_CSV"
echo "PLOT_PNG=$PLOT_PNG"
echo "RUN_EXIT=$run_exit"
if [ "$run_exit" -eq 0 ]; then
  echo "RUN_SUCCEEDED=yes"
else
  echo "RUN_SUCCEEDED=no"
fi

grep -E '^(foreground_put_phase_time_ms|drain_wait_time_ms|total_ingest_time_ms|flush_count|flush_total_time_ms|compaction_count|compaction_total_time_ms|rss_before_drain_wait_bytes|rss_after_drain_wait_bytes|rss_drain_wait_delta_bytes|rss_bytes)=' "$RAW_FILE"
