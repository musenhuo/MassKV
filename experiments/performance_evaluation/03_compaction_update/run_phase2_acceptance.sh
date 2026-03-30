#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build_phase2_record_only}"
RUN_ID="${FLOWKV_PHASE2_ACCEPTANCE_RUN_ID:-phase2_acceptance_$(date -u +%Y%m%d_%H%M%S)}"
OUT_DIR="$ROOT_DIR/experiments/performance_evaluation/03_compaction_update/results/$RUN_ID"
LOG_DIR="$OUT_DIR/logs"
TIME_DIR="$OUT_DIR/time"
SUMMARY_CSV="$OUT_DIR/summary.csv"
BENCH_CSV="$OUT_DIR/benchmark_compare.csv"
REPORT_MD="$OUT_DIR/PHASE2_ACCEPTANCE.md"

mkdir -p "$LOG_DIR" "$TIME_DIR"

TARGETS=(
  version_l1_selection_test
  db_l1_route_smoke_test
  db_l1_recovery_smoke_test
  manifest_l1_snapshot_test
  manifest_batch_txn_replay_test
)

echo "[phase2_acceptance] build_dir=$BUILD_DIR"
echo "[phase2_acceptance] out_dir=$OUT_DIR"
cmake --build "$BUILD_DIR" --target "${TARGETS[@]}" -j"${FLOWKV_BUILD_JOBS:-4}"

declare -a CASES=()

run_case() {
  local case_name="$1"
  local cmd="$2"
  local log_file="$LOG_DIR/${case_name}.log"
  local time_file="$TIME_DIR/${case_name}.time"
  local rc_file="$LOG_DIR/${case_name}.rc"

  echo "[phase2_acceptance] running case=$case_name"
  set +e
  /usr/bin/time -f "MAX_RSS_KB=%M\nELAPSED_SEC=%e" -o "$time_file" \
    bash -lc "cd \"$BUILD_DIR/tests\" && $cmd" >"$log_file" 2>&1
  local rc=$?
  set -e
  echo "$rc" > "$rc_file"
  CASES+=("$case_name")
}

run_case "version_l1_selection_test" "./version_l1_selection_test"
run_case "db_l1_route_smoke_test" "./db_l1_route_smoke_test"
run_case "db_l1_recovery_default" "./db_l1_recovery_smoke_test"
run_case "db_l1_recovery_legacy" "FLOWKV_L1_RANGE_SCAN_RECORDS=0 FLOWKV_L1_MANIFEST_TRACK_TABLES=1 ./db_l1_recovery_smoke_test"
run_case "manifest_l1_snapshot_test" "./manifest_l1_snapshot_test"
run_case "manifest_batch_txn_replay_test" "./manifest_batch_txn_replay_test"

echo "case,status,exit_code,max_rss_kb,elapsed_sec,log_file,time_file" > "$SUMMARY_CSV"
overall_failed=0
peak_rss=0

for case_name in "${CASES[@]}"; do
  rc="$(cat "$LOG_DIR/${case_name}.rc")"
  if [[ "$rc" == "0" ]]; then
    status="PASS"
  else
    status="FAIL"
    overall_failed=1
  fi

  max_rss_kb="$(awk -F= '/^MAX_RSS_KB=/{print $2}' "$TIME_DIR/${case_name}.time")"
  elapsed_sec="$(awk -F= '/^ELAPSED_SEC=/{print $2}' "$TIME_DIR/${case_name}.time")"
  max_rss_kb="${max_rss_kb:-0}"
  elapsed_sec="${elapsed_sec:-0}"
  if [[ "$max_rss_kb" =~ ^[0-9]+$ ]] && (( max_rss_kb > peak_rss )); then
    peak_rss="$max_rss_kb"
  fi

  printf "%s,%s,%s,%s,%s,%s,%s\n" \
    "$case_name" "$status" "$rc" "$max_rss_kb" "$elapsed_sec" \
    "$LOG_DIR/${case_name}.log" "$TIME_DIR/${case_name}.time" >> "$SUMMARY_CSV"
done

tableless_hit=0
record_only_hit=0
record_fallback_hit=0
tableless_skip_fix_hit=0
legacy_tableless_hit=0

if [[ "$(cat "$LOG_DIR/version_l1_selection_test.rc")" == "0" ]]; then
  # version_l1_selection_test now includes PickOverlappedL1Records unique-block/window assertions.
  record_only_hit=1
fi
grep -q "recover L1 hybrid state(tableless mode)" "$LOG_DIR/db_l1_recovery_default.log" && tableless_hit=1 || true
grep -q "fallback to table execution" "$LOG_DIR/db_l1_recovery_default.log" && record_fallback_hit=1 || true
grep -q "skip L1 manifest consistency fix in tableless snapshot recovery mode" "$LOG_DIR/db_l1_recovery_default.log" && tableless_skip_fix_hit=1 || true
grep -q "recover L1 hybrid state(tableless mode)" "$LOG_DIR/db_l1_recovery_legacy.log" && legacy_tableless_hit=1 || true

bench_ran=0
bench_default_status="SKIP"
bench_legacy_status="SKIP"
bench_default_throughput=""
bench_legacy_throughput=""
bench_default_rss=""
bench_legacy_rss=""

if [[ "${FLOWKV_PHASE2_ACCEPTANCE_RUN_BENCH:-0}" == "1" ]]; then
  BENCH_TARGET="write_online_benchmark"
  BENCH_BIN="$BUILD_DIR/experiments/performance_evaluation/03_compaction_update/$BENCH_TARGET"
  cmake --build "$BUILD_DIR" --target "$BENCH_TARGET" -j"${FLOWKV_BUILD_JOBS:-4}"

  run_bench_case() {
    local case_name="$1"
    local env_prefix="$2"
    local log_file="$LOG_DIR/${case_name}.log"
    local time_file="$TIME_DIR/${case_name}.time"
    local rc_file="$LOG_DIR/${case_name}.rc"
    local db_dir="$OUT_DIR/dbfiles/$case_name"
    mkdir -p "$OUT_DIR/dbfiles"
    rm -rf "$db_dir"

    set +e
    /usr/bin/time -f "MAX_RSS_KB=%M\nELAPSED_SEC=%e" -o "$time_file" \
      bash -lc "$env_prefix \"$BENCH_BIN\" \
        --variant=direction_b_full \
        --build-mode=online \
        --maintenance-mode=manual \
        --db-dir=\"$db_dir\" \
        --distribution=uniform \
        --write-ops=${FLOWKV_PHASE2_BENCH_WRITE_OPS:-500000} \
        --threads=1 \
        --compaction-threads=${FLOWKV_PHASE2_BENCH_COMPACTION_THREADS:-2} \
        --prefix-count=${FLOWKV_PHASE2_BENCH_PREFIX_COUNT:-5000} \
        --flush-batch=${FLOWKV_PHASE2_BENCH_FLUSH_BATCH:-500000} \
        --l0-compaction-trigger=4 \
        --l0-write-stall-threshold=31 \
        --pool-size-bytes=${FLOWKV_PHASE2_BENCH_POOL_SIZE:-4294967296} \
        --use-direct-io=0 \
        --keep-db-files=0" >"$log_file" 2>&1
    local rc=$?
    set -e
    echo "$rc" > "$rc_file"
  }

  bench_ran=1
  run_bench_case "write_online_phase2_default" ""
  run_bench_case "write_online_phase2_legacy" "FLOWKV_L1_RANGE_SCAN_RECORDS=0 FLOWKV_L1_MANIFEST_TRACK_TABLES=1"

  bench_default_rc="$(cat "$LOG_DIR/write_online_phase2_default.rc")"
  bench_legacy_rc="$(cat "$LOG_DIR/write_online_phase2_legacy.rc")"
  [[ "$bench_default_rc" == "0" ]] && bench_default_status="PASS" || bench_default_status="FAIL"
  [[ "$bench_legacy_rc" == "0" ]] && bench_legacy_status="PASS" || bench_legacy_status="FAIL"

  bench_default_throughput="$(awk -F= '/^foreground_put_throughput_ops=/{print $2}' "$LOG_DIR/write_online_phase2_default.log" | tail -n1)"
  bench_legacy_throughput="$(awk -F= '/^foreground_put_throughput_ops=/{print $2}' "$LOG_DIR/write_online_phase2_legacy.log" | tail -n1)"
  bench_default_rss="$(awk -F= '/^rss_bytes=/{print $2}' "$LOG_DIR/write_online_phase2_default.log" | tail -n1)"
  bench_legacy_rss="$(awk -F= '/^rss_bytes=/{print $2}' "$LOG_DIR/write_online_phase2_legacy.log" | tail -n1)"

  {
    echo "case,status,foreground_put_throughput_ops,rss_bytes,log_file,time_file"
    echo "phase2_default,$bench_default_status,${bench_default_throughput:-},${bench_default_rss:-},$LOG_DIR/write_online_phase2_default.log,$TIME_DIR/write_online_phase2_default.time"
    echo "legacy_fallback,$bench_legacy_status,${bench_legacy_throughput:-},${bench_legacy_rss:-},$LOG_DIR/write_online_phase2_legacy.log,$TIME_DIR/write_online_phase2_legacy.time"
  } > "$BENCH_CSV"
fi

{
  echo "# Phase 2 Acceptance Report"
  echo
  echo "- Run ID: \`$RUN_ID\`"
  echo "- Build Dir: \`$BUILD_DIR\`"
  echo "- Peak RSS (tests, /usr/bin/time): \`${peak_rss} KB\`"
  echo
  echo "## Test Summary"
  echo
  echo "| Case | Status | Exit | Max RSS (KB) | Elapsed (s) |"
  echo "|---|---:|---:|---:|---:|"
  tail -n +2 "$SUMMARY_CSV" | while IFS=, read -r case_name status rc rss elapsed _ _; do
    echo "| \`$case_name\` | $status | $rc | $rss | $elapsed |"
  done
  echo
  echo "## Key Path Assertions (default recovery run)"
  echo
  echo "- tableless recovery hit: \`$tableless_hit\`"
  echo "- record-only compaction hit: \`$record_only_hit\`"
  echo "- record->table fallback hit (expect 0): \`$record_fallback_hit\`"
  echo "- tableless skip consistency-fix log hit: \`$tableless_skip_fix_hit\`"
  echo "- legacy mode still tableless hit (expect 0): \`$legacy_tableless_hit\`"
  echo
  if [[ "$bench_ran" == "1" ]]; then
    echo "## Benchmark Compare (optional)"
    echo
    echo "| Mode | Status | Foreground Throughput (ops/s) | RSS Bytes |"
    echo "|---|---:|---:|---:|"
    echo "| phase2_default | $bench_default_status | ${bench_default_throughput:-N/A} | ${bench_default_rss:-N/A} |"
    echo "| legacy_fallback | $bench_legacy_status | ${bench_legacy_throughput:-N/A} | ${bench_legacy_rss:-N/A} |"
    echo
    echo "- Raw benchmark csv: \`$BENCH_CSV\`"
    echo
  else
    echo "## Benchmark Compare (optional)"
    echo
    echo "- skipped (\`FLOWKV_PHASE2_ACCEPTANCE_RUN_BENCH=1\` to enable)."
    echo
  fi
  echo "## Raw Files"
  echo
  echo "- Summary CSV: \`$SUMMARY_CSV\`"
  echo "- Logs: \`$LOG_DIR\`"
  echo "- Time files: \`$TIME_DIR\`"
} > "$REPORT_MD"

echo "[phase2_acceptance] report=$REPORT_MD"
echo "[phase2_acceptance] summary_csv=$SUMMARY_CSV"

if [[ "$overall_failed" != "0" ]]; then
  echo "[phase2_acceptance] failed: at least one test case returned non-zero"
  exit 1
fi

echo "[phase2_acceptance] all test cases passed"
