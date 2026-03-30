#!/usr/bin/env bash
set -euo pipefail

cd /home/zwt/compare_flow_massKV/MassKV

RUN_ID="${RUN_ID:-$(date -u +%Y%m%d_%H%M%S)_200m_phase2_vs_legacy_flush1}"
OUT_DIR="experiments/performance_evaluation/03_compaction_update/results/${RUN_ID}"
RAW_DIR="${OUT_DIR}/raw"
mkdir -p "${RAW_DIR}"

BIN="${BIN:-build_mem40m_singleflush/experiments/performance_evaluation/03_compaction_update/write_online_benchmark}"
DB_ROOT="${DB_ROOT:-/mnt/nvme0/flowkv_exp/performance_evaluation/03_compaction_update/dbfiles}"

WRITE_OPS="${WRITE_OPS:-200000000}"
PREFIX_COUNT="${PREFIX_COUNT:-2000000}"
THREADS="${THREADS:-1}"
FLUSH_THREADS="${FLUSH_THREADS:-1}"
COMPACTION_THREADS="${COMPACTION_THREADS:-16}"
FLUSH_BATCH="${FLUSH_BATCH:-200000000}"
POOL_SIZE_BYTES="${POOL_SIZE_BYTES:-274877906944}"
USE_DIRECT_IO="${USE_DIRECT_IO:-1}"

run_case() {
  local case_name="$1"
  local env_prefix="$2"

  local db_dir="${DB_ROOT}/${RUN_ID}_${case_name}_direction_b_full_uniform_${WRITE_OPS}_${PREFIX_COUNT}_online"
  local raw_file="${RAW_DIR}/${case_name}.txt"
  local time_file="${RAW_DIR}/${case_name}.time"
  local metrics_file="${RAW_DIR}/${case_name}.metrics"

  rm -rf "${db_dir}"

  echo "[phase2_compare] case=${case_name}"
  /usr/bin/time -f "MAX_RSS_KB=%M\nELAPSED_SEC=%e" -o "${time_file}" \
    bash -lc "${env_prefix} \"${BIN}\" \
      --variant=direction_b_full \
      --build-mode=online \
      --maintenance-mode=manual \
      --db-dir=\"${db_dir}\" \
      --distribution=uniform \
      --write-ops=${WRITE_OPS} \
      --threads=${THREADS} \
      --flush-threads=${FLUSH_THREADS} \
      --compaction-threads=${COMPACTION_THREADS} \
      --prefix-count=${PREFIX_COUNT} \
      --flush-batch=${FLUSH_BATCH} \
      --l0-compaction-trigger=4 \
      --l0-write-stall-threshold=31 \
      --pool-size-bytes=${POOL_SIZE_BYTES} \
      --use-direct-io=${USE_DIRECT_IO} \
      --keep-db-files=0" > "${raw_file}" 2>&1

  grep -E '^(threads|flush_threads_effective|compaction_threads_effective|write_ops|prefix_count|flush_batch|foreground_put_throughput_ops|end_to_end_throughput_ops|foreground_put_phase_time_ms|drain_wait_time_ms|total_ingest_time_ms|flush_count|flush_total_time_ms|compaction_count|compaction_total_time_ms|rss_before_drain_wait_bytes|rss_after_drain_wait_bytes|rss_drain_wait_delta_bytes|rss_bytes)=' \
    "${raw_file}" > "${metrics_file}"
}

run_case "phase2_default" ""
run_case "legacy_fallback" "FLOWKV_L1_RANGE_SCAN_RECORDS=0 FLOWKV_L1_MANIFEST_TRACK_TABLES=1"

SUMMARY_CSV="${OUT_DIR}/summary.csv"
{
  echo "case,foreground_put_throughput_ops,end_to_end_throughput_ops,total_ingest_time_ms,flush_total_time_ms,compaction_total_time_ms,rss_after_drain_wait_bytes,max_rss_kb,elapsed_sec"
  for case_name in phase2_default legacy_fallback; do
    metrics_file="${RAW_DIR}/${case_name}.metrics"
    time_file="${RAW_DIR}/${case_name}.time"
    fg="$(awk -F= '/^foreground_put_throughput_ops=/{print $2}' "${metrics_file}" | tail -n1)"
    e2e="$(awk -F= '/^end_to_end_throughput_ops=/{print $2}' "${metrics_file}" | tail -n1)"
    ingest_ms="$(awk -F= '/^total_ingest_time_ms=/{print $2}' "${metrics_file}" | tail -n1)"
    flush_ms="$(awk -F= '/^flush_total_time_ms=/{print $2}' "${metrics_file}" | tail -n1)"
    comp_ms="$(awk -F= '/^compaction_total_time_ms=/{print $2}' "${metrics_file}" | tail -n1)"
    rss_after="$(awk -F= '/^rss_after_drain_wait_bytes=/{print $2}' "${metrics_file}" | tail -n1)"
    max_rss_kb="$(awk -F= '/^MAX_RSS_KB=/{print $2}' "${time_file}" | tail -n1)"
    elapsed_sec="$(awk -F= '/^ELAPSED_SEC=/{print $2}' "${time_file}" | tail -n1)"
    echo "${case_name},${fg},${e2e},${ingest_ms},${flush_ms},${comp_ms},${rss_after},${max_rss_kb},${elapsed_sec}"
  done
} > "${SUMMARY_CSV}"

REPORT_MD="${OUT_DIR}/RESULTS.md"
{
  echo "# 200M Phase2 vs Legacy (flush_threads=1)"
  echo
  echo "- RUN_ID: \`${RUN_ID}\`"
  echo "- write_ops: \`${WRITE_OPS}\`"
  echo "- prefix_count: \`${PREFIX_COUNT}\`"
  echo "- threads: \`${THREADS}\`"
  echo "- flush_threads: \`${FLUSH_THREADS}\`"
  echo "- compaction_threads: \`${COMPACTION_THREADS}\`"
  echo "- flush_batch: \`${FLUSH_BATCH}\`"
  echo "- pool_size_bytes: \`${POOL_SIZE_BYTES}\`"
  echo "- use_direct_io: \`${USE_DIRECT_IO}\`"
  echo
  echo "## Summary"
  echo
  echo "| case | foreground_put_throughput_ops | end_to_end_throughput_ops | total_ingest_time_ms | flush_total_time_ms | compaction_total_time_ms | rss_after_drain_wait_bytes | max_rss_kb | elapsed_sec |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|"
  tail -n +2 "${SUMMARY_CSV}" | while IFS=, read -r case_name fg e2e ingest_ms flush_ms comp_ms rss_after max_rss_kb elapsed_sec; do
    echo "| ${case_name} | ${fg} | ${e2e} | ${ingest_ms} | ${flush_ms} | ${comp_ms} | ${rss_after} | ${max_rss_kb} | ${elapsed_sec} |"
  done
  echo
  echo "## Raw Files"
  echo
  echo "- summary: \`${SUMMARY_CSV}\`"
  echo "- raw dir: \`${RAW_DIR}\`"
} > "${REPORT_MD}"

echo "[phase2_compare] done"
echo "RUN_ID=${RUN_ID}"
echo "OUT_DIR=${OUT_DIR}"
echo "SUMMARY_CSV=${SUMMARY_CSV}"
echo "REPORT_MD=${REPORT_MD}"
