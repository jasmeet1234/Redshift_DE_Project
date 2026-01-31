#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Run everything locally:
#   - docker compose: zookeeper + kafka
#   - create topics
#   - bootstrap duckdb (optional legacy)
#   - run consumer-clickhouse + (optional) consumer-redshift + ui + producer
#
# Producer source priority:
#   1) --source-url <http(s)://...parquet>
#   2) --input <local parquet file/dir>   (or URL if you pass one)
#   3) dataset.source_url in configs/app.yaml (default)
#
# Usage:
#   ./scripts/run_all_local.sh
#   ./scripts/run_all_local.sh --source-url https://.../sample.parquet
#   ./scripts/run_all_local.sh --input /path/to/parquet_or_dir
#
# Optional:
#   ./scripts/run_all_local.sh --with-redshift
#   ./scripts/run_all_local.sh --with-duckdb
#   ./scripts/run_all_local.sh --no-ui
#   ./scripts/run_all_local.sh --no-producer
#   ./scripts/run_all_local.sh --no-consumers
# ------------------------------------------------------------

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

# Ensure repo root is on Python import path (fixes: ModuleNotFoundError: src)
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

INPUT=""
SOURCE_URL=""
WITH_REDSHIFT="false"
WITH_DUCKDB="false"
NO_UI="false"
NO_PRODUCER="false"
NO_CONSUMERS="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT="${2:-}"
      shift 2
      ;;
    --source-url)
      SOURCE_URL="${2:-}"
      shift 2
      ;;
    --with-redshift)
      WITH_REDSHIFT="true"
      shift
      ;;
    --with-duckdb)
      WITH_DUCKDB="true"
      shift
      ;;
    --no-ui)
      NO_UI="true"
      shift
      ;;
    --no-producer)
      NO_PRODUCER="true"
      shift
      ;;
    --no-consumers)
      NO_CONSUMERS="true"
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/run_all_local.sh [--source-url <parquet_http_url>] [--input <parquet_path_or_dir>]
                                  [--with-redshift] [--with-duckdb] [--no-ui] [--no-producer] [--no-consumers]

If neither --source-url nor --input are provided, the producer uses dataset.source_url from configs/app.yaml.
EOF
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

mkdir -p data/tmp logs

echo "[1/5] Starting Kafka stack (docker compose)…"
docker compose up -d

echo "[2/5] Creating Kafka topics…"
bash scripts/create_kafka_topics.sh

if [[ "${WITH_DUCKDB}" == "true" ]]; then
  echo "[3/5] Bootstrapping DuckDB…"
  # Prefer module invocation so imports are consistent
  python -m scripts.bootstrap_duckdb
else
  echo "[3/5] Skipping DuckDB bootstrap (ClickHouse is primary analytics store)…"
fi

PIDS=()

cleanup() {
  echo ""
  echo "Shutting down processes…"
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  echo "Stopping docker compose…"
  docker compose down
}
trap cleanup EXIT

run_bg() {
  local name="$1"
  shift
  echo "Starting: $name"
  ( "$@" ) >"logs/${name}.log" 2>&1 &
  PIDS+=("$!")
  echo "  -> pid ${PIDS[-1]} (logs/${name}.log)"
}

echo "[4/5] Starting app processes…"

if [[ "${NO_CONSUMERS}" == "false" ]]; then
  run_bg "consumer_clickhouse" python -m src.main consumer-clickhouse

  if [[ "${WITH_REDSHIFT}" == "true" ]]; then
    run_bg "consumer_redshift" python -m src.main consumer-redshift
  fi
fi

if [[ "${NO_UI}" == "false" ]]; then
  run_bg "ui" python -m src.main ui
fi

if [[ "${NO_PRODUCER}" == "false" ]]; then
  PRODUCER_ARGS=()
  if [[ -n "${SOURCE_URL}" ]]; then
    PRODUCER_ARGS+=(--source-url "${SOURCE_URL}")
  elif [[ -n "${INPUT}" ]]; then
    PRODUCER_ARGS+=(--input "${INPUT}")
  fi

  run_bg "producer_provisioned" python -m src.main producer --dataset-type provisioned "${PRODUCER_ARGS[@]}"
  run_bg "producer_serverless" python -m src.main producer --dataset-type serverless "${PRODUCER_ARGS[@]}"
fi

echo "[5/5] All services started."
echo ""
echo "Logs:"
echo "  tail -f logs/consumer_clickhouse.log"
echo "  tail -f logs/producer_provisioned.log"
echo "  tail -f logs/producer_serverless.log"
echo "  tail -f logs/ui.log"
if [[ "${WITH_REDSHIFT}" == "true" ]]; then
  echo "  tail -f logs/consumer_redshift.log"
fi
echo ""
echo "UI (default Streamlit): open the URL printed in logs/ui.log"
echo ""
echo "Press Ctrl+C to stop everything."
echo ""

while true; do
  sleep 1
done
