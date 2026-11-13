#!/usr/bin/env bash
# End-to-end test on public AIND data (s3://aind-open-data), with the local config (Docker).
# Downloads the processed asset (~6.5 GB); the raw session is streamed.
# Usage: bash tests/test_pipeline_s3.sh [extra nextflow args, e.g. -resume]
# Env: RESULTS_PATH (default tests/results_s3), WORK_PATH (default: a new temp dir, where nextflow runs)
set -e

ECEPHYS_URL="s3://aind-open-data/866061_2026-11-05_11-30-44"
SORTED_URL="s3://aind-open-data/866061_2026-11-05_11-30-44_sorted_2026-11-06_13-45-17"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PIPELINE_PATH="$(dirname "$SCRIPT_DIR")"
RESULTS_PATH="${RESULTS_PATH:-$SCRIPT_DIR/results_s3}"
WORK_PATH="${WORK_PATH:-$(mktemp -d)}"

if [ -f "$PIPELINE_PATH/pipeline/nextflow_local_custom.config" ]; then
    CONFIG_FILE="$PIPELINE_PATH/pipeline/nextflow_local_custom.config"
else
    CONFIG_FILE="$PIPELINE_PATH/pipeline/nextflow_local.config"
fi
echo "Config: $CONFIG_FILE"
echo "Results: $RESULTS_PATH"
echo "Nextflow work dir: $WORK_PATH"

# nextflow writes .nextflow/ and work/ in the directory it is launched from
cd "$WORK_PATH"
RESULTS_PATH="$RESULTS_PATH" NXF_SYNTAX_PARSER=v1 nextflow \
    -C "$CONFIG_FILE" -log "$RESULTS_PATH/nextflow/nextflow.log" \
    run "$PIPELINE_PATH/pipeline/main.nf" \
    --ecephys_url "$ECEPHYS_URL" \
    --sorted_url "$SORTED_URL" \
    --params_file "$SCRIPT_DIR/params_test.json" \
    "$@"

python3 "$SCRIPT_DIR/check_results.py" --results-path "$RESULTS_PATH" --sorted-url "$SORTED_URL"
