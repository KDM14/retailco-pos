#!/usr/bin/env bash
set -e

# RetailCo Streaming Pipeline Launcher
# Usage:
#   ./streaming/run_streaming.sh [local|cloud] [PROJECT_ID]

MODE=${1:-"local"}
PROJECT_ID=${2:-"dhanush-week6-2026"}
DATASET="retailco_analytical_marts"
TABLE="streaming_pos_transactions"

echo "=========================================================="
echo " Starting RetailCo POS Streaming Pipeline"
echo " Mode: $MODE | Project: $PROJECT_ID"
echo "=========================================================="

if [ "$MODE" = "local" ]; then
    echo "Running with Apache Beam DirectRunner (Local test)..."
    python3 streaming/beam_pipeline.py \
        --runner=DirectRunner \
        --input-file="data/raw/retailco_raw_sample.csv" \
        --output-table="${PROJECT_ID}:${DATASET}.${TABLE}" \
        --pii-config="streaming/pii_governance.yaml"
else
    echo "Submitting streaming job to Google Cloud Dataflow..."
    SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/retailco-transactions-sub"
    TEMP_LOCATION="gs://${PROJECT_ID}-dataflow-temp/tmp"
    STAGING_LOCATION="gs://${PROJECT_ID}-dataflow-temp/staging"

    python3 streaming/beam_pipeline.py \
        --runner=DataflowRunner \
        --project="$PROJECT_ID" \
        --region="asia-south1" \
        --subscription="$SUBSCRIPTION" \
        --output-table="${PROJECT_ID}:${DATASET}.${TABLE}" \
        --pii-config="streaming/pii_governance.yaml" \
        --temp_location="$TEMP_LOCATION" \
        --staging_location="$STAGING_LOCATION" \
        --enable_streaming_engine \
        --max_num_workers=4
fi
