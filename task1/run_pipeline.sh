#!/bin/bash
# Script to execute the Python pipeline using environment variables

set -e

if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
  echo "Error: GOOGLE_CLOUD_PROJECT environment variable not set."
  exit 1
fi

echo "Starting pipeline execution..."
echo "Projeto GCP: $GOOGLE_CLOUD_PROJECT"
echo "Bucket de Entrada GCS: gs://${GCS_BUCKET_INPUT}"
echo "Dataflow Worker service account: $DATAFLOW_SERVICE_ACCOUNT" 

python ecommerce_pipeline.py \
 --runner=DataflowRunner \
 --gcs_input_path="gs://${GCS_BUCKET_INPUT}/*.jsonl" \
 --gcs_destination_bucket="${GCS_BUCKET_INPUT}-processed" \
 --dataset_id="${BQ_DATABASE}" \
 --visits_table="${BQ_VISITS}" \
 --events_table="${BQ_EVENTS}" \
 --purchase_items_table="${BQ_PURCHASE_ITEMS}" \
 --staging_location="gs://${GCS_BUCKET_INPUT}-dataflow-temp/staging" \
 --temp_location="gs://${GCS_BUCKET_INPUT}-dataflow-temp/temp" \
 --region="us-central1" \
 --write_disposition="append" \
 --project="${GOOGLE_CLOUD_PROJECT}" \
 --service_account_email="${DATAFLOW_SERVICE_ACCOUNT}"

echo "Pipeline ready to run."