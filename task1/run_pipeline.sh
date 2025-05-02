#!/bin/bash
# Script to execute the Python pipeline using environment variables

set -e

if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
  echo "Error: GOOGLE_CLOUD_PROJECT environment variable not set."
  exit 1
fi
if [ -z "$GCS_BUCKET_INPUT" ]; then
  echo "Error: GCS_BUCKET_INPUT environment variable not set."
  exit 1
fi
if [ -z "$DATAFLOW_SERVICE_ACCOUNT" ]; then
  echo "Error: DATAFLOW_SERVICE_ACCOUNT environment variable not set."
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
 --dataset_id="ecommerce_clickstream" \
 --visits_table="visits" \
 --events_table="events" \
 --purchase_items_table="purchase_items" \
 --staging_location="gs://${GCS_BUCKET_INPUT}-dataflow-temp/staging" \
 --temp_location="gs://${GCS_BUCKET_INPUT}-dataflow-temp/temp" \
 --region="us-east1" \
 --write_disposition="append" \
 --project="${GOOGLE_CLOUD_PROJECT}" \
 --service_account_email="${DATAFLOW_SERVICE_ACCOUNT}"

echo "Pipeline ready to run."