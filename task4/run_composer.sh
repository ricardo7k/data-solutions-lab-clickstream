#!/bin/bash

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

# Copy metadata
gsutil cp task4/metadata.json gs://dls-clickstream-data-dataflow-temp/template/metadata.json

# Create flex template
gcloud dataflow flex-template build "gs://dls-clickstream-data-dataflow-temp/template/metadata.json" \
--image "us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/composer_subscriber_pull" \
--sdk-language PYTHON \
--metadata-file "task4//metadata.json" \
--project $GOOGLE_CLOUD_PROJECT

# create composer container image with task1/ecommerce_pipeline.py
cp task1/ecommerce_pipeline.py task4/
cd task4/
gcloud builds submit --tag us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/composer_subscriber_pull

gsutil cp ecommerce_pipeline_dag.py gs://us-central1-maestro-clickst-7c5386de-bucket/dags/ecommerce_pipeline_dag.py

echo "Compposer ready to run."