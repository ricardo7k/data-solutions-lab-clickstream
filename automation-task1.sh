#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration Variables ---
# Replace these with your actual project and resource names

PROJECT_ID="data-solutions-lab"
REGION="us-central1" # Your GCP region
BUCKET_NAME="data-solution-lab-data" # Bucket for input data
TEMPLATE_BUCKET="data-solution-lab-data" # Bucket to store the built Flex Template (can be the same as BUCKET_NAME)
TEMPLATE_FOLDER="dataflow_flex_templates" # Folder within the template bucket
IMAGE_REPO="data-solutions-lab" # Artifact Registry repository name
IMAGE_NAME="website_analytics_pipeline" # Name for your Docker image

# Paths relative to the script's directory
DOCKERFILE="./Dockerfile"
PIPELINE_FILE="./website_analytics_pipeline.py"
METADATA_FILE="./metadata.json" # Make sure this is in the same directory

# Full path for the Docker image in Artifact Registry
# Format: LOCATION-docker.pkg.dev/PROJECT_ID/REPOSITORY/IMAGE_NAME:TAG
# Using 'latest' tag for simplicity, consider versioning for production
IMAGE_PATH="${REGION}-docker.pkg.dev/${PROJECT_ID}/${IMAGE_REPO}/${IMAGE_NAME}:latest"

# Full GCS path for the built Flex Template file
TEMPLATE_GCS_PATH="gs://${TEMPLATE_BUCKET}/${TEMPLATE_FOLDER}/metadata.json"

# --- Validate essential files exist ---
echo "Validating essential files..."
if [ ! -f "$DOCKERFILE" ]; then
    echo "Error: Dockerfile not found at $DOCKERFILE"
    exit 1
fi
if [ ! -f "$PIPELINE_FILE" ]; then
    echo "Error: Pipeline file not found at $PIPELINE_FILE"
    exit 1
fi
if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: Metadata file not found at $METADATA_FILE"
    exit 1
fi
echo "Files validated."

# --- Step 1: Build and Push the Docker Image to Artifact Registry ---
echo "Building and pushing Docker image: ${IMAGE_PATH}"
# The '.' at the end specifies the build context (current directory)
gcloud builds submit . \
  --tag "${IMAGE_PATH}" \
  --project "${PROJECT_ID}"

echo "Docker image built and pushed successfully."

# --- Step 2: Build the Flex Template file in GCS ---
echo "Building Flex Template: ${TEMPLATE_GCS_PATH}"
# The --metadata-file points to the local metadata.json
gcloud dataflow flex-template build "${TEMPLATE_GCS_PATH}" \
  --image "${IMAGE_PATH}" \
  --sdk-language PYTHON \
  --metadata-file "${METADATA_FILE}" \
  --project "${PROJECT_ID}"

echo "Flex Template built successfully at ${TEMPLATE_GCS_PATH}"

# --- Step 3: Optional: Launch the Dataflow Job ---
# Uncomment the following lines if you want the script to also launch the job

echo "Launching Dataflow job..."
JOB_NAME="${IMAGE_NAME}-load-$(date '+%Y%m%d-%H%M%S')" # Unique job name
TEMP_LOCATION="gs://${TEMPLATE_BUCKET}/tmp" # GCS temp location
STAGING_LOCATION="gs://${TEMPLATE_BUCKET}/staging" # GCS staging location


echo "Comando gcloud dataflow jobs run \"${JOB_NAME}\" \
  --project=\"${PROJECT_ID}\" \
  --region=\"${REGION}\" \
  --gcs-location=\"${TEMPLATE_GCS_PATH}\" \
  --parameters \
input=\"gs://${BUCKET_NAME}/challenge-lab-data-dar/*.jsonl\",\
output_dataset=\"data-solutions-lab.data_solutions_lab\",\
visits_table=\"visits\",\
events_table=\"events\",\
purchase_items_table=\"purchase_items\",\
tempLocation=\"${TEMP_LOCATION}\",\
stagingLocation=\"${STAGING_LOCATION}\""


gcloud dataflow jobs run "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --gcs-location="${TEMPLATE_GCS_PATH}" \
  --parameters \
input="gs://${BUCKET_NAME}/challenge-lab-data-dar/*.jsonl",\
output_dataset="data-solutions-lab.data_solutions_lab",\
visits_table="visits",\
events_table="events",\
purchase_items_table="purchase_items",\
tempLocation="${TEMP_LOCATION}",\
stagingLocation="${STAGING_LOCATION}"

echo "Dataflow job '${JOB_NAME}' launched successfully."

echo "Automation script finished."