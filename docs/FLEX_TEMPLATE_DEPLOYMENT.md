
---

**File: `docs/FLEX_TEMPLATE_DEPLOYMENT.md`**

```markdown
# Building and Deploying as a Dataflow Flex Template

To run this pipeline robustly and scalably on Google Cloud, building it as a Dataflow Flex Template is recommended. This allows easy execution via the UI, `gcloud`, or orchestration tools like Cloud Composer.

## Prerequisites

*   **Google Cloud SDK:** Installed and authenticated (`gcloud auth login`, `gcloud config set project YOUR_PROJECT_ID`). Ensure Application Default Credentials (ADC) are set up (`gcloud auth application-default login`).
*   **Docker:** Installed and running locally, or access to a Docker daemon.
*   **Apache Beam SDK:** Installed locally (`pip install 'apache-beam[gcp]'`). Used for the template build process, not for local execution.
*   **Artifact Registry:** A Docker repository created in your GCP project (e.g., `gcloud artifacts repositories create my-docker-repo --repository-format=docker --location=us-central1`). Ensure the Cloud Build service account has push permissions.
*   **Google Cloud Storage (GCS):** A bucket for Dataflow temporary files (`--temp_location`) and to store the final Flex Template specification file. Ensure the Dataflow service account has read/write permissions.
*   **Permissions:** Your user account or the service account used for deployment needs permissions for Cloud Build, Artifact Registry, GCS, and Dataflow (e.g., roles like `roles/cloudbuild.builds.editor`, `roles/artifactregistry.writer`, `roles/storage.admin`, `roles/dataflow.developer`). The Dataflow job itself will run using the Dataflow Worker Service Account, which needs access to GCS (input/temp) and BigQuery (output).

## Required Files

Ensure the following files are in the same directory when running build commands:

*   `website_analytics_pipeline.py`: Your pipeline code.
*   `Dockerfile`: Defines the Docker image for the template.
*   `metadata.json`: Describes the template parameters for the UI and API.
*   (Optional) `requirements.txt`: If your pipeline has external Python dependencies.

## Build and Deployment Steps

1.  **Dockerfile:**
    Verify the `Dockerfile` uses a compatible Apache Beam base image, copies necessary files, and sets the entry point environment variable.

    ```dockerfile
    # Use an official Apache Beam SDK base image matching your Python version
    FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
    FROM apache/beam_python3.11_sdk:latest

    # Set the working directory
    WORKDIR /template

    # Copy the template launcher files
    COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

    # Copy pipeline code and any dependency files
    COPY website_analytics_pipeline.py .
    # COPY requirements.txt . # Uncomment if you have requirements.txt

    # (Optional) Install dependencies if requirements.txt exists
    # RUN pip install --no-cache-dir -r requirements.txt # Uncomment if needed

    # Set the environment variable expected by the launcher
    ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/website_analytics_pipeline.py"

    # (Optional) Set other environment variables if needed by your pipeline
    # ENV MY_CONFIG_VAR="some_value"
    ```

2.  **Template Metadata (`metadata.json`):**
    Confirm `metadata.json` accurately describes the template and its runtime parameters.

    ```json
    {
        "name": "Website Analytics Load",
        "description": "Dataflow Flex Template to load website analytics data (JSONL) from GCS to BigQuery.",
        "parameters": [
            {
                "name": "input",
                "label": "Input GCS File Pattern",
                "helpText": "GCS path pattern for input JSONL files. Example: gs://your-bucket/data/*.jsonl",
                "paramType": "GCS_READ_FILE"
            },
            {
                "name": "output_dataset",
                "label": "BigQuery Output Dataset ID",
                "helpText": "BigQuery dataset ID where the output tables will be created/updated.",
                "paramType": "TEXT"
            },
            {
                "name": "visits_table",
                "label": "BigQuery Visits Table Name",
                "helpText": "Name of the output table for session visits.",
                "paramType": "TEXT",
                "defaultValue": "visits"
            },
            {
                "name": "events_table",
                "label": "BigQuery Events Table Name",
                "helpText": "Name of the output table for individual events.",
                "paramType": "TEXT",
                "defaultValue": "events"
            },
            {
                "name": "purchase_items_table",
                "label": "BigQuery Purchase Items Table Name",
                "helpText": "Name of the output table for purchased items.",
                "paramType": "TEXT",
                "defaultValue": "purchase_items"
            },
            {
                "name": "write_disposition",
                "label": "BigQuery Write Disposition",
                "helpText": "How to write to the BigQuery tables (WRITE_TRUNCATE or WRITE_APPEND). Default: WRITE_APPEND.",
                "isOptional": true,
                "paramType": "TEXT",
                "regexes": [
                    "^(WRITE_TRUNCATE|WRITE_APPEND)$"
                ],
                "defaultValue": "WRITE_APPEND"
            },
             {
                "name": "temp_location",
                "label": "Dataflow Temp Location",
                "helpText": "GCS path for Dataflow temporary files. Example: gs://your-bucket/temp/",
                "paramType": "GCS_WRITE_FOLDER"
            }
            // Add other parameters like region, job_name if needed as optional TEXT params
        ]
    }
    ```
    *(Note: Adjusted `write_disposition` values to match Beam constants and added `temp_location` as a required parameter)*

3.  **Build the Docker Image:**
    Use Cloud Build to build the image and push it to Artifact Registry. Run this command from the directory containing the files:

    ```bash
    # Set variables for convenience
    export PROJECT_ID=$(gcloud config get-value project)
    export AR_REPO_NAME="your-artifact-registry-repo-name" # Ex: my-docker-repo
    export AR_REGION="your-artifact-registry-region"     # Ex: us-central1
    export IMAGE_NAME="website-analytics-pipeline"
    export IMAGE_TAG="latest" # Or use a version like "v1.0.0"

    # Construct the full image URL
    export IMAGE_URL="${AR_REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

    # Run the build using Cloud Build
    echo "Building and pushing image: $IMAGE_URL"
    gcloud builds submit . --tag $IMAGE_URL --project $PROJECT_ID
    ```
    *   Replace `your-artifact-registry-repo-name` and `your-artifact-registry-region` with your actual values.

4.  **Build the Flex Template Specification:**
    Use the `gcloud dataflow flex-template build` command to create the template specification file in GCS. This file points to the Docker image and metadata.

    ```bash
    # Define the GCS bucket and path for the template spec file
    export TEMPLATE_BUCKET="your-gcs-bucket-for-templates" # Ex: gs://my-dataflow-templates
    export TEMPLATE_NAME="website-analytics-load"
    export TEMPLATE_PATH="${TEMPLATE_BUCKET}/flex/${TEMPLATE_NAME}.json"

    # Run the template build command
    echo "Building Flex Template spec at: $TEMPLATE_PATH"
    gcloud dataflow flex-template build $TEMPLATE_PATH \
        --image "$IMAGE_URL" \
        --sdk-language "PYTHON" \
        --metadata-file "metadata.json" \
        --project $PROJECT_ID
    ```
    *   Replace `your-gcs-bucket-for-templates` with your GCS bucket name.
    *   Ensure `$IMAGE_URL` points to the image built in the previous step.

5.  **Run the Job from the Template:**

    *   **Via Google Cloud Console UI:**
        *   Navigate to Dataflow -> Jobs -> "+ Create Job from Template".
        *   Select "Custom Template".
        *   In "Template Path", enter the GCS path of the file generated in step 4 (`$TEMPLATE_PATH`).
        *   The UI will load the parameters defined in `metadata.json`. Fill them in (Input GCS File Pattern, BigQuery Output Dataset ID, etc.).
        *   Configure execution options (Region, Job Name, Max Workers, Service Account, Network, etc.).
        *   Click "Run Job".

    *   **Via `gcloud` (Command Line):**
        ```bash
        # Define runtime parameters
        JOB_NAME="website-analytics-$(date +%Y%m%d-%H%M%S)"
        INPUT_PATH="gs://your-input-bucket/data/*.jsonl"
        OUTPUT_DATASET="your_bigquery_dataset"
        TEMP_GCS_PATH="gs://your-temp-bucket/temp/run-$(date +%Y%m%d-%H%M%S)" # Unique temp path recommended
        WRITE_DISP="WRITE_APPEND" # Or WRITE_TRUNCATE
        REGION="us-central1" # Or your desired region

        echo "Launching Dataflow job: $JOB_NAME"
        gcloud dataflow flex-template run "$JOB_NAME" \
            --template-file-gcs-location "$TEMPLATE_PATH" \
            --project "$PROJECT_ID" \
            --region "$REGION" \
            --parameters input="$INPUT_PATH" \
            --parameters output_dataset="$OUTPUT_DATASET" \
            --parameters temp_location="$TEMP_GCS_PATH" \
            --parameters write_disposition="$WRITE_DISP"
            # Add --parameters for visits_table, events_table, purchase_items_table if not using defaults
            # Add --max-workers=N, --service-account-email=..., etc. as needed
        ```
        *   Adjust the parameter values (`INPUT_PATH`, `OUTPUT_DATASET`, etc.) and the region as needed.

## Orchestration with Cloud Composer

While manual or `gcloud` execution is suitable for testing or ad-hoc runs, **Google Cloud Composer** (managed Apache Airflow) is recommended for production orchestration.

You can create an Airflow DAG (Directed Acyclic Graph) using the `DataflowFlexTemplateOperator` to:

*   **Schedule:** Trigger this Dataflow job automatically (e.g., daily at 2 AM).
*   **Trigger:** Start the job based on events (e.g., using `GCSObjectExistenceSensor` to wait for input files).
*   **Coordinate:** Manage dependencies, running this job only after upstream tasks complete, and triggering downstream tasks upon its success.
*   **Monitor & Retry:** Leverage Airflow's UI for monitoring, logging, alerting, and automatic retries on failure.

**Example Airflow Operator Usage:**

```python
from __future__ import annotations

import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowFlexTemplateOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='website_analytics_pipeline_dag',
    schedule=datetime.timedelta(days=1), # Example: Run daily
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=['dataflow', 'analytics'],
) as dag:
    start_flex_template_job = DataflowFlexTemplateOperator(
        task_id='run_website_analytics_load',
        project_id='your-gcp-project-id',
        location='us-central1', # Region where the job should run
        template_gcs_path='gs://your-gcs-bucket-for-templates/flex/website-analytics-load.json',
        parameters={
            'input': 'gs://your-input-bucket/data/{{ ds_nodash }}/*.jsonl', # Example using Airflow macros
            'output_dataset': 'your_bigquery_dataset',
            'temp_location': 'gs://your-temp-bucket/temp/composer/{{ run_id }}',
            'write_disposition': 'WRITE_APPEND',
            # Add other parameters as needed
        },
        # Optional: Specify service account, network, etc.
        # service_account_email='your-dataflow-worker-sa@...',
    )


Integrating this Flex Template into a Cloud Composer DAG provides a robust, automated, and manageable solution for running your data pipeline.

