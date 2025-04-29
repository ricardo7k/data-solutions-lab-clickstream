# Running the Pipeline Locally (for Testing)

You can execute the Apache Beam pipeline on your local machine using the `DirectRunner`. This is useful for rapid development, debugging, and testing pipeline logic without deploying to Dataflow. However, it still requires access to Google Cloud resources (GCS for input/temp, BigQuery for output) from your machine.

## Prerequisites

*   **Python:** A version compatible with the Apache Beam SDK (e.g., Python 3.11, matching the `Dockerfile`).
*   **Google Cloud SDK:** Installed. You need to be authenticated with credentials that have permissions to read/write the necessary GCS buckets and BigQuery dataset/tables.
*   **Application Default Credentials (ADC):** Set up by running `gcloud auth application-default login`. This allows the Beam pipeline to authenticate to GCP services when run locally.
*   **Python Libraries:** Apache Beam SDK with GCP support.

## Environment Setup

1.  **Create and Activate a Virtual Environment (Recommended):**
    ```bash
    # Ensure you are using the correct Python version
    python3.11 -m venv .venv
    source .venv/bin/activate
    # On Windows use: .\venv\Scripts\activate
    ```
    *(Adjust `python3.11` if needed for your system)*

2.  **Install Dependencies:**
    ```bash
    # Upgrade pip and core tools
    pip install --upgrade pip wheel setuptools build pyyaml jsonschema

    # Install Apache Beam with GCP extras
    pip install 'apache-beam[gcp]'

    # If you have a requirements.txt for other dependencies:
    # pip install -r requirements.txt
    ```

3.  **Authenticate with Google Cloud (if not already done):**
    Ensure your local environment can authenticate to GCP services.
    ```bash
    gcloud auth application-default login
    ```
    Follow the prompts to log in via your browser. This stores credentials that the Beam SDK will automatically pick up.

## Executing the Pipeline Locally

Run the main Python script (`website_analytics_pipeline.py`), providing the necessary arguments via the command line. Crucially, specify `DirectRunner`.

```bash
# Set environment variables for your resources (optional, but keeps the command cleaner)
export GCP_PROJECT="your-gcp-project-id"
export INPUT_PATTERN="gs://your-input-bucket/path/to/sample-*.jsonl" # Use a small sample for local testing
export TEMP_GCS_BUCKET="gs://your-temp-bucket/temp/local-run"
export BQ_DATASET="your_bigquery_dataset"
export BQ_VISITS_TABLE="visits_local_test" # Use distinct table names for testing
export BQ_EVENTS_TABLE="events_local_test"
export BQ_PURCHASE_ITEMS_TABLE="purchase_items_local_test"
export WRITE_MODE="WRITE_TRUNCATE" # Usually TRUNCATE for local tests

# Execute the script using DirectRunner
echo "Running pipeline locally..."
python3.11 website_analytics_pipeline.py \
  --runner=DirectRunner \
  --project=$GCP_PROJECT \
  --input=$INPUT_PATTERN \
  --output_dataset=$BQ_DATASET \
  --visits_table=$BQ_VISITS_TABLE \
  --events_table=$BQ_EVENTS_TABLE \
  --purchase_items_table=$BQ_PURCHASE_ITEMS_TABLE \
  --temp_location=$TEMP_GCS_BUCKET \
  --write_disposition=$WRITE_MODE
  # Add --region if needed by specific I/O connectors, though often optional for DirectRunner
  # Add any other custom arguments your pipeline defines

  echo "Local execution finished."
```
*   **Replace:** Update the placeholder values (`your-gcp-project-id`, `gs://your-input-bucket/...`, etc.) with your actual GCP resource names.
*   **`--runner=DirectRunner`:** This is the key flag that tells Beam to run locally.
*   **`--project`:** Specifies the GCP project containing your GCS buckets and BigQuery dataset.
*   **`--temp_location`:** Even the `DirectRunner` might stage some temporary files, especially when interacting with GCP services. Provide a GCS path it can write to.
*   **`--write_disposition`:** Set to `WRITE_TRUNCATE` to clear test tables before each run, or `WRITE_APPEND` to add data. Use distinct table names (`_local_test`) to avoid impacting production tables.
*   **Input Data:** It's highly recommended to use a small subset of your actual data (`sample-*.jsonl`) for local testing to keep execution times reasonable.

The pipeline will run entirely on your machine, reading from GCS, performing transformations in memory, and writing results to BigQuery using your ADC. Monitor the console output for progress logs and any errors. Execution time will depend on the amount of data processed and your machine's resources.
