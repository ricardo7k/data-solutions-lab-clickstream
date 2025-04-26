# Data Solutions Lab - clickstream

## Website Analytics Pipeline 

This project contains an Apache Beam pipeline written in Python designed to process website visit data stored in JSONL format in Google Cloud Storage (GCS). The pipeline transforms the data and loads it into structured tables in Google BigQuery, suitable for analytical querying.

The pipeline is intended to be built as a Dataflow Flex Template for easy deployment and execution on Google Cloud Dataflow.

## Data Schema

The input data represents website visits, each containing session information and a list of events (Page View, Add Item to Cart, Purchase). The original data has nested structures (the list of events, and the list of items within a purchase event).

The pipeline transforms this data into a flattened structure across three BigQuery tables:

1.  **`visits`**: Contains session-level information.

2.  **`events`**: Contains details for each individual event within a session.

3.  **`purchase_items`**: Contains details for each item purchased within a purchase event.

Logical composite keys are generated during processing to handle potential data inconsistencies (like duplicated session IDs in the source):

* `visits` Logical PK: `session_id`

* `events` Logical Composite PK: `session_id` + `event_unique_id` (generated UUID)

* `purchase_items` Logical Composite PK: `order_id` + `item_index_in_order` (generated index)

## Pipeline Structure

The pipeline is defined in `website_analytics_pipeline.py` and uses the Apache Beam Python SDK. Key components include:

* **BigQuery Schema Definitions**: Python dictionaries defining the schema for the three output tables.

* **`ParseJsonl` (DoFn)**: A transformation to read and parse each line of the input JSONL files. Includes error handling for invalid JSON lines.

* **`TransformVisitData` (DoFn)**: The core transformation that takes a parsed visit object and generates rows for the `visits`, `events`, and `purchase_items` tables. It generates the `event_unique_id` and `item_index_in_order` for the composite keys. Uses tagged outputs to direct rows to different PCollections.

* **`run` Function**: Sets up the pipeline execution. It parses command-line arguments (input GCS path, output BigQuery dataset and table names), configures `PipelineOptions`, determines the Google Cloud project ID, constructs the BigQuery table specifications, and defines the sequence of transformations.

* **Pipeline Definition**: Connects the transformations using the `|` operator, reading from GCS and writing to BigQuery.

## Building and Deploying as a Flex Template

To run this pipeline on Dataflow, it's recommended to build it as a Flex Template.

1.  **Prerequisites:**

    * Google Cloud SDK installed and authenticated.

    * Docker installed.

    * Apache Beam SDK (`apache-beam[gcp]`) installed locally (for building).

    * An Artifact Registry repository configured in your GCP project.

    * A Google Cloud Storage bucket for staging and temporary files.

2.  **Dockerfile:**
    Use the `Dockerfile` in the same directory as `website_analytics_pipeline.py` to containerize your pipeline code. Use an official Apache Beam SDK base image.

    ```dockerfile
    # Use an official Apache Beam SDK image as a base
    # Choose a Python version compatible with your code and Dataflow
    FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
    FROM apache/beam_python3.11_sdk:latest

    COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
    COPY website_analytics_pipeline.py /template/

    # Define the environment variable expected by the Flex Template launcher
    ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/website_analytics_pipeline.py"
    ```

3.  **Flex Template file :**
    Use the `metadata.json` file to describe your template and its parameters.

    ```json
    {
        "name": "Website Analytics Load",
        "description": "Dataflow Flex Template to load website analytics data from GCS to BigQuery.",
        "parameters": [
            {
            "name": "input",
            "label": "Input GCS File Pattern",
            "helpText": "Cloud Storage file pattern of the JSONL input files (e.g., gs://your-bucket/*.jsonl).",
            "regexes": ["gs://.*"]
            },
            {
            "name": "output_dataset",
            "label": "BigQuery Output Dataset ID",
            "helpText": "BigQuery dataset ID to write the output tables to."
            },
            {
            "name": "visits_table",
            "label": "BigQuery Visits Table Name",
            "helpText": "Name of the BigQuery table for visits."
            },
            {
            "name": "events_table",
            "label": "BigQuery Events Table Name",
            "helpText": "Name of the BigQuery table for events."
            },
            {
            "name": "purchase_items_table",
            "label": "BigQuery Purchase Items Table Name",
            "helpText": "Name of the BigQuery table for purchase items."
            }
        ]
        }
    ```

4.  **Build the Docker Image:**
    Use Cloud Build to build the Docker image and push it to your Artifact Registry. Run this command in the directory containing your `Dockerfile` and `website_analytics_pipeline.py`:

    ```bash
    gcloud builds submit --tag <URL-DOCKER-IMAGE> .
    ```
    * Update the tag `<URL-DOCKER-IMAGE>` to match your desired Artifact Registry location and image name.

5.  **Build the Flex Template:**
    Use the `gcloud dataflow flex-template build` command to create the final Flex Template file in GCS.

    ```bash
    gcloud dataflow flex-template build gs://<GS-BUCKET>/metadata.json \
    --image <URL-DOCKER-IMAGE> \
    --sdk-language PYTHON \
    --metadata-file metadata.json
    ```
    * Update the `<URL-DOCKER-IMAGE>` and `<GS-BUCKET>` to your desired GCS location for the output template file.
    * Ensure the `--image` tag matches the image you built.
    * Ensure `--metadata-file` points to your local `metadata.json` file.    

6.  **Launch the Job from Dataflow UI:**
    * Go to the Google Cloud Console -> Dataflow.
    * Click "+ Create Job from Template".
    * Select "Custom template from Cloud Storage".
    * Enter the GCS path to the **output template file** you created in Step 5 (e.g., `gs://your-template-bucket/templates/metadata.json`).
    * The UI should load the parameters defined in your `metadata.json`. Fill them out.
    * Configure other job options (Region, Service Account, Machine Type, etc.).
    * Click "Run Job".

## Running Locally (for Testing)

You can run the pipeline locally for testing purposes. Ensure you have the Google Cloud SDK authenticated (`gcloud auth application-default login`) and the necessary libraries installed (`pip install apache-beam[gcp]`).

```bash python31 website_analytics_pipeline.py \
  --runner=DirectRunner \
  --input="gs://data-solution-lab-data/challenge-lab-data-dar/*.jsonl" \
  --output_dataset=data_solutions_lab \
  --visits_table=visits \
  --events_table=events \
  --purchase_items_table=purchase_items \
  --project=data-solutions-lab \
  --temp_location=gs://data-solution-lab-data/temp
```

### Useful Links
https://cloud.google.com/dataflow/docs/guides/troubleshoot-custom-container?hl=pt-br
