---
# Previous sections (Task 1, Task 2, Task 3) remain above this line
---

## Task 4: Orchestrating with Cloud Composer

This task demonstrates how to use Google Cloud Composer (managed Apache Airflow) to automate the execution of the Dataflow pipeline built in Task 1. The orchestration pipeline (DAG - Directed Acyclic Graph) will be triggered automatically when a new clickstream data file arrives in a specified Google Cloud Storage bucket. After the Dataflow job successfully processes the file, a notification message will be published to a Pub/Sub topic.

### Overview

1.  **Trigger**: A Cloud Storage trigger associated with the Composer environment monitors a specific GCS bucket/prefix. When a new file matching the criteria appears, it triggers an Airflow DAG run.
2.  **Orchestration**: Cloud Composer runs the defined Airflow DAG.
3.  **Processing**: The DAG launches the Dataflow Flex Template job (from Task 1), passing the triggering file's path as a parameter.
4.  **Notification**: Upon successful completion of the Dataflow job, the DAG publishes a message to a designated Pub/Sub topic.
5.  **Subscription**: A simple Pub/Sub subscriber can listen to this topic to receive notifications about processed files.

### Prerequisites

*   All prerequisites from Task 1 (Dataflow pipeline & Flex Template).
*   A Google Cloud Project with the following APIs enabled:
    *   Cloud Composer API
    *   Cloud Storage API
    *   Dataflow API
    *   Pub/Sub API
    *   BigQuery API
    *   Cloud Build API (if Composer environment uses it)
*   Google Cloud SDK (`gcloud`) installed and authenticated.
*   **A Cloud Composer Environment**: Create one in your project if you don't have one. Ensure its service account has necessary permissions (e.g., roles/composer.worker, roles/dataflow.admin, roles/pubsub.publisher, roles/storage.objectAdmin, roles/bigquery.dataEditor, roles/bigquery.jobUser).
    ```bash
    # Example Composer v2 creation command (adjust parameters as needed)
    gcloud composer environments create maestro_clickstream \
        --location YOUR_REGION \
        --image-version=composer-2.12.1-airflow-2.10.5 # Choose appropriate versions
    ```
*   **The Dataflow Flex Template GCS Path**: The path to the template file created in Task 1 (e.g., `gs://YOUR_TEMPLATE_BUCKET/metadata.json`).
*   **A GCS Bucket for Triggering**: This bucket will be monitored for new file uploads. It can be the same bucket used by simulators or a dedicated one (e.g., `gs://YOUR_TRIGGER_BUCKET/`).
*   **A Pub/Sub Topic for Notifications**: This topic will receive success messages after Dataflow processing.
    ```bash
    gcloud pubsub topics create composer-job-notifications --project=YOUR_PROJECT_ID
    ```
*   **(Optional) A Pub/Sub Subscription for Verification**: To easily check the notifications.
    ```bash
    gcloud pubsub subscriptions create composer-job-notifications-sub \
    --topic=composer-job-notifications
    ```

### Setup: Configure GCS Trigger for Composer 

You need to configure your Composer environment to trigger a specific DAG when a file lands in your designated GCS bucket. This is typically done via the Google Cloud Console:

1.  Navigate to your **Composer environment** in the Google Cloud Console.
2.  Go to the **DAGs** tab.
3.  Find the section related to **Triggers** or **Event-driven DAGs**.
4.  Click **Create Trigger** (or similar).
5.  Select **Cloud Storage** as the trigger type.
6.  Configure the trigger:
    *   **Event Type**: `google.cloud.storage.object.v1.finalized` (object creation/overwrite).
    *   **Bucket**: Enter the name of `YOUR_TRIGGER_BUCKET`.
    *   **(Optional) Object Prefix**: If you only want files in a specific folder to trigger the DAG (e.g., `input/`).
    *   **DAG ID**: Enter the ID of the DAG you will create below (e.g., `gcs_to_bigquery_orchestration`).
    *   **Run Configuration**: This allows passing event data (like the bucket and filename) to the DAG run. Composer usually handles this automatically for GCS triggers.
7.  Save the trigger.

### Airflow DAG (`gcs_to_bigquery_orchestration.py`)

```bash
pip install apache-airflow['google']
```

Create a Python file (e.g., `gcs_to_bigquery_orchestration.py`) with the following content. This DAG uses the `DataflowFlexTemplateOperator` to launch the job and `PubSubPublishMessageOperator` to send the notification.





gcloud composer environments create maestro-clickstream-pipe-3 \
    --location us-central1 \
    --image-version=composer-2.12.1-airflow-2.10.5 \
  --service-account=cloudrun-subscriber-sa@data-solutions-lab.iam.gserviceaccount.com

Go to your Cloud Composer environment in the Google Cloud Console.
Navigate to the PyPI Packages tab.
Click Edit.
Add a new entry:
Package Name: apache-airflow-providers-google
Version: Leave blank (to get the latest compatible version) or specify a minimum version if you know you need one (e.g., >=8.0.0). Usually, the latest is fine.
Click Save.

copy gcs_to_bigquery_orchestration.py to the composer DAG foldder

rm -rf .venv
python3.11 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.10.5"
pip install apache-airflow-providers-google apache-airflow-providers-apache-beam
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email ricardo7k@yahoo.com.br \
    --password admin


{
"bucket":"gs://data-solutions-lab-composer",
"name":"visits-2024-07-05.jsonl"
}