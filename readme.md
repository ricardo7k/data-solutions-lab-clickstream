# E-commerce Clickstream Data Engineering and Analysis on Google Cloud

## Project Overview
This project focuses on building a comprehensive data engineering solution on Google Cloud Platform (GCP) to process, analyze, and manage e-commerce clickstream data. The primary goals were to migrate historical batch data and establish real-time processing pipelines, making the data accessible and useful for various analytical purposes, including business analytics, web analytics, and machine learning.

The initial data, representing website visits, including user sessions, device types, geolocations, and event sequences (page views, add-to-cart, purchases), was stored as JSON files in Google Cloud Storage (GCS).

## Tasks Completed & Implementation Details

This project involved several key tasks, leveraging various GCP services, as detailed below:

1.  **Batch Data Migration and Processing (Tasks 1 & 2 Combined):**
    *   **Objective:** Migrate historical clickstream data from GCS to BigQuery with a schema optimized for analytical querying, moving away from the complex nested JSON structure.
    *   **Implementation:** An **Apache Beam** batch pipeline was developed (and executed using **Google Cloud Dataflow**) to:
        *   Read raw JSON data from the GCS bucket.
        *   Parse and transform the data, flattening the nested event structure into a more query-friendly relational format in BigQuery.
        *   Load the transformed data into designated BigQuery tables.
    *   **Visualization:** A **Looker Studio** dashboard was created to visualize the batch-processed data, showcasing metrics like:
        *   Visits by Page
        *   Most Popular Items
        *   Sales by Category
        *   Visits by Device Type

2.  **Real-time Data Processing (Task 3):**
    *   **Objective:** Process incoming clickstream data in real-time as it's generated.
    *   **Implementation:**
        *   A **data simulator** was created to publish synthetic visit data as messages to a **Google Cloud Pub/Sub** topic.
        *   **Push Subscription:** A Cloud Run service was set up as a push subscriber to parse incoming Pub/Sub messages and write the data directly to BigQuery. A real-time Looker Studio report was built on this data.
        *   **Pull Subscription:** A **Compute Engine** instance group with autoscaling and health checks was configured to run a pull subscriber application, processing messages from the same Pub/Sub topic and writing to BigQuery.
        *   **Streaming Analytics Pipeline:** An **Apache Beam** streaming pipeline (running on **Dataflow**) was implemented to:
            *   Read from the Pub/Sub topic.
            *   Write raw event data to GCS files at regular intervals.
            *   Parse messages and write structured data to BigQuery.
            *   Calculate page views per minute using windowing functions.
        *   **Monitoring & Alerting:** The streaming pipeline was enhanced to calculate page views per minute and write this metric to **Cloud Logging**. A **Log-based Metric** and a **Monitoring Dashboard** were created. An **Alerting Policy** was configured to send an email notification if the page view rate exceeded a defined threshold (simulating DoS detection).

3.  **Workflow Orchestration (Task 4):**
    *   **Objective:** Automate the batch processing workflow.
    *   **Implementation:** A **Google Cloud Composer** (Apache Airflow) DAG was created to:
        *   Trigger automatically when a new clickstream data file lands in a specific GCS bucket.
        *   Execute the Dataflow batch job (developed in Tasks 1 & 2) to process the file.
        *   Publish a notification message to a Pub/Sub topic upon successful completion of the Dataflow job.
        *   A separate subscriber was set up to receive these notifications (e.g., logging, email).

4.  **Data Governance and Sharing (Task 5):**
    *   **Objective:** Establish a governed way to share the processed data across the organization.
    *   **Implementation:** **Google Cloud Dataplex** was used to build a data mesh architecture:
        *   **Zones:** Configured `raw` (for original GCS JSON data) and `curated` (for processed BigQuery tables) zones.
        *   **Data Cataloging & Metadata:** Automatically cataloged assets. Applied custom **Tags** and **Tag Templates** to curated BigQuery tables for better understanding and discovery.
        *   **Security:** Implemented Dataplex security policies for controlled access to data assets.

## Technologies Used

*   **Data Storage:** Google Cloud Storage (GCS), BigQuery
*   **Data Processing:** Apache Beam (Python SDK), Google Cloud Dataflow
*   **Messaging:** Google Cloud Pub/Sub
*   **Serverless/Compute:** Cloud Functions, Cloud Run, Compute Engine (VMs, Instance Groups)
*   **Orchestration:** Google Cloud Composer (Apache Airflow)
*   **Data Governance:** Google Cloud Dataplex
*   **Monitoring & Logging:** Cloud Monitoring, Cloud Logging
*   **BI & Visualization:** Looker Studio
*   **Development:** Jupyter Notebooks (potentially for Beam development)

## Schema Design

The original nested JSON schema was redesigned for BigQuery to facilitate easier SQL querying. This typically involves flattening the `events` array and potentially creating separate tables for different event types or unnesting structures within BigQuery queries or views.

## Looker Studio Dashboard

[This is an external link to Looker](https://lookerstudio.google.com/reporting/cf13ae3c-22eb-4125-b4da-19f5b649912b)

## Getting Started

To get started with any of the project components, follow the instructions below. More detailed instructions are available in the respective project's README files.

### Configuration
* **Define environment variables:**
```bash
export GOOGLE_CLOUD_PROJECT=`<YOUR PROJECT ID>`
export SERVICE_ACCOUNT=`<YOUR SERVICE ACCOUNT>`
export GCS_BUCKET_INPUT=`<YOUR GCS BUCKET NAME>`
export BQ_DATABASE=`<DEV EMAIL>`

export PUBSUB_TOPIC_ID=`<YOUR PUBSUB TOPIC ID>`
export PUBSUB_SUB_PULL_ID=`<YOUR PUBSUB PULL SUBSCRIPTION ID>`
export PUBSUB_SUB_PUSH_ID=`<YOUR PUBSUB PUSH SUBSCRIPTION ID>`
export PUBSUB_TOPIC_CONF_ID=`<YOUR PUBSUB CONFIRMATION TOPIC ID>`
export PUBSUB_SUB_CONF_ID=`<YOUR PUBSUB CONFIRMATION TOPIC ID>`

export BQ_DATABASE=`<YOUR BIGQUERY DATABASE NAME>`
export BQ_VISITS=`<YOUR BIGQUERY VISITS TABLE NAME>`
export BQ_PURCHASES=`<YOUR BIGQUERY PURCHASES TABLE NAME>`
export BQ_EVENTS=`<YOUR BIGQUERY EVENTS TABLE NAME>`
export BQ_PAGE_VIEWS=`<YOUR BIGQUERY PAGE VIEWS TABLE NAME>`
export DEV_EMAIL=`<YOUR DEV EMAIL>`

gcloud config set project $GOOGLE_CLOUD_PROJECT
gcloud auth application-default login
```

### Prerequisites
* **Install envieronment python 3.11**
```bash
deactivate
rm -rf .venv/
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools
pip install 'apache-beam[gcp]'
pip install google-cloud-bigquery
pip install google-cloud-pubsub
pip install google-cloud-secret-manager
pip install google-cloud-composer
pip install flask
pip install "apache-airflow==2.10.5"
pip install apache-airflow-providers-google apache-airflow-providers-apache-beam
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email $DEV_EMAIL \
    --password admin
```
`Note: If you encounter errors when running DirectRunner:`

```bash
pip cache purge
pip uninstall apache-beam
pip install 'apache-beam[gcp]'
```

* **Create BigQuery database:**
```bash
python utils/create_ecommerce_bq.py --dataset_id=$BQ_DATABASE \
--visits_table=$BQ_VISITS \
--events_table=$BQ_EVENTS \
--purchase_items_table=$BQ_PURCHASES \
--page_views_table=$BQ_PAGE_VIEWS
```

* **Create storage bucket**
```bash
gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT

gsutil -m cp -r 'gs://challenge-lab-data-dar/**' gs://$GCS_BUCKET_INPUT/

gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT-dataflow-temp

gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT-processed

gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT-dataflow-ws

gsutil cp -n /dev/null gs://$GCS_BUCKET_INPUT-dataflow-ws/staging/
gsutil cp -n /dev/null gs://$GCS_BUCKET_INPUT-dataflow-ws/temp/
gsutil cp -n /dev/null gs://$GCS_BUCKET_INPUT-dataflow-ws/template/


gsutil ls -p $GOOGLE_CLOUD_PROJECT | while read bucket; do
  gsutil retention clear $bucket
done
```

* **Create Pub/Sub topic and pull subscription:**
```bash
gcloud pubsub topics create $PUBSUB_TOPIC_ID --project=$GOOGLE_CLOUD_PROJECT
gcloud pubsub topics create $PUBSUB_TOPIC_CONF_ID --project=$GOOGLE_CLOUD_PROJECT

gcloud pubsub subscriptions create $PUBSUB_SUB_PULL_ID \
 --topic=$PUBSUB_TOPIC_ID \
 --project=$GOOGLE_CLOUD_PROJECT \
 --ack-deadline=10
 gcloud pubsub subscriptions create $PUBSUB_SUB_CONF_ID \
 --topic=$PUBSUB_TOPIC_CONF_ID \
 --project=$GOOGLE_CLOUD_PROJECT \
 --ack-deadline=10
```

* **Create Secrets:**
```bash
gcloud secrets delete bigquery_dataset_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_visits_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_events_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_purchase_items_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_page_views_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_service_account --project=$GOOGLE_CLOUD_PROJECT --quiet

echo -n $BQ_DATABASE | gcloud secrets create bigquery_dataset_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n $BQ_VISITS | gcloud secrets create bigquery_visits_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n $BQ_EVENTS | gcloud secrets create bigquery_events_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n $BQ_PURCHASES | gcloud secrets create bigquery_purchase_items_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n $BQ_PAGE_VIEWS | gcloud secrets create bigquery_page_views_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n $SERVICE_ACCOUNT | gcloud secrets create bigquery_service_account --data-file=- --project=$GOOGLE_CLOUD_PROJECT

```

* **Enable services:**
```bash
gcloud services enable \
    compute.googleapis.com \
    dataflow.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    composer.googleapis.com \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    datastudio.googleapis.com \
    pubsub.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com \
    --project=$GOOGLE_CLOUD_PROJECT
```

* **PUBSUB Message Emulator:**
```bash
chmod +x task3/subscribers/cloudrun_push/msg_emulator_local.sh
```

* **Create repository Artifac Registry**
```bash
gcloud artifacts repositories create ecommerce-app \
  --repository-format=docker \
  --location=us-central1 \
  --project=$GOOGLE_CLOUD_PROJECT
```

```bash
gcloud artifacts repositories add-iam-policy-binding ecommerce-app \
  --location=us-central1 \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/artifactregistry.writer \
  --project=$GOOGLE_CLOUD_PROJECT
```

* **SET PERMISSIONS**
```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/bigquery.dataEditor
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/bigquery.jobUser
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/storage.objectViewer
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/storage.bucketViewer
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/composer.user
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/composer.worker
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/logging.logWriter
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/dataflow.worker
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/dataflow.developer
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/pubsub.viewer
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/cloudscheduler.admin



gcloud secrets add-iam-policy-binding bigquery_dataset_id \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
gcloud secrets add-iam-policy-binding bigquery_visits_table_id \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
gcloud secrets add-iam-policy-binding bigquery_events_table_id \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
gcloud secrets add-iam-policy-binding bigquery_purchase_items_table_id \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
gcloud secrets add-iam-policy-binding bigquery_page_views_table_id \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
gcloud secrets add-iam-policy-binding bigquery_service_account \
  --project=$GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/secretmanager.secretAccessor
```

* Some commands to run, initiate, build and etc, are in the readme of the tasks