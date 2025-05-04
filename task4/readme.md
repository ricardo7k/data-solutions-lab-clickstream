# Task 4: Workflow Orchestration with Cloud Composer

This task focuses on automating the batch processing workflow using Google Cloud Composer (Apache Airflow).

## Steps

1.  **Create Composer Environment:**

```bash
gcloud composer environments create maestro-clickstream-pipe-3 \
--location us-central1 \
--image-version=composer-3-airflow-2.10.5-build.0 \
--service-account=$SERVICE_ACCOUNT
```

2.  **Create Composer DAG :**

Prepare the Dataflow Flex template

# Metadata

```bash
gsutil cp task4/metadata.json gs://${GCS_BUCKET_INPUT}-dataflow-temp/template/metadata.json

gcloud dataflow flex-template build "gs://${GCS_BUCKET_INPUT}-dataflow-temp/template/metadata.json" \
--image "us-central1-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/ecommerce-app/composer_subscriber_pull" \
--sdk-language PYTHON \
--metadata-file "task4/metadata.json" \
--project $GOOGLE_CLOUD_PROJECT
```

# Create composer container image with task1/ecommerce_pipeline.py

```bash
cp task1/ecommerce_pipeline.py task4/
cd task4/
gcloud builds submit --tag us-central1-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/ecommerce-app/composer_subscriber_pull

gsutil cp ./task4/ecommerce_pipeline_dag.py gs://<YOUR COMPOSER URL>/dags/ecommerce_pipeline_dag.py
gsutil cp ./task4/ecommerce_pipeline_dag.py gs://us-central1-maestro-clickst-f6942587-bucket/dags/ecommerce_pipeline_dag.py
```

3.  **Prepare Cloud Run Function Trigger:**
    *   Modify the Cloud Run function in the `function_trigger` folder to correctly interact with Cloud Composer. This might involve updating environment variables or request payloads to align with your Composer setup.

4.  **Configure Cloud Storage Trigger:**
    *   Set up a Cloud Storage trigger (including enabling EventArc API, and grant the right permissions) for your cloud functions to start Composer DAG. This ensures the DAG runs automatically whenever a new file is uploaded to the designated Cloud Storage bucket.

# Composer Env vars
Composer local var, GCS_BUCKET_INPUT to your bucket

# Extra
```bash
./task4/composer_run_composer.sh <- Shortcut ;)
```
