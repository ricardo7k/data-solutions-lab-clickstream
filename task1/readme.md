* DataflowRunner to append
```bash
python task1/ecommerce_pipeline.py \
 --runner=DataflowRunner \
 --gcs_input_path="gs://$GCS_BUCKET_INPUT/*.jsonl" \
 --gcs_destination_bucket="$GCS_BUCKET_INPUT-processed" \
 --dataset_id="ecommerce_clickstream" \
 --visits_table="visits" \
 --events_table="events" \
 --purchase_items_table="purchase_items" \
 --staging_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/staging" \
 --temp_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/temp" \
 --region="us-east1" \
 --project="$GOOGLE_CLOUD_PROJECT" \
 --write_disposition="append"
 ```

 * DataflowRunner to truncate
```bash
python task1/ecommerce_pipeline.py \
 ...
 --write_disposition="truncate"
 ```

* Task 3 Scheduler
```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/cloudrun_scheduler_ecommerce_pipeline ./task1/

gcloud --project=dsl-clickstream run jobs create ecommerce-pipeline-append-job \
    --image us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/cloudrun_scheduler_ecommerce_pipeline \
    --region us-central1 \
    --set-env-vars GOOGLE_CLOUD_PROJECT=dsl-clickstream,GCS_BUCKET_INPUT=dls-clickstream-data \
    --service-account cloud-run@dsl-clickstream.iam.gserviceaccount.com \
    --max-retries 3

gcloud --project=dsl-clickstream run jobs update ecommerce-pipeline-append-job \
    --region=us-central1 \                                                                
    --set-env-vars GOOGLE_CLOUD_PROJECT=dsl-clickstream,GCS_BUCKET_INPUT=dls-clickstream-data,DATAFLOW_SERVICE_ACCOUNT=dataflow-worker-sa@dsl-clickstream.iam.gserviceaccount.com \
    --schedule="*/15 * * * *" \
    --location=us-central1 \
    --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/dsl-clickstream/jobs/ecommerce-pipeline-append-job:run" \
    --oauth-service-account-email cloud-run@dsl-clickstream.iam.gserviceaccount.com \
    --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform
```