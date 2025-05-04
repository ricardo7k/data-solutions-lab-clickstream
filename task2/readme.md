# Batch Data Migration and Processing (Tasks 1 & 2 Combined)

* **DataflowRunner to append**
Same code from the `task1/ecommerce_pipeline.py` file

```bash
python task1/ecommerce_pipeline.py \
 --runner=DataflowRunner \
 --gcs_input_path="gs://$GCS_BUCKET_INPUT/*.jsonl" \
 --gcs_destination_bucket="$GCS_BUCKET_INPUT-processed" \
 --dataset_id=$BQ_DATABASE \
 --visits_table=$BQ_VISITS \
 --events_table=$BQ_EVENTS \
 --purchase_items_table=$BQ_PURCHASES \
 --staging_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/staging" \
 --temp_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/temp" \
 --region="us-central1" \
 --project="$GOOGLE_CLOUD_PROJECT" \
 --write_disposition="append"
 ```