```bash
python task1/ecommerce_pipeline.py \
 --runner=DataflowRunner \
 --gcs_input_path="gs://$GCS_BUCKET_INPUT/*.jsonl" \
 --dataset_id="ecommerce_clickstream" \
 --visits_table="visits" \
 --events_table="events" \
 --purchase_items_table="purchase_items" \
 --staging_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/staging" \
 --temp_location="gs://$GCS_BUCKET_INPUT-dataflow-temp/temp" \
 --region="us-central1" \
 --project="$GOOGLE_CLOUD_PROJECT" \
 --write_disposition="append"
 ```