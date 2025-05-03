# Real-time Data Processing (Task 3)

* **Use gcloud builds to send image to Artifact Registry**
```bash
gcloud builds gcloud builds submit --tag (your-artifact-registry-repository)/cloudrun_subscriber_push

gcloud run deploy cloudrun-subscriber-push \
 --image="us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/cloudrun_subscriber_push" \
 --platform="managed" \
 --region="us-central1" \
 --service-account="cloud-run@dsl-clickstream.iam.gserviceaccount.com" \
 --set-env-vars="GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT" \
 --allow-"unauthenticated" \
 --project=$GOOGLE_CLOUD_PROJECT

gcloud pubsub subscriptions create $PUBSUB_SUB_PUSH_ID \
  --topic=$PUBSUB_TOPIC_ID \
  --project=$GOOGLE_CLOUD_PROJECT \
  --push-endpoint=https://cloudrun-subscriber-push-917499180574.us-central1.run.app \
  --ack-deadline=10 
```
* **Create Mig solution**
Create image and send to Artifact Registry
```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/mig_subscriber_pull
```
* Create the healthcheck
```bash
gcloud compute health-checks create http mig-subscriber-health-check \
  --project=$GOOGLE_CLOUD_PROJECT \
  --request-path=/healthz \
  --port=8080 \
  --check-interval=30s \
  --timeout=10s \
  --healthy-threshold=2 \
  --unhealthy-threshold=3 \
  --region=us-central1
```

* **Create Mig solution**

* Create the template for mig
```bash
gcloud compute instance-templates create-with-container mig-subscriber-template \
  --project=$GOOGLE_CLOUD_PROJECT \
  --machine-type=e2-medium \
  --network-interface=network=default,stack-type=IPV4_ONLY,no-address \
  --container-image=us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/mig_subscriber_pull \
  --service-account=cloud-run@dsl-clickstream.iam.gserviceaccount.com \
  --metadata=google-logging-enabled=true \
  --scopes=cloud-platform \
  --tags=pubsub-subscriber \
  --region=us-central1 \
  --container-env=PUBSUB_SUB_PULL_ID=$PUBSUB_SUB_PULL_ID \
  --container-env=GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT \
  --container-env=PUBSUB_TOPIC_ID=$PUBSUB_TOPIC_ID
```

* Create the mig
```bash
gcloud compute instance-groups managed create mig-pubsub-subscriber \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=us-central1 \
  --zones=us-central1-a,us-central1-b,us-central1-c \
  --base-instance-name=subscriber-vm \
  --template=mig-subscriber-template \
  --size=1 \
  --health-check="https://www.googleapis.com/compute/v1/projects/dsl-clickstream/regions/us-central1/healthChecks/mig-subscriber-health-check" \
  --initial-delay=300
```

* Set Mig Autoscaling
```bash
gcloud compute instance-groups managed set-autoscaling mig-pubsub-subscriber \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=us-central1 \
  --max-num-replicas=3 \
  --min-num-replicas=1 \
  --stackdriver-metric-filter=resource.type\ \
=\ pubsub_subscription\ AND\ resource.labels.subscription_id\ =\ \"ecommerce_clickstreamd_pull_sub\" \
  --update-stackdriver-metric=pubsub.googleapis.com/subscription/num_undelivered_messages \
  --stackdriver-metric-single-instance-assignment=10.0
```

* Open Firewall rule
```bash
gcloud compute firewall-rules create allow-health-check \
    --allow tcp:80 \
    --source-ranges 130.211.0.0/22,35.191.0.0/16 \
    --network default
```

* Run streaming dataflow for click_views
```bash
python task3/subscribers/streaming_ingestion_pageviews_pipeline.py \
  --runner=DataflowRunner \
  --pubsub_subscription="projects/$GOOGLE_CLOUD_PROJECT/subscriptions/$PUBSUB_SUB_PULL_ID" \
  --project=${GOOGLE_CLOUD_PROJECT} \
  --region=us-central1 \
  --staging_location="gs://${GCS_BUCKET_INPUT}-ws/staging" \
  --temp_location="gs://${GCS_BUCKET_INPUT}-ws/temp" \
  --gcs_raw_output_path="gs://${GCS_BUCKET_INPUT}" \
  --bq_dataset="ecommerce_clickstream" \
  --bq_page_views_table="page_views" \
  --window_size_minutes=1 \
  --num_gcs_shards=5 \
  --dos=10 \
  --streaming
```