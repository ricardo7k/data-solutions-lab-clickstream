# Real-time Data Simulation and Processing Examples

This section provides tools and examples for simulating real-time website visit data and processing it using various Google Cloud services like Pub/Sub, Cloud Run, and Dataflow.

## Overview

This project demonstrates different approaches to handling streaming data:

1.  **Data Simulation**:
    *   **Pub/Sub Simulator**: Generates synthetic website visit data and publishes it to a Google Cloud Pub/Sub topic.
    *   **GCS Streaming Simulator**: Generates synthetic website visit data and uploads it as JSONL files to Google Cloud Storage at regular intervals, simulating a file-based streaming source.

2.  **Data Consumption/Processing**:
    *   **Pub/Sub Push (Cloud Run)**: A Cloud Run service subscribed to the Pub/Sub topic via a push subscription, receiving messages via HTTP POST requests. **This example demonstrates fetching necessary credentials or configurations securely from Google Secret Manager.**
    *   **Pub/Sub Pull (Local Docker/MIG)**: Examples of consuming messages from a Pub/Sub topic using a pull subscription, suitable for local testing (Docker) or scalable processing (Managed Instance Group - MIG). **The MIG setup assumes the instances fetch necessary credentials or configurations securely from Google Secret Manager.**
    *   **Dataflow Streaming (GCS)**: An Apache Beam pipeline running on Dataflow that monitors a GCS bucket for new files and processes them in near real-time.

## Prerequisites

*   A Google Cloud Project with the following APIs enabled:
    *   Pub/Sub API
    *   Cloud Run API
    *   Cloud Build API
    *   Compute Engine API (for MIG)
    *   Dataflow API
    *   Artifact Registry API (if building container images)
    *   **Secret Manager API**
*   Google Cloud SDK (`gcloud`) installed and authenticated (`gcloud auth login` and `gcloud auth application-default login`).
*   Docker installed (for local testing and building container images).
*   Python 3 installed.
*   Required Python libraries: `google-cloud-pubsub`, `apache-beam[gcp]`, `google-cloud-secret-manager` (install via `pip`).
*   A Google Cloud Storage (GCS) bucket for staging, temporary files, and simulated data uploads.
*   An Artifact Registry repository (if building container images for Cloud Run/MIG).
*   **Secrets stored in Google Secret Manager** (e.g., API keys, database passwords) that the Cloud Run service or MIG instances need to access.
*   Appropriate IAM permissions for the Cloud Run service account and the Compute Engine service account (used by MIG instances) to access the required secrets in Secret Manager (e.g., the `Secret Manager Secret Accessor` role).

## Setup

1.  **Create Pub/Sub Topic:**
    ```bash
    gcloud pubsub topics create website-visits-topic --project=YOUR_PROJECT_ID
    ```
    *(Replace `YOUR_PROJECT_ID` with your actual Google Cloud project ID)*

2.  **Create Pub/Sub Subscriptions (Choose based on your consumer):**

    *   **For Cloud Run (Push):**
        ```bash
        # Note: The push endpoint URL will be available after deploying the Cloud Run service.
        # You might need to create the subscription *after* the initial Cloud Run deployment
        # or update it later.
        gcloud pubsub subscriptions create website-visits-topic-sub-push \
          --topic=website-visits-topic \
          --project=YOUR_PROJECT_ID \
          --ack-deadline=10 \
          --push-endpoint=YOUR_CLOUDRUN_SERVICE_URL
        ```
        *(Replace `YOUR_CLOUDRUN_SERVICE_URL` with the URL provided by Cloud Run)*

    *   **For Local Docker / MIG (Pull):**
        ```bash
        gcloud pubsub subscriptions create website-visits-topic-sub-pull \
          --topic=website-visits-topic \
          --project=YOUR_PROJECT_ID \
          --ack-deadline=60 # Adjust ack deadline as needed
        ```

3.  **(Optional) Create Secrets in Secret Manager:**
    If you haven't already, create the secrets your applications will need.
    ```bash
    # Example: Create a secret
    echo "YOUR_SECRET_VALUE" | gcloud secrets create my-api-key --data-file=- --project=YOUR_PROJECT_ID

    # Grant access to the Cloud Run service account (replace with actual account)
    gcloud secrets add-iam-policy-binding my-api-key \
      --member="serviceAccount:YOUR_CLOUDRUN_SA@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/secretmanager.secretAccessor" \
      --project=YOUR_PROJECT_ID

    # Grant access to the Compute Engine default service account (or specific one used by MIG)
    gcloud secrets add-iam-policy-binding my-api-key \
      --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
      --role="roles/secretmanager.secretAccessor" \
      --project=YOUR_PROJECT_ID
    ```
    *(Replace placeholders with your actual secret names, values, service account emails, and project details)*

## Running the Simulators

### 1. Pub/Sub Data Simulator (`pubsub_publisher.py`)

This script publishes generated visit data directly to your Pub/Sub topic.

*   **Configuration**: Update `PROJECT_ID` and `TOPIC_ID` within the `pubsub_publisher.py` script or provide them as command-line arguments.
*   **Run**:
    ```bash
    python pubsub_publisher.py \
      --project_id=YOUR_PROJECT_ID \
      --topic_id=website-visits-topic \
      --visits_per_minute=30 \
      --duration_minutes=5
    ```

### 2. GCS Streaming Data Simulator (`gcs_generator_uploader.py`)

This script generates visit data and uploads it as batch files (`.jsonl`) to a GCS bucket.

*   **Configuration**: Update script variables or use command-line arguments.
*   **Run**:
    ```bash
    python gcs_generator_uploader.py \
      --bucket_name=YOUR_GCS_BUCKET_NAME \
      --destination_prefix=streaming-data/ \
      --visits_per_file=50 \
      --upload_interval=30 \
      --simulation_duration=10 \
      --project_id=YOUR_PROJECT_ID # Optional: If needed by underlying libraries
    ```
    *(Replace `YOUR_GCS_BUCKET_NAME` with your bucket name)*

## Running the Consumers / Processing Pipelines

### 1. Cloud Run Push Subscriber (`task3/push`)

This requires building a container image and deploying it to Cloud Run. The application code within the container should use the Secret Manager client library to fetch secrets at runtime.

*   **Build**: Navigate to the `task3/push` directory (assuming it contains a `Dockerfile`, `requirements.txt` including `google-cloud-secret-manager`, and the subscriber code).
    ```bash
    # Replace REGION and REPO_NAME accordingly
    export AR_REPO_URL=YOUR_REGION-docker.pkg.dev/YOUR_PROJECT_ID/YOUR_REPO_NAME/cloud-run-subscriber
    gcloud builds submit --tag $AR_REPO_URL .
    ```
*   **Deploy**: Ensure the service account used by Cloud Run has permissions to access the required secrets.
    ```bash
    gcloud run deploy cloudrunsubscriber \
      --image=$AR_REPO_URL \
      --project=YOUR_PROJECT_ID \
      --region=YOUR_REGION \
      --allow-unauthenticated \
      # Specify the service account if not using the default Compute Engine SA
      # --service-account=YOUR_CLOUDRUN_SA@YOUR_PROJECT_ID.iam.gserviceaccount.com
    ```
    *(Remember to create/update the push subscription with the service URL)*

### 2. Local Docker Pull Subscriber (`task3/pull`)

This runs a container locally that pulls messages from the Pub/Sub subscription. For local testing accessing Secret Manager, ensure your Application Default Credentials (ADC) have the necessary permissions.

*   **Build**: Navigate to the `task3/pull` directory (assuming it contains a `Dockerfile`, `requirements.txt` including `google-cloud-secret-manager`, and subscriber code).
    ```bash
    docker build -t pubsub-subscriber-pull .
    ```
*   **Run**: Mount your ADC file.
    ```bash
    # Example assuming credentials.json is generated by gcloud ADC
    # Adjust path and filename as necessary
    docker run \
      -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
      -v ~/.config/gcloud/application_default_credentials.json:/app/credentials.json:ro \
      pubsub-subscriber-pull \
      --project_id=YOUR_PROJECT_ID \
      --subscription_id=website-visits-topic-sub-pull \
      # Add arguments for secret names if needed by the script
      # --secret_name=projects/YOUR_PROJECT_ID/secrets/my-api-key/versions/latest
    ```

### 3. MIG-based Pull Subscriber (`gcloud` commands)

This sets up a Managed Instance Group (MIG) that automatically scales. The application running on the instances (defined in the Instance Template) should use the Secret Manager client library to fetch secrets.

*   **Prerequisite**: Create an Instance Template (e.g., `instance-template-pull-subscriber-xxxx`) that:
    *   Runs your pull subscriber application on startup.
    *   Uses a service account that has permissions to access the required secrets in Secret Manager.
    *   Installs necessary libraries, including `google-cloud-secret-manager`.

*   **Create and Configure MIG**:
    ```bash
    # Create the Instance Template (Example using container deployment)
    gcloud beta compute instance-templates create-with-container YOUR_INSTANCE_TEMPLATE_NAME \
    --project=YOUR_PROJECT_ID \
    --machine-type=e2-medium \
    --network-interface=subnet=YOUR_SUBNET_NAME,no-address \
    --zone=YOUR_REGION \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=YOUR_SERVICE_ACCOUNT_EMAIL \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --container-image=YOUR_CONTAINER_IMAGE_URL \
    --container-restart-policy=always \
    --create-disk=auto-delete=yes,boot=yes,device-name=YOUR_INSTANCE_TEMPLATE_NAME-disk,image=projects/cos-cloud/global/images/cos-stable-117,mode=rw,size=10,type=pd-balanced

    # Create the MIG
    gcloud compute instance-groups managed create instance-group-pull-subscriber \
      --template=YOUR_INSTANCE_TEMPLATE_NAME \
      --size=1 \
      --zone=YOUR_ZONE \
      --project=YOUR_PROJECT_ID

    # Configure Autoscaling based on Pub/Sub backlog
    gcloud compute instance-groups managed set-autoscaling instance-group-pull-subscriber \
      --zone=YOUR_ZONE \
      --project=YOUR_PROJECT_ID \
      --mode=on \
      --min-num-replicas=1 \
      --max-num-replicas=5 \
      --stackdriver-metric-filter="resource.type = pubsub_subscription AND resource.labels.subscription_id = website-visits-topic-sub-pull" \
      --update-stackdriver-metric=pubsub.googleapis.com/subscription/num_undelivered_messages \
      --stackdriver-metric-single-instance-assignment=10.0 # Target 10 messages per instance
    ```
    *(Replace `YOUR_INSTANCE_TEMPLATE_NAME`, `YOUR_ZONE`, and adjust scaling parameters)*

### 4. Dataflow GCS Streaming Pipeline (`task3/streaming/streaming-gcs-pipeline.py`)

This pipeline reads the files uploaded by the GCS simulator. (Note: Secret Manager integration is less common directly in the pipeline definition itself unless needed for external connections within transforms, but possible if required).

*   **Run**: Execute the Python script, specifying the DataflowRunner and necessary parameters.
    ```bash
    python task3/streaming/streaming-gcs-pipeline.py \
      --runner=DataflowRunner \
      --input_gcs_directory=gs://YOUR_GCS_BUCKET_NAME/streaming-data/ \
      --file_pattern='*.jsonl' \
      --poll_interval_seconds=10 \
      --output_dataset=YOUR_BIGQUERY_DATASET \
      --visits_table=visits_streaming \
      --events_table=events_streaming \
      --purchase_items_table=purchase_items_streaming \
      # Add any other required output tables (e.g., page_views_table)
      --project=YOUR_PROJECT_ID \
      --region=YOUR_DATAFLOW_REGION \
      --temp_location=gs://YOUR_GCS_BUCKET_NAME/temp/ \
      --streaming # Ensure streaming mode is enabled
    ```
    *(Replace placeholders with your specific GCS, BigQuery, and project details)*

