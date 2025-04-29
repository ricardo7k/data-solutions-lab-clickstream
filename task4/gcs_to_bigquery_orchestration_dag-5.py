from __future__ import annotations

import base64
import logging
import pendulum
from time import gmtime, strftime

from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.trigger_rule import TriggerRule

# Configure logging
logging.basicConfig(level=logging.INFO)

# Define your project/environment variables
PROJECT_ID = "data-solutions-lab" # Replace with your Project ID
GCE_REGION = "us-central1"     # Replace with your region (e.g., us-central1, southamerica-east1)
DATAFLOW_FLEX_TEMPLATE_GCS_PATH = "gs://data-solution-lab-data/dataflow_flex_templates/metadata.json" # Path to your Flex Template
PUBSUB_TOPIC = "composer-job-notifications" # Name of the Pub/Sub topic created

try:
    BIGQUERY_OUTPUT_DATASET = Variable.get("bigquery_output_dataset", default_var="data_solutions_lab") # Use your gcloud value as default
    VISITS_TABLE = Variable.get("visits_table", default_var="visits")
    EVENTS_TABLE = Variable.get("events_table", default_var="events")
    PURCHASE_ITEMS_TABLE = Variable.get("purchase_items_table", default_var="purchase_items")
except KeyError as e:
    # Log or raise error if variables do not exist
    logging.error(f"Airflow Variable not found: {e}")
    raise

# Default configuration for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Create the DAG
with DAG(
    dag_id="gcs_event_dataflow_pubsub",
    default_args=default_args,
    description="Triggers Dataflow Flex Template via GCS Event and notifies via Pub/Sub",
    schedule=None, # Set to None as it will be triggered externally by Eventarc
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcp", "dataflow", "pubsub", "eventarc", "storage"],
) as dag:

    # Task to trigger the Dataflow Flex Template job
    # Eventarc passes event information to dag_run.conf
    # We need to extract the file name from conf (the conf structure depends on Eventarc)
    # Example: The GCS event structure via Eventarc may contain information in conf.
    # You will need to verify the exact structure that Eventarc sends to Composer
    # in dag_run.conf to extract the file path/name.
    # Generally, it might look like dag_run.conf['bucket'] and dag_run.conf['name']
    # If the structure is different, adjust the lines below.

    # Example of how to access event data via dag_run.conf
    # assuming the structure is like {'bucket': 'my-bucket', 'name': 'path/to/file.txt'}
    input_bucket = "{{ dag_run.conf['bucket'] if dag_run.conf else '' }}"
    input_file_path = "{{ dag_run.conf['name'] if dag_run.conf else '' }}"
    gcs_input_object = f"gs://{input_bucket}/{input_file_path}"

    # Ensures the job_name is unique for each execution
    timex=strftime("%Y%m%d%H%M%S", gmtime())
    dataflow_job_name = f"flex-template-{timex}-dag-to-bq"

    logging.info({
            "launchParameter": {
                "containerSpecGcsPath": DATAFLOW_FLEX_TEMPLATE_GCS_PATH,
                "jobName": dataflow_job_name,
                 "parameters": {
                    "input": gcs_input_object,
                    "output_dataset": BIGQUERY_OUTPUT_DATASET,
                    "visits_table": VISITS_TABLE,
                    "events_table": EVENTS_TABLE,
                    "purchase_items_table": PURCHASE_ITEMS_TABLE,
                },
                "environment": {
                    "serviceAccountEmail": "{{ var.value.dataflow_service_account }}",
                }
            }
        })

    run_dataflow_flex_template = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_flex_template",
        project_id=PROJECT_ID,
        location=GCE_REGION,
        body={
            # The main body should only contain 'launchParameter'
            "launchParameter": {
                # Path to the template specification file
                "containerSpecGcsPath": DATAFLOW_FLEX_TEMPLATE_GCS_PATH,
                # Dataflow job name
                "jobName": dataflow_job_name,
                # Parameters to be passed to your template
                 "parameters": {
                    # Correct the input parameter name
                    "input": gcs_input_object,
                    "output_dataset": BIGQUERY_OUTPUT_DATASET,
                    "visits_table": VISITS_TABLE,
                    "events_table": EVENTS_TABLE,
                    "purchase_items_table": PURCHASE_ITEMS_TABLE,
                },
                # Dataflow execution environment (optional)
                "environment": {
                    "serviceAccountEmail": "{{ var.value.dataflow_service_account }}",
                    # "machineType": "n1-standard-1",
                    # "diskSizeGb": 50,
                    # "tempLocation": "gs://YOUR_TEMP_BUCKET/temp" # Add if necessary
                }
            }
        },
        gcp_conn_id="google_cloud_default" # Ensure your GCP connection is configured
    )

    # Task to publish message to Pub/Sub after Dataflow completion
    # This task only runs if the previous task (Dataflow) is successful
    publish_completion_message = PubSubPublishMessageOperator(
        task_id="publish_completion_message",
        project_id=PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=[
            {
                # The message data must be bytes (encoded)
                "data": base64.b64encode(f"Dataflow job {dataflow_job_name} finished processing {gcs_input_object} successfully.".encode('utf-8')),
                # Optional: Add attributes to the message
                "attributes": {
                    "status": "completed",
                    "input_file": gcs_input_object,
                    "dataflow_job_id": dataflow_job_name # Use the job name as an identifier
                }
            }
        ],
        gcp_conn_id="google_cloud_default" # Ensure your GCP connection is configured
    )

    # Define task order: Run Dataflow -> Publish to Pub/Sub
    run_dataflow_flex_template >> publish_completion_message