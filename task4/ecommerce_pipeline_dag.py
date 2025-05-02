from __future__ import annotations

import datetime
import logging
import os
from time import gmtime, strftime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.trigger_rule import TriggerRule

# @TODO: Add Logic to not start other dataflow, when one is running

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
GCS_BUCKET_INPUT = os.environ.get("GCS_BUCKET_INPUT")
GCS_PROCESSED_BUCKET_FINAL = f"{os.environ.get('GCS_BUCKET_INPUT')}-processed"
FLEX_TEMPLATE_GCS_PATH = "gs://dls-clickstream-data-dataflow-temp/template/metadata.json"
FLEX_TEMPLATE_PARAMETERS = {
    "gcs_input_path": f"gs://{os.environ.get('GCS_BUCKET_INPUT')}/*.jsonl",
    "dataset_id": "ecommerce_clickstream",
    "visits_table": "visits",
    "events_table": "events",
    "purchase_items_table": "purchase_items",
    "write_disposition": "append",
    "gcs_destination_bucket": GCS_PROCESSED_BUCKET_FINAL,
    "launched_by": "composer"
}
FLEX_TEMPLATE_ENVIRONMENT = {
    "serviceAccountEmail": "cloud-run@dsl-clickstream.iam.gserviceaccount.com",
    "machineType": "e2-medium",
    "stagingLocation": f"gs://{os.environ.get('GCS_BUCKET_INPUT')}-dataflow-temp/staging",
    "tempLocation": f"gs://{os.environ.get('GCS_BUCKET_INPUT')}-dataflow-temp/temp",
}
PUBSUB_TOPIC_ID = "ecommerce-pipeline-done"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 1, 1),
    "catchup": False,
    "tags": ["ecommerce", "dataflow", "bigquery", "pubsub"],
}

with DAG(
    dag_id="gcs_event_dataflow_pubsub",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ecommerce", "dataflow", "bigquery", "pubsub"],
) as dag:
    timex=strftime("%Y%m%d%H%M%S", gmtime())
    dataflow_job_name = f"flex-template-{timex}-dag-to-bq"
    logging.warning(f"Dataflow job name: {dataflow_job_name}")
    start = EmptyOperator(task_id="start")
    run_dataflow_flex_template = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_flex_template",
        project_id=PROJECT_ID,
        location="us-central1",
        body={
            "launchParameter": {
                "containerSpecGcsPath": FLEX_TEMPLATE_GCS_PATH,
                "jobName": dataflow_job_name,
                "parameters": FLEX_TEMPLATE_PARAMETERS,
                "environment": FLEX_TEMPLATE_ENVIRONMENT
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    move_processed_file = GCSToGCSOperator(
        task_id="move_processed_file_to_archive",
        source_bucket="{{ dag_run.conf.bucket }}",
        source_object="{{ dag_run.conf.name }}",
        destination_bucket=GCS_PROCESSED_BUCKET_FINAL,
        destination_object="{{ dag_run.conf.name }}",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        gcp_conn_id="google_cloud_default",
    )

    delete_source_file = GCSDeleteObjectsOperator( 
        task_id="delete_source_file",
        bucket_name="{{ dag_run.conf.bucket }}", 
        objects=["{{ dag_run.conf.name }}"], 
        trigger_rule=TriggerRule.ALL_SUCCESS,
        gcp_conn_id="google_cloud_default",
    )

    publish_success_message = PubSubPublishMessageOperator(
        task_id="publish_success_message",
        project_id=PROJECT_ID,
        topic=PUBSUB_TOPIC_ID,
        messages=[
            {
                "data": (
                    "File Processing of Bucket completed via Dataflow Flex Template and moved to GS."
                ).encode('utf-8'),
                "attributes": {
                    "source_bucket": "{{ dag_run.conf.bucket }}",
                    "source_object": "{{ dag_run.conf.name }}",
                    "destination_bucket": GCS_PROCESSED_BUCKET_FINAL,
                    "dataflow_job_name": dataflow_job_name
                }
            }
        ],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        gcp_conn_id="google_cloud_default",
    )

    end = EmptyOperator(task_id="end")

    start >> run_dataflow_flex_template >> move_processed_file >> delete_source_file >> publish_success_message >> end
