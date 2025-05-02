import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import json
import logging
import os
from datetime import datetime
import uuid
import argparse
import sys
from apache_beam.runners.runner import PipelineState
from urllib.parse import urlparse
import fnmatch

# Configure logging
logging.basicConfig(level=logging.INFO)

# BigQuery table schemas
VISITS_TABLE_SCHEMA = {
    "fields": [
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING"},
        {"name": "device_type", "type": "STRING"},
        {"name": "geolocation", "type": "STRING"},
        {"name": "user_agent", "type": "STRING"},
        {"name": "start_timestamp", "type": "TIMESTAMP"},
        {"name": "end_timestamp", "type": "TIMESTAMP"},
        {"name": "event_count", "type": "INTEGER"},
    ]
}

EVENTS_TABLE_SCHEMA = {
    "fields": [
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_unique_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referrer_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    ]
}

PURCHASE_ITEMS_TABLE_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "item_index_in_order", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "price", "type": "FLOAT64"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "purchase_event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


# --- Parsing and Extraction Functions ---
class ParseJsonl(beam.DoFn):
    # Transforms JSONL lines into Python dictionaries
    def process(self, element):
        try:
            yield json.loads(element)
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON: {element}. Error: {e}")

class ExtractVisitData(beam.DoFn):
    # Extracts session data for the visits table
    def process(self, session_data):
        try:
            session_id = session_data.get("session_id")
            if not session_id:
                logging.warning(f"Session without session_id, skipping: {session_data}")
                return

            user_id = session_data.get("user_id")
            device_type = session_data.get("device_type")
            geolocation = session_data.get("geolocation")
            user_agent = session_data.get("user_agent")
            events = session_data.get("events", [])

            start_timestamp = None
            end_timestamp = None
            event_count = len(events)

            timestamps = []
            for event_entry in events:
                 event_details = event_entry.get("event", {})
                 timestamp_str = event_details.get("timestamp")
                 if timestamp_str:
                     try:
                         dt_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                         timestamps.append(dt_obj)
                     except ValueError:
                         logging.warning(f"Invalid timestamp format: {timestamp_str} in session {session_id}")

            if timestamps:
                timestamps.sort()
                start_timestamp = timestamps[0].isoformat()
                end_timestamp = timestamps[-1].isoformat()

            visit_record = {
                "session_id": session_id,
                "user_id": user_id,
                "device_type": device_type,
                "geolocation": geolocation,
                "user_agent": user_agent,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "event_count": event_count,
            }
            yield visit_record

        except Exception as e:
            logging.error(f"Error extracting visit data for session {session_data.get('session_id', 'N/A')}: {e}")

class ExtractEventData(beam.DoFn):
    # Extracts individual event data for the events table
    def process(self, session_data):
        session_id = session_data.get("session_id")
        user_id = session_data.get("user_id")
        device_type = session_data.get("device_type")
        events = session_data.get("events", [])

        if not session_id:
             logging.warning(f"Session without session_id, skipping events: {session_data}")
             return

        for event_entry in events:
            event_details = event_entry.get("event", {})
            event_type = event_details.get("event_type")
            timestamp = event_details.get("timestamp")
            details = event_details.get("details", {})

            if not event_type or not timestamp:
                 logging.warning(f"Event without event_type or timestamp in session {session_id}, skipping: {event_entry}")
                 continue

            event_unique_id = str(uuid.uuid4())

            event_record = {
                "session_id": session_id,
                "event_unique_id": event_unique_id,
                "event_type": event_type,
                "timestamp": timestamp,
                "device_type": device_type,
                "user_id": user_id,
                "page_url": details.get("page_url"),
                "referrer_url": details.get("referrer_url"),
                "product_id": details.get("product_id"),
                "product_name": details.get("product_name"),
                "category": details.get("category"),
                "price": details.get("price"),
                "quantity": details.get("quantity"),
                "order_id": details.get("order_id"),
                "amount": details.get("amount"),
                "currency": details.get("currency"),
            }
            yield event_record

class ExtractPurchaseItemData(beam.DoFn):
    # Extracts individual purchase item data for the purchase items table
    def process(self, session_data):
        session_id = session_data.get("session_id")
        events = session_data.get("events", [])

        if not session_id:
            return

        for event_entry in events:
            event_details = event_entry.get("event", {})
            event_type = event_details.get("event_type")
            timestamp = event_details.get("timestamp")
            details = event_details.get("details", {})

            if event_type == "purchase":
                order_id = details.get("order_id")
                items = details.get("items", [])

                if not order_id:
                    logging.warning(f"Purchase event without order_id in session {session_id}, skipping items: {event_entry}")
                    continue

                for item_index, item in enumerate(items):
                    item_record = {
                        "order_id": order_id,
                        "item_index_in_order": item_index,
                        "session_id": session_id,
                        "product_id": item.get("product_id"),
                        "product_name": item.get("product_name"),
                        "category": item.get("category"),
                        "price": item.get("price"),
                        "quantity": item.get("quantity"),
                        "purchase_event_timestamp": timestamp
                    }
                    if item_record.get("product_id") is None:
                         logging.warning(f"Purchase item without product_id in order {order_id}, session {session_id}, skipping: {item}")
                         continue
                    yield item_record

# --- GCS File Movement Function ---
def move_gcs_file(source_path: str, destination_bucket_name: str):
    # Moves source file(s) in GCS after successful processing
    logging.info(f"Initiating move for source file(s): {source_path} to destination bucket: {destination_bucket_name}")

    storage_client = storage.Client()

    try:
        parsed_source = urlparse(source_path)
        if parsed_source.scheme != 'gs':
            logging.error(f"Invalid source path (not gs://): {source_path}")
            return

        source_bucket_name = parsed_source.netloc
        source_blob_prefix_or_pattern = parsed_source.path.lstrip('/')

        source_bucket = storage_client.bucket(source_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        prefix = source_blob_prefix_or_pattern
        pattern = None
        if '*' in source_blob_prefix_or_pattern or '?' in source_blob_prefix_or_pattern:
             prefix = source_blob_prefix_or_pattern.split('*')[0].split('?')[0]
             pattern = source_blob_prefix_or_pattern

        source_blobs = source_bucket.list_blobs(prefix=prefix)

        moved_count = 0
        for blob in source_blobs:
            if pattern and not fnmatch.fnmatch(blob.name, pattern):
                 logging.info(f"Skipping blob that does not match pattern: {blob.name}")
                 continue

            logging.info(f"Processing blob for move: {blob.name}")
            destination_blob_name = blob.name

            try:
                logging.info(f"Copying gs://{source_bucket_name}/{blob.name} to gs://{destination_bucket_name}/{destination_blob_name}...")
                source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
                logging.info("Copy complete.")

                logging.info(f"Deleting gs://{source_bucket_name}/{blob.name}...")
                blob.delete()
                logging.info("Deletion complete.")
                moved_count += 1

            except Exception as move_error:
                logging.error(f"Error moving blob {blob.name}: {move_error}")

        if moved_count > 0:
            logging.info(f"Move complete. Total files moved: {moved_count}")
        else:
             logging.warning("No matching files found to move after pipeline execution.")

    except Exception as e:
        logging.error(f"General error during GCS move process: {e}")


# --- Apache Beam Pipeline Construction ---
def run(argv=None):
    # Define the project ID (can be from env var or args)
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print("Error: The GOOGLE_CLOUD_PROJECT environment variable is not set.")
        sys.exit(1)

    # Set up command-line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--gcs_input_path",
        dest="gcs_input_path",
        required=True,
        help="Path of the file(s) in Cloud Storage to read from (e.g. gs://YOUR_BUCKET/path/*.jsonl)"
    )
    parser.add_argument(
        "--dataset_id",
        dest="dataset_id",
        required=True,
        help="BigQuery dataset ID to write to."
    )
    parser.add_argument(
        "--visits_table",
        dest="visits_table",
        default="visits",
        help="BigQuery table ID for visits data."
    )
    parser.add_argument(
        "--events_table",
        dest="events_table",
        default="events",
        help="BigQuery table ID for events data."
    )
    parser.add_argument(
        "--purchase_items_table",
        dest="purchase_items_table",
        default="purchase_items",
        help="BigQuery table ID for purchase items data."
    )
    parser.add_argument(
        "--write_disposition",
        dest="write_disposition",
        default="append",
        choices=["append", "truncate"],
        help="BigQuery write disposition ('append' or 'truncate')."
    )
    parser.add_argument(
        "--gcs_destination_bucket",
        dest="gcs_destination_bucket",
        required=True,
        help="Name of the Cloud Storage bucket to move processed files to."
    )

    # Add standard Beam/Dataflow arguments
    parser.add_argument(
        "--runner",
        dest="runner",
        default="DirectRunner",
        help="Pipeline runner (e.g., DirectRunner, DataflowRunner)."
    )
    parser.add_argument(
        "--project",
        dest="project",
        help="Google Cloud project ID."
    )
    parser.add_argument(
        "--region",
        dest="region",
        help="Google Cloud region for Dataflow."
    )
    parser.add_argument(
        "--staging_location",
        dest="staging_location",
        help="Cloud Storage staging location for Dataflow."
    )
    parser.add_argument(
        "--temp_location",
        dest="temp_location",
        help="Cloud Storage temp location for Dataflow."
    )
    # args = parser.parse_args(argv)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Configure pipeline options
    pipeline_options = PipelineOptions(args=pipeline_args, save_main_session=True) 

    # Define table IDs based on arguments
    dataset_id = known_args.dataset_id
    visits_table_id = known_args.visits_table
    events_table_id = known_args.events_table
    purchase_items_table_id = known_args.purchase_items_table

    effective_project_id = known_args.project if known_args.project else project_id

    visits_bq_table = f"{effective_project_id}:{dataset_id}.{visits_table_id}"
    events_bq_table = f"{effective_project_id}:{dataset_id}.{events_table_id}"
    purchase_items_bq_table = f"{effective_project_id}:{dataset_id}.{purchase_items_table_id}"

    # Define BigQuery write disposition
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
    if known_args.write_disposition.lower() == "truncate":
        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE

    # Create the Pipeline
    pipeline = beam.Pipeline(options=pipeline_options)

    # --- Pipeline Definition ---
    try:
        # Read data from GCS
        lines = pipeline | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.gcs_input_path)
    except Exception as e:
        logging.error(f"No files on GCS: {e}")
        exit()

    # Parse JSONL lines
    parsed_data = lines | 'ParseJsonl' >> beam.ParDo(ParseJsonl())

    # Extract different data types
    visits_data = parsed_data | 'ExtractVisitData' >> beam.ParDo(ExtractVisitData())
    events_data = parsed_data | 'ExtractEventData' >> beam.ParDo(ExtractEventData())
    purchase_items_data = parsed_data | 'ExtractPurchaseItemData' >> beam.ParDo(ExtractPurchaseItemData())

    # Write data to BigQuery
    visits_data | 'WriteVisitsToBigQuery' >> beam.io.WriteToBigQuery(
        visits_bq_table,
        schema=VISITS_TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition
    )

    events_data | 'WriteEventsToBigQuery' >> beam.io.WriteToBigQuery(
        events_bq_table,
        schema=EVENTS_TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition
    )

    purchase_items_data | 'WritePurchaseItemsToBigQuery' >> beam.io.WriteToBigQuery(
        purchase_items_bq_table,
        schema=PURCHASE_ITEMS_TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition
    )

    logging.info("Starting Beam pipeline execution...")
    result = pipeline.run()
    logging.info("Pipeline started. Waiting for completion...")

    # Return the result object and relevant args for post-execution steps
    return result, known_args.gcs_input_path, known_args.gcs_destination_bucket


if __name__ == '__main__':
    # Run the pipeline and get the result and arguments
    pipeline_result, source_gcs_path, destination_gcs_bucket = run(argv=sys.argv[1:])

    # Wait for the pipeline to finish (crucial for DataflowRunner)
    logging.info("Waiting for final pipeline completion...")
    pipeline_result.wait_until_finish() # This line waits for the Dataflow job to finish

    logging.info(f"Pipeline finished with state: {pipeline_result.state}")

    # Move source file(s) only if pipeline succeeded
    if pipeline_result.state == PipelineState.DONE:
        logging.info("Pipeline completed successfully. Initiating source file move...")
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--launched_by",
            dest="launched_by",
            default="unknown",
            help="Identifier for the entity that launched the pipeline (e.g., composer, local)."
        )
        known_args, pipeline_args = parser.parse_known_args(sys.argv[1:])
        if known_args.launched_by != 'composer':
            move_gcs_file(source_gcs_path, destination_gcs_bucket)
        else:
            logging.info("Launched by composer. Skipping source file move.")
    else:
        logging.error("Pipeline failed or was cancelled. Source file(s) will NOT be moved.")
