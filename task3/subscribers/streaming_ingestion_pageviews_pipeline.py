import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json
import logging
import os
import argparse
import sys
from datetime import datetime
from apache_beam.io.gcp.gcsio import GcsIO

# Configure logging with minimal format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define schema for the page_views table
PAGE_VIEWS_TABLE_SCHEMA = {
    "fields": [
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "page_view_count", "type": "INTEGER", "mode": "REQUIRED"},
    ]
}

# Define threshold for potential DoS detection
DOS_THRESHOLD_PAGE_VIEWS_PER_MINUTE = 10

# --- Core Pipeline DoFns ---

class ParseJsonMessageData(beam.DoFn):
    def process(self, message):
        try:
            if message.data:
                json_str = message.data.decode('utf-8')
                data = json.loads(json_str)
                yield (json_str, json.loads(json_str))
            else:
                logging.warning(f"Received message with no data: {message.message_id}")
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error parsing message: {e}", exc_info=True)

class FilterPageViews(beam.DoFn):
    def process(self, element):
        raw_data, parsed_json = element
        try:
            events = parsed_json.get("events", [])
            for event_entry in events:
                event_details = event_entry.get("event", {})
                event_type = event_details.get("event_type")
                if event_type == 'page_view':
                    yield 1
        except Exception as e:
            logging.error(f"Error filtering page views: {e}", exc_info=True)

class FormatPageViewCount(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        window_end_utc = window.end.to_utc_datetime()
        formatted_data = {
            "window_end": window_end_utc.replace(microsecond=0).isoformat(),
            "page_view_count": count,
        }
        # Optional: Log before BQ write (remove if not needed)
        # logging.info(f"Formatting for BQ: {formatted_data}")
        yield formatted_data

class CheckForPotentialDoS(beam.DoFn):
    def __init__(self, dos_threshold):
        self._dos_threshold = dos_threshold

    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        window_end_utc = window.end.to_utc_datetime()
        window_end_str = window_end_utc.replace(microsecond=0).isoformat()

        logging.info(f"Minute page view count: {count} in window ending {window_end_str}")

        if count > self._dos_threshold:
             logging.warning(
                 f"Potential DoS detected: High page view count ({count}) in window ending {window_end_str}. "
                 f"Threshold: {self._dos_threshold}"
             )
        yield element

class WriteMinuteRawDataToGCS(beam.DoFn):
    def process(self, element, base_output_path):
        key, elements = element
        window_end_timestamp_str, shard_id = key

        try:
            window_end_utc = datetime.fromisoformat(window_end_timestamp_str.replace('Z', '+00:00'))
        except ValueError:
             logging.error(f"Invalid timestamp format in key: {window_end_timestamp_str}. Cannot generate file name.")
             return

        file_timestamp = window_end_utc.strftime("%Y-%m-%d-T-%H-%M")
        file_name = f"raw-visits-{file_timestamp}-{shard_id}.jsonl"
        full_path = os.path.join(base_output_path, file_name)

        gcs = GcsIO()
        try:
            with gcs.open(full_path, 'wb') as f:
                for elem in elements:
                    f.write(elem.encode('utf-8') + b'\n')
            logging.info(f"Successfully wrote file to GCS: {full_path}")
        except Exception as e:
            logging.error(f"Error writing file {full_path} to GCS: {e}", exc_info=True)


# --- Apache Beam Pipeline Construction (Streaming Pub/Sub to GCS and PageViews BQ) ---

def run_streaming_pipeline(argv=None):
    """Builds and runs the streaming pipeline."""
    argv = sys.argv[1:] if argv is None else argv

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        logging.error("Error: GOOGLE_CLOUD_PROJECT environment variable not set.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Streaming pipeline for ecommerce clickstream page views.")
    parser.add_argument("--pubsub_subscription", required=True, help="Pub/Sub subscription.")
    parser.add_argument("--gcs_raw_output_path", required=True, help="Cloud Storage path for raw messages.")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset ID.")
    parser.add_argument("--bq_page_views_table", default="page_views", help="BigQuery table ID for page views.")
    parser.add_argument("--window_size_minutes", type=int, default=1, help="Window size in minutes.")
    parser.add_argument("--num_gcs_shards", type=int, default=5, help="Number of GCS output shards per minute window.")
    parser.add_argument("--dos", type=int, default=10, help="DoS threshold for page views.")
    parser.add_argument('--streaming', action='store_true', default=True, help='Run in streaming mode.')
    parser.add_argument("--runner", default="DirectRunner", help="Pipeline runner.")
    parser.add_argument("--project", help="Google Cloud project ID (overrides GOOGLE_CLOUD_PROJECT if set).")
    parser.add_argument("--region", help="Google Cloud region (required for DataflowRunner).")
    parser.add_argument("--staging_location", help="Cloud Storage staging location (required for DataflowRunner).")
    parser.add_argument("--temp_location", help="Cloud Storage temp location (required for DataflowRunner).")
    parser.add_argument("--service-account", help="Service account email to run the job.")

    args = parser.parse_args(argv)

    global DOS_THRESHOLD_PAGE_VIEWS_PER_MINUTE
    DOS_THRESHOLD_PAGE_VIEWS_PER_MINUTE = args.dos

    pipeline_options = PipelineOptions(argv, save_main_session=True, streaming=args.streaming)

    effective_project_id = args.project if args.project else project_id
    if not effective_project_id:
         logging.error("Project ID is not defined. Use --project or set GOOGLE_CLOUD_PROJECT.")
         sys.exit(1)

    page_views_bq_table = f"{effective_project_id}:{args.bq_dataset}.{args.bq_page_views_table}"

    window_size_seconds = args.window_size_minutes * 60

    logging.info(f"BigQuery page views table: {page_views_bq_table}")
    logging.info("Pipeline graph building...")

    with beam.Pipeline(options=pipeline_options) as pipeline:

        messages = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            subscription=args.pubsub_subscription,
            with_attributes=True
        )

        minute_windowed_messages = (
            messages
            | 'MinuteWindowMessages' >> beam.WindowInto(
                FixedWindows(window_size_seconds)
            )
        )

        # Process Branch 1: Write Raw Data to GCS
        keyed_minute_raw_data = (
            minute_windowed_messages
            | 'ExtractMinuteRawDataAndKey' >> beam.Map(
                lambda msg, num_shards: (
                    (
                        beam.window.Timestamp(msg.publish_time.timestamp()).to_utc_datetime().replace(microsecond=0).isoformat(),
                        f"{(hash(msg.message_id) % num_shards):02d}"
                    ),
                    msg.data.decode('utf-8')
                ),
                num_shards=args.num_gcs_shards
            )
        )

        grouped_minute_raw_data = keyed_minute_raw_data | 'GroupMinuteRawDataByWindowAndShard' >> beam.GroupByKey()

        grouped_minute_raw_data | 'WriteMinuteWindowedFiles' >> beam.ParDo(
            WriteMinuteRawDataToGCS(),
            base_output_path=args.gcs_raw_output_path
        )

        # Process Branch 2: Process Messages for Page Views
        parsed_data_for_pageviews = minute_windowed_messages | 'ParseJsonMessageDataForPageViews' >> beam.ParDo(ParseJsonMessageData())

        filtered_page_views = parsed_data_for_pageviews | 'FilterPageViews' >> beam.ParDo(FilterPageViews())

        keyed_page_views = filtered_page_views | 'AddDummyKey' >> beam.Map(lambda x: (None, x))

        grouped_page_views = keyed_page_views | 'GroupPageViewsByKey' >> beam.GroupByKey()
        page_view_counts = grouped_page_views | 'CountGroupedPageViews' >> beam.Map(lambda element: (element[0], len(element[1])))

        page_view_counts_with_dos_check = page_view_counts | 'CheckForPotentialDoS' >> beam.ParDo(CheckForPotentialDoS(DOS_THRESHOLD_PAGE_VIEWS_PER_MINUTE))

        # Format output for BigQuery
        formatted_page_view_counts = page_view_counts_with_dos_check | 'FormatPageViewCount' >> beam.ParDo(FormatPageViewCount())

        # Write page view counts to BigQuery
        formatted_page_view_counts | 'WritePageViewsToBigQuery' >> beam.io.WriteToBigQuery(
            page_views_bq_table,
            schema=PAGE_VIEWS_TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

    logging.info("Pipeline graph built. Starting execution...")


if __name__ == '__main__':
     run_streaming_pipeline()