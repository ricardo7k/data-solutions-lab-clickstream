import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import json
import logging
import uuid
import argparse
import sys
from datetime import datetime # Import datetime to parse timestamps

# Import necessary modules for fileio streaming
from apache_beam.io import fileio
from apache_beam.transforms.window import FixedWindows # Import FixedWindows for time-based windows
import apache_beam.transforms.combiners as combiners # Import combiners for counting


from apache_beam.io.filesystem import FileMetadata
# Removed: from apache_beam.state import StateSpec, ReadModifyWriteStateSpec


# Configure basic logging for the pipeline
# Dataflow automatically captures logs at INFO level and higher
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO) # Changed to INFO to capture more details

# --- BigQuery Table Schemas (as dictionaries for Beam) ---
# Define the schema for the visits table
visits_schema = {
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

# Define the schema for the events table
events_schema = {
    "fields": [
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"}, # Part of Composite PK
        {"name": "event_unique_id", "type": "STRING", "mode": "REQUIRED"}, # Part of Composite PK
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
    ]
}

# Define the schema for the purchase_items table
purchase_items_schema = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"}, # Part of Composite PK
        {"name": "item_index_in_order", "type": "INTEGER", "mode": "REQUIRED"}, # Part of Composite PK
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"}, # FK for convenience
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "price", "type": "FLOAT64"},
        {"name": "quantity", "type": "INTEGER"},
    ]
    # Note: BigQuery streaming inserts do not support WRITE_TRUNCATE.
    # For streaming, you must use WRITE_APPEND. If you need to handle
    # late data or updates, consider using BigQuery partitioning
    # or clustering, or a separate batch job for deduplication/merging.
}

# Removed page_views_schema as we are logging instead of writing to a dedicated table


# --- Beam Transformations (DoFn Classes) ---

class ParseJsonl(beam.DoFn):
    """
    Parses each line of a file as a JSON object.
    Input is a string with the file content.
    Logs a warning for invalid JSON lines.
    """
    def process(self, element):
        # The element is the content of a file as a single string
        logging.info(f"Parsing file content.") # Log added
        lines = element.splitlines()
        logging.info(f"Found {len(lines)} lines in the file.") # Log added
        for line in lines:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # Log specific warning for JSON decoding errors
                logging.warning(f"Skipping invalid JSON line: {line[:100]}...")
            except Exception as e:
                # Log unexpected errors during parsing
                logging.error(f"An unexpected error occurred parsing line: {line[:100]}... Error: {e}")


class TransformVisitDataStreaming(beam.DoFn):
    """
    Transforms a single parsed visit object into rows for the visits, events,
    and purchase_items tables, generating composite keys.
    Designed for streaming, processing one visit at a time.
    """
    # Define tags for multiple outputs
    OUTPUT_TAG_VISITS = 'visits'
    OUTPUT_TAG_EVENTS = 'events'
    OUTPUT_TAG_PURCHASE_ITEMS = 'purchase_items'

    def process(self, visit_data):
        """Processes a single visit dictionary."""
        session_id = visit_data.get("session_id")
        if not session_id:
            # Log warning for visits missing session ID
            logging.warning(f"Visit data missing session_id. Skipping visit.")
            return # Skip visits without session ID

        logging.info(f"Processing session_id: {session_id}") # Log added

        # --- Process Visit Data ---
        # Extract visit-level information
        visit_row = {
            "session_id": session_id,
            "user_id": visit_data.get("user_id"),
            "device_type": visit_data.get("device_type"),
            "geolocation": visit_data.get("geolocation"),
            "user_agent": visit_data.get("user_agent"),
            "start_timestamp": None, # Set later when processing events
            "end_timestamp": None,   # Set later when processing events
            "event_count": 0,      # Update later when processing events
        }

        # --- Process Events and Itens de Compra ---
        event_wrappers = visit_data.get("events", [])
        visit_row["event_count"] = len(event_wrappers)
        logging.info(f"Session {session_id} has {len(event_wrappers)} events.") # Log added

        events = []
        for wrapper in event_wrappers:
            actual_event = wrapper.get("event")
            if isinstance(actual_event, dict):
                 events.append(actual_event)
            else:
                 # Log warning for malformed event structures
                 logging.warning(f"Invalid event structure found in session {session_id}. Skipping event.")

        # Sort events by timestamp to determine visit start/end times
        if events:
            try:
                # Parse timestamp strings to datetime objects for sorting
                for event in events:
                    if event.get("timestamp"):
                        try:
                            event["timestamp_dt"] = datetime.fromisoformat(event["timestamp"].replace('Z', '+00:00'))
                        except ValueError:
                            logging.warning(f"Invalid timestamp format for event in session {session_id}: {event.get('timestamp')}. Skipping timestamp parsing.")
                            event["timestamp_dt"] = None # Mark as invalid

                # Sort by the parsed datetime objects
                valid_events = [e for e in events if e.get("timestamp_dt")]
                if valid_events:
                    valid_events.sort(key=lambda x: x["timestamp_dt"])
                    visit_row["start_timestamp"] = valid_events[0].get("timestamp")
                    visit_row["end_timestamp"] = valid_events[-1].get("timestamp")
                    logging.info(f"Session {session_id}: Start: {visit_row['start_timestamp']}, End: {visit_row['end_timestamp']}") # Log added
                else:
                     logging.warning(f"No valid timestamps found for sorting events in session {session_id}.")

            except TypeError:
                 # Log warning if timestamps are missing or are of unexpected types for sorting
                 logging.warning(f"Could not sort events for session {session_id} due to missing/invalid timestamp types.")
                 # Fallback attempt to set timestamps from the original list if sorting fails
                 if events and events[0].get("timestamp"): visit_row["start_timestamp"] = events[0].get("timestamp")
                 if events and events[-1].get("timestamp"): visit_row["end_timestamp"] = events[-1].get("timestamp")


        # --- Generate event_unique_id and process events ---
        for i, actual_event in enumerate(events):
            # Generate a unique ID for this event within the session
            # Using a combination of session_id and a UUID for robustness against source duplicates
            event_unique_id = f"{session_id}-{uuid.uuid4()}"

            event_type = actual_event.get("event_type")
            timestamp = actual_event.get("timestamp") # Keep original string timestamp for output
            details = actual_event.get("details", {})

            # Create a base event row with common fields and generated ID
            current_event_row = {
                "session_id": session_id,
                "event_unique_id": event_unique_id, # Include the generated unique ID
                "event_type": event_type,
                "timestamp": timestamp, # Use original string timestamp for BigQuery
                "page_url": None,
                "referrer_url": None,
                "product_id": None,
                "product_name": None,
                "category": None,
                "price": None,
                "quantity": None,
                "order_id": None,
                "amount": None,
                "currency": None,
            }

            # Populate detail fields based on event type
            if event_type == "page_view":
                current_event_row["page_url"] = details.get("page_url")
                current_event_row["referrer_url"] = details.get("referrer_url")
                logging.debug(f"Session {session_id}, Event {event_unique_id}: Page View - {current_event_row['page_url']}") # Detailed log
            elif event_type == "add_item_to_cart":
                current_event_row["product_id"] = details.get("product_id")
                current_event_row["product_name"] = details.get("product_name")
                current_event_row["category"] = details.get("category")
                current_event_row["price"] = details.get("price")
                current_event_row["quantity"] = details.get("quantity")
                logging.debug(f"Session {session_id}, Event {event_unique_id}: Add Item - {current_event_row['product_name']}") # Detailed log
            elif event_type == "purchase":
                order_id = details.get("order_id")
                current_event_row["order_id"] = order_id
                current_event_row["amount"] = details.get("amount")
                current_event_row["currency"] = details.get("currency")
                logging.info(f"Session {session_id}, Event {event_unique_id}: Purchase - Order ID: {order_id}") # Log added

                # Process purchase items if order_id is present
                if order_id:
                    items = details.get("items", [])
                    logging.info(f"Order {order_id} in session {session_id} has {len(items)} items.") # Log added
                    for item_index, item in enumerate(items): # Use enumerate for index
                        if isinstance(item, dict):
                            purchase_item_row = {
                                "order_id": order_id,
                                "item_index_in_order": item_index, # Include the index
                                "session_id": session_id, # Add session_id for convenience
                                "product_id": item.get("product_id"),
                                "product_name": item.get("product_name"),
                                "category": item.get("category"),
                                "price": item.get("price"),
                                "quantity": item.get("quantity"),
                            }
                            logging.debug(f"Order {order_id}, Item {item_index}: Product {item.get('product_name')}") # Detailed log
                            # Yield purchase item row for purchase_items output
                            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_PURCHASE_ITEMS, purchase_item_row)
                        else:
                             # Log warning for malformed purchase item structures
                             logging.warning(f"Invalid purchase item structure found in order {order_id}, session {session_id}. Skipping item.")
                else:
                    # Log warning if purchase event is missing order_id
                    logging.warning(f"Purchase event in session {session_id} missing order_id. Cannot process items.")


            # Yield event row for events output
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_EVENTS, current_event_row)

        # Yield visit row for visits output
        yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_VISITS, visit_row)


# Helper function to format windowed counts for logging
def format_windowed_count_for_log(element, window=beam.DoFn.WindowParam):
    """Formats a windowed count element for structured logging."""
    # element is the count (integer)
    # window is the Window object associated with the element
    return {
        "window_end": window.end.to_rfc3339(), # Format timestamp as RFC3339 string
        "page_view_count": element,
    }


def run(argv=None, save_main_session=True):
    """
    Constructs and runs the Dataflow streaming pipeline.
    Reads files from GCS as they arrive, parses, and writes data to BigQuery.
    """
    logging.info("Pipeline starting.") # Log added

    # Define command-line arguments using argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_gcs_directory',
        dest='input_gcs_directory',
        required=True,
        help='Cloud Storage directory to monitor for new files (e.g., gs://your-bucket/input_data/).')

    parser.add_argument(
        '--output_dataset',
        dest='output_dataset',
        required=True,
        help='BigQuery dataset ID to write output tables to.')

    parser.add_argument(
        '--visits_table',
        dest='visits_table',
        required=True,
        help='BigQuery visits table ID.')

    parser.add_argument(
        '--events_table',
        dest='events_table',
        required=True,
        help='BigQuery events table ID.')

    parser.add_argument(
        '--purchase_items_table',
        dest='purchase_items_table',
        required=True,
        help='BigQuery purchase items table ID.')

    # Removed page_views_table argument as we are logging instead of writing to a dedicated table
    # parser.add_argument(
    #     '--page_views_table',
    #     dest='page_views_table',
    #     required=True,
    #     help='BigQuery page views per minute table ID.')


    # Add arguments for file monitoring
    parser.add_argument(
        '--file_pattern',
        dest='file_pattern',
        default='*.jsonl', # Pattern for JSONL files
        help='File pattern within the GCS input directory (e.g., *.jsonl). Default is *.jsonl.')

    parser.add_argument(
        '--interval',
        dest='interval',
        type=int,
        default=60, # Default to 60 seconds
        help='Interval in seconds to check the GCS directory for new files. Default is 60.')


    # Parse known arguments, leaving the rest for PipelineOptions
    # Pass the received argv to argparse
    known_args, pipeline_args = parser.parse_known_args(argv)

    logging.info(f"Pipeline arguments: {pipeline_args}") # Log added
    logging.info(f"Known arguments: {known_args}") # Log added

    # Initialize PipelineOptions with the remaining arguments
    # Removed: Set the runner to DataflowRunner for cloud execution
    pipeline_args.extend([
        # '--runner=DataflowRunner', # Removed to allow the runner to be decided by the environment
        '--region=us-central1', # Consider changing to your region
        '--streaming', # Essential for streaming pipelines
        # Add other standard Dataflow options here if not provided via gcloud command:
        # '--project=YOUR_PROJECT_ID', # Replace YOUR_PROJECT_ID with your project ID
        # '--temp_location=gs://your-bucket/temp', # Replace with your bucket and path
        # '--staging_location=gs://your-bucket/staging', # Replace with your bucket and path
        # '--disk_size_gb=50', # Example: Increase disk size if needed
        # '--machine_type=n1-standard-1', # Example: Machine type
        # '--num_workers=1', # Example: Initial number of workers
        # '--max_num_workers=10', # Example: Maximum number of workers for autoscaling
    ])

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=save_main_session)

    # --- Get Project ID from PipelineOptions ---
    # Access the project ID from the GoogleCloudOptions view
    try:
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        project = google_cloud_options.project
        if not project:
             # This case should be rare with DataflowRunner, but include a check for robustness.
             raise ValueError("Project ID is required but not found in PipelineOptions.")
        # Log the determined project ID
        logging.info(f"Using Project ID from PipelineOptions: {project}")

    except Exception as e:
        # Log error if Project ID cannot be determined from options
        logging.error(f"Failed to determine Project ID from PipelineOptions: {e}")
        # Re-raise the exception, as Project ID is essential
        raise ValueError("Project ID is required to run the pipeline.") from e


    # Construct complete BigQuery table specifications
    visits_table_spec = f'{project}:{known_args.output_dataset}.{known_args.visits_table}'
    events_table_spec = f'{project}:{known_args.output_dataset}.{known_args.events_table}'
    purchase_items_table_spec = f'{project}:{known_args.output_dataset}.{known_args.purchase_items_table}'
    # Removed page_views_table_spec as we are logging instead of writing to a dedicated table
    # page_views_table_spec = f'{project}:{known_args.output_dataset}.{known_args.page_views_table}'


    # --- Log final table specifications (Optional, can be removed for production) ---
    logging.info(f"Writing to BigQuery Tables:")
    logging.info(f"  Visits: {visits_table_spec}")
    logging.info(f"  Events: {events_table_spec}")
    logging.info(f"  Purchase Items: {purchase_items_table_spec}")
    # Removed log for page views table
    # logging.info(f"  Page Views per Minute: {page_views_table_spec}")


    # --- Build the Streaming Pipeline ---
    with beam.Pipeline(options=pipeline_options) as pipeline:
        logging.info("Pipeline definition started.") # Log added
        # Read new files from GCS as they arrive
        # MatchContinuously monitors the input directory for new files
        files = (pipeline
             | 'MatchFiles' >> fileio.MatchContinuously(
                 f'{known_args.input_gcs_directory}{known_args.file_pattern}',
                 interval=known_args.interval # Check interval in seconds
             ).with_output_types(FileMetadata) # Added type hint for clarity
             | 'ReadMatches' >> fileio.ReadMatches().with_output_types(beam.io.fileio.ReadableFile) # Added type hint
             # Read file content from the ReadableFile
             | 'ReadFileContents' >> beam.Map(lambda readable_file: readable_file.read()).with_output_types(bytes) # Added type hint
            )
        logging.info("Finished defining file reading steps.") # Log added


        # Decodificar bytes para string (assumindo UTF-8)
        decoded_file_contents = files | 'DecodeFileContents' >> beam.Map(lambda b: b.decode('utf-8')).with_output_types(str) # Added type hint
        logging.info("Finished defining decoding step.") # Log added

        # Analisar cada linha do conteúdo do arquivo como um objeto JSON
        # Since ReadMatches reads the entire file, we need ParseJsonl to handle the lines
        parsed_visits = decoded_file_contents | 'ParseJsonl' >> beam.ParDo(ParseJsonl()).with_output_types(dict) # Added type hint
        logging.info("Finished defining JSON parsing step.") # Log added


        # Transform parsed visit data into rows for different tables using tagged outputs
        transformed_data = parsed_visits | 'TransformVisitDataStreaming' >> beam.ParDo(
            TransformVisitDataStreaming()).with_outputs(
                TransformVisitDataStreaming.OUTPUT_TAG_VISITS,
                TransformVisitDataStreaming.OUTPUT_TAG_EVENTS,
                TransformVisitDataStreaming.OUTPUT_TAG_PURCHASE_ITEMS)
        logging.info("Finished defining transformation step.") # Log added


        # Separate outputs into different PCollections
        visits_pcollection = transformed_data[TransformVisitDataStreaming.OUTPUT_TAG_VISITS]
        events_pcollection = transformed_data[TransformVisitDataStreaming.OUTPUT_TAG_EVENTS]
        purchase_items_pcollection = transformed_data[TransformVisitDataStreaming.OUTPUT_TAG_PURCHASE_ITEMS]
        logging.info("Finished separating outputs.") # Log added

        # --- Calculate Page Views per Minute and Log ---
        page_views_per_minute_counts = (
            events_pcollection
            | 'FilterPageViews' >> beam.Filter(lambda event: event.get("event_type") == "page_view") # Filter for page views
            # Assign event timestamps as the element timestamps for windowing
            # Parse timestamp string to datetime object before creating TimestampedValue
            | 'AddEventTimestamps' >> beam.Map(lambda event: beam.window.TimestampedValue(event, datetime.fromisoformat(event.get("timestamp").replace('Z', '+00:00')).timestamp())) # Parse and convert to Unix timestamp
            # Window into 1-minute fixed windows
            | 'WindowIntoOneMinute' >> beam.WindowInto(FixedWindows(60))
            # Count elements (page views) within each window
            # Use Count.PerElement() for windowed PCollections
            | 'CountPageViews' >> combiners.Count.PerElement() # Changed from Count.Globally()
            # Count.PerElement() outputs (None, count). We need to extract the count.
            | 'ExtractCount' >> beam.Map(lambda element: element[1]) # Extract the count from (None, count)
            # Format the windowed count for logging
            | 'FormatWindowedCountsForLog' >> beam.Map(format_windowed_count_for_log) # Renamed helper function
            # Log the structured page view count data
            | 'LogPageViewCounts' >> beam.Map(lambda count_data: logging.info(json.dumps(count_data))) # Log as JSON string
        )
        logging.info("Finished defining page views calculation and logging steps.") # Log added


        # --- Write Other Data to BigQuery ---
        logging.info("Defining BigQuery write steps for visits, events, and purchase items.") # Log added
        try:
            # Write visits
            visits_pcollection | 'WriteVisitsToBigQuery' >> beam.io.WriteToBigQuery(
                visits_table_spec,
                schema=visits_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Always append in streaming
                method="STREAMING_INSERTS"
            )
            logging.info(f"Defined write step for {visits_table_spec}") # Log added

            # Write events
            events_pcollection | 'WriteEventsToBigQuery' >> beam.io.WriteToBigQuery(
                events_table_spec,
                schema=events_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS"
            )
            logging.info(f"Defined write step for {events_table_spec}") # Log added

            # Write purchase items
            purchase_items_pcollection | 'WritePurchaseItemsToBigQuery' >> beam.io.WriteToBigQuery(
                purchase_items_table_spec,
                schema=purchase_items_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS"
            )
            logging.info(f"Defined write step for {purchase_items_table_spec}") # Log added

        except Exception as e:
            logging.error(f"Error defining BigQuery write steps: {e}")

    logging.info("Pipeline definition complete. Running pipeline...") # Log added


# --- Entry Point ---
if __name__ == '__main__':
    # Pass command-line arguments (excluding script name) to the run function
    run(argv=sys.argv[1:])
