import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import json
import logging
import uuid
import argparse
import sys

# Configure basic logging for the pipeline
# Dataflow automatically captures logs at INFO level and above
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

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
}


# --- Beam Transforms (DoFn classes) ---

class ParseJsonl(beam.DoFn):
    """
    Parses each line as a JSON object.
    Logs a warning for invalid JSON lines.
    """
    def process(self, element):
        try:
            yield json.loads(element)
        except json.JSONDecodeError:
            # Log specific warning for JSON decode errors
            logging.warning(f"Skipping invalid JSON line: {element[:100]}...")
        except Exception as e:
            # Log unexpected errors during parsing
            logging.error(f"An unexpected error occurred parsing line: {element[:100]}... Error: {e}")


class TransformVisitData(beam.DoFn):
    """
    Transforms a single visit dictionary into rows for visits, events,
    and purchase_items tables, generating composite keys.
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
            return # Skip visits without a session ID

        # --- Process Visit Data ---
        # Extract visit-level information
        visit_row = {
            "session_id": session_id,
            "user_id": visit_data.get("user_id"),
            "device_type": visit_data.get("device_type"),
            "geolocation": visit_data.get("geolocation"),
            "user_agent": visit_data.get("user_agent"),
            "start_timestamp": None,
            "end_timestamp": None,
            "event_count": 0, # Will update after processing events
        }

        # --- Process Events and Purchase Items ---
        event_wrappers = visit_data.get("events", [])
        visit_row["event_count"] = len(event_wrappers)

        # Extract actual event dictionaries and sort by timestamp
        events = []
        for wrapper in event_wrappers:
            actual_event = wrapper.get("event")
            if isinstance(actual_event, dict):
                 events.append(actual_event)
            else:
                 # Log warning for malformed event structures
                 logging.warning(f"Invalid event structure found in session {session_id}. Skipping event.")

        if events:
            # Sort events by timestamp to determine visit start/end times
            try:
                events.sort(key=lambda x: x.get("timestamp"))
                visit_row["start_timestamp"] = events[0].get("timestamp")
                visit_row["end_timestamp"] = events[-1].get("timestamp")
            except TypeError:
                 # Log warning if timestamps are missing or invalid for sorting
                 logging.warning(f"Could not sort events for session {session_id} due to missing/invalid timestamp.")
                 # Attempt to set timestamps from unsorted list if sort fails
                 if events and events[0].get("timestamp"): visit_row["start_timestamp"] = events[0].get("timestamp")
                 if events and events[-1].get("timestamp"): visit_row["end_timestamp"] = events[-1].get("timestamp")


        # --- Generate event_unique_id and process events ---
        for i, actual_event in enumerate(events):
            # Generate a unique ID for this event within the session
            # Using a combination of session_id and a UUID for robustness against source duplicates
            event_unique_id = f"{session_id}-{uuid.uuid4()}"

            event_type = actual_event.get("event_type")
            timestamp = actual_event.get("timestamp")
            details = actual_event.get("details", {})

            # Create a base event row with common fields and generated ID
            current_event_row = {
                "session_id": session_id,
                "event_unique_id": event_unique_id, # Include the generated unique ID
                "event_type": event_type,
                "timestamp": timestamp,
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
            elif event_type == "add_item_to_cart":
                current_event_row["product_id"] = details.get("product_id")
                current_event_row["product_name"] = details.get("product_name")
                current_event_row["category"] = details.get("category")
                current_event_row["price"] = details.get("price")
                current_event_row["quantity"] = details.get("quantity")
            elif event_type == "purchase":
                order_id = details.get("order_id")
                current_event_row["order_id"] = order_id
                current_event_row["amount"] = details.get("amount")
                current_event_row["currency"] = details.get("currency")

                # Process purchase items if order_id is present
                if order_id:
                    items = details.get("items", [])
                    # --- Generate item_index_in_order and process items ---
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
                            # Yield purchase item row to the purchase_items output
                            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_PURCHASE_ITEMS, purchase_item_row)
                        else:
                             # Log warning for malformed purchase item structures
                             logging.warning(f"Invalid purchase item structure found in order {order_id}, session {session_id}. Skipping item.")
                else:
                    # Log warning if purchase event is missing order_id
                    logging.warning(f"Purchase event in session {session_id} missing order_id. Cannot process items.")


            # Yield event row to the events output
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_EVENTS, current_event_row)

        # Yield visit row to the visits output
        yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_VISITS, visit_row)


def run(argv=None, save_main_session=True):
    """
    Builds and runs the Dataflow pipeline.
    Parses command-line arguments and configures pipeline options.
    """
    # Define command-line arguments using argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input Cloud Storage file pattern to read from (e.g., gs://your-bucket/*.jsonl).')

    parser.add_argument(
        '--output_dataset',
        dest='output_dataset',
        required=True,
        help='BigQuery dataset ID to write the output tables to.')

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

    # Parse known arguments, leaving unknown ones for PipelineOptions
    # Pass the received argv to argparse
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initialize PipelineOptions with the remaining arguments
    # Dataflow runner automatically adds standard options like --project, --region, etc.
    # These will be parsed by PipelineOptions.
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=save_main_session)

    # --- Get Project ID from PipelineOptions ---
    # Access the project ID from the GoogleCloudOptions view
    try:
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        project = google_cloud_options.project
        if not project:
             # This case should be rare with Flex Templates passing --project,
             # but include a check for robustness.
             raise ValueError("Project ID is required but was not found in PipelineOptions.")
        # Log the determined project ID
        logging.info(f"Using Project ID from PipelineOptions: {project}")

    except Exception as e:
        # Log error if project ID cannot be determined from options
        logging.error(f"Failed to determine Project ID from PipelineOptions: {e}")
        # Re-raise the exception as Project ID is essential
        raise ValueError("Project ID is required to run the pipeline.") from e


    # Construct full BigQuery table specifications
    visits_table_spec = f'{project}:{known_args.output_dataset}.{known_args.visits_table}'
    events_table_spec = f'{project}:{known_args.output_dataset}.{known_args.events_table}'
    purchase_items_table_spec = f'{project}:{known_args.output_dataset}.{known_args.purchase_items_table}'

    # --- Log final table specs (Optional, can be removed for production) ---
    # logging.info(f"Writing to BigQuery Tables:")
    # logging.info(f"  Visits: {visits_table_spec}")
    # logging.info(f"  Events: {events_table_spec}")
    # logging.info(f"  Purchase Items: {purchase_items_table_spec}")


    # --- Build the Pipeline ---
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read JSONL lines from Cloud Storage
        lines = pipeline | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input)

        # Parse each line as a JSON object
        parsed_visits = lines | 'ParseJsonl' >> beam.ParDo(ParseJsonl())

        # Transform visit data into rows for different tables using tagged outputs
        # This step generates the composite key components (event_unique_id, item_index_in_order)
        transformed_data = parsed_visits | 'TransformVisitData' >> beam.ParDo(
            TransformVisitData()).with_outputs(
                TransformVisitData.OUTPUT_TAG_VISITS,
                TransformVisitData.OUTPUT_TAG_EVENTS,
                TransformVisitData.OUTPUT_TAG_PURCHASE_ITEMS)

        # Separate the outputs into different PCollections
        visits_pcollection = transformed_data[TransformVisitData.OUTPUT_TAG_VISITS]
        events_pcollection = transformed_data[TransformVisitData.OUTPUT_TAG_EVENTS]
        purchase_items_pcollection = transformed_data[TransformVisitData.OUTPUT_TAG_PURCHASE_ITEMS]

        # Write each PCollection to the respective BigQuery table
        visits_pcollection | 'WriteVisitsToBigQuery' >> beam.io.WriteToBigQuery(
            visits_table_spec,
            schema=visits_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE # Use WRITE_APPEND for incremental loads
        )

        events_pcollection | 'WriteEventsToBigQuery' >> beam.io.WriteToBigQuery(
            events_table_spec,
            schema=events_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

        purchase_items_pcollection | 'WritePurchaseItemsToBigQuery' >> beam.io.WriteToBigQuery(
            purchase_items_table_spec,
            schema=purchase_items_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

# --- Entry Point ---
if __name__ == '__main__':
    # Pass command-line arguments (excluding script name) to the run function
    run(argv=sys.argv[1:])
