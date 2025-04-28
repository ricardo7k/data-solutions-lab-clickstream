import base64
import json
import logging
import os
import uuid # To generate event_unique_id, although BigQuery can also generate row IDs
from datetime import datetime, timezone

# Import Flask for the web server
from flask import Flask, request, jsonify

# Import the BigQuery client library
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Get BigQuery details from environment variables (set during Cloud Run deployment)
PROJECT_ID = os.environ.get('GCP_PROJECT') # Cloud Run automatically sets this
BIGQUERY_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID') # Set this env var during deployment
BIGQUERY_VISITS_TABLE_ID = os.environ.get('BIGQUERY_VISITS_TABLE_ID') # Set this env var during deployment
BIGQUERY_EVENTS_TABLE_ID = os.environ.get('BIGQUERY_EVENTS_TABLE_ID') # Set this env var during deployment
BIGQUERY_PURCHASE_ITEMS_TABLE_ID = os.environ.get('BIGQUERY_PURCHASE_ITEMS_TABLE_ID') # Set this env var during deployment

# Construct the full table IDs
BIGQUERY_VISITS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_VISITS_TABLE_ID}'
BIGQUERY_EVENTS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_EVENTS_TABLE_ID}'
BIGQUERY_PURCHASE_ITEMS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_PURCHASE_ITEMS_TABLE_ID}'


# --- BigQuery Client Initialization ---
# Initialize the BigQuery client
bq_client = bigquery.Client()

# --- Flask App Setup ---
app = Flask(__name__)

# --- BigQuery Schemas (for insertion) ---
# Define the schema fields for the insertion tables
# These should match the schemas used in the Dataflow pipeline
# Ensure FLOAT64 and INTEGER types match BigQuery expectations

# Schema for the visits table
VISITS_SCHEMA_FIELDS = [
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("geolocation", "STRING"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("end_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("event_count", "INTEGER"),
]

# Schema for the events table
EVENTS_SCHEMA_FIELDS = [
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("event_unique_id", "STRING", mode="REQUIRED"), # Part of Composite PK (Generated)
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    # Flattened detail fields (nullable)
    bigquery.SchemaField("page_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("referrer_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
]

# Schema for the purchase_items table
PURCHASE_ITEMS_SCHEMA_FIELDS = [
    bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("item_index_in_order", "INTEGER", mode="REQUIRED"), # Part of Composite PK (Generated)
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"), # FK for convenience
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("quantity", "INTEGER"),
]


# --- Data Processing Logic ---

def process_visit_data(visit_data):
    """
    Processes a single visit dictionary, extracts data for visits, events,
    and purchase_items, and prepares rows for BigQuery insertion, generating composite keys.
    Returns three lists of rows: visits_rows, events_rows, purchase_items_rows.
    """
    session_id = visit_data.get("session_id")
    if not session_id:
        logger.warning("Visit data missing session_id. Skipping visit.")
        return [], [], [] # Return empty lists

    visits_rows = []
    events_rows = []
    purchase_items_rows = []

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
        "event_count": 0,      # Set later when processing events
    }
    # Add the visit row to the list
    visits_rows.append(visit_row)


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
             logger.warning(f"Invalid event structure found in session {session_id}. Skipping event.")

    # Sort events by timestamp to determine visit start/end times
    if events:
        try:
            events.sort(key=lambda x: x.get("timestamp"))
            visit_row["start_timestamp"] = events[0].get("timestamp")
            visit_row["end_timestamp"] = events[-1].get("timestamp")
        except TypeError:
             # Log warning if timestamps are missing or invalid for sorting
             logger.warning(f"Could not sort events for session {session_id} due to missing/invalid timestamp.")
             # Attempt to set timestamps from unsorted list if sort fails
             if events and events[0].get("timestamp"): visit_row["start_timestamp"] = events[0].get("timestamp")
             if events and events[-1].get("timestamp"): visit_row["end_timestamp"] = events[-1].get("timestamp")


    # --- Generate event_unique_id and process events ---
    for i, actual_event in enumerate(events):
        # Generate a unique ID for this event within the session
        # Using a combination of session_id and a UUID for robustness against source duplicates
        event_unique_id = f"{session_id}-{uuid.uuid4()}"

        event_type = actual_event.get("event_type")
        timestamp_str = actual_event.get("timestamp")
        details = actual_event.get("details", {})

        # Prepare the row for insertion into the events table
        # Initialize all detail fields to None
        event_row = {
            "session_id": session_id,
            "event_unique_id": event_unique_id, # Include the generated unique ID
            "event_type": event_type,
            "timestamp": timestamp_str, # Use the string timestamp directly for insertion
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
        # Extract details for all event types and add to the event_row
        if event_type == "page_view":
            event_row["page_url"] = details.get("page_url")
            event_row["referrer_url"] = details.get("referrer_url")
        elif event_type == "add_item_to_cart":
            event_row["product_id"] = details.get("product_id")
            event_row["product_name"] = details.get("product_name")
            event_row["category"] = details.get("category")
            event_row["price"] = details.get("price")
            event_row["quantity"] = details.get("quantity")
        elif event_type == "purchase":
            order_id = details.get("order_id")
            event_row["order_id"] = order_id
            event_row["amount"] = details.get("amount")
            event_row["currency"] = details.get("currency")

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
                        # Add the purchase item row to the list
                        purchase_items_rows.append(purchase_item_row)
                    else:
                         # Log warning for malformed purchase item structures
                         logger.warning(f"Invalid purchase item structure found in order {order_id}, session {session_id}. Skipping item.")
            else:
                # Log warning if purchase event is missing order_id
                logger.warning(f"Purchase event in session {session_id} missing order_id. Cannot process items.")

        # Add the prepared event row to the list
        events_rows.append(event_row)

    # Return the three lists of rows
    return visits_rows, events_rows, purchase_items_rows


def insert_into_bigquery(rows, table_spec):
    """Inserts a list of rows into the specified BigQuery table using streaming inserts."""
    if not rows:
        return

    try:
        # Use insert_rows_json for streaming inserts
        # Ensure row_ids are unique within the batch for best results
        # For the visits table, use session_id as row_id (assuming each message is a unique visit)
        # For the events table, use event_unique_id as row_id
        # For the purchase_items table, use a combination of order_id and item_index_in_order as row_id
        if table_spec.endswith(f'.{BIGQUERY_VISITS_TABLE_ID}'):
             row_ids = [r.get("session_id") for r in rows]
        elif table_spec.endswith(f'.{BIGQUERY_EVENTS_TABLE_ID}'):
             row_ids = [r.get("event_unique_id") for r in rows]
        elif table_spec.endswith(f'.{BIGQUERY_PURCHASE_ITEMS_TABLE_ID}'):
             row_ids = [f"{r.get('order_id')}_{r.get('item_index_in_order')}" for r in rows]
        else:
             row_ids = None # Allow BigQuery to generate IDs if the schema is not recognized

        errors = bq_client.insert_rows_json(table_spec, rows, row_ids=row_ids)

        if errors:
            logger.error(f"Errors occurred during BigQuery insertion for table {table_spec}:")
            for error in errors:
                logger.error(error)
            # Depending on requirements, you might re-raise the exception
            # or log and continue. For real-time, logging and continuing might be preferred
            # to avoid blocking, but messages might be lost.
        else:
            logger.info(f"Successfully inserted {len(rows)} rows into {table_spec}")

    except Exception as e:
        # Log any exceptions during the insertion process
        logger.error(f"An exception occurred during BigQuery insertion for table {table_spec}: {e}")
        # Return 500 status code to indicate a server error
        # Pub/Sub will retry sending the message later
        return 'Internal Server Error', 500

# --- Flask Route for Pub/Sub Push Messages ---

@app.route('/', methods=['POST'])
def index():
    """
    Receives Pub/Sub push messages, processes the data, and writes to BigQuery.
    """
    # Verify the request is a POST request
    if request.method != 'POST':
        return 'Method Not Allowed', 405

    # Get the Pub/Sub message from the request body
    envelope = request.get_json()
    if not envelope:
        msg = 'No Pub/Sub message received'
        logger.warning(f'Bad Request: {msg}')
        return f'Bad Request: {msg}', 400

    # Validate the Pub/Sub message structure
    if not isinstance(envelope, dict) or 'message' not in envelope or not isinstance(envelope['message'], dict):
         msg = 'Invalid Pub/Sub message format'
         logger.warning(f'Bad Request: {msg}')
         return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    # Extract the base64 encoded data
    if 'data' not in pubsub_message or not isinstance(pubsub_message['data'], str):
         msg = 'No data field in Pub/Sub message'
         logger.warning(f'Bad Request: {msg}')
         return f'Bad Request: {msg}', 400

    try:
        # Decode the base64 data
        data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        visit_data = json.loads(data)

        # Process the visit data and get rows for insertion for each table
        visits_to_insert, events_to_insert, purchase_items_to_insert = process_visit_data(visit_data)

        # Insert the prepared rows into the respective BigQuery tables
        if visits_to_insert:
            insert_into_bigquery(visits_to_insert, BIGQUERY_VISITS_TABLE_SPEC)
        if events_to_insert:
            insert_into_bigquery(events_to_insert, BIGQUERY_EVENTS_TABLE_SPEC)
        if purchase_items_to_insert:
            insert_into_bigquery(purchase_items_to_insert, BIGQUERY_PURCHASE_ITEMS_TABLE_SPEC)


        # Return a 200 status code to acknowledge the message
        # This tells Pub/Sub that the message was processed successfully
        return 'OK', 200

    except json.JSONDecodeError:
        msg = 'Invalid JSON in Pub/Sub message data'
        logger.error(f'Bad Request: {msg}')
        # Return 400 to indicate client error (message won't be retried)
        return f'Bad Request: {msg}', 400
    except Exception as e:
        # Log any other exceptions during processing
        logger.error(f'An error occurred during message processing: {e}')
        # Return 500 status code to indicate a server error
        # Pub/Sub will retry sending the message later
        return 'Internal Server Error', 500

# --- Entry Point for Cloud Run ---
# Cloud Run expects the application to be served on the port specified by the PORT environment variable
if __name__ == '__main__':
    # Get the port from the environment variable, default to 8080
    port = int(os.environ.get('PORT', 8080))
    # Run the Flask app
    app.run(host='0.0.0.0', port=port)
