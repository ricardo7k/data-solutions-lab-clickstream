import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
import time
import threading # To run the health check server concurrently

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DEBUGGING: Log script start ---
logger.info("--- Pub/Sub Pull Subscriber script starting ---")

# Import the Pub/Sub client library
from google.cloud import pubsub_v1
# Import the BigQuery client library
from google.cloud import bigquery
# Import the Secret Manager client library
from google.cloud import secretmanager

# Import Flask for the health check web server
from flask import Flask, request, jsonify, Response # Import Response

# --- Secret Manager Configuration ---
# Define the names of the secrets in Secret Manager
# These secrets should store the configuration values
logger.info("Conatiner Begin")

YOUR_PROJECT_NUMBER=345953109572
PROJECT_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/GCP_PROJECT/versions/latest"
PUBSUB_SUBSCRIPTION_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/PUBSUB_SUBSCRIPTION_ID/versions/latest"
BIGQUERY_DATASET_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/BIGQUERY_DATASET_ID/versions/latest"
BIGQUERY_VISITS_TABLE_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/BIGQUERY_VISITS_TABLE_ID/versions/latest"
BIGQUERY_EVENTS_TABLE_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/BIGQUERY_EVENTS_TABLE_ID/versions/latest"
BIGQUERY_PURCHASE_ITEMS_TABLE_ID_SECRET_NAME = f"projects/{YOUR_PROJECT_NUMBER}/secrets/BIGQUERY_PURCHASE_ITEMS_TABLE_ID/versions/latest"

# --- Secret Manager Client Initialization ---
secret_manager_client = secretmanager.SecretManagerServiceClient()

def access_secret_version(secret_version_name):
    """Access the payload for the given secret version."""
    try:
        response = secret_manager_client.access_secret_version(name=secret_version_name)
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        logger.error(f"Failed to access secret {secret_version_name}: {e}")
        # Re-raise the exception as configuration is critical
        raise

# --- Retrieve Configuration from Secret Manager ---
logger.info("Retrieving configuration from Secret Manager...")
try:
    PROJECT_ID = access_secret_version(PROJECT_ID_SECRET_NAME)
    PUBSUB_SUBSCRIPTION_ID = access_secret_version(PUBSUB_SUBSCRIPTION_ID_SECRET_NAME)
    BIGQUERY_DATASET_ID = access_secret_version(BIGQUERY_DATASET_ID_SECRET_NAME)
    BIGQUERY_VISITS_TABLE_ID = access_secret_version(BIGQUERY_VISITS_TABLE_ID_SECRET_NAME)
    BIGQUERY_EVENTS_TABLE_ID = access_secret_version(BIGQUERY_EVENTS_TABLE_ID_SECRET_NAME)
    BIGQUERY_PURCHASE_ITEMS_TABLE_ID = access_secret_version(BIGQUERY_PURCHASE_ITEMS_TABLE_ID_SECRET_NAME)

    logger.info("Configuration retrieved successfully.")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Pub/Sub Subscription ID: {PUBSUB_SUBSCRIPTION_ID}")
    logger.info(f"BigQuery Dataset ID: {BIGQUERY_DATASET_ID}")
    logger.info(f"BigQuery Visits Table ID: {BIGQUERY_VISITS_TABLE_ID}")
    logger.info(f"BigQuery Events Table ID: {BIGQUERY_EVENTS_TABLE_ID}")
    logger.info(f"BigQuery Purchase Items Table ID: {BIGQUERY_PURCHASE_ITEMS_TABLE_ID}")


except Exception as e:
    logger.error(f"Failed to load configuration from Secret Manager: {e}")
    exit(1) # Exit if configuration loading fails

# Construct the full resource paths
PUBSUB_SUBSCRIPTION_PATH = f'projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}'
BIGQUERY_VISITS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_VISITS_TABLE_ID}'
BIGQUERY_EVENTS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_EVENTS_TABLE_ID}'
BIGQUERY_PURCHASE_ITEMS_TABLE_SPEC = f'{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_PURCHASE_ITEMS_TABLE_ID}'


# --- Pub/Sub Subscriber Client Initialization ---
# Initialize the subscriber client
subscriber = pubsub_v1.SubscriberClient()


# --- BigQuery Client Initialization ---
# Initialize the BigQuery client
bq_client = bigquery.Client()

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
        # For a pull subscriber, decide how to handle errors.
        # Re-raising here would likely cause the process to crash,
        # leading to MIG restart and message redelivery.
        # A more graceful approach might be to log and NOT acknowledge the message,
        # allowing Pub/Sub to redeliver after the ack deadline.
        # For this example, we'll log and let the message be redelivered.
        pass # Do not re-raise the exception


# --- Pub/Sub Pull Subscriber Logic ---

def process_message(message):
    """Processes a single Pub/Sub message."""
    logger.info(f"Received message: {message.message_id}")

    try:
        # Decode the message data directly from message.data for pull messages
        # message.data is already bytes, decode it to string
        data = message.data.decode('utf-8')
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

        logger.info(f"Processed message: {message.message_id}")

        # Acknowledge the message if processing and insertion were successful
        # If insert_into_bigquery raised an exception and we re-raised it,
        # this line would not be reached, and Pub/Sub would redeliver.
        message.ack()
        logger.info(f"Acknowledged message: {message.message_id}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in message {message.message_id}. Nacking message.")
        # Nack the message to indicate it cannot be processed successfully
        message.nack()
    except Exception as e:
        logger.error(f"An error occurred processing message {message.message_id}: {e}")
        # Do NOT acknowledge the message. Pub/Sub will redeliver after ack deadline.
        pass # Let the message be redelivered

def run_pull_subscriber():
    """Runs the Pub/Sub pull subscriber loop."""
    logger.info(f"Starting Pub/Sub pull subscriber for subscription: {PUBSUB_SUBSCRIPTION_PATH}")

    # The subscriber client is asynchronous. The pull method returns a Future.
    # Use a StreamingPullFuture for long-lived connections.
    # The callback is executed on a thread pool.
    streaming_pull_future = subscriber.subscribe(PUBSUB_SUBSCRIPTION_PATH, callback=process_message)

    logger.info("Listening for messages.") # Removed "Press Ctrl+C to exit." as it's for interactive use

    try:
        # The subscribe method is non-blocking. The future keeps the connection open.
        # Call result() to block the main thread and keep the script running.
        # This will run until the future is cancelled (e.g., by KeyboardInterrupt or other signal)
        streaming_pull_future.result()
    except KeyboardInterrupt:
        # Stop the subscriber when Ctrl+C is pressed (less likely in a container, but good practice)
        streaming_pull_future.cancel()
        # streaming_pull_future.result() # Removed waiting for result after cancel
        logger.info("Pull subscriber stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.error(f"An error occurred in the streaming pull future: {e}")
        streaming_pull_future.cancel()
        # For a MIG, exiting will cause the VM to be restarted by the MIG.
        exit(1) # Exit with a non-zero status to indicate failure


# --- Health Check Server ---
# # Create a Flask app instance for the health check
# health_app = Flask("health_checker")

# @health_app.route('/healthz', methods=['GET'])
# def healthz():
#     """Simple health check endpoint."""
#     # You can add more sophisticated checks here, e.g., check Pub/Sub connection, BigQuery connection
#     # For a basic check, just returning 200 OK is sufficient to indicate the process is running.
#     return Response("OK", status=200, mimetype='text/plain')

# def run_health_check_server():
#     """Runs the Flask health check server."""
#     # Get the port from the environment variable, default to 8080
#     # MIG health checks typically probe port 8080 by default for containerized apps
#     port = int(os.environ.get('PORT', 8080))
#     logger.info(f"Starting health check server on port {port}")
#     # Use a production-ready WSGI server like Waitress in a real deployment
    # For this example, Flask's built-in server is sufficient
#     health_app.run(host='0.0.0.0', port=port)


# --- Entry Point ---
if __name__ == "__main__":
    # Run the health check server in a separate thread
    # health_thread = threading.Thread(target=run_health_check_server)
    # health_thread.daemon = True # Allow the main thread to exit even if the health thread is running
    # health_thread.start()
    # logger.info("Health check server thread started.")

    # Run the main pull subscriber logic in the main thread
    run_pull_subscriber()

    # The main thread will block on streaming_pull_future.result()
    # When it exits (e.g., due to an error or signal), the daemon health thread will also exit.
