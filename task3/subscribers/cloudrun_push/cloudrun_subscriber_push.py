import base64
import json
import os
import logging
from flask import Flask, request, jsonify
from google.cloud import bigquery
import uuid
from datetime import datetime
from google.cloud import secretmanager

# Configure standard logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Flask app
app = Flask(__name__)

# Initialize BigQuery client
bigquery_client = bigquery.Client()

# Initialize Secret Manager client
secretmanager_client = secretmanager.SecretManagerServiceClient()

# Get Google Cloud Project ID
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not PROJECT_ID:
    logging.error("GOOGLE_CLOUD_PROJECT environment variable not set.")

def access_secret(secret_id):
    """Access the latest version of a secret."""
    if not PROJECT_ID:
        logging.error(f"Cannot access secret '{secret_id}': PROJECT_ID is not set.")
        return None
    try:
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = secretmanager_client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}': {e}")
        return None

# Get BigQuery dataset and table IDs from Secret Manager
BIGQUERY_DATASET_ID = access_secret('bigquery_dataset_id')
BIGQUERY_VISITS_TABLE_ID = access_secret('bigquery_visits_table_id')
BIGQUERY_EVENTS_TABLE_ID = access_secret('bigquery_events_table_id')
BIGQUERY_PURCHASE_ITEMS_TABLE_ID = access_secret('bigquery_purchase_items_table_id')

if not BIGQUERY_DATASET_ID or not BIGQUERY_VISITS_TABLE_ID or not BIGQUERY_EVENTS_TABLE_ID or not BIGQUERY_PURCHASE_ITEMS_TABLE_ID:
    logging.error("BigQuery IDs not retrieved from Secret Manager.")

# Construct the full table IDs
VISITS_TABLE_REF = f"{BIGQUERY_DATASET_ID}.{BIGQUERY_VISITS_TABLE_ID}" if BIGQUERY_DATASET_ID and BIGQUERY_VISITS_TABLE_ID else None
EVENTS_TABLE_REF = f"{BIGQUERY_DATASET_ID}.{BIGQUERY_EVENTS_TABLE_ID}" if BIGQUERY_DATASET_ID and BIGQUERY_EVENTS_TABLE_ID else None
PURCHASE_ITEMS_TABLE_REF = f"{BIGQUERY_DATASET_ID}.{BIGQUERY_PURCHASE_ITEMS_TABLE_ID}" if BIGQUERY_DATASET_ID and BIGQUERY_PURCHASE_ITEMS_TABLE_ID else None


@app.route('/', methods=['POST'])
def index():
    """Processes Pub/Sub push messages containing session data."""
    request_json = request.get_json()
    if not request_json or 'message' not in request_json:
        logging.warning('Received invalid request: missing message')
        return 'Bad Request', 400

    pubsub_message = request_json['message']

    if 'data' not in pubsub_message:
        logging.warning('Received message without data')
        return 'OK', 200

    try:
        session_data_bytes = base64.b64decode(pubsub_message['data'])
        session_data_str = session_data_bytes.decode('utf-8')
        session_data = json.loads(session_data_str)

        session_id = session_data.get("session_id")
        user_id = session_data.get("user_id")
        device_type = session_data.get("device_type")
        geolocation = session_data.get("geolocation")
        user_agent = session_data.get("user_agent")
        events = session_data.get("events", [])

        visits_records = []
        events_records = []
        purchase_items_records = []

        logging.info(f"Processing session: {session_id} with {len(events)} events")

        # Extract data for the 'visits' table
        if session_id:
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
                start_timestamp = timestamps[0].isoformat() if timestamps[0] else None
                end_timestamp = timestamps[-1].isoformat() if timestamps[-1] else None

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
            visits_records.append(visit_record)
            logging.info(f"Prepared visit record for session: {session_id}")

        # Iterate through all events
        for event_entry in events:
            event_details = event_entry.get("event", {})
            event_type = event_details.get("event_type")
            timestamp = event_details.get("timestamp")
            details = event_details.get("details", {})

            if not event_type or not timestamp:
                 logging.warning(f"Event missing type or timestamp in session {session_id}, skipping: {event_entry}")
                 continue

            event_unique_id = str(uuid.uuid4())

            if event_type in ["page_view", "add_item_to_cart", "remove_item_from_cart", "view_item", "promo_view", "promo_click"]:
                record = {
                    "session_id": session_id,
                    "event_unique_id": event_unique_id,
                    "event_type": event_type,
                    "timestamp": timestamp,
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
                    "device_type": device_type,
                    "user_id": user_id,
                }
                events_records.append(record)
                logging.info(f"Processed {event_type}: Session={session_id}, Timestamp={timestamp}")

            elif event_type == "purchase":
                order_id = details.get("order_id")
                purchase_items = details.get("items", [])

                if not order_id:
                    logging.warning(f"Purchase event in session {session_id} missing order_id, skipping items: {event_entry}")
                    continue

                logging.info(f"Processing purchase: Session={session_id}, Order={order_id}, Timestamp={timestamp} with {len(purchase_items)} items")

                for item_index, item in enumerate(purchase_items):
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
                    if item_record.get("product_id") is None or item_record.get("order_id") is None or item_record.get("session_id") is None or item_record.get("item_index_in_order") is None:
                         logging.warning(f"Purchase item missing REQUIRED fields in order {order_id}, session {session_id}, skipping: {item}")
                         continue

                    purchase_items_records.append(item_record)
                    logging.info(f"  - Processed purchase item: Order={order_id}, Product={item.get('product_id')}")

            else:
                logging.warning(f"Unknown event type '{event_type}' in session {session_id}, skipping: {event_entry}")

        # Insert records into BigQuery
        if visits_records:
            if not VISITS_TABLE_REF:
                 logging.error("BigQuery visits table info missing. Cannot insert visits data.")
            else:
                errors = bigquery_client.insert_rows_json(VISITS_TABLE_REF, visits_records)
                if errors:
                    logging.error(f"Errors inserting {len(visits_records)} visits: {errors}")
                else:
                    logging.info(f"Successfully inserted {len(visits_records)} visits.")

        if events_records:
            if not EVENTS_TABLE_REF:
                 logging.error("BigQuery events table info missing. Cannot insert events data.")
            else:
                errors = bigquery_client.insert_rows_json(EVENTS_TABLE_REF, events_records)
                if errors:
                    logging.error(f"Errors inserting {len(events_records)} events: {errors}")
                else:
                    logging.info(f"Successfully inserted {len(events_records)} events.")

        if purchase_items_records:
            if not PURCHASE_ITEMS_TABLE_REF:
                 logging.error("BigQuery purchase items table info missing. Cannot insert purchase items data.")
            else:
                errors = bigquery_client.insert_rows_json(PURCHASE_ITEMS_TABLE_REF, purchase_items_records)
                if errors:
                    logging.error(f"Errors inserting {len(purchase_items_records)} purchase items: {errors}")
                else:
                    logging.info(f"Successfully inserted {len(purchase_items_records)} purchase items.")

    except Exception as e:
        logging.error(f"Error processing Pub/Sub message: {e}", exc_info=True)
        return 'Internal Server Error', 500

    return 'OK', 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)