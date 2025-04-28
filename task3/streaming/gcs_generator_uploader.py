import os
import time
import logging
import argparse
import json
import uuid
import random # Import the random module
from datetime import datetime, timezone, timedelta
import tempfile # Para criar arquivos temporários

# Import the Google Cloud Storage client library
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- GCS Client Initialization ---
# Initialize the GCS client
# The client library will automatically find credentials in GCP environments
# or use Application Default Credentials locally.
storage_client = storage.Client()

# --- Data Generation Functions (Copied from Pub/Sub Simulator) ---

def generate_page_view_details():
    """Generates random details for a page_view event."""
    pages = [
        "https://example.com/home",
        "https://example.com/products",
        "https://example.com/products/item1",
        "https://example.com/products/item2",
        "https://example.com/cart",
        "https://example.com/checkout",
        "https://example.com/about",
        "https://example.com/contact"
    ]
    referrers = [
        None, # Direct traffic
        "https://google.com",
        "https://bing.com",
        "https://facebook.com",
        "https://twitter.com",
        "https://example.com/home",
        "https://example.com/products"
    ]
    return {
        "page_url": random.choice(pages),
        "referrer_url": random.choice(referrers)
    }

def generate_add_item_to_cart_details():
    """Generates random details for an add_item_to_cart event."""
    products = {
        "HDW-001": {"name": "Laptop X200", "category": "hardware", "price": 999.99},
        "HDW-002": {"name": "Monitor 4K", "category": "hardware", "price": 349.50},
        "SFT-101": {"name": "Productivity Suite", "category": "software", "price": 149.00},
        "PER-201": {"name": "Wireless Mouse", "category": "peripherals", "price": 25.00},
        "PER-202": {"name": "Mechanical Keyboard", "category": "peripherals", "price": 75.00}
    }
    product_id = random.choice(list(products.keys()))
    product_info = products[product_id]
    return {
        "product_id": product_id,
        "product_name": product_info["name"],
        "category": product_info["category"],
        "price": product_info["price"], # Price per unit
        "quantity": random.randint(1, 3) # Add 1 to 3 items
    }

def generate_purchase_details():
    """Generates random details for a purchase event."""
    products = {
        "HDW-001": {"name": "Laptop X200", "category": "hardware", "price": 999.99},
        "HDW-002": {"name": "Monitor 4K", "category": "hardware", "price": 349.50},
        "SFT-101": {"name": "Productivity Suite", "category": "software", "price": 149.00},
        "PER-201": {"name": "Wireless Mouse", "category": "peripherals", "price": 25.00},
        "PER-202": {"name": "Mechanical Keyboard", "category": "peripherals", "price": 75.00}
    }
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    num_items = random.randint(1, 4)
    items = []
    total_amount = 0
    for _ in range(num_items):
        product_id = random.choice(list(products.keys()))
        product_info = products[product_id]
        quantity = random.randint(1, 2)
        item_price = round(product_info["price"] * quantity, 2)
        items.append({
            "product_id": product_id,
            "product_name": product_info["name"],
            "category": product_info["category"],
            "price": product_info["price"], # Price per unit
            "quantity": quantity
        })
        total_amount += item_price

    return {
        "order_id": order_id,
        "amount": round(total_amount, 2),
        "currency": "USD",
        "items": items
    }


def generate_event(event_type, timestamp):
    """Generates a single event dictionary."""
    details = {}
    if event_type == "page_view":
        details = generate_page_view_details()
    elif event_type == "add_item_to_cart":
        details = generate_add_item_to_cart_details()
    elif event_type == "purchase":
        details = generate_purchase_details()
    else:
        # Should not happen with defined types, but handle defensively
        logging.warning(f"Unknown event type: {event_type}")
        return None

    # Structure matches the sample JSONL provided earlier
    return {
        "event": {
            "event_type": event_type,
            # Generate standard ISO 8601 format
            "timestamp": timestamp.isoformat(timespec='seconds'),
            "details": details
        }
    }

def generate_visit():
    """Generates a single synthetic visit dictionary."""
    session_id = f"SID-{uuid.uuid4().hex[:8].upper()}"
    user_id = f"UID-{random.randint(1000, 9999)}" # Simple user ID

    device_types = ["desktop", "mobile", "tablet"]
    device_type = random.choice(device_types)

    # Simple random geolocation (within a plausible range)
    latitude = round(random.uniform(-90, 90), 6)
    longitude = round(random.uniform(-180, 180), 6)
    geolocation = f"{latitude},{longitude}"

    # Simplified user agents
    user_agents = {
        "desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "mobile": "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
        "tablet": "Mozilla/5.0 (iPad; CPU OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    }
    user_agent = user_agents.get(device_type, user_agents["desktop"]) # Default to desktop if type is unexpected

    # Simulate events within a visit
    # A visit must have at least one page view
    event_types_sequence = ["page_view"]
    # Add more events randomly
    possible_follow_on_events = ["page_view", "add_item_to_cart"]
    for _ in range(random.randint(0, 5)): # 0 to 5 additional events
        event_types_sequence.append(random.choice(possible_follow_on_events))

    # Add a purchase event sometimes, usually after adding an item to cart
    if "add_item_to_cart" in event_types_sequence and random.random() < 0.3: # 30% chance of purchase after adding item
         event_types_sequence.append("purchase")
    elif random.random() < 0.05: # 5% chance of purchase even without explicit add_item
         event_types_sequence.append("purchase")


    # Simulate timestamps within the visit (within a short duration, e.g., 5 minutes)
    start_time = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 10)) # Visits in the last 10 mins
    event_timestamps = [start_time + timedelta(seconds=random.randint(0, 300)) for _ in range(len(event_types_sequence))]
    event_timestamps.sort() # Ensure events are in chronological order

    events = []
    for i, event_type in enumerate(event_types_sequence):
        event = generate_event(event_type, event_timestamps[i])
        if event:
            events.append(event)

    return {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": device_type,
        "geolocation": geolocation,
        "user_agent": user_agent,
        "events": events
    }


# --- Data Generation and Upload Logic ---

def generate_and_upload_file(bucket_name, destination_prefix, num_visits_per_file):
    """
    Generates a batch of visit data, writes it to a temporary file,
    and uploads the file to GCS.

    Args:
        bucket_name (str): The GCS bucket name.
        destination_prefix (str): The GCS folder path to upload files into (e.g., 'incoming_data/').
        num_visits_per_file (int): The number of synthetic visits to generate for this file.

    Returns:
        bool: True if upload was successful, False otherwise.
    """
    logger.info(f"Generating {num_visits_per_file} visits for a new file...")
    visit_data_lines = []
    for _ in range(num_visits_per_file):
        visit = generate_visit()
        visit_data_lines.append(json.dumps(visit)) # Get JSON string

    # Join lines with newline character for JSONL format
    file_content = "\n".join(visit_data_lines)

    # Create a temporary local file
    # Use delete=False so the file is not deleted immediately after closing
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".jsonl") as tmp_file:
        tmp_file.write(file_content)
        tmp_file_path = tmp_file.name # Get the path to the temporary file

    logger.info(f"Generated data written to temporary file: {tmp_file_path}")

    # Construct the destination blob name using a timestamp for uniqueness
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    destination_blob_name = os.path.join(destination_prefix, f"visits-{timestamp_str}-{uuid.uuid4().hex[:6]}.jsonl")


    # Upload the temporary file to GCS
    logger.info(f"Attempting to upload {tmp_file_path} to gs://{bucket_name}/{destination_blob_name}")

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        blob.upload_from_filename(tmp_file_path)

        logger.info(f"File {destination_blob_name} uploaded successfully.")
        upload_success = True

    except Exception as e:
        logger.error(f"Failed to upload {tmp_file_path} to gs://{bucket_name}/{destination_blob_name}: {e}")
        upload_success = False

    finally:
        # Clean up the temporary local file
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
            logger.info(f"Temporary file removed: {tmp_file_path}")

    return upload_success


def run_generator_uploader(bucket_name, destination_prefix, visits_per_file, upload_interval_seconds, simulation_duration_minutes):
    """
    Runs the data generator and uploader simulation.
    Generates and uploads files at regular intervals for a set duration.

    Args:
        bucket_name (str): The GCS bucket name.
        destination_prefix (str): The GCS folder path to upload files into (e.g., 'incoming_data/').
        visits_per_file (int): The number of synthetic visits to generate for each file.
        upload_interval_seconds (int): Time to wait between uploads in seconds.
        simulation_duration_minutes (int): How long the simulation should run in minutes.
    """
    logger.info(f"Starting GCS data generation and upload simulation...")
    logger.info(f"Target GCS bucket: {bucket_name}")
    logger.info(f"Target GCS prefix: {destination_prefix}")
    logger.info(f"Visits per file: {visits_per_file}")
    logger.info(f"Upload interval: {upload_interval_seconds} seconds")
    logger.info(f"Simulation duration: {simulation_duration_minutes} minutes")

    start_time = time.time()
    uploads_count = 0

    while time.time() - start_time < simulation_duration_minutes * 60:
        logger.info(f"--- Starting upload cycle {uploads_count + 1} ---")
        success = generate_and_upload_file(bucket_name, destination_prefix, visits_per_file)

        if success:
            uploads_count += 1
            logger.info(f"Upload cycle {uploads_count} completed successfully.")
        else:
            logger.warning(f"Upload cycle {uploads_count + 1} failed.")
            # Decide how to handle failures in simulation (retry, skip, stop)
            # For this simulation, we log and continue

        # Wait for the specified interval
        if time.time() - start_time < simulation_duration_minutes * 60: # Only wait if simulation is not over
             logger.info(f"Waiting for {upload_interval_seconds} seconds until next upload cycle...")
             time.sleep(upload_interval_seconds)


    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Simulation finished after {duration:.2f} seconds. Total files uploaded: {uploads_count}")


# --- Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates synthetic visit data and uploads files to Google Cloud Storage at intervals.")
    parser.add_argument(
        '--bucket_name',
        dest='bucket_name',
        required=True,
        help='The name of the GCS bucket to upload files to.')
    parser.add_argument(
        '--destination_prefix',
        dest='destination_prefix',
        required=True,
        help='The GCS folder path to upload files into (e.g., incoming_data/).')
    parser.add_argument(
        '--visits_per_file',
        dest='visits_per_file',
        type=int,
        default=100, # Default number of visits per generated file
        help='The number of synthetic visits to generate for each file. Defaults to 100.')
    parser.add_argument(
        '--upload_interval',
        dest='upload_interval',
        type=int,
        default=60, # Default to 60 seconds (1 minute)
        help='Time to wait between file uploads (in seconds). Defaults to 60.')
    parser.add_argument(
        '--simulation_duration',
        dest='simulation_duration',
        type=int,
        default=10, # Default to 10 minutes
        help='How long the simulation should run (in minutes). Defaults to 10.')


    args = parser.parse_args()

    run_generator_uploader(
        args.bucket_name,
        args.destination_prefix,
        args.visits_per_file,
        args.upload_interval,
        args.simulation_duration
    )

