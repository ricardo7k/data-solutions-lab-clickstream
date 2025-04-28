import json
import time
import random
import uuid
import logging
import argparse
from datetime import datetime, timezone, timedelta

# Import the Pub/Sub client library
from google.cloud import pubsub_v1

# --- Configuration Variables ---
# Replace with your actual Google Cloud Project ID and Pub/Sub Topic ID
PROJECT_ID = "data-solutions-lab"
TOPIC_ID = "website-visits-topic" # Choose a name for your Pub/Sub topic

# Simulation control variables
VISITS_PER_MINUTE = 60 # Number of visits to simulate per minute
SIMULATION_DURATION_MINUTES = 5 # How long the simulator should run

# --- Pub/Sub Publisher Client Initialization ---
# Initialize the publisher client
publisher = pubsub_v1.PublisherClient()
# The topic path is in the format projects/PROJECT_ID/topics/TOPIC_ID
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# --- Data Generation Functions ---

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
        "price": product_info["price"],
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
            # If the timestamp datetime object is timezone-aware and in UTC, 
            # timestamp.isoformat(timespec='seconds') might already produce 
            # a string ending in +00:00 (e.g., 2025-04-27T14:34:22+00:00). 
            # Appending 'Z' to this results in the invalid format 
            # 2025-04-27T14:34:22+00:00Z.
            # Generate standard ISO 8601 format without manually appending 'Z'
            # BigQuery's TIMESTAMP type accepts standard ISO 8601 formats like 
            # YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DDTHH:MM:SS+HH:MM. 
            # The extra Z after the timezone offset could cause some issues.
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
        "tablet": "Mozilla/5.0 (iPad; CPU OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1"
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

# --- Simulator Logic ---

def publish_visit(visit_data):
    """Publishes a single visit dictionary as a Pub/Sub message."""
    try:
        # Convert the dictionary to a JSON string, then encode to bytes
        data_bytes = json.dumps(visit_data).encode("utf-8")

        # Publish the message
        future = publisher.publish(topic_path, data_bytes)

        # Futures.add_done_callback() can be used for asynchronous handling
        # For a simple simulator, waiting on the future confirms publication
        # Note: Waiting can slow down the simulator if publishing is slow
        # future.result() # Uncomment to wait for each message publication confirmation

        # Log successful publication (optional, can be noisy)
        # logging.info(f"Published message for session: {visit_data.get('session_id')}")

    except Exception as e:
        # Log errors during publication
        logging.error(f"Failed to publish message for session {visit_data.get('session_id')}: {e}")


def run_simulator(visits_per_minute, simulation_duration_minutes, topic_id, project_id):
    """Runs the visit data simulator."""
    PROJECT_ID = project_id
    TOPIC_ID = topic_id
    messages_per_second = visits_per_minute / 60.0
    total_messages_to_send = visits_per_minute * simulation_duration_minutes
    send_interval_seconds = 1.0 / messages_per_second if messages_per_second > 0 else 0

    logging.info(f"Starting simulator for {simulation_duration_minutes} minutes, sending {visits_per_minute} visits/minute ({messages_per_second:.2f} visits/second).")
    logging.info(f"Targeting Pub/Sub topic: {topic_path}")

    start_time = time.time()
    messages_sent = 0

    while time.time() - start_time < simulation_duration_minutes * 60:
        if messages_sent >= total_messages_to_send and simulation_duration_minutes > 0:
             logging.info("Total messages for duration sent, stopping.")
             break

        visit_data = generate_visit()
        publish_visit(visit_data)
        messages_sent += 1

        # Calculate time to wait to maintain the desired rate
        elapsed_time = time.time() - start_time
        expected_messages_sent = elapsed_time * messages_per_second
        messages_behind = messages_sent - expected_messages_sent

        if send_interval_seconds > 0 and messages_behind < 1: # Only sleep if we are not significantly behind
             time_to_sleep = send_interval_seconds - (time.time() - start_time - (messages_sent - 1) * send_interval_seconds)
             if time_to_sleep > 0:
                 time.sleep(time_to_sleep)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Simulator finished after {duration:.2f} seconds. Total messages sent: {messages_sent}")

# --- Entry Point ---
if __name__ == "__main__":
    # You can modify the variables directly above or use command-line arguments here
    parser = argparse.ArgumentParser()
    parser.add_argument('--visits_per_minute', type=int, default=60, help='Visits to simulate per minute')
    parser.add_argument('--duration_minutes', type=int, default=5, help='Simulation duration in minutes')
    parser.add_argument('--project_id', type=str, default=PROJECT_ID, help='Google Cloud Project ID')
    parser.add_argument('--topic_id', type=str, default=TOPIC_ID, help='Pub/Sub Topic ID')
    args = parser.parse_args()
    run_simulator(args.visits_per_minute, args.duration_minutes, args.topic_id, args.project_id)

    # Run with predefined variables
    # run_simulator(VISITS_PER_MINUTE, SIMULATION_DURATION_MINUTES)
    # pubsub_simulator.py
    # --visits_per_minute=50/
    # --duration_minutes=1/
    # --project_id=data-solutions-lab/
    # --topic_id=website-visits-topic