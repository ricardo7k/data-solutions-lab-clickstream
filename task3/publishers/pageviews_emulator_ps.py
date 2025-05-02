import json
import random
import time
from datetime import datetime, timedelta
import uuid
import argparse
import sys
import logging
from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO)

# Sample data for synthetic generation
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGE_URLS = [
    "https://example.com/home",
    "https://example.com/products",
    "https://example.com/cart",
    "https://example.com/checkout",
    "https://example.com/confirmation",
    "https://example.com/about",
    "https://example.com/contact"
]
PRODUCT_CATALOG = {
    "HDW-001": {"name": "Laptop X200", "category": "hardware", "price": 999.99},
    "HDW-002": {"name": "Desktop Z500", "category": "hardware", "price": 1299.99},
    "HDW-003": {"name": "Gaming PC Y900", "category": "hardware", "price": 1899.99},
    "HDW-004": {"name": "Ultrabook A400", "category": "hardware", "price": 1199.99},
    "HDW-005": {"name": "Workstation Pro 9000", "category": "hardware", "price": 2599.99},
    "HDW-006": {"name": "Mini PC Cube", "category": "hardware", "price": 699.99},
    "PER-001": {"name": "Wireless Mouse", "category": "peripherals", "price": 29.99},
    "PER-002": {"name": "Mechanical Keyboard", "category": "peripherals", "price": 89.99},
    "PER-003": {"name": "27\" 4K Monitor", "category": "peripherals", "price": 399.99},
    "PER-004": {"name": "USB-C Docking Station", "category": "peripherals", "price": 129.99},
    "PER-005": {"name": "Noise Cancelling Headphones", "category": "peripherals", "price": 199.99},
    "PER-006": {"name": "Webcam HD 1080p", "category": "peripherals", "price": 49.99},
    "SFT-001": {"name": "Office Suite Pro", "category": "software", "price": 199.99},
    "SFT-002": {"name": "Antivirus Shield", "category": "software", "price": 49.99},
    "SFT-003": {"name": "Photo Editor Pro", "category": "software", "price": 79.99},
    "SFT-004": {"name": "Project Manager Plus", "category": "software", "price": 299.99},
    "SFT-005": {"name": "Video Editor Pro", "category": "software", "price": 149.99},
    "SFT-006": {"name": "Music Studio 2024", "category": "software", "price": 89.99},
}
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15"
]
CURRENCIES = ["USD"]

# --- Data Generation Functions ---
def generate_synthetic_session_data(session_id, user_id, start_time):
    """Generates synthetic data for a single session."""
    device_type = random.choice(DEVICE_TYPES)
    geolocation = f"{random.uniform(-90, 90):.6f},{random.uniform(-180, 180):.6f}"
    user_agent = random.choice(USER_AGENTS)

    session_data = {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": device_type,
        "geolocation": geolocation,
        "user_agent": user_agent,
        "events": []
    }

    # Simulate a sequence of events within the session
    current_time = start_time
    num_events = random.randint(1, 15)

    # Always start with a page view on home or products
    initial_page = random.choice(["https://example.com/home", "https://example.com/products"])
    session_data["events"].append(generate_page_view_event(current_time, initial_page, None))
    current_time += timedelta(seconds=random.randint(5, 60))

    last_page = initial_page

    for i in range(num_events - 1):
        # Ensure timestamps are sequential within a session
        event_timestamp = current_time + timedelta(seconds=random.randint(5, 120))
        current_time = event_timestamp

        event_type = random.choices(
            ["page_view", "add_item_to_cart", "purchase"],
            weights=[0.6, 0.3, 0.1],
            k=1
        )[0]

        event_details = {}
        if event_type == "page_view":
            page_url = random.choice(PAGE_URLS)
            referrer_url = last_page if random.random() > 0.2 else random.choice(PAGE_URLS + [None])
            event_details = generate_page_view_event_details(page_url, referrer_url)
            last_page = page_url
        elif event_type == "add_item_to_cart":
            product_id, product_info = random.choice(list(PRODUCT_CATALOG.items()))
            quantity = random.randint(1, 5)
            event_details = generate_add_item_to_cart_details(product_id, product_info, quantity)
        elif event_type == "purchase":
            # Simulate adding some items before purchase if cart is empty or has few items
            current_cart_items = [
                e.get("event", {}).get("details", {})
                for e in session_data["events"]
                if e.get("event", {}).get("event_type") == "add_item_to_cart"
            ]
            if len(current_cart_items) < random.randint(1, 3):
                 for _ in range(random.randint(1, 3 - len(current_cart_items))):
                     product_id, product_info = random.choice(list(PRODUCT_CATALOG.items()))
                     quantity = random.randint(1, 3)
                     # Add item event before the purchase event timestamp
                     item_add_timestamp = event_timestamp - timedelta(seconds=random.randint(5, 30))
                     session_data["events"].append(generate_add_item_to_cart_event(item_add_timestamp, product_id, product_info, quantity))

            # Collect items added to cart in this session (simple approach)
            items_in_cart = []
            for event_entry in session_data["events"]:
                if event_entry.get("event", {}).get("event_type") == "add_item_to_cart":
                     details = event_entry.get("event", {}).get("details", {})
                     # Create a simplified item structure for the purchase event
                     items_in_cart.append({
                         "product_id": details.get("product_id"),
                         "product_name": details.get("product_name"),
                         "category": details.get("category"),
                         "price": details.get("price"),
                         "quantity": details.get("quantity"),
                     })

            if items_in_cart:
                order_id = f"ORD-{random.randint(1000, 9999)}"
                currency = random.choice(CURRENCIES)
                # Calculate total amount from items
                amount = sum((item.get("price", 0) or 0) * (item.get("quantity", 0) or 0) for item in items_in_cart)
                event_details = generate_purchase_details(order_id, amount, currency, items_in_cart)
            else:
                # If no items were added to cart, skip the purchase event
                continue

        # Add the generated event to the session's events list
        if event_details:
             session_data["events"].append({
                 "event": {
                     "event_type": event_type,
                     "timestamp": event_timestamp.isoformat(timespec='seconds'),
                     "details": event_details
                 }
             })

    # Sort events by timestamp before returning the session data
    session_data["events"].sort(key=lambda x: x["event"]["timestamp"])

    return session_data

def generate_page_view_event(timestamp, page_url, referrer_url):
     """Helper to format a page_view event."""
     return {
         "event": {
             "event_type": "page_view",
             "timestamp": timestamp.isoformat(timespec='seconds'),
             "details": generate_page_view_event_details(page_url, referrer_url)
         }
     }

def generate_page_view_event_details(page_url, referrer_url):
     """Helper to generate page_view details."""
     return {
         "page_url": page_url,
         "referrer_url": referrer_url
     }

def generate_add_item_to_cart_event(timestamp, product_id, product_info, quantity):
     """Helper to format an add_item_to_cart event."""
     return {
         "event": {
             "event_type": "add_item_to_cart",
             "timestamp": timestamp.isoformat(timespec='seconds'),
             "details": generate_add_item_to_cart_details(product_id, product_info, quantity)
         }
     }

def generate_add_item_to_cart_details(product_id, product_info, quantity):
     """Helper to generate add_item_to_cart details."""
     return {
         "product_id": product_id,
         "product_name": product_info["name"],
         "category": product_info["category"],
         "price": product_info["price"],
         "quantity": quantity,
     }

def generate_purchase_details(order_id, amount, currency, items):
     """Helper to generate purchase details."""
     # Ensure items in purchase details match the schema
     formatted_items = []
     for item in items:
         formatted_items.append({
             "product_id": item.get("product_id"),
             "product_name": item.get("product_name"),
             "category": item.get("category"),
             "price": item.get("price"),
             "quantity": item.get("quantity"),
         })

     return {
         "order_id": order_id,
         "amount": round(amount, 2),
         "currency": currency,
         "items": formatted_items
     }


# --- Pub/Sub Publishing ---
def publish_message(publisher, topic_path, message_data):
    """Publishes a single message to Pub/Sub."""
    try:
        data = json.dumps(message_data).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result()
        logging.info(f"Published message for session_id: {message_data.get('session_id')}")
    except Exception as e:
        logging.error(f"Failed to publish message for session_id {message_data.get('session_id')}: {e}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generates synthetic e-commerce clickstream data and publishes to Pub/Sub."
    )
    parser.add_argument(
        "--topic_id",
        dest="topic_id",
        required=True,
        help="Your Pub/Sub topic ID (e.g., ecommerce-clickstream-events)"
    )
    parser.add_argument(
        "--visits_per_minute",
        dest="visits_per_minute",
        type=int,
        default=10,
        help="Number of synthetic sessions to generate and publish per minute."
    )
    parser.add_argument(
        "--duration_minutes",
        dest="duration_minutes",
        type=int,
        default=5,
        help="Duration in minutes for the simulator to run."
    )
    parser.add_argument(
        "--start_datetime",
        dest="start_datetime",
        help="Start datetime for timestamps in YYYY-MM-DDTHH:MM:SS format (defaults to now)."
    )
    parser.add_argument(
        "--project_id",
        dest="project_id",
        required=True,
        help="Project ID"
    )

    args = parser.parse_args(argv)

    topic_id = args.topic_id
    visits_per_minute = args.visits_per_minute
    duration_minutes = args.duration_minutes
    project_id = args.project_id

    if visits_per_minute <= 0 or duration_minutes <= 0:
        print("Error: visits_per_minute and duration_minutes must be positive integers.")
        sys.exit(1)

    # Calculate total number of sessions and interval between sessions
    total_sessions_to_generate = visits_per_minute * duration_minutes
    interval_seconds = 60.0 / visits_per_minute if visits_per_minute > 0 else 0

    if args.start_datetime:
        try:
            # Allow parsing ISO 8601 format including time
            start_time_obj = datetime.fromisoformat(args.start_datetime.replace('Z', '+00:00'))
        except ValueError:
            print(f"Error: Invalid start_datetime format. Please use YYYY-MM-DDTHH:MM:SS (e.g., 2024-07-01T09:00:00).")
            sys.exit(1)
    else:
        start_time_obj = datetime.now()

    # Create a Pub/Sub publisher client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    print(f"Topic path: {topic_path}")

    logging.info(f"Starting synthetic data generation and publishing to topic: {topic_path}")
    logging.info(f"Generating approximately {visits_per_minute} sessions per minute for {duration_minutes} minutes.")
    logging.info(f"Total sessions to generate: {total_sessions_to_generate}")
    logging.info(f"Session start times will begin around: {start_time_obj.isoformat(timespec='seconds')}")
    logging.info(f"Interval between sessions: {interval_seconds:.2f} seconds")

    current_session_start_time = start_time_obj

    # Generate and publish sessions based on rate and duration
    for i in range(total_sessions_to_generate):
        session_id = f"SID-{random.randint(1000, 9999)}-{i}" # Add index for uniqueness
        user_id = f"UID-{random.randint(1000, 9999)}"

        synthetic_session = generate_synthetic_session_data(session_id, user_id, current_session_start_time)
        publish_message(publisher, topic_path, synthetic_session)

        # Advance the start time for the next session based on the interval
        current_session_start_time += timedelta(seconds=interval_seconds)

        # Add a small delay to simulate the rate
        if interval_seconds > 0:
             time.sleep(interval_seconds)


    logging.info("Finished synthetic data generation and publishing.")


if __name__ == "__main__":
    main(sys.argv[1:])
