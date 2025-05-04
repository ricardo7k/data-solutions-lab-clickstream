import os
import argparse
from google.cloud import bigquery

# Define the schema for the visits table
visits_schema_fields = [
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("geolocation", "STRING"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("end_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("event_count", "INTEGER"),
]

# Define the schema for the events table
events_schema_fields = [
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("event_unique_id", "STRING", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
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

# Define the schema for the purchase_items table
purchase_items_schema_fields = [
    bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("item_index_in_order", "INTEGER", mode="REQUIRED"), # Part of Composite PK
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"), # FK for convenience
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("quantity", "INTEGER"),
]

# Define the schema for the page_views table
page_views_schema_fields = [
    # Using TIMESTAMP to store the end of the window
    bigquery.SchemaField("window_end", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("page_view_count", "INTEGER", mode="REQUIRED"),
]

# Create an argument parser
parser = argparse.ArgumentParser(description="Creates BigQuery dataset and tables for ecommerce clickstream data.")

# Add an argument for the dataset_id
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
    "--page_views_table",
    dest="page_views_table",
    default="page_views",
    help="BigQuery table ID for page views items data."
)


# Parse the command-line arguments
args = parser.parse_args()

# Get the project_id from the GOOGLE_CLOUD_PROJECT environment variable
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
dataset_id = args.dataset_id
visits_table_id = args.visits_table
events_table_id = args.events_table
purchase_items_table_id = args.purchase_items_table
page_views_table_id = args.page_views_table

visits_bq_table = f"{project_id}:{dataset_id}.{visits_table_id}"
events_bq_table = f"{project_id}:{dataset_id}.{events_table_id}"
purchase_items_bq_table = f"{project_id}:{dataset_id}.{purchase_items_table_id}"

# Check if the environment variable is set
if not project_id:
    print("Error: The GOOGLE_CLOUD_PROJECT environment variable is not set.")
    exit(1) # Exit the script with an error

# Get the dataset_id from the command-line argument
dataset_id = args.dataset_id

# --- Rest of the code uses the obtained variables ---

print(f"Using Project ID: {project_id}")
print(f"Using Dataset ID: {dataset_id}")

# Construct a BigQuery client object.
client = bigquery.Client()

# Construct a full Dataset object to send to the API.
dataset_ref = client.dataset(dataset_id, project=project_id)
dataset = bigquery.Dataset(dataset_ref)

# Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Create the dataset
try:
    print(f"\nAttempting to create dataset: {project_id}.{dataset_id}")
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Dataset '{dataset_id}' created successfully in project '{project_id}'")
except Exception as e:
    # Handle the case where the dataset already exists
    if "Already Exists" in str(e):
        print(f"Dataset '{dataset_id}' already exists in project '{project_id}'. Skipping creation.")
    else:
        print(f"Error creating dataset '{dataset_id}': {e}")


# Function to create a table within the dataset
def create_bigquery_table(table_id, schema_fields):
    # The table reference now uses the dataset object which already has the correct project_id and dataset_id
    table_ref = dataset.table(table_id)
    table = bigquery.Table(table_ref, schema=schema_fields)
    try:
        print(f"Attempting to create table: {project_id}.{dataset_id}.{table_id}")
        table = client.create_table(table)  # Make an API request.
        print(f"Table '{table_id}' created successfully in dataset '{dataset_id}'")
    except Exception as e:
        # Handle the case where the table already exists
        if "Already Exists" in str(e):
            print(f"Table '{table_id}' already exists in dataset '{dataset_id}'. Skipping creation.")
        else:
            print(f"Error creating table '{table_id}': {e}")

# Create the tables
create_bigquery_table(visits_table_id, visits_schema_fields)
create_bigquery_table(events_table_id, events_schema_fields)
create_bigquery_table(purchase_items_table_id, purchase_items_schema_fields)
create_bigquery_table(page_views_table_id, page_views_schema_fields)

print("\nScript execution finished.")