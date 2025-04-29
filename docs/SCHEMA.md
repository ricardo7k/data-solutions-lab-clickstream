# Data Schema - Website Analytics Pipeline

## Input Data (JSONL)

The input data consists of files in JSON Lines (JSONL) format, where each line represents a website visit session. The expected structure per line is:

```json
{
  "session_id": "string",
  "user_id": "string", // Optional
  "session_start_time": "timestamp_string", // Ex: "2023-10-27T10:00:00Z"
  "session_end_time": "timestamp_string",
  "page_visits": [ // List of events that occurred during the session
    {
      "event_type": "page_view", // or "add_to_cart", "purchase", "click_view"
      "timestamp": "timestamp_string",
      "page_url": "string",
      // Event-specific fields...
      // Ex (add_to_cart): "item_id": "string", "quantity": integer
      // Ex (purchase): "order_id": "string", "total_amount": float, "items": [{"item_id": "string", "price": float, "quantity": integer}]
      // Ex (click_view): "target_element": "string", "target_url": "string"
    }
    // ... other events
  ]
  // Other session metadata...
}

*   **Note:** The exact structure may vary, but the pipeline expects `session_id` and a list of events (`page_visits` or similar) with `event_type` and `timestamp`. The example above includes common event types like `page_view`, `add_to_cart`, `purchase`, and `click_view`.

## Output Tables (BigQuery)

The pipeline transforms the nested data into a relational structure with three tables in BigQuery:

1.  **`visits`**: Contains session-level information.
    *   **Logical Primary Key:** `session_id`
    *   **Fields:** `session_id`, `user_id`, `session_start_time`, `session_end_time`, `duration_seconds`, `event_count`, etc. (As defined in the Python schema in `website_analytics_pipeline.py`)

2.  **`events`**: Contains details of each individual event within a session.
    *   **Logical Composite Primary Key:** `session_id` + `event_unique_id` (generated UUID)
    *   **Fields:** `session_id`, `event_unique_id`, `event_type`, `timestamp`, `page_url`, `item_id` (for add_to_cart), `order_id` (for purchase), `target_element` (for click_view), etc. (As defined in the Python schema)

3.  **`purchase_items`**: Contains details of each item purchased within a `purchase` event.
    *   **Logical Composite Primary Key:** `order_id` + `item_index_in_order` (generated index)
    *   **Fields:** `order_id`, `item_index_in_order`, `item_id`, `price`, `quantity`, `timestamp` (from the purchase event), `session_id`. (As defined in the Python schema)

## Logical Key Generation

Logical keys are generated during processing to handle potential inconsistencies in the source data (like duplicate `session_id`s) and to ensure uniqueness in the event and item tables:

*   **`visits`**: Uses `session_id` directly. It's assumed that aggregation or selection logic (if duplicate `session_id`s exist in the source) is handled before or during loading.
*   **`events`**: An `event_unique_id` (UUID) is generated for each event within a session, forming a unique key with `session_id`.
*   **`purchase_items`**: An `item_index_in_order` (index based on the item's position in the `items` list of the purchase event) is generated, forming a unique key with `order_id`.

This flow ensures that data is read, parsed, transformed, and loaded into the appropriate BigQuery tables in a scalable and resilient manner. Error handling for parsing is included, and further error handling (e.g., for transformation logic) could be added within the DoFns.