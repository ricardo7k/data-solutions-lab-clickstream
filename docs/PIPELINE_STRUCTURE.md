# Apache Beam Pipeline Structure

The clickstream data processing pipeline is defined in `website_analytics_pipeline.py` using the Apache Beam Python SDK.

## Core Components

1.  **BigQuery Schema Definitions:**
    *   Python dictionaries (`VISITS_SCHEMA`, `EVENTS_SCHEMA`, `PURCHASE_ITEMS_SCHEMA`) define the structure of the target BigQuery tables. This is used by the `WriteToBigQuery` transform.

2.  **`ParseJsonl` (DoFn):**
    *   **Function:** Reads each line (string) from the input files in GCS.
    *   **Logic:** Attempts to decode the string as JSON.
    *   **Error Handling:** Catches `json.JSONDecodeError` for malformed lines and sends them to a separate error output (PCollection tagged 'error'), logging the error. Valid lines are emitted to the main output.
    *   **Input:** `str` (line from the file)
    *   **Main Output:** `dict` (parsed JSON object)
    *   **Error Output:** `str` (original line with error)

3.  **`TransformVisitData` (DoFn):**
    *   **Function:** The core transformation logic. Processes each visit object (parsed JSON).
    *   **Logic:**
        *   Extracts session information to create a record for the `visits` table.
        *   Iterates over the list of events (e.g., `page_visits`):
            *   Recognizes various `event_type` values like `page_view`, `click_view`, `add_to_cart`, `purchase`, etc.
            *   For each event, generates an `event_unique_id` (UUID).
            *   Creates a record for the `events` table, including `session_id`, `event_unique_id`, and event-specific details (like `page_url`, `item_id`, `target_element`).
            *   If the event type is `purchase` and contains a list of `items`:
                *   Iterates over the `items`.
                *   For each item, generates an `item_index_in_order` (based on position 0, 1, 2...).
                *   Creates a record for the `purchase_items` table, including `order_id`, `item_index_in_order`, `session_id`, and the purchase event's timestamp.
    *   **Tagged Outputs:** Uses `beam.pvalue.TaggedOutput` to direct the generated records to distinct PCollections, one for each destination table (`visits`, `events`, `purchase_items`).
    *   **Input:** `dict` (parsed visit object)
    *   **Outputs (Tagged):**
        *   `'visits_output'`: `dict` (record for the `visits` table)
        *   `'events_output'`: `dict` (record for the `events` table)
        *   `'purchase_items_output'`: `dict` (record for the `purchase_items` table)

4.  **`run(argv=None, save_main_session=True)` Function:**
    *   **Setup:** Orchestrates the pipeline definition and execution.
    *   **Command-Line Arguments:** Uses `argparse` to receive parameters like input path (`--input`), output dataset/tables (`--output_dataset`, etc.), GCP project (`--project`), temporary location (`--temp_location`), and write disposition (`--write_disposition`).
    *   **Pipeline Options:** Configures `PipelineOptions`, including `runner`, `project`, `temp_location`, `region`, `job_name`, and Flex Template-specific options (`enable_streaming_engine`, `use_public_ips`).
    *   **Project ID:** Automatically determines the GCP project ID if not provided.
    *   **Table Specifications:** Constructs the full BigQuery table names (e.g., `project:dataset.table`).
    *   **Write Disposition:** Sets `create_disposition` (always `CREATE_IF_NEEDED`) and `write_disposition` (`WRITE_TRUNCATE` or `WRITE_APPEND` based on the `--write_disposition` argument).

## Pipeline Flow

The pipeline connects the transformations using the `|` operator:

```python
# Inside the run() function

with beam.Pipeline(options=pipeline_options) as pipeline:
    # 1. Read JSONL files from GCS
    lines = pipeline | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input)

    # 2. Parse JSON and handle errors
    parsed_data = lines | 'ParseJsonl' >> beam.ParDo(ParseJsonl()).with_outputs('error', main='parsed')
    # (Optional: Handle the 'parsed_data.error' output, e.g., write to a dead-letter table)

    # 3. Transform data and split by destination table
    transformed_data = parsed_data.parsed | 'TransformVisitData' >> beam.ParDo(TransformVisitData()).with_outputs(
        'visits_output', 'events_output', 'purchase_items_output'
    )

    # 4. Write each PCollection to its respective BigQuery table
    transformed_data.visits_output | 'WriteVisitsToBQ' >> beam.io.WriteToBigQuery(
        visits_table_spec,
        schema=VISITS_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=known_args.write_disposition # Resolved earlier
        # Consider adding custom_gcs_temp_location if needed
    )

    transformed_data.events_output | 'WriteEventsToBQ' >> beam.io.WriteToBigQuery(
        events_table_spec,
        schema=EVENTS_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=known_args.write_disposition
    )

    transformed_data.purchase_items_output | 'WritePurchaseItemsToBQ' >> beam.io.WriteToBigQuery(
        purchase_items_table_spec,
        schema=PURCHASE_ITEMS_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=known_args.write_disposition
    )
```
