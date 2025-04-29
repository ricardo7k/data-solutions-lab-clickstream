# Data Solutions Lab - Clickstream Pipeline for Website Analytics

This project contains an Apache Beam pipeline written in Python, designed to process website visit data (JSONL format) stored in Google Cloud Storage (GCS). The pipeline transforms the data and loads it into structured tables in Google BigQuery, ready for analysis.

The pipeline is built as a Dataflow Flex Template for easy deployment and execution on Google Cloud Dataflow.

## Overview

*   **Input:** JSONL files in GCS containing website session and event data.
*   **Processing:** Apache Beam/Dataflow pipeline that parses, transforms, and enriches the data.
*   **Output:** Three tables in BigQuery: `visits`, `events`, and `purchase_items`.
*   **Deployment:** Dataflow Flex Template.

## Project Structure

*   `website_analytics_pipeline.py`: Main Apache Beam pipeline code.
*   `Dockerfile`: Defines the containerized environment for the Flex Template.
*   `metadata.json`: Metadata for building the Flex Template.
*   `README.md`: This file.
*   `docs/`: Directory containing detailed documentation.
    *   `SCHEMA.md`: Details about the input and output data schema.
    *   `PIPELINE_STRUCTURE.md`: Explanation of the pipeline structure and transformations.
    *   `FLEX_TEMPLATE_DEPLOYMENT.md`: Instructions for building and deploying the Flex Template.
    *   `RUN_LOCALLY.md`: Instructions for running the pipeline locally for testing.

## Getting Started

1.  **Understand the Schema:** See `docs/SCHEMA.md`.
2.  **Explore the Pipeline:** See `docs/PIPELINE_STRUCTURE.md`.
3.  **Run Locally (Optional):** Follow the instructions in `docs/RUN_LOCALLY.md`.
4.  **Deploy to Dataflow:** Follow the instructions in `docs/FLEX_TEMPLATE_DEPLOYMENT.md`.

## Useful Links

* Troubleshoot: https://cloud.google.com/dataflow/docs/guides/troubleshoot-custom-container?hl=pt-br
