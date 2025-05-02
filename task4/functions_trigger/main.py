import base64
import json
import logging
import composer2_airflow_rest_api
logger = logging.getLogger(__name__)

# Correct signature for event triggers (CloudEvents)
def trigger_dag_gcf(cloud_event, context=None):
    try:
        event_data = cloud_event.data

        if event_data is None:
             logger.error("No data found in CloudEvent.")
             return "Error: Event data missing.", 400

        if isinstance(event_data, bytes):
            try:
                event_data_str = event_data.decode('utf-8')
                if 'message' in event_data_str:
                     message_data_str = json.loads(event_data_str)['message']['data']
                     decoded_payload_bytes = base64.b64decode(message_data_str)
                     data = json.loads(decoded_payload_bytes.decode('utf-8'))
                else:
                     data = json.loads(event_data_str)
                logger.info("Event data decoded from bytes/JSON.")
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError, base64.Error) as e:
                logger.error(f"Failed to decode event data (bytes): {e}", exc_info=True)
                logger.error(f"Received data (bytes): {event_data!r}") # Log received bytes for debug
                return "Error: Failed to decode event data.", 400
        elif isinstance(event_data, dict):
            data = event_data
            logger.info("Event data is already a dictionary.")
        else:
            logger.error(f"Unexpected event data type: {type(event_data)}")
            return "Error: Invalid event data type.", 400

        bucket = data.get('bucket')
        name = data.get('name')

        if not bucket or not name:
            logger.error(f"Decoded event did not contain 'bucket' or 'name': {data}")
            return "Error: Missing 'bucket' or 'name' data in decoded event.", 400

        logger.info(f"GCS event received for: gs://{bucket}/{name}")

        web_server_url = ("https://a0fbba8e0d75487f9f36da53d13fe9a1-dot-us-central1.composer.googleusercontent.com")
        dag_id = 'gcs_event_dataflow_pubsub'
        composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)

        return "Event received and processed by the function.", 200

    except Exception as e:
        logger.error(f"Unexpected error in function: {e}", exc_info=True)
        return f"Internal function error: {e}", 500