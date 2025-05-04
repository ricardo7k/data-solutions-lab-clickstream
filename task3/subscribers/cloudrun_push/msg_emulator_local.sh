#!/bin/bash

ENDPOINT_URL="http://localhost:8080/"

CURRENT_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

SESSION_DATA='
{
  "session_id": "SID-SHELL-'"$(date +%s)"'",
  "user_id": "UID-SHELL-'"$(date +%s%N | cut -c8-)"'",
  "device_type": "desktop",
  "geolocation": "-23.5505,-46.6333",
  "user_agent": "Simulated Shell Script",
  "events": [
    {
      "event": {
        "event_type": "page_view",
        "timestamp": "'"$CURRENT_TIMESTAMP"'",
        "details": {
          "page_url": "https://example.com/home",
          "referrer_url": null
        }
      }
    },
    {
      "event": {
        "event_type": "add_item_to_cart",
        "timestamp": "'"$CURRENT_TIMESTAMP"'",
        "details": {
          "product_id": "PROD-XYZ",
          "product_name": "Test Product",
          "category": "test",
          "price": 99.99,
          "quantity": 1
        }
      }
    }
  ]
}
'

ENCODED_DATA=$(echo -n "$SESSION_DATA" | base64 -w 0)

PUBSUB_PAYLOAD='
{
  "message": {
    "data": "'"$ENCODED_DATA"'",
    "messageId": "'$(uuidgen)'",
    "publishTime": "'"$CURRENT_TIMESTAMP"'",
    "attributes": {
      "origin": "shell-emulator",
      "timestamp": "'$(date +%s)'"
    }
  }
}
'

echo "Sending simulated Pub/Sub message to $ENDPOINT_URL..."

curl -X POST \
  -H "Content-Type: application/json" \
  -d "$PUBSUB_PAYLOAD" \
  "$ENDPOINT_URL"

echo ""
echo "Request sent."
