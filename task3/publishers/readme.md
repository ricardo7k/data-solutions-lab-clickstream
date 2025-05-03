* **Create Pub/Sub Publisher**

* Example: Generate 5 sessions per minute for 2 minutes, starting now (using defaults for rate/duration)
```bash
python task3/publishers/pageviews_emulator_ps.py \
 --topic_id="$PUBSUB_TOPIC_ID" \
 --visits_per_minute=5 \
 --duration_minutes=2 \
 --project_id="$GOOGLE_CLOUD_PROJECT"
```

or

* Example: Generate 20 sessions per minute for 10 minutes, starting at a specific time
```bash
python task3/publishers/pageviews_emulator_ps.py \
 --topic_id="$PUBSUB_TOPIC_ID" \
 --visits_per_minute=20 \
 --duration_minutes=10 \
 --start_datetime="2024-07-01T10:00:00" \
 --project_id="$GOOGLE_CLOUD_PROJECT"
```