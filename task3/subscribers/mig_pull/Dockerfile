FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT}
ENV PUBSUB_SUB_PULL_ID=${PUBSUB_SUB_PULL_ID}
COPY mig_subscriber_pull.py .
ENTRYPOINT ["python", "mig_subscriber_pull.py"]