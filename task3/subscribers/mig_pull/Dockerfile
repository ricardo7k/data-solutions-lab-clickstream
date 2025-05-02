FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV GOOGLE_CLOUD_PROJECT="dsl-clickstream"
ENV PUBSUB_SUB_PULL_ID="ecommerce_clickstreamd_pull_sub"
COPY mig_subscriber_pull.py .
ENTRYPOINT ["python", "mig_subscriber_pull.py"]