FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.11_sdk:latest

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ENV GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT
ENV GCS_BUCKET_INPUT=$GCS_BUCKET_INPUT
ENV PUBSUB_TOPIC_ID=$PUBSUB_TOPIC_ID
ENV PUBSUB_SUB_PULL_ID=$PUBSUB_SUB_PULL_ID
ENV PUBSUB_SUB_PUSH_ID=$PUBSUB_SUB_PUSH_ID
ENV DATAFLOW_SERVICE_ACCOUNT=$SERVICE_ACCOUNT

COPY ecommerce_pipeline.py /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/ecommerce_pipeline.py"