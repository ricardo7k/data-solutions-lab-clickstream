FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.11_sdk:latest

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ENV GOOGLE_CLOUD_PROJECT="dsl-clickstream"
ENV GCS_BUCKET_INPUT="dls-clickstream-data"
ENV PUBSUB_TOPIC_ID="ecommerce_clickstream"
ENV PUBSUB_SUB_PULL_ID="ecommerce_clickstreamd_pull_sub"
ENV PUBSUB_SUB_PUSH_ID="ecommerce_clickstreamd_push_sub"
ENV DATAFLOW_SERVICE_ACCOUNT="cloud-run@dsl-clickstream.iam.gserviceaccount.com"

COPY ecommerce_pipeline.py /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/ecommerce_pipeline.py"