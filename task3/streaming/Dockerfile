FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.11_sdk:latest

WORKDIR /app

# Copy the requirements file and install dependencies
# Create a requirements.txt file with:
# apache-beam[gcp]
# google-cloud-storage
# google-cloud-bigquery
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY ./streaming-gcs-pipeline.py /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/streaming-gcs-pipeline.py"

CMD []
