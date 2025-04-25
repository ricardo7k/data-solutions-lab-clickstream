FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.11_sdk:latest

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY website_analytics_pipeline.py /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/website_analytics_pipeline.py"