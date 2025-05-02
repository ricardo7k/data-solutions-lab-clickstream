FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ecommerce_pipeline.py .

COPY run_pipeline.sh .
RUN chmod +x /app/run_pipeline.sh

ENTRYPOINT ["/app/run_pipeline.sh"]

CMD []