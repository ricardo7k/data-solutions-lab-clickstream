FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cloudrun_subscriber_push.py .
ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:8080", "cloudrun_subscriber_push:app"]