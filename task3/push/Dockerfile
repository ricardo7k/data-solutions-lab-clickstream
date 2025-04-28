# Use a lightweight Python base image
# Choose a Python version compatible with your code and Cloud Run
FROM python:3.11-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the subscriber application code into the container
# Assuming you save the Python code as subscriber_app.py
COPY pubsub_subscriber.py .
CMD ["python3.11", "pubsub_subscriber.py"]
