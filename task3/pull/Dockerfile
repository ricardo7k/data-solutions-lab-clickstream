# Use a lightweight Python base image
FROM python:3.11-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pull subscriber application code into the container
COPY pubsub_subscriber_pull.py .

# The script will run continuously pulling messages
CMD ["python3.11", "pubsub_subscriber_pull.py"]