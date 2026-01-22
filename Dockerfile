# Use an official Python runtime as a parent image
FROM python:3.11-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (for uvloop)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create directory for database
RUN mkdir -p data

# Run the application
CMD ["python", "-m", "src.main"]
