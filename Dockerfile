# Use official Airflow image as base
FROM apache/airflow:3.1.5

# Switch to root to manage file permissions
USER root

# Copy requirements file with proper permissions
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Switch back to airflow user
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow
