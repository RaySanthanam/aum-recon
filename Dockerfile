# Use official Airflow image as base
FROM apache/airflow:3.1.5

# Switch to root to install system dependencies if needed
USER root

# Install any system dependencies here if required
# RUN apt-get update && apt-get install -y <package> && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./utils /opt/airflow/utils
COPY --chown=airflow:root ./config /opt/airflow/config
COPY --chown=airflow:root ./plugins /opt/airflow/plugins
COPY --chown=airflow:root ./tests /opt/airflow/tests

# Set working directory
WORKDIR /opt/airflow
