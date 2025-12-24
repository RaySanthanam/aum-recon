# Use the official Airflow image as base
FROM artifactory.idfcfirstbank.com/optimus-docker/apache/airflow:latest

# Switch to airflow user (usually UID 50000)
USER 50000

# Copy requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies as airflow user
# RUN pip3 install --upgrade --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r /tmp/requirements.txt

RUN pip3 install --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org
RUN pip3 install --upgrade --no-cache-dir -r /tmp/requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org

# (Optional) set PATH to include user base packages
ENV PATH=$PATH:/home/airflow/.local/bin
