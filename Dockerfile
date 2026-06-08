# Step 1: Start from the official Apache Airflow base image
FROM apache/airflow:2.6.0-python3.9

# Step 2: Switch to the root user temporarily if you need to install system-level packages
# (Optional, but useful if you need to install linux tools like 'nc' or 'curl')
USER root
RUN apt-get update && apt-get install -y netcat-openbsd && apt-get clean

# Step 3: Switch back to the standard 'airflow' user to install Python libraries safely
USER airflow

# Step 4: Upgrade pip and install the exact libraries your scripts need
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir requests kafka-python