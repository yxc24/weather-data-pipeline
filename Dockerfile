FROM openjdk:11-jdk-slim

USER root

# Update package lists and install required packages
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    ant \
    curl \
    python3 \
    python3-pip \
    libpq-dev \
    gcc \
    libc6-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/local/openjdk-11
ENV PATH $PATH:$JAVA_HOME/bin

# Install Spark
RUN curl -O https://dlcdn.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz && \
    tar -xvf spark-3.4.3-bin-hadoop3.tgz && \
    mv spark-3.4.3-bin-hadoop3 /opt/spark && \
    rm spark-3.4.3-bin-hadoop3.tgz

# Set the JAVA_HOME environment variable for Spark
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Upgrade pip and install packages
RUN pip3 install --upgrade pip
COPY requirements.txt /weather-data-pipeline/requirements.txt
RUN pip3 install --no-cache-dir -r /weather-data-pipeline/requirements.txt

# Copy project files
COPY dags/ /weather-data-pipeline/dags/
COPY scripts/ /weather-data-pipeline/scripts/
COPY lib/ /weather-data-pipeline/lib/

WORKDIR /weather-data-pipeline

CMD ["python3", "scripts/weather_data_processor.py"]