# Use Bitnami Spark 3.4.1 (includes Java + Spark)
FROM bitnami/spark:3.4.1

WORKDIR /opt/spark-app

# Copy only Python dependencies
COPY requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your streaming script
COPY spark_streaming.py ./spark_streaming.py

# Default command (can be overridden by docker-compose)
CMD ["spark-submit", "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1", \
     "/opt/spark-app/spark_streaming.py"]
