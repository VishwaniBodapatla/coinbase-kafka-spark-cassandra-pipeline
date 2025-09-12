import time
import json
import os
import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from kafka import KafkaAdminClient

# ---------------------------
# Configuration
# ---------------------------
DATA_DIR = "/opt/spark-app/required_data"
PORTFOLIO_FILE = os.path.join(DATA_DIR, "my_Portfolio.json")
CASSANDRA_KEYSPACE = "crypto_keyspace"
CASSANDRA_TABLE = "crypto_trades"
KAFKA_BOOTSTRAP = "broker:29092"
TOPIC = "crypto_trades"

# ---------------------------
# Load portfolio
# ---------------------------
with open(PORTFOLIO_FILE) as f:
    portfolio = json.load(f)

# ---------------------------
# Cassandra connection
# ---------------------------
while True:
    try:
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect()
        logging.info("Connected to Cassandra!")
        break
    except Exception:
        logging.warning("Cassandra not ready, retrying in 5 seconds...")
        time.sleep(5)

# Create keyspace and table
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}};
""")

session.execute(f"""
CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
    symbol text,
    event_time bigint,
    coin_name text,
    price double,
    size double,
    side text,
    profit double,
    PRIMARY KEY (symbol, event_time)
);
""")

# ---------------------------
# Spark session with Cassandra
# ---------------------------
spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------
# Kafka topic schema
# ---------------------------
schema = StructType([
    StructField("event", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("coin_name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("price", StringType(), True),   # cast later
    StructField("size", StringType(), True),    # cast later
    StructField("side", StringType(), True),
])

# ---------------------------
# Wait for Kafka topic
# ---------------------------
def wait_for_topic(bootstrap_servers, topic, interval=5):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    while True:
        try:
            topics = admin.list_topics()
            if topic in topics:
                logging.info(f"Topic '{topic}' exists, starting streaming.")
                return
            else:
                logging.info(f"Topic '{topic}' not found. Retrying in {interval}s...")
        except Exception as e:
            logging.warning(f"Kafka not ready: {e}. Retrying in {interval}s...")
        time.sleep(interval)

wait_for_topic(KAFKA_BOOTSTRAP, TOPIC)

# ---------------------------
# Read streaming data from Kafka
# ---------------------------
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Cast numeric columns
parsed_df = parsed_df.withColumn("price", col("price").cast("double")) \
                     .withColumn("size", col("size").cast("double"))

# ---------------------------
# Compute profit UDF
# ---------------------------
def compute_profit(symbol, price):
    entry = portfolio.get(symbol, None)
    if not entry:
        return 0.0
    try:
        buy_price = float(entry.get("buy_price", 0))
        size = float(entry.get("size", 0))
        return (price - buy_price) * size
    except Exception:
        return 0.0

profit_udf = udf(compute_profit, DoubleType())
final_df = parsed_df.withColumn("profit", profit_udf(col("symbol"), col("price")))

# ---------------------------
# Write to Cassandra
# ---------------------------

columns_to_write = ["symbol", "event_time", "coin_name", "price", "size", "side", "profit"]
final_df_selected = final_df.select(*columns_to_write)

query = final_df_selected.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write
                  .format("org.apache.spark.sql.cassandra")
                  .mode("append")
                  .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
                  .save()) \
    .start()


query.awaitTermination()
