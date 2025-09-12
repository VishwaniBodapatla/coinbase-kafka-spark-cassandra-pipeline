# CoinBase Kafka Spark Cassandra Pipeline

Streams live cryptocurrency trades from **CoinBase**, processes them with **Spark Streaming**, stores results in **Cassandra**, and orchestrates workflows with **Airflow** using **Docker**.

---

## Project Structure

```text
.
├── airflow
│   ├── Dockerfile
│   ├── dags
│   │   └── Crypto_Kafka_Stream.py
│   └── script
│       └── entrypoint.sh
├── required_data
│   ├── coin_map.json
│   └── my_Portfolio.json
├── spark
│   ├── Dockerfile
│   ├── requirements.txt
│   └── spark_streaming.py
├── docker-compose.yaml
├── README.md
└── requirements.txt
