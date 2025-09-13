# Coinbase Kafka Spark Cassandra Pipeline

This project implements a **real-time cryptocurrency data pipeline** using **Airflow, Kafka, Spark Streaming, and Cassandra**.  
It streams live trades from **CoinBase**, processes them in real time, and stores enriched results for further analysis.
All components run in Docker containers for easy deployment and reproducibility.

---

## 📌 Summary  

**Airflow orchestrates → WebSocket streams CoinBase trades → Kafka transports → Spark processes & computes profits → Cassandra stores results.**

---

<img src="https://github.com/user-attachments/assets/5fc3f03e-3d05-4331-9b78-66171cbc99ef" alt="CoinBase_Kafka_Spark_Airflow_Cassandra" style="width:600px;"/>



## 🚀 Workflow Overview  

1. **Triggering the DAG (Airflow)**  
   - Airflow runs the DAG (`Crypto_Kafka_Stream.py`).  
   - DAG starts a Python task that connects to the **CoinBase WebSocket API**.

<img src="https://github.com/user-attachments/assets/19259b1c-1749-4f15-9fff-ee7a791e4fab" 
     alt="project2Airflow" 
     width="600" 
     height="200"/>

2. **Data Ingestion (WebSocket → Kafka)**  
   - The WebSocket subscribes only to cryptocurrencies listed in `my_Portfolio.json`.  
   - `coin_map.json` is used for symbol-to-name lookup.  
   - Trade messages are transformed and published into a Kafka topic (`crypto_trades`).
  
   <img width="600" height="200" alt="Screenshot From 2025-09-12 18-40-00" src="https://github.com/user-attachments/assets/9cc2b40b-de58-4a2e-a367-7073eeecac8f" />


3. **Stream Processing (Kafka → Spark)**  
   - Spark Structured Streaming consumes data from Kafka.  
   - Data is parsed, cast into proper schema, and enriched with **profit calculation** based on portfolio buy prices & sizes.  

4. **Storage (Spark → Cassandra)**  
   - Processed data is written into a **Cassandra keyspace (`crypto_keyspace`)** and table (`crypto_trades`).  
   - Each row contains symbol, event time, trade size, price, side, and computed profit.
  
     <img width="600" height="200" alt="cassandraProfit" src="https://github.com/user-attachments/assets/36a9166f-1afc-4eac-90be-cd236a775aea" />
---

## 📂 Project Structure  

```plaintext
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
