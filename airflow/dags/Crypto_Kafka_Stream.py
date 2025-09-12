from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import websocket
import json
import time
import threading
import logging
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'vishwani',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DATA_DIR = "/opt/airflow/required_data"
COIN_MAP_FILE = os.path.join(DATA_DIR, "coin_map.json")
PORTFOLIO_FILE = os.path.join(DATA_DIR, "my_Portfolio.json")



# Load coin map
with open(COIN_MAP_FILE, "r") as f:
    coin_map = json.load(f)

# Load portfolio
with open(PORTFOLIO_FILE, "r") as f:
    portfolio = json.load(f)

# Extract symbols from portfolio dict keys
symbols = list(portfolio.keys())

# Coinbase subscription message
subscribe_message = {
    "type": "subscribe",
    "channels": [{"name": "matches", "product_ids": symbols}]
}


def run_websocket(duration_sec=60):
    KAFKA_BROKER = 'broker:29092'   # inside Docker
    TOPIC = 'crypto_trades'

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Kafka Producer connected successfully.")
    except Exception as e:
        logging.error(f"Failed to connect Kafka: {e}")
        return

    def on_message(ws, message):
        msg = json.loads(message)
        if msg.get("type") != "match":
            return

        transformed = {
            "event": "trade",
            "event_time": int(time.time() * 1000),
            "coin_name": coin_map.get(msg["product_id"], msg["product_id"]),
            "symbol": msg["product_id"],
            "trade_id": msg["trade_id"],
            "price": msg["price"],
            "size": msg["size"],
            "side": msg["side"]
        }

        try:
            producer.send(TOPIC, value=transformed)
            producer.flush()
            logging.info(f"Sent to Kafka: {transformed['symbol']} @ {transformed['price']}")
        except Exception as e:
            logging.error(f"Error sending to Kafka: {e}")

    def on_error(ws, error):
        logging.error(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        logging.info("Closed Coinbase WebSocket connection")

    def on_open(ws):
        logging.info("Connected to Coinbase WebSocket")
        ws.send(json.dumps(subscribe_message))

    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    thread = threading.Thread(target=ws.run_forever)
    thread.start()
    time.sleep(duration_sec)
    ws.close()
    thread.join()
    logging.info(f"Stopped streaming after {duration_sec} seconds.")
    producer.close()


with DAG(
    'coinbase_automation_v4',
    default_args=default_args,
    description='Stream Coinbase portfolio data into Kafka',
    schedule_interval='@daily',
    start_date=datetime(2025, 9, 8, 10, 0),
    catchup=False,
    tags=['crypto', 'streaming'],
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_portfolio_data_from_CoinbaseApi',
        python_callable=run_websocket,
        op_args=[60]
    )
