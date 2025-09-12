#!/bin/bash
set -e

# Install requirements if requirements.txt exists
if [ -f "/opt/airflow/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip install --upgrade pip
    pip install -r /opt/airflow/requirements.txt
fi

# Initialize Airflow DB and create admin user
if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init

    echo "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || echo "Admin user already exists"
fi

# Upgrade DB schema
echo "Upgrading Airflow DB..."
airflow db upgrade

# Run Airflow command
if [ "$1" = "webserver" ]; then
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    exec airflow scheduler
else
    exec "$@"
fi
