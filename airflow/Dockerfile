FROM apache/airflow:2.7.0-python3.9

WORKDIR /opt/airflow

# Copy DAGs only (other files will be mounted as volumes)
COPY ./airflow/dags ./dags

# Install Python dependencies if requirements.txt exists
COPY ./requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy entrypoint script (do not chmod, Docker volume will mount it with correct perms)
COPY ./airflow/script/entrypoint.sh ./script/entrypoint.sh
