#!/bin/bash

# Setup Airflow database
echo "Initializing Airflow database..."
airflow db init

# Start Airflow services
echo "Starting Airflow webserver and scheduler..."
airflow webserver -p 8080 &
airflow scheduler &

echo "Airflow setup completed!"
