#!/usr/bin/env bash
set -e

echo "⏳ Waiting for Postgres..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "✅ Postgres ready!"

echo "🔧 Running Airflow DB migrations..."
airflow db migrate || airflow db init

echo "🔌 Creating default connections..."
airflow connections create-default-connections || true

echo "👤 Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com || true

echo "🚀 Starting Airflow..."
airflow webserver & airflow scheduler
