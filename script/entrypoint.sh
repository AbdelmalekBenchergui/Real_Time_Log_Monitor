#!/bin/bash
set -e

echo "Starting Airflow entrypoint..."

airflow db migrate

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin \
  || true

exec "$@"
