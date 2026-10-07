#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if ! airflow users create \
    --username "${AIRFLOW_UI_USERNAME:-airflow}" \
    --password "${AIRFLOW_UI_PASSWORD:-airflow}" \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email airflow@localhost; then
    airflow users reset-password \
        --username "${AIRFLOW_UI_USERNAME:-airflow}" \
        --password "${AIRFLOW_UI_PASSWORD:-airflow}"
fi

exec airflow standalone
