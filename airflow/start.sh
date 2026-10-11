#!/usr/bin/env bash
set -euo pipefail

# PID files persist in the airflow-home volume; after an unclean stop they
# block the webserver from starting in the new container.
rm -f "${AIRFLOW_HOME:-/opt/airflow}"/airflow-*.pid

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
