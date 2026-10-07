FROM apache/airflow:2.10.4

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless libpostgresql-jdbc-java \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "pyspark==3.5.3" "psycopg2-binary==2.9.10"
