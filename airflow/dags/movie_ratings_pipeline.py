"""Orchestrate the local MovieLens + IMDb batch pipeline."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

from movie_pipeline.sources import extract, verify


def run_spark_script(script: str, *, jdbc: bool = False) -> None:
    """Run a PySpark job inside the Airflow container."""
    command = ["spark-submit"]
    if jdbc:
        command.extend(["--jars", "/usr/share/java/postgresql.jar"])
    command.extend([
        "--master", os.getenv("SPARK_MASTER", "local[4]"),
        "--driver-memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        f"/workspace/src/movie_pipeline/{script}",
    ])
    subprocess.run(
        command,
        check=True,
        env={**os.environ, "PYTHONPATH": "/workspace/src"},
    )


def run_spark_transform() -> None:
    run_spark_script("transform.py")


def load_database() -> None:
    run_spark_script("load_postgres.py", jdbc=True)


def check_quality() -> None:
    report_path = Path("/workspace/data/processed/quality_report.json")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report["raw_rating_rows"] != 32_000_204:
        raise ValueError(f"Unexpected MovieLens rating count: {report['raw_rating_rows']}")
    if report["valid_rating_rows"] != report["raw_rating_rows"]:
        raise ValueError(f"Invalid ratings found: {report['rejected_rating_rows']}")
    if report["movie_rows"] != 87_585:
        raise ValueError(f"Unexpected MovieLens movie count: {report['movie_rows']}")
    if report["movies_with_imdb_rating"] == 0:
        raise ValueError("No IMDb ratings joined to MovieLens movies")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def check_database() -> None:
    report = json.loads(Path("/workspace/data/processed/quality_report.json").read_text(encoding="utf-8"))
    with psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "movie_ratings"),
        user=os.getenv("POSTGRES_USER", "movie"),
        password=os.getenv("POSTGRES_PASSWORD", "movie"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM public.fact_ratings")
            fact_count = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM public.dim_movies")
            movie_count = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM public.movie_metrics")
            metric_count = cursor.fetchone()[0]
    if fact_count != report["valid_rating_rows"]:
        raise ValueError(f"PostgreSQL fact count {fact_count} differs from Spark {report['valid_rating_rows']}")
    if movie_count != report["movie_rows"] or metric_count != movie_count:
        raise ValueError(f"PostgreSQL movie counts differ: dimensions={movie_count}, metrics={metric_count}")
    print(f"PostgreSQL verified: {fact_count} ratings, {movie_count} movies")


with DAG(
    dag_id="movie_ratings_pipeline",
    description="Verify sources, cleanse with Spark, load PostgreSQL, and check output",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["big-data", "movie-ratings"],
) as dag:
    verify_sources = PythonOperator(task_id="verify_sources", python_callable=verify)
    extract_movielens = PythonOperator(task_id="extract_movielens", python_callable=extract)
    spark_transform = PythonOperator(task_id="spark_transform", python_callable=run_spark_transform)
    validate_output = PythonOperator(task_id="validate_output", python_callable=check_quality)
    load_postgres = PythonOperator(task_id="load_postgres", python_callable=load_database)
    validate_database = PythonOperator(task_id="validate_database", python_callable=check_database)

    verify_sources >> extract_movielens >> spark_transform >> validate_output >> load_postgres >> validate_database
