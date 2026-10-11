"""Orchestrate the local MovieLens + IMDb batch pipeline."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import psycopg2

from movie_pipeline.sources import download, extract, verify


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


def run_external_step(step: str) -> None:
    subprocess.run(
        ["python", "/workspace/src/movie_pipeline/external_sources.py", "--step", step],
        check=True,
        env={**os.environ, "PYTHONPATH": "/workspace/src"},
    )


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
            cursor.execute("SELECT count(*) FROM public.powerbi_movies")
            powerbi_movie_count = cursor.fetchone()[0]
            cursor.execute("SELECT movielens_rating_count FROM public.powerbi_overview")
            powerbi_rating_count = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM public.powerbi_release_year")
            powerbi_year_count = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM public.powerbi_genres")
            powerbi_genre_count = cursor.fetchone()[0]
    if fact_count != report["valid_rating_rows"]:
        raise ValueError(f"PostgreSQL fact count {fact_count} differs from Spark {report['valid_rating_rows']}")
    if movie_count != report["movie_rows"] or metric_count != movie_count:
        raise ValueError(f"PostgreSQL movie counts differ: dimensions={movie_count}, metrics={metric_count}")
    if powerbi_movie_count != movie_count or powerbi_year_count == 0 or powerbi_genre_count == 0:
        raise ValueError("Power BI views are missing or have unexpected row counts")
    if powerbi_rating_count != fact_count:
        raise ValueError(f"Power BI total {powerbi_rating_count} differs from fact count {fact_count}")
    print(f"PostgreSQL verified: {fact_count} ratings, {movie_count} movies, "
          f"{powerbi_year_count} release years, {powerbi_genre_count} genre/year rows")


with DAG(
    dag_id="movie_ratings_pipeline",
    description="Download sources, cleanse with Spark, load PostgreSQL, and enrich movies",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["big-data", "movie-ratings"],
) as dag:
    download_sources = PythonOperator(task_id="download_sources", python_callable=download)
    verify_sources = PythonOperator(task_id="verify_sources", python_callable=verify)
    extract_movielens = PythonOperator(task_id="extract_movielens", python_callable=extract)
    spark_transform = PythonOperator(task_id="spark_transform", python_callable=run_spark_transform)
    validate_output = PythonOperator(task_id="validate_output", python_callable=check_quality)
    load_postgres = PythonOperator(task_id="load_postgres", python_callable=load_database)
    validate_database = PythonOperator(task_id="validate_database", python_callable=check_database)
    with TaskGroup(group_id="external_enrichment", tooltip="Fetch and load external movie data") as external_enrichment:
        fetch_wikipedia = PythonOperator(
            task_id="fetch_wikipedia_box_office", python_callable=run_external_step,
            op_kwargs={"step": "wikipedia"},
        )
        fetch_wikidata = PythonOperator(
            task_id="fetch_wikidata_movie_details", python_callable=run_external_step,
            op_kwargs={"step": "wikidata"},
        )
        fetch_oscars = PythonOperator(
            task_id="fetch_oscars_awards", python_callable=run_external_step,
            op_kwargs={"step": "oscars"},
        )
        publish_external = PythonOperator(
            task_id="publish_external_to_postgres", python_callable=run_external_step,
            op_kwargs={"step": "publish"},
        )
        [fetch_wikipedia, fetch_wikidata, fetch_oscars] >> publish_external

    download_sources >> verify_sources >> extract_movielens >> spark_transform >> validate_output >> load_postgres >> validate_database >> external_enrichment
