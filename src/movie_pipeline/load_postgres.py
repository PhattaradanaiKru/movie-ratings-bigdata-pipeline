"""Load cleaned Parquet tables into PostgreSQL, then publish them atomically."""

from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession, functions as F

from movie_pipeline.sources import ROOT

OUTPUT = ROOT / "data" / "processed"
TABLES = ("dim_movies", "movie_metrics", "genre_metrics", "fact_ratings")
JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'movie_ratings')}"
)


def db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "movie_ratings"),
        user=os.getenv("POSTGRES_USER", "movie"),
        password=os.getenv("POSTGRES_PASSWORD", "movie"),
    )


def table_frame(spark: SparkSession, name: str):
    frame = spark.read.parquet(str(OUTPUT / name))
    if name == "fact_ratings":
        return frame.select(
            F.col("userId").alias("user_id"),
            F.col("movieId").alias("movie_id"),
            "rating",
            F.col("timestamp").alias("rating_timestamp"),
            "rated_at",
            "rating_year",
        )
    if name == "genre_metrics":
        return frame.select("genre", "release_year", "rating_count", "average_rating")

    common = [
        F.col("movieId").alias("movie_id"),
        "title", "genres",
        F.col("imdbId").alias("imdb_id"),
        F.col("tmdbId").alias("tmdb_id"),
        "tconst", "release_year", "runtime_minutes",
        "imdb_average_rating", "imdb_vote_count",
    ]
    if name == "dim_movies":
        return frame.select(*common)
    return frame.select(
        *common,
        "movielens_rating_count", "movielens_average_rating", "movielens_rating_stddev",
    )


def main() -> None:
    missing = [name for name in TABLES if not (OUTPUT / name).is_dir()]
    if missing:
        raise FileNotFoundError(f"Missing cleaned Parquet tables: {', '.join(missing)}")

    spark = (
        SparkSession.builder.appName("movie-ratings-postgres-load")
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        for name in TABLES:
            print(f"Loading {name} into PostgreSQL staging table", flush=True)
            frame = table_frame(spark, name)
            (
                frame.write.format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", f"public.{name}_load")
                .option("driver", "org.postgresql.Driver")
                .option("user", os.getenv("POSTGRES_USER", "movie"))
                .option("password", os.getenv("POSTGRES_PASSWORD", "movie"))
                .option("batchsize", "10000")
                .option("numPartitions", "4")
                .option("reWriteBatchedInserts", "true")
                .mode("overwrite")
                .save()
            )
    finally:
        spark.stop()

    # The published tables remain intact if a staging load fails. Rename them
    # together only after all four staging tables have been written.
    with db_connection() as connection:
        with connection.cursor() as cursor:
            for name in TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS public.{name}_previous")
                cursor.execute("SELECT to_regclass(%s)", (f"public.{name}",))
                if cursor.fetchone()[0] is not None:
                    cursor.execute(f"ALTER TABLE public.{name} RENAME TO {name}_previous")
                cursor.execute(f"ALTER TABLE public.{name}_load RENAME TO {name}")
            for name in TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS public.{name}_previous")
            cursor.execute("CREATE UNIQUE INDEX dim_movies_movie_id_idx ON public.dim_movies (movie_id)")
            cursor.execute("CREATE UNIQUE INDEX movie_metrics_movie_id_idx ON public.movie_metrics (movie_id)")

    with db_connection() as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for name in TABLES:
                cursor.execute(f"ANALYZE public.{name}")
    print("Published cleaned tables to PostgreSQL", flush=True)


if __name__ == "__main__":
    main()
