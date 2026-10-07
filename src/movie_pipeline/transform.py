"""Build analysis-ready Parquet tables with PySpark."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

from movie_pipeline.sources import IMDB, MOVIELENS, ROOT

OUTPUT = ROOT / "data" / "processed"

RATINGS_SCHEMA = T.StructType(
    [
        T.StructField("userId", T.LongType(), False),
        T.StructField("movieId", T.LongType(), False),
        T.StructField("rating", T.DoubleType(), False),
        T.StructField("timestamp", T.LongType(), False),
    ]
)
MOVIES_SCHEMA = T.StructType(
    [
        T.StructField("movieId", T.LongType(), False),
        T.StructField("title", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ]
)
LINKS_SCHEMA = T.StructType(
    [
        T.StructField("movieId", T.LongType(), False),
        T.StructField("imdbId", T.StringType(), True),
        T.StructField("tmdbId", T.StringType(), True),
    ]
)
IMDB_RATINGS_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), False),
        T.StructField("averageRating", T.DoubleType(), True),
        T.StructField("numVotes", T.LongType(), True),
    ]
)
IMDB_BASICS_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), False),
        T.StructField("titleType", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
        T.StructField("isAdult", T.StringType(), True),
        T.StructField("startYear", T.StringType(), True),
        T.StructField("endYear", T.StringType(), True),
        T.StructField("runtimeMinutes", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ]
)


def csv(spark: SparkSession, path: Path, schema: T.StructType, *, sep: str = ","):
    return (
        spark.read.option("header", "true")
        .option("sep", sep)
        .option("nullValue", r"\N")
        .schema(schema)
        .csv(str(path))
    )


def main() -> None:
    movie_dir = MOVIELENS / "ml-32m"
    required = [
        movie_dir / "ratings.csv",
        movie_dir / "movies.csv",
        movie_dir / "links.csv",
        IMDB / "title.ratings.tsv.gz",
        IMDB / "title.basics.tsv.gz",
    ]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing input files: {', '.join(missing)}. Run make extract first.")

    spark = (
        SparkSession.builder.appName("movie-ratings-pipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        ratings = csv(spark, movie_dir / "ratings.csv", RATINGS_SCHEMA)
        valid_ratings = ratings.filter(
            F.col("userId").isNotNull()
            & F.col("movieId").isNotNull()
            & F.col("rating").between(0.5, 5.0)
            & (F.col("timestamp") > 0)
        )
        movies = csv(spark, movie_dir / "movies.csv", MOVIES_SCHEMA).dropDuplicates(["movieId"])
        links = csv(spark, movie_dir / "links.csv", LINKS_SCHEMA).dropDuplicates(["movieId"])
        imdb_ratings = csv(spark, IMDB / "title.ratings.tsv.gz", IMDB_RATINGS_SCHEMA, sep="\t")
        imdb_basics = (
            csv(spark, IMDB / "title.basics.tsv.gz", IMDB_BASICS_SCHEMA, sep="\t")
            .filter(F.col("titleType") == "movie")
            .select("tconst", F.col("startYear").cast("int").alias("release_year"),
                    F.col("runtimeMinutes").cast("int").alias("runtime_minutes"))
        )

        imdb_id = F.when(F.length("imdbId") < 7, F.lpad("imdbId", 7, "0")).otherwise(F.col("imdbId"))
        dim_movies = (
            movies.join(links, "movieId", "left")
            .withColumn("tconst", F.when(F.col("imdbId").isNotNull(), F.concat(F.lit("tt"), imdb_id)))
            .join(imdb_basics, "tconst", "left")
            .join(imdb_ratings, "tconst", "left")
            .select(
                "movieId", "title", "genres", "imdbId", "tmdbId", "tconst",
                "release_year", "runtime_minutes",
                F.col("averageRating").alias("imdb_average_rating"),
                F.col("numVotes").alias("imdb_vote_count"),
            )
        )

        fact_ratings = (
            valid_ratings.withColumn("rated_at", F.to_timestamp(F.from_unixtime("timestamp")))
            .withColumn("rating_year", F.year("rated_at"))
            .select("userId", "movieId", "rating", "timestamp", "rated_at", "rating_year")
        )
        movie_aggregates = fact_ratings.groupBy("movieId").agg(
            F.count("*").alias("movielens_rating_count"),
            F.avg("rating").alias("movielens_average_rating"),
            F.stddev("rating").alias("movielens_rating_stddev"),
        )
        movie_metrics = dim_movies.join(movie_aggregates, "movieId", "left")
        genre_metrics = (
            fact_ratings.join(dim_movies.select("movieId", "genres", "release_year"), "movieId", "inner")
            .withColumn("genre", F.explode(F.split("genres", r"\|")))
            .filter(F.col("genre") != "(no genres listed)")
            .groupBy("genre", "release_year")
            .agg(F.count("*").alias("rating_count"), F.avg("rating").alias("average_rating"))
        )

        OUTPUT.mkdir(parents=True, exist_ok=True)
        fact_ratings.write.mode("overwrite").partitionBy("rating_year").parquet(str(OUTPUT / "fact_ratings"))
        dim_movies.write.mode("overwrite").parquet(str(OUTPUT / "dim_movies"))
        movie_metrics.write.mode("overwrite").parquet(str(OUTPUT / "movie_metrics"))
        genre_metrics.write.mode("overwrite").parquet(str(OUTPUT / "genre_metrics"))

        report = {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "MovieLens 32M + IMDb non-commercial datasets",
            "raw_rating_rows": ratings.count(),
            "valid_rating_rows": valid_ratings.count(),
            "movie_rows": dim_movies.count(),
            "movies_with_imdb_rating": dim_movies.filter(F.col("imdb_average_rating").isNotNull()).count(),
        }
        report["rejected_rating_rows"] = report["raw_rating_rows"] - report["valid_rating_rows"]
        (OUTPUT / "quality_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
