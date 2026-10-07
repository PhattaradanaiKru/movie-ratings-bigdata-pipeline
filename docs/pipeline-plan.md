# Pipeline development plan

## Current stage: local Airflow + Spark + PostgreSQL batch pipeline

Airflow orchestrates source verification, MovieLens extraction, a local PySpark transform, output quality checks, PostgreSQL loading, and database count checks. Source files stay in `data/raw/`. The transform reads MovieLens CSV and IMDb GZip TSV, writes Parquet tables to `data/processed/`, then loads cleaned tables into PostgreSQL through staging tables.

## Next infrastructure stage

1. Store raw files and Parquet in HDFS or S3-compatible object storage.
2. Register Parquet tables in a Hive-compatible catalog if the assignment requires that warehouse path.
3. Use PostgreSQL SQL queries for a dashboard; add Trino/Presto if the assignment requires it.
4. Add a catalog refresh task to the existing Airflow DAG after Hive-compatible tables exist.
5. Record source snapshot dates and checksums; keep each task safe to retry.

## Join contract

- `MovieLens ratings.movieId -> movies.movieId -> links.movieId`.
- `links.imdbId -> IMDb tconst`, prefix `tt` and zero-pad to at least seven digits.
- `links.tmdbId` is reserved for a later TMDB enrichment stage. An API key is required.
- Keep MovieLens stars and IMDb aggregate ratings as separate measures. They have different scales and populations.

## Data quality gates

- Source checksum and archive integrity.
- Rating between 0.5 and 5.0; valid IDs and timestamp.
- Report valid and rejected rating rows.
- Report how many MovieLens movies match IMDb rating rows.
- Compare row counts and a small sample of aggregates with source data before publishing a dashboard.
