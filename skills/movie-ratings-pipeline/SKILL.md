---
name: movie-ratings-pipeline
description: Build or update this project's MovieLens and IMDb movie ratings ingestion, Spark transforms, data quality checks, and analytics tables.
---

# Movie ratings pipeline

Use this skill when changing the project's data pipeline or analytics model.

- Read `README.md` for runnable commands and `docs/pipeline-plan.md` for the stage boundaries and join contract.
- Treat `data/raw/` as immutable downloaded snapshots. Never edit or commit source datasets. Store repeatable source URLs and checksums in code or documentation.
- Join MovieLens to IMDb through `links.csv` IDs, not title text. Preserve unmatched movies and report match coverage.
- Keep individual MovieLens 0.5–5 ratings separate from IMDb aggregate 1–10 ratings. Include vote counts when ranking or comparing titles.
- Make ingestion and transformations safe to rerun without multiplying records. Keep source snapshot dates in generated metadata.
- Run the smallest meaningful verification for a change; for source changes use `make verify`, and for transformation changes check schema, row counts, and sample aggregates.
- The current implemented stage is Airflow controlling local batch Spark and loading cleaned Parquet into PostgreSQL. Add HDFS, Hive, Trino, and dashboard components incrementally rather than claiming they already run.
