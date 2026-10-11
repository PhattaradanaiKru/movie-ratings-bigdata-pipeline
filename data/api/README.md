# Wikidata API export

`wikidata_films.csv` is a dated export of 76,388 rows returned by the [Wikidata Query Service](https://query.wikidata.org/sparql). Each row has an IMDb identifier (`P345`), a Wikidata item ID, and its country of origin label (`P495`) when present. It contains no IMDb ratings, vote counts, or movie titles.

- Query selection: all 87,585 IMDb IDs linked from the MovieLens 32M movie catalog, sent to Wikidata in 438 sequential batches of at most 200 IDs.
- Coverage: 76,350 distinct IMDb IDs found (87.17%); 11,235 had no Wikidata match. A few IDs match more than one Wikidata item, so the CSV has 76,388 rows.
- Cached API responses saved: 2026-10-11 12:38–12:59 UTC. The ignored `data/raw/external/wikidata_*.json` files and their SHA-256 values are recorded in the local `data/processed/external_quality_report.json`.
- CSV SHA-256: `82990006af231481291ee0378a33bef0c217d892f97baf26d25f88d368073b7b`.
- Regenerate from local cached API responses after the pipeline finishes: `docker compose exec airflow python /workspace/src/movie_pipeline/external_sources.py --step export-wikidata`. The exporter fails if any expected batch snapshot is missing; it does not request missing batches.

[Wikidata structured data is CC0](https://www.wikidata.org/wiki/Wikidata:Licensing). The list of queried IDs was selected from [MovieLens 32M](https://grouplens.org/datasets/movielens/32m/); follow its usage conditions when redistributing or using this selected export. This CSV is a Git-tracked example; the pipeline reads its own ignored API snapshots and publishes them to PostgreSQL.
