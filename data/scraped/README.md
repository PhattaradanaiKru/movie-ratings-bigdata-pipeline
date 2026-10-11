# Wikipedia box-office scrape

`wikipedia_highest_grossing.csv` contains 49 rows extracted from the “Highest-grossing films” table of [Wikipedia: List of highest-grossing films](https://en.wikipedia.org/wiki/List_of_highest-grossing_films). It contains rank, title, worldwide gross in USD, and release year. It does not contain MovieLens or IMDb ratings, Wikidata responses, or Oscar data.

- Snapshot saved: 2026-10-11 10:35:09 UTC
- Raw HTML SHA-256: `2eb8c4fd63a47509841c2a109896bbff6486c8a97f38c58087c4fa8dbaf0d4a9`
- Transformation: `scrape_grosses` in `src/movie_pipeline/external_sources.py`; run `docker compose exec airflow python /workspace/src/movie_pipeline/external_sources.py --step export-wikipedia` to regenerate from the local snapshot.
- Attribution: Wikipedia contributors, [page history](https://en.wikipedia.org/w/index.php?title=List_of_highest-grossing_films&action=history). Adapted by selecting four table columns and converting formatted numbers to integers. Source page content is available under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/); this extracted CSV is shared under the same terms.

The page is editable and grosses can change. This file is a dated example of the web scraping output, while the pipeline continues to refresh its own ignored raw snapshot and PostgreSQL table.
