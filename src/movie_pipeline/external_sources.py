"""Fetch external movie enrichments from HTML, a public API, and a pinned CSV."""

from __future__ import annotations

import argparse
from contextlib import closing
import csv
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
import os
import re
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup

from movie_pipeline.sources import RAW, ROOT

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
OSCARS_SOURCE_COMMIT = "3b22c2c4bfb9fd6793043ee41d2a14f50ffd0201"
OSCARS_URL = ("https://raw.githubusercontent.com/mauraanish/Oscars-Data/"
              f"{OSCARS_SOURCE_COMMIT}/oscars-data.csv")
OSCARS_SHA256 = "f23dc531eef122dfc9154566a497e37e4e0f0c3eafd54a17dc9328fe6d325961"
SNAPSHOT_DIR = RAW / "external"
USER_AGENT = os.getenv(
    "EXTERNAL_SOURCE_USER_AGENT",
    "MovieRatingsFinalProject/1.0 (https://github.com/PhattaradanaiKru/movie-ratings-bigdata-pipeline)",
)


def connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "movie_ratings"),
        user=os.getenv("POSTGRES_USER", "movie"),
        password=os.getenv("POSTGRES_PASSWORD", "movie"),
    )


def retry_delay(header: str | None, attempt: int) -> float:
    if header:
        try:
            return max(float(header), 0.0)
        except ValueError:
            try:
                return max((parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds(), 0.0)
            except (TypeError, ValueError):
                pass
    return min(2 ** (attempt + 1), 120)


def fetch(url: str, *, accept: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept})
    for attempt in range(8):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code not in (429, 502, 503, 504) or attempt == 7:
                raise
            delay = retry_delay(error.headers.get("Retry-After"), attempt)
            print(f"HTTP {error.code}; waiting {delay:.0f}s before retry", flush=True)
            time.sleep(delay)
        except urllib.error.URLError:
            if attempt == 7:
                raise
            time.sleep(retry_delay(None, attempt))
    raise AssertionError("unreachable")


def snapshot(path: Path, url: str, *, accept: str) -> bytes:
    if path.is_file():
        return path.read_bytes()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = fetch(url, accept=accept)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_bytes(content)
    temporary.replace(path)
    return content


def scrape_grosses(html: bytes) -> list[tuple[int, str, int, int]]:
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.select("table.wikitable"):
        caption = table.find("caption")
        if not caption or "Highest-grossing films" not in caption.get_text(" ", strip=True):
            continue
        rows = []
        for tr in table.select("tr"):
            cells = tr.find_all(["th", "td"], recursive=False)
            if len(cells) < 5 or not cells[0].get_text(" ", strip=True).isdigit():
                continue
            rank = int(cells[0].get_text(" ", strip=True))
            title = cells[2].get_text(" ", strip=True).rstrip(" †‡*")
            gross_text = cells[3].get_text(" ", strip=True)
            year_text = cells[4].get_text(" ", strip=True)
            gross = re.search(r"\$\s*([\d,]+)", gross_text)
            year = re.search(r"\b(19|20)\d{2}\b", year_text)
            if gross and year and title:
                rows.append((rank, title, int(gross.group(1).replace(",", "")), int(year.group())))
        if not rows or len({row[0] for row in rows}) != len(rows):
            raise ValueError("Wikipedia gross table is empty or has duplicate ranks")
        return rows
    raise ValueError("Wikipedia highest-grossing table not found")


def parse_oscars(data: bytes) -> list[tuple[int, int, str, str | None, str | None, bool]]:
    if hashlib.sha256(data).hexdigest() != OSCARS_SHA256:
        raise ValueError("Oscars CSV checksum differs from pinned source")
    # This contributed CSV mixes UTF-8 and Windows-1252 bytes in older names.
    decoded = data.decode("utf-8-sig", errors="surrogateescape")
    decoded = "".join(bytes([ord(ch) - 0xDC00]).decode("cp1252", errors="replace")
                      if 0xDC80 <= ord(ch) <= 0xDCFF else ch for ch in decoded)
    reader = csv.DictReader(io.StringIO(decoded))
    if reader.fieldnames != ["id", "year", "category", "film", "nominee", "won"]:
        raise ValueError("Unexpected Oscars CSV columns")
    rows = []
    for row in reader:
        award_id, ceremony_year = int(row["id"]), int(row["year"])
        if not (1929 <= ceremony_year <= 2024) or row["won"] not in ("0", "1"):
            raise ValueError("Unexpected Oscars CSV value")
        category = row["category"].strip()
        if not category:
            raise ValueError("Oscars CSV has an empty award category")
        rows.append((award_id, ceremony_year, category,
                     row["film"].strip() or None, row["nominee"].strip() or None,
                     row["won"] == "1"))
    if len(rows) < 10000 or len({row[0] for row in rows}) != len(rows):
        raise ValueError("Oscars CSV is incomplete or has duplicate IDs")
    return rows


def selected_imdb_ids(db, limit: int | None) -> list[str]:
    with db.cursor() as cursor:
        query = "SELECT tconst FROM public.movie_metrics WHERE tconst IS NOT NULL ORDER BY tconst"
        if limit is None:
            cursor.execute(query)
        else:
            cursor.execute(query + " LIMIT %s", (limit,))
        return [row[0] for row in cursor.fetchall()]


def wikidata_query(ids: list[str]) -> str:
    if not ids or any(not re.fullmatch(r"tt\d{7,9}", value) for value in ids):
        raise ValueError("Invalid IMDb ID in Wikidata request")
    values = " ".join(f'"{value}"' for value in ids)
    return (
        "SELECT ?imdb ?film ?countryLabel WHERE { "
        f"VALUES ?imdb {{ {values} }} "
        "?film wdt:P345 ?imdb . "
        "OPTIONAL { ?film wdt:P495 ?country . } "
        'SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } '
        "}"
    )


def wikidata_snapshot_path(batch: list[str], index: int) -> Path:
    digest = hashlib.sha256("\n".join(batch).encode()).hexdigest()[:12]
    return SNAPSHOT_DIR / f"wikidata_{index:03d}_{digest}.json"


def wikidata_bindings(batch: list[str], index: int) -> list[dict]:
    path = wikidata_snapshot_path(batch, index)
    if path.is_file():
        return json.loads(path.read_bytes())["results"]["bindings"]
    query = wikidata_query(batch)
    url = WIKIDATA_URL + "?" + urllib.parse.urlencode({"query": query, "format": "json"})
    try:
        payload = json.loads(snapshot(path, url, accept="application/sparql-results+json"))
        time.sleep(1.0)
        return payload["results"]["bindings"]
    except (TimeoutError, urllib.error.HTTPError) as error:
        if isinstance(error, urllib.error.HTTPError) and error.code not in (502, 503, 504):
            raise
        if len(batch) <= 25:
            raise
        print(f"Wikidata batch {index + 1} timed out; splitting {len(batch)} IDs", flush=True)
        midpoint = len(batch) // 2
        bindings = (wikidata_bindings(batch[:midpoint], index)
                    + wikidata_bindings(batch[midpoint:], index))
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_text(json.dumps({"results": {"bindings": bindings},
                                         "combined_from_smaller_queries": True}), encoding="utf-8")
        temporary.replace(path)
        return bindings


def fetch_wikidata(ids: list[str], *, batch_size: int = 200) -> list[tuple[str, str, str | None]]:
    results: dict[tuple[str, str], set[str]] = {}
    for offset in range(0, len(ids), batch_size):
        batch = ids[offset:offset + batch_size]
        path = wikidata_snapshot_path(batch, offset // batch_size)
        cached = path.is_file()
        for binding in wikidata_bindings(batch, offset // batch_size):
            imdb = binding["imdb"]["value"]
            film_url = binding["film"]["value"]
            qid = film_url.rsplit("/", 1)[-1]
            if imdb not in batch or not re.fullmatch(r"Q\d+", qid):
                raise ValueError("Unexpected Wikidata API result")
            country = binding.get("countryLabel", {}).get("value")
            results.setdefault((imdb, qid), set())
            if country:
                results[(imdb, qid)].add(country)
        if not cached:
            print(f"Wikidata batch {offset // batch_size + 1}/{(len(ids) + batch_size - 1) // batch_size} "
                  f"queried; {len(results)} matched IDs so far", flush=True)
    return [(imdb, qid, "; ".join(sorted(countries)) or None)
            for (imdb, qid), countries in sorted(results.items())]


def publish(db, grosses, wikidata, oscars, api_snapshots: list[Path]) -> dict[str, int]:
    web_snapshot = datetime.fromtimestamp(
        (SNAPSHOT_DIR / "wikipedia_highest_grossing.html").stat().st_mtime, timezone.utc
    )
    api_snapshot = datetime.fromtimestamp(
        max(path.stat().st_mtime for path in api_snapshots), timezone.utc
    )
    oscars_snapshot = datetime.fromtimestamp(
        (SNAPSHOT_DIR / "oscars-data.csv").stat().st_mtime, timezone.utc
    )
    with db:
        with db.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS public.web_top_grossing_films ("
                           "rank integer PRIMARY KEY, title text NOT NULL, "
                           "worldwide_gross_usd bigint NOT NULL, release_year integer NOT NULL, "
                           "source_url text NOT NULL, snapshot_at_utc timestamptz NOT NULL)")
            cursor.execute("CREATE TABLE IF NOT EXISTS public.api_wikidata_films ("
                           "imdb_id text NOT NULL, wikidata_id text NOT NULL, "
                           "country_of_origin text, source_url text NOT NULL, "
                           "snapshot_at_utc timestamptz NOT NULL, "
                           "PRIMARY KEY (imdb_id, wikidata_id))")
            cursor.execute("CREATE TABLE IF NOT EXISTS public.oscars_awards ("
                           "award_id integer PRIMARY KEY, ceremony_year integer NOT NULL, "
                           "category text NOT NULL, film_title text, nominee text, "
                           "won boolean NOT NULL, source_url text NOT NULL, "
                           "snapshot_at_utc timestamptz NOT NULL)")
            cursor.execute("TRUNCATE public.web_top_grossing_films, "
                           "public.api_wikidata_films, public.oscars_awards")
            cursor.executemany(
                "INSERT INTO public.web_top_grossing_films VALUES (%s,%s,%s,%s,%s,%s)",
                [(rank, title, gross, year, WIKIPEDIA_URL, web_snapshot)
                 for rank, title, gross, year in grosses],
            )
            execute_values(
                cursor,
                "INSERT INTO public.api_wikidata_films VALUES %s",
                [(imdb, qid, country, WIKIDATA_URL, api_snapshot)
                 for imdb, qid, country in wikidata],
                page_size=1000,
            )
            cursor.executemany(
                "INSERT INTO public.oscars_awards VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                [(*row, OSCARS_URL, oscars_snapshot) for row in oscars],
            )
            cursor.execute("DROP VIEW IF EXISTS public.powerbi_box_office")
            cursor.execute("DROP VIEW IF EXISTS public.powerbi_wikidata_movies")
            cursor.execute("DROP VIEW IF EXISTS public.powerbi_oscars_awards")
            cursor.execute("DROP VIEW IF EXISTS public.powerbi_oscars_movies")
            cursor.execute(Path(__file__).with_name("external_views.sql").read_text(encoding="utf-8"))
            cursor.execute("SELECT count(movie_id) FROM public.powerbi_box_office")
            web_matched = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM public.powerbi_wikidata_movies "
                           "WHERE wikidata_id IS NOT NULL")
            api_matched = cursor.fetchone()[0]
            cursor.execute("SELECT count(movie_id) FROM public.powerbi_oscars_movies")
            oscars_matched = cursor.fetchone()[0]
            if web_matched == 0 or api_matched == 0 or oscars_matched == 0:
                raise ValueError("External sources did not match any MovieLens movies")
    print(f"Published {len(grosses)} scraped grosses, {len(wikidata)} Wikidata API matches, "
          f"and {len(oscars)} Oscar award records")
    return {"web_matched_movies": web_matched, "wikidata_matched_movies": api_matched,
            "oscars_matched_movies": oscars_matched}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-limit", type=int, default=None,
                        help="Optional development limit; default queries every IMDb ID")
    parser.add_argument("--api-batch-size", type=int, default=200)
    args = parser.parse_args()
    if args.api_limit is not None and args.api_limit < 1:
        parser.error("--api-limit must be positive")
    if not 1 <= args.api_batch_size <= 200:
        parser.error("--api-batch-size must be between 1 and 200")
    html = snapshot(SNAPSHOT_DIR / "wikipedia_highest_grossing.html", WIKIPEDIA_URL, accept="text/html")
    grosses = scrape_grosses(html)
    oscars_data = snapshot(SNAPSHOT_DIR / "oscars-data.csv", OSCARS_URL, accept="text/csv")
    oscars = parse_oscars(oscars_data)
    with closing(connection()) as db:
        ids = selected_imdb_ids(db, args.api_limit)
    wikidata = fetch_wikidata(ids, batch_size=args.api_batch_size)
    if not wikidata:
        raise ValueError("Wikidata API returned no matching films")
    api_snapshots = [wikidata_snapshot_path(ids[offset:offset + args.api_batch_size],
                                           offset // args.api_batch_size)
                     for offset in range(0, len(ids), args.api_batch_size)]
    with closing(connection()) as db:
        coverage = publish(db, grosses, wikidata, oscars, api_snapshots)
    matched_ids = len({imdb for imdb, _, _ in wikidata})
    report = {"created_at_utc": datetime.now(timezone.utc).isoformat(),
              "web_scraped_rows": len(grosses), "wikidata_api_rows": len(wikidata),
              "oscars_award_rows": len(oscars),
              "wikidata_requested_ids": len(ids), "wikidata_matched_ids": matched_ids,
              "wikidata_unmatched_ids": len(ids) - matched_ids,
              "wikidata_coverage_percent": round(100 * matched_ids / len(ids), 2),
              "wikidata_batch_size": args.api_batch_size,
              "wikipedia_url": WIKIPEDIA_URL,
              "wikidata_url": WIKIDATA_URL, "oscars_source_url": OSCARS_URL,
              "web_snapshot_sha256": hashlib.sha256(html).hexdigest(),
              "oscars_snapshot_sha256": hashlib.sha256(oscars_data).hexdigest(),
              "wikidata_snapshot_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                            for path in api_snapshots},
              **coverage}
    destination = ROOT / "data" / "processed" / "external_quality_report.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items()
                      if key != "wikidata_snapshot_sha256"}, indent=2))


if __name__ == "__main__":
    main()
