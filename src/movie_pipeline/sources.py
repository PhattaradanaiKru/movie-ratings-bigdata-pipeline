"""Download and verify the fixed source datasets."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import shutil
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
MOVIELENS = RAW / "movielens"
IMDB = RAW / "imdb"

SOURCES = {
    MOVIELENS / "ml-32m.zip": "https://files.grouplens.org/datasets/movielens/ml-32m.zip",
    MOVIELENS / "ml-32m.zip.md5": "https://files.grouplens.org/datasets/movielens/ml-32m.zip.md5",
    IMDB / "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    IMDB / "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
}
REQUIRED_IN_ZIP = {"ml-32m/ratings.csv", "ml-32m/movies.csv", "ml-32m/links.csv", "ml-32m/tags.csv"}


def download() -> None:
    for destination, url in SOURCES.items():
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and destination.stat().st_size:
            print(f"already present: {destination.relative_to(ROOT)}")
            continue
        temporary = destination.with_name(destination.name + ".part")
        print(f"downloading: {url}")
        try:
            with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)


def verify() -> None:
    missing = [str(path.relative_to(ROOT)) for path in SOURCES if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing source files: {', '.join(missing)}")

    expected = (MOVIELENS / "ml-32m.zip.md5").read_text(encoding="ascii").split()[0].lower()
    digest = hashlib.md5()  # noqa: S324 - upstream publishes an MD5 checksum
    with (MOVIELENS / "ml-32m.zip").open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("MovieLens ZIP checksum does not match the published checksum")

    with zipfile.ZipFile(MOVIELENS / "ml-32m.zip") as archive:
        if archive.testzip() is not None:
            raise ValueError("MovieLens ZIP contains a damaged file")
        if not REQUIRED_IN_ZIP.issubset(archive.namelist()):
            raise ValueError("MovieLens ZIP is missing required CSV files")

    for path in (IMDB / "title.ratings.tsv.gz", IMDB / "title.basics.tsv.gz"):
        with gzip.open(path, "rb") as source:
            if not source.readline().startswith(b"tconst\t"):
                raise ValueError(f"Unexpected IMDb header: {path}")
    print("Source files verified")


def extract() -> None:
    target = MOVIELENS / "ml-32m"
    if all((target / Path(name).name).is_file() for name in REQUIRED_IN_ZIP):
        print("MovieLens CSV files already extracted")
        return
    verify()
    with zipfile.ZipFile(MOVIELENS / "ml-32m.zip") as archive:
        for name in REQUIRED_IN_ZIP:
            archive.extract(name, MOVIELENS)
    print(f"Extracted MovieLens CSV files to {target.relative_to(ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("download", "verify", "extract"))
    action = parser.parse_args().action
    {"download": download, "verify": verify, "extract": extract}[action]()


if __name__ == "__main__":
    main()
