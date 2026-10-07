.PHONY: download verify extract transform spark-transform airflow-up airflow-trigger airflow-down db-counts clean-processed

PYTHON ?= python3

download:
	PYTHONPATH=src $(PYTHON) -m movie_pipeline.sources download

verify:
	PYTHONPATH=src $(PYTHON) -m movie_pipeline.sources verify

extract:
	PYTHONPATH=src $(PYTHON) -m movie_pipeline.sources extract

transform:
	PYTHONPATH=src $(PYTHON) -m movie_pipeline.transform

spark-transform:
	docker compose run --rm spark-transform

airflow-up:
	docker compose up -d --build --wait --wait-timeout 300 airflow

airflow-trigger:
	docker compose exec airflow airflow dags trigger movie_ratings_pipeline

airflow-down:
	docker compose down

db-counts:
	docker compose exec -T postgres psql -U movie -d movie_ratings -c \
		'SELECT (SELECT count(*) FROM fact_ratings) AS ratings, (SELECT count(*) FROM dim_movies) AS movies;'

clean-processed:
	rm -rf data/processed
