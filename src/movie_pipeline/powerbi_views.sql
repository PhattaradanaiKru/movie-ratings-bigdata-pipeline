-- Power BI imports these small, analysis-ready views rather than the 32M-row fact table.
-- MovieLens and IMDb measures deliberately retain their original scales.

CREATE VIEW public.powerbi_overview AS
SELECT count(*) AS movie_count,
       count(*) FILTER (WHERE movielens_rating_count > 0) AS rated_movie_count,
       coalesce(sum(movielens_rating_count), 0) AS movielens_rating_count,
       count(*) FILTER (WHERE imdb_average_rating IS NOT NULL) AS imdb_movie_count,
       coalesce(sum(imdb_vote_count), 0) AS imdb_vote_count
FROM public.movie_metrics;

CREATE VIEW public.powerbi_movies AS
SELECT movie_id, title, genres, release_year, runtime_minutes, tconst,
       movielens_rating_count, movielens_average_rating,
       imdb_vote_count, imdb_average_rating
FROM public.movie_metrics;

CREATE VIEW public.powerbi_release_year AS
SELECT release_year,
       count(*) FILTER (WHERE movielens_rating_count > 0) AS rated_movie_count,
       coalesce(sum(movielens_rating_count), 0) AS movielens_rating_count,
       sum(movielens_average_rating * movielens_rating_count)
           / nullif(sum(movielens_rating_count), 0) AS movielens_average_rating,
       count(*) FILTER (WHERE imdb_average_rating IS NOT NULL) AS imdb_movie_count,
       coalesce(sum(imdb_vote_count), 0) AS imdb_vote_count,
       sum(imdb_average_rating * imdb_vote_count)
           / nullif(sum(imdb_vote_count), 0) AS imdb_weighted_average_rating
FROM public.movie_metrics
WHERE release_year IS NOT NULL
GROUP BY release_year;

CREATE VIEW public.powerbi_genres AS
SELECT genre, release_year, rating_count, average_rating AS movielens_average_rating
FROM public.genre_metrics;
