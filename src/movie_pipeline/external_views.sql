-- Exact normalized title + release year matching only; unmatched films stay visible.
CREATE VIEW public.powerbi_box_office AS
WITH candidates AS (
    SELECT movie_id, title, release_year, movielens_rating_count,
           movielens_average_rating, imdb_vote_count, imdb_average_rating,
           regexp_replace(lower(regexp_replace(title, ' \([0-9]{4}\)$', '')),
                          '[^a-z0-9]+', '', 'g') AS title_key
    FROM public.movie_metrics
), unique_candidates AS (
    SELECT *, count(*) OVER (PARTITION BY title_key, release_year) AS match_count
    FROM candidates
)
SELECT web.rank, web.title AS web_title, web.release_year,
       web.worldwide_gross_usd, web.source_url, web.snapshot_at_utc,
       movie.movie_id, movie.title AS movielens_title,
       movie.movielens_rating_count, movie.movielens_average_rating,
       movie.imdb_vote_count, movie.imdb_average_rating
FROM public.web_top_grossing_films AS web
LEFT JOIN unique_candidates AS movie
  ON movie.title_key = regexp_replace(lower(web.title), '[^a-z0-9]+', '', 'g')
 AND movie.release_year = web.release_year AND movie.match_count = 1;

CREATE VIEW public.powerbi_wikidata_movies AS
SELECT movie.movie_id, movie.title, movie.release_year,
       movie.movielens_rating_count, movie.movielens_average_rating,
       movie.imdb_vote_count, movie.imdb_average_rating,
       api.wikidata_id, api.country_of_origin, api.snapshot_at_utc
FROM public.movie_metrics AS movie
LEFT JOIN (
    SELECT imdb_id, string_agg(DISTINCT wikidata_id, '; ' ORDER BY wikidata_id) AS wikidata_id,
           string_agg(DISTINCT country_of_origin, '; ' ORDER BY country_of_origin) AS country_of_origin,
           max(snapshot_at_utc) AS snapshot_at_utc
    FROM public.api_wikidata_films
    GROUP BY imdb_id
) AS api ON movie.tconst = api.imdb_id;

-- One row per movie and producing country, so co-productions count in each country.
CREATE VIEW public.powerbi_wikidata_countries AS
WITH countries AS (
    -- api_wikidata_films stores each film's countries as one '; '-joined label list.
    SELECT DISTINCT api.imdb_id, trim(country.name) AS country
    FROM public.api_wikidata_films AS api
    CROSS JOIN LATERAL unnest(string_to_array(api.country_of_origin, ';')) AS country(name)
    WHERE trim(country.name) <> ''
)
SELECT movie.movie_id, countries.country,
       count(*) OVER (PARTITION BY movie.movie_id) AS movie_country_count,
       movie.movielens_rating_count, movie.movielens_average_rating,
       movie.imdb_vote_count, movie.imdb_average_rating
FROM public.movie_metrics AS movie
JOIN countries ON movie.tconst = countries.imdb_id;

CREATE VIEW public.powerbi_oscars_movies AS
WITH awards AS (
    SELECT ceremony_year, film_title,
           count(*) AS nomination_count,
           count(*) FILTER (WHERE won) AS win_count,
           min(source_url) AS source_url, min(snapshot_at_utc) AS snapshot_at_utc,
           regexp_replace(lower(film_title), '[^a-z0-9]+', '', 'g') AS title_key
    FROM public.oscars_awards
    WHERE film_title IS NOT NULL
    GROUP BY ceremony_year, film_title
), candidates AS (
    SELECT movie_id, title, release_year, movielens_rating_count,
           movielens_average_rating, imdb_vote_count, imdb_average_rating,
           regexp_replace(lower(regexp_replace(title, ' \([0-9]{4}\)$', '')),
                          '[^a-z0-9]+', '', 'g') AS title_key
    FROM public.movie_metrics
), unique_candidates AS (
    SELECT *, count(*) OVER (PARTITION BY title_key, release_year) AS match_count
    FROM candidates
)
SELECT awards.ceremony_year, awards.film_title, awards.nomination_count,
       awards.win_count, awards.source_url, awards.snapshot_at_utc,
       movie.movie_id, movie.title AS movielens_title, movie.release_year,
       movie.movielens_rating_count, movie.movielens_average_rating,
       movie.imdb_vote_count, movie.imdb_average_rating
FROM awards
LEFT JOIN unique_candidates AS movie
  ON awards.ceremony_year >= 1980
 AND movie.release_year = awards.ceremony_year - 1
 AND movie.title_key = awards.title_key AND movie.match_count = 1;

CREATE VIEW public.powerbi_oscars_awards AS
SELECT award.award_id, award.ceremony_year, award.category,
       award.film_title, award.nominee, award.won,
       award.source_url, award.snapshot_at_utc,
       movie.movie_id, movie.movielens_average_rating, movie.movielens_rating_count,
       movie.imdb_average_rating, movie.imdb_vote_count
FROM public.oscars_awards AS award
LEFT JOIN public.powerbi_oscars_movies AS movie
  ON movie.ceremony_year = award.ceremony_year
 AND movie.film_title = award.film_title;
