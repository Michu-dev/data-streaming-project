create database streamoutput;

\c streamoutput

create table netflix_movie_ratings_agg (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    film_id int,
    title varchar(2000),
    no_characters integer,
    ratings_number bigint,
    ratings_sum bigint,
    unique_people_voting int
);

create table anomalies (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    film_id int,
    ratings_number bigint,
    avg_rating real
);

\q



