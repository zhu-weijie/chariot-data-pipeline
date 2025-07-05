CREATE SCHEMA IF NOT EXISTS movies;

CREATE TABLE IF NOT EXISTS movies.movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    genres TEXT[]
);
