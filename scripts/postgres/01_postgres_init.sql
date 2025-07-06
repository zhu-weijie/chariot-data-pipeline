CREATE SCHEMA IF NOT EXISTS movies;

CREATE TABLE IF NOT EXISTS movies.movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    genres TEXT[]
);

CREATE TABLE IF NOT EXISTS movies.ratings (
    user_id INT,
    movie_id INT,
    rating DECIMAL(2,1),
    timestamp BIGINT,
    PRIMARY KEY (user_id, movie_id)
);