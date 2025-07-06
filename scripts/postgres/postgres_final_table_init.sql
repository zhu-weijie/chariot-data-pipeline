CREATE TABLE IF NOT EXISTS movies.ratings_summary (
    movie_id INT PRIMARY KEY,
    average_rating DECIMAL(10, 5),
    rating_count INT
);