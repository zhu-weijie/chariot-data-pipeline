from src.loaders.postgres_loader import PostgresLoader


def test_transform_batch_handles_genres_correctly():
    loader = PostgresLoader()
    raw_batch = [
        {
            "movieId": 1,
            "title": "Toy Story (1995)",
            "genres": "Adventure|Animation|Children|Comedy|Fantasy\r",
        },
        {
            "movieId": 2,
            "title": "Jumanji (1995)",
            "genres": "Adventure|Children|Fantasy",
        },
        {"movieId": 3, "title": "Movie with no genres", "genres": ""},
    ]
    expected_output = [
        (
            1,
            "Toy Story (1995)",
            ["Adventure", "Animation", "Children", "Comedy", "Fantasy"],
        ),
        (2, "Jumanji (1995)", ["Adventure", "Children", "Fantasy"]),
        (3, "Movie with no genres", [""]),
    ]

    transformed_batch = loader._transform_batch(raw_batch)

    assert transformed_batch == expected_output
