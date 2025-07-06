import pytest
from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader


@pytest.mark.integration
def test_postgres_pipeline_hwm_flow():
    extractor = MySQLExtractor()
    loader = PostgresLoader()

    initial_hwm = loader.get_high_water_mark()
    assert initial_hwm == 0, "Initial high-water mark should be 0 on a clean DB"

    batch = extractor.read_batch(batch_size=5, high_water_mark=initial_hwm)
    assert len(batch) == 5
    loader.write_batch(batch)

    first_run_hwm = loader.get_high_water_mark()
    assert first_run_hwm == 5, "High-water mark should be 5 after first run"

    next_batch = extractor.read_batch(batch_size=5, high_water_mark=first_run_hwm)
    assert len(next_batch) == 5
    assert next_batch[0]["movieId"] == 6, "Second batch should start after the HWM"
    loader.write_batch(next_batch)

    second_run_hwm = loader.get_high_water_mark()
    assert second_run_hwm == 10, "High-water mark should be 10 after second run"
