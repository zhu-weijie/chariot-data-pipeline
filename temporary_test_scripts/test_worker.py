from src.logging_config import setup_logging
from src.aggregators.ratings_aggregator import RatingsAggregator

setup_logging()

aggregator = RatingsAggregator()
aggregator.process_batch(batch_id=1)
