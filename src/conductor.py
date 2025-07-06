import structlog
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from config.config import settings
from src.interfaces.extractor import Extractor
from src.interfaces.loader import Loader

log = structlog.get_logger()


class PipelineConductor:
    def __init__(self, extractor: Extractor, loaders: List[Loader]):
        self.extractor = extractor
        self.loaders = loaders
        self.batch_size = settings.etl.batch_size
        log.info(
            "Conductor initialized",
            extractor=type(extractor).__name__,
            loaders=[type(loader).__name__ for loader in loaders],
        )

    def _run_pipeline_for_loader(self, loader: Loader):
        loader_name = type(loader).__name__
        log.info("Starting pipeline", loader=loader_name)

        try:
            high_water_mark = loader.get_high_water_mark()
            log.info("Initial high-water mark", loader=loader_name, hwm=high_water_mark)

            while True:
                log.info(
                    "Extracting batch for loader",
                    loader=loader_name,
                    high_water_mark=high_water_mark,
                    batch_size=self.batch_size,
                )
                batch = self.extractor.read_batch(
                    batch_size=self.batch_size, high_water_mark=high_water_mark
                )

                if not batch:
                    log.info(
                        "No new data found for loader. Pipeline finished.",
                        loader=loader_name,
                    )
                    break

                loader.write_batch(batch)

                high_water_mark = self.extractor.get_next_high_water_mark(batch)
                log.info(
                    "Batch processed. New high-water mark.",
                    loader=loader_name,
                    hwm=high_water_mark,
                )

            return f"Pipeline for {loader_name} completed successfully."
        except Exception as e:
            log.error("Pipeline failed for loader", loader=loader_name, error=str(e))
            raise

    def run_concurrently(self):
        log.info("Starting concurrent pipeline execution...")

        with ThreadPoolExecutor(max_workers=len(self.loaders)) as executor:
            future_to_loader = {
                executor.submit(self._run_pipeline_for_loader, loader): type(
                    loader
                ).__name__
                for loader in self.loaders
            }

            for future in as_completed(future_to_loader):
                loader_name = future_to_loader[future]
                try:
                    result = future.result()
                    log.info("Pipeline result", loader=loader_name, result=result)
                except Exception as exc:
                    log.error(
                        "A pipeline generated an exception",
                        loader=loader_name,
                        exception=str(exc),
                    )

        log.info("All concurrent pipelines have finished.")
