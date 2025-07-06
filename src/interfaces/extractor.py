from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Extractor(ABC):
    @abstractmethod
    def read_batch(self, batch_size: int, high_water_mark: Any) -> List[Dict]:
        pass

    @abstractmethod
    def get_next_high_water_mark(self, batch: List[Dict]) -> Any:
        pass
