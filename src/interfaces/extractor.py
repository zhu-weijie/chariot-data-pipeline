from abc import ABC, abstractmethod
from typing import List, Dict


class Extractor(ABC):
    @abstractmethod
    def read_batch(self, batch_size: int, high_water_mark: int) -> List[Dict]:
        pass
