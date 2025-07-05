from abc import ABC, abstractmethod
from typing import List, Dict


class Loader(ABC):
    @abstractmethod
    def get_high_water_mark(self) -> int:
        pass

    @abstractmethod
    def write_batch(self, batch: List[Dict]) -> None:
        pass
