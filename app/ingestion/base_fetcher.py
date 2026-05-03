from abc import ABC, abstractmethod
from typing import Any, Dict, List

from app.utils.logger import get_logger
from app.utils.models import RawAPIData
from config.settings import APIConfig

logger = get_logger(__name__)


class BaseDataFetcher(ABC):
    def __init__(self, config: APIConfig):
        self.config = config
        self.logger = logger

    @abstractmethod
    def fetch(self, **kwargs: Any) -> List[RawAPIData]:
        pass

    @abstractmethod
    def parse(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        pass

    def validate_source(self) -> bool:
        return True