import logging
import sys
from typing import Any, Dict, Optional

from pythonjsonlogger import jsonlogger


class StructuredLogger:
    
    def __init__(self, name: str, level: str = "INFO"):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(getattr(logging, level.upper()))
        
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = jsonlogger.JsonFormatter(
                '%(timestamp)s %(level)s %(name)s %(message)s %(correlation_id)s',
                rename_fields={'levelname': 'level', 'asctime': 'timestamp'}
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
    def _log(self, level: int, message: str, **kwargs: Any) -> None:
        extra = {
            'correlation_id': kwargs.get('correlation_id', 'N/A'),
            'service': 'energy-platform',
            **{k: v for k, v in kwargs.items() if k != 'correlation_id'}
        }
        self._logger.log(level, message, extra=extra)
    
    def info(self, message: str, **kwargs: Any) -> None:
        self._log(logging.INFO, message, **kwargs)
    
    def error(self, message: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, message, **kwargs)
    
    def warning(self, message: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, message, **kwargs)
    
    def debug(self, message: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, message, **kwargs)
    
    def critical(self, message: str, **kwargs: Any) -> None:
        self._log(logging.CRITICAL, message, **kwargs)


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)