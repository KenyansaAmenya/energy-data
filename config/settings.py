import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db_name: str
    raw_collection: str = "raw_api_data"
    fuel_collection: str = "fuel_prices"
    electricity_collection: str = "electricity_prices"
    gas_collection: str = "natural_gas_prices"
    pipeline_runs_collection: str = "pipeline_runs"
    quality_checks_collection: str = "data_quality_checks"

    @classmethod
    def from_env(cls) -> "MongoConfig":
        uri = os.getenv("MONGO_URI")
        if not uri:
            raise ValueError("MONGO_URI environment variable is required")
        return cls(
            uri=uri,
            db_name=os.getenv("MONGO_DB_NAME", "energy_platform")
        )


@dataclass(frozen=True)
class APIConfig:
    """OilPriceAPI configuration settings."""
    oilprice_api_key: str
    oilprice_base_url: str
    oilprice_request_timeout: int
    oilprice_max_retries: int
    oilprice_retry_delay: int

    @classmethod
    def from_env(cls) -> "APIConfig":
        oilprice_api_key = os.getenv("OILPRICE_API_KEY")
        if not oilprice_api_key:
            raise ValueError("OILPRICE_API_KEY environment variable is required")
        
        return cls(
            oilprice_api_key=oilprice_api_key,
            oilprice_base_url=os.getenv("OILPRICE_BASE_URL", "https://api.oilpriceapi.com/v1"),
            oilprice_request_timeout=int(os.getenv("OILPRICE_REQUEST_TIMEOUT", "30")),
            oilprice_max_retries=int(os.getenv("OILPRICE_MAX_RETRIES", "3")),
            oilprice_retry_delay=int(os.getenv("OILPRICE_RETRY_DELAY", "5"))
        )


@dataclass(frozen=True)
class PipelineConfig:
    batch_size: int
    max_retries: int
    retry_delay_seconds: int

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "5"))
        )


@dataclass(frozen=True)
class FlaskConfig:
    env: str
    port: int
    secret_key: str

    @classmethod
    def from_env(cls) -> "FlaskConfig":
        secret_key = os.getenv("SECRET_KEY")
        if not secret_key:
            raise ValueError("SECRET_KEY environment variable is required")
        return cls(
            env=os.getenv("FLASK_ENV", "development"),
            port=int(os.getenv("FLASK_PORT", "5000")),
            secret_key=secret_key
        )


@dataclass(frozen=True)
class AppConfig:
    mongo: MongoConfig
    api: APIConfig
    pipeline: PipelineConfig
    flask: FlaskConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            mongo=MongoConfig.from_env(),
            api=APIConfig.from_env(),
            pipeline=PipelineConfig.from_env(),
            flask=FlaskConfig.from_env()
        )


_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    global _config
    if _config is None:
        _config = AppConfig.from_env()
    return _config