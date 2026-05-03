from typing import Literal
from app.ingestion.base_fetcher import BaseDataFetcher
from app.ingestion.oilprice_fetcher import OilPriceFetcher
from app.ingestion.excel_fetcher import ExcelFetcher
from config.settings import APIConfig, PipelineConfig


class FetcherFactory:
    
    @staticmethod
    def create_fetcher(
        source_type: Literal["oilprice", "eia", "json", "csv", "excel"],
        api_config: APIConfig,
        pipeline_config: PipelineConfig
    ) -> BaseDataFetcher:
        
        if source_type == "oilprice":
            return OilPriceFetcher(api_config, pipeline_config)
        elif source_type == "eia":
            return OilPriceFetcher(api_config, pipeline_config)
        elif source_type == "excel":
            return ExcelFetcher(api_config)
        elif source_type == "json":
            raise NotImplementedError("JSON fetcher not implemented yet")
        elif source_type == "csv":
            raise NotImplementedError("CSV fetcher not implemented yet")
        else:
            raise ValueError(f"Unknown source type: {source_type}")