from io import BytesIO
from typing import Any, List, Dict

import pandas as pd
import requests

from app.ingestion.base_fetcher import BaseDataFetcher
from app.utils.date_utils import generate_batch_id, utc_now
from app.utils.logger import get_logger
from app.utils.models import ProductType, RawAPIData
from config.settings import APIConfig

logger = get_logger(__name__)

class ExcelFetcher(BaseDataFetcher):
    def __init__(self, config: APIConfig):
        super().__init__(config)
        self.logger = logger

    def fetch(self, source_url: str, product_type: ProductType, **kwargs: Any) -> List[RawAPIData]:
        batch_id = generate_batch_id()

        try: 
            response = requests.get(
                source_url,
                timeout=60,
                headers={"User-Agent": "EnergyDataPlatform/1.0"}
            )    
            response.raise_for_status()

            # FIX: Decode bytes to str for RawAPIData
            raw_content = response.content
            if isinstance(raw_content, bytes):
                try:
                    raw_content_str = raw_content.decode('utf-8')
                except UnicodeDecodeError:
                    raw_content_str = raw_content.decode('latin-1')
            else:
                raw_content_str = str(raw_content)

            raw_data = RawAPIData(
                batch_id=batch_id,
                source_url=source_url,
                raw_content=raw_content_str,  # FIX: RawAPIData expects str
                format_type="excel",
                ingestion_timestamp=utc_now(),
                metadata={
                    "product_type": product_type.value,
                    "content_type": response.headers.get("Content-Type")
                }
            )

            self.logger.info(
                "Fetched Excel data",
                batch_id=batch_id,
                source_url=source_url,
                size_bytes=len(response.content),
                correlation_id=batch_id
            )

            return [raw_data]

        except requests.RequestException as e:
            self.logger.error(
                "Failed to fetch Excel data",
                batch_id=batch_id,  
                error=str(e),
                correlation_id=batch_id
            )    
            raise

    def parse(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        try:
            content = raw_data.raw_content
            if isinstance(content, str):
                content = content.encode('latin-1')
            
            df = pd.read_excel(BytesIO(content))  
            df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

            records = df.to_dict("records")

            for record in records: 
                record["batch_id"] = raw_data.batch_id
                record["ingestion_timestamp"] = raw_data.ingestion_timestamp

            self.logger.info(
                "Parsed Excel data",
                batch_id=raw_data.batch_id,
                record_count=len(records),
                correlation_id=raw_data.batch_id
            )        

            return records

        except Exception as e:
            self.logger.error(
                "Excel parsing failed",
                batch_id=raw_data.batch_id,
                error=str(e),
                correlation_id=raw_data.batch_id
            )    
            raise