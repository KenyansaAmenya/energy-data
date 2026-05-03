import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base_fetcher import BaseDataFetcher
from app.utils.date_utils import generate_batch_id, utc_now
from app.utils.logger import get_logger
from app.utils.models import ProductType, RawAPIData
from config.settings import APIConfig, PipelineConfig

logger = get_logger(__name__)


class XMLFetcher(BaseDataFetcher):

    def __init__(self, config: APIConfig, pipeline_config: PipelineConfig):
        super().__init__(config)
        self.pipeline_config = pipeline_config
        self.logger = logger
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def _make_request(self, url: str, params: Dict[str, str]) -> str:
        self.logger.info(
            "Making API request",
            url=url,
            correlation_id=params.get("batch_id", "unknown")
        )
        
        response = requests.get(
            url,
            params=params,
            timeout=30,
            headers={
                "User-Agent": "EnergyDataPlatform/1.0",
                "Accept": "application/json, application/xml"
            }
        )
        response.raise_for_status()
        return response.text
    
    def fetch(self, product_type: ProductType, **kwargs: Any) -> List[RawAPIData]:
        batch_id = generate_batch_id()
        url = self._get_url_for_product(product_type)
        
        params = {
            "api_key": self.config.gpp_api_key,
            "batch_id": batch_id,
            **kwargs
        }
        
        try:
            raw_content = self._make_request(url, params)
            
            # Detect format
            format_type = "json" if raw_content.strip().startswith(("{", "[")) else "xml"
            
            raw_data = RawAPIData(
                batch_id=batch_id,
                source_url=url,
                raw_content=raw_content,
                format_type=format_type,
                ingestion_timestamp=utc_now(),
                metadata={
                    "product_type": product_type.value,
                    "params": kwargs
                }
            )
            
            self.logger.info(
                "Successfully fetched data",
                batch_id=batch_id,
                product_type=product_type.value,
                content_length=len(raw_content),
                format_type=format_type,
                correlation_id=batch_id
            )
            
            return [raw_data]
            
        except requests.RequestException as e:
            self.logger.error(
                "Failed to fetch data after retries",
                batch_id=batch_id,
                error=str(e),
                correlation_id=batch_id
            )
            raise IngestionError(f"Failed to fetch {product_type.value}: {str(e)}") from e
    
    def _get_url_for_product(self, product_type: ProductType) -> str:
        mapping = {
            ProductType.FUEL: self.config.gpp_base_url,
            ProductType.ELECTRICITY: self.config.gpp_electricity_url,
            ProductType.NATURAL_GAS: self.config.gpp_gas_url
        }
        url = mapping.get(product_type)
        if not url:
            raise ValueError(f"No URL configured for product type: {product_type}")
        return url
    
    def parse(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        try:
            if raw_data.format_type == "json":
                return self._parse_json(raw_data)
            else:
                return self._parse_xml(raw_data)
                
        except Exception as e:
            self.logger.error(
                "Parsing failed",
                batch_id=raw_data.batch_id,
                error=str(e),
                correlation_id=raw_data.batch_id
            )
            raise ParseError(f"Failed to parse: {str(e)}") from e
    
    def _parse_json(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        try:
            data = json.loads(raw_data.raw_content)
            
            if isinstance(data, dict):
                if "response" in data and isinstance(data["response"], dict):
                    inner = data["response"]
                    if "data" in inner:
                        records = inner["data"]
                    elif "series" in inner:
                        records = inner["series"]
                    else:
                        records = []
                elif "data" in data:
                    records = data["data"]
                elif "series" in data:
                    records = data["series"]
                else:
                    records = [data] if any(k in data for k in ["country", "price", "name"]) else []
            else:
                records = data if isinstance(data, list) else []
            
            parsed = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                    
                parsed_record = {
                    "country": self._extract_field(record, ["country", "name", "region", "area"]),
                    "price": self._extract_price(record),
                    "currency": self._extract_field(record, ["currency", "currencyUnit"], "USD"),
                    "unit": self._extract_field(record, ["unit", "units", "measurement"], "per_liter"),
                    "reporting_date": self._extract_field(record, ["date", "period", "reportingDate", "reporting_date"], utc_now().isoformat()),
                    "product_type": raw_data.metadata.get("product_type", "fuel")
                }
                
                if parsed_record["country"] and parsed_record["price"] is not None:
                    parsed_record["batch_id"] = raw_data.batch_id
                    parsed_record["ingestion_timestamp"] = raw_data.ingestion_timestamp
                    parsed.append(parsed_record)
            
            self.logger.info(
                "Parsed JSON data",
                batch_id=raw_data.batch_id,
                record_count=len(parsed),
                correlation_id=raw_data.batch_id
            )
            
            return parsed
            
        except json.JSONDecodeError as e:
            self.logger.error("JSON decode failed", error=str(e))
            return []
    
    def _parse_xml(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        root = ET.fromstring(raw_data.raw_content)
        records = []
        
        for country_elem in root.findall(".//country"):
            record = self._parse_country_element(country_elem)
            if record:
                record["batch_id"] = raw_data.batch_id
                record["ingestion_timestamp"] = raw_data.ingestion_timestamp
                records.append(record)
        
        return records
    
    def _parse_country_element(self, elem: ET.Element) -> Optional[Dict[str, Any]]:
        try:
            return {
                "country": self._get_text(elem, "name"),
                "price": self._get_float(elem, "price"),
                "currency": self._get_text(elem, "currency", "USD"),
                "unit": self._get_text(elem, "unit", "per_liter"),
                "reporting_date": self._get_text(elem, "date")
            }
        except (ValueError, AttributeError) as e:
            self.logger.warning("Failed to parse country element", error=str(e))
            return None
    
    @staticmethod
    def _extract_field(record: Dict, keys: List[str], default: Any = None) -> Any:
        for key in keys:
            if key in record and record[key] is not None:
                val = record[key]
                # Handle nested objects
                if isinstance(val, dict):
                    return val.get("name") or val.get("value") or str(val)
                return val
        return default
    
    @staticmethod
    def _extract_price(record: Dict) -> Optional[float]:
        for key in ["price", "value", "amount", "cost", "rate"]:
            if key in record:
                try:
                    val = record[key]
                    if isinstance(val, (int, float)):
                        return float(val)
                    if isinstance(val, str):
                        return float(val.replace(",", "").replace("$", ""))
                except (ValueError, TypeError):
                    continue
        return None
    
    @staticmethod
    def _get_text(elem: ET.Element, tag: str, default: Optional[str] = None) -> Optional[str]:
        child = elem.find(tag)
        return child.text if child is not None else default
    
    @staticmethod
    def _get_float(elem: ET.Element, tag: str) -> Optional[float]:
        text = XMLFetcher._get_text(elem, tag)
        return float(text) if text else None


class IngestionError(Exception):
    pass


class ParseError(Exception):
    pass