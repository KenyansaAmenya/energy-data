import json
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.ingestion.base_fetcher import BaseDataFetcher
from app.utils.date_utils import generate_batch_id, utc_now
from app.utils.models import ProductType, RawAPIData
from config.settings import APIConfig, PipelineConfig


class OilPriceFetcher(BaseDataFetcher):

    def __init__(self, config: APIConfig, pipeline_config: PipelineConfig):
        super().__init__(config)
        self.pipeline_config = pipeline_config

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Token {self.config.oilprice_api_key}",
            "Content-Type": "application/json"
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True
    )
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.config.oilprice_base_url.rstrip('/')}{endpoint}"

        self.logger.info(
            "Making OilPriceAPI request",
            url=url,
            params=params
        )

        response = requests.get(
            url,
            headers=self._get_headers(),
            params=params,
            timeout=self.config.oilprice_request_timeout
        )
        response.raise_for_status()
        return response.json()

    def fetch(
        self,
        product_type: ProductType,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs: Any
    ) -> List[RawAPIData]:
        batch_id = generate_batch_id()
        
        commodity_codes = self._get_commodity_codes(product_type)
        all_raw_records = []
        
        for code in commodity_codes:
            try:
                params = {
                    "by_code": code
                }
                
                self.logger.info(
                    "Fetching OilPriceAPI data",
                    commodity=code,
                    params=params
                )
                
                response_data = self._make_request("/prices/latest", params=params)
                
                raw_data = RawAPIData(
                    batch_id=batch_id,
                    source_url=f"{self.config.oilprice_base_url}/prices/latest",
                    raw_content=json.dumps(response_data),
                    format_type="json",
                    ingestion_timestamp=utc_now(),
                    metadata={
                        "product_type": product_type.value,
                        "commodity_code": code,
                        "api_version": "v1"
                    }
                )
                
                all_raw_records.append(raw_data)
                
                self.logger.info(
                    "Successfully fetched OilPriceAPI data",
                    batch_id=batch_id,
                    product_type=product_type.value,
                    commodity_code=code
                )
                
            except requests.RequestException as e:
                self.logger.error(
                    "Failed to fetch OilPriceAPI data",
                    batch_id=batch_id,
                    product_type=product_type.value,
                    commodity_code=code,
                    error=str(e)
                )
        
        return all_raw_records

    def _get_commodity_codes(self, product_type: ProductType) -> List[str]:
        mapping = {
            ProductType.FUEL: ["WTI_USD", "BRENT_CRUDE_USD"],
            ProductType.NATURAL_GAS: ["NATURAL_GAS_USD"],
            ProductType.ELECTRICITY: []
        }
        return mapping.get(product_type, ["WTI_USD"])

    def parse(self, raw_data: RawAPIData) -> List[Dict[str, Any]]:
        try:
            data = json.loads(raw_data.raw_content)
            records = []

            product_type = raw_data.metadata.get("product_type")
            commodity_code = raw_data.metadata.get("commodity_code")

            if data.get("status") == "success" and "data" in data:
                price_data = data["data"]
            
                if isinstance(price_data, dict) and "price" in price_data:
                    record = self._parse_single_price(price_data, product_type, commodity_code)
                    if record:
                        record["batch_id"] = raw_data.batch_id
                        record["ingestion_timestamp"] = raw_data.ingestion_timestamp
                        records.append(record)
                elif isinstance(price_data, list):
                    for item in price_data:
                        record = self._parse_single_price(item, product_type, commodity_code)
                        if record:
                            record["batch_id"] = raw_data.batch_id
                            record["ingestion_timestamp"] = raw_data.ingestion_timestamp
                            records.append(record)

            self.logger.info(
                "Parsed OilPriceAPI data",
                batch_id=raw_data.batch_id,
                record_count=len(records),
                commodity_code=commodity_code
            )

            return records

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(
                "Failed to parse OilPriceAPI response",
                batch_id=raw_data.batch_id,
                error=str(e)
            )
            raise

    def _parse_single_price(
        self,
        item: Dict[str, Any],
        product_type: str,
        commodity_code: str
    ) -> Optional[Dict[str, Any]]:
        try:
            price_value = item.get("price")
            if price_value is None:
                return None

            mapped_product, unit = self._map_commodity_to_product(commodity_code)

            if not mapped_product:
                mapped_product = product_type if product_type else "fuel"

            timestamp = item.get("created_at") or item.get("updated_at") or item.get("timestamp") or utc_now().isoformat()
            
            if "T" in timestamp:
                reporting_date = timestamp.split("T")[0]
            else:
                reporting_date = timestamp[:10] if len(timestamp) >= 10 else timestamp

            return {
                "country": "Global",
                "product_type": mapped_product,
                "price": float(price_value),
                "currency": item.get("currency", "USD"),
                "unit": unit,
                "source": "oilpriceapi",
                "reporting_date": reporting_date,
                "commodity_code": commodity_code,
                "price_type": item.get("type", "spot_price"),
                "formatted_price": item.get("formatted")
            }

        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning("Failed to parse OilPriceAPI data item", error=str(e))
            return None

    def _map_commodity_to_product(self, commodity_code: str) -> tuple:
        fuel_codes = {
            "WTI_USD": ("fuel", "per_barrel"),
            "BRENT_CRUDE_USD": ("fuel", "per_barrel"),
        }
        gas_codes = {
            "NATURAL_GAS_USD": ("natural_gas", "per_mmbtu"),
        }

        if commodity_code in fuel_codes:
            return fuel_codes[commodity_code]
        elif commodity_code in gas_codes:
            return gas_codes[commodity_code]
        return (None, "per_unit")

    def validate_source(self) -> bool:
        try:
            response = self._make_request("/prices/latest", params={"by_code": "WTI_USD"})
            return response.get("status") == "success" and "data" in response
        except Exception as e:
            self.logger.warning(f"OilPriceAPI validation failed: {e}")
            return False