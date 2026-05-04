from typing import Any, Dict, List, Optional
from app.utils.date_utils import parse_iso_datetime, utc_now
from app.utils.logger import get_logger
from app.utils.models import ProductType

logger = get_logger(__name__)

class BaseTransformer:
    def __init__(self, name: str):
        self.name = name
        self.logger = logger

    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError 


class CountryNormalizer(BaseTransformer):
    COUNTRY_MAPPINGS = {
        "usa": "United States",
        "us": "United States",
        "uk": "United Kingdom",
        "great britain": "United Kingdom",
        "uae": "United Arab Emirates",
        "russia": "Russian Federation"
    }           

    def __init__(self):
        super().__init__(name="CountryNormalizer")
    
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        country = record.get("country")
        if isinstance(country, str):
            normalized = self.COUNTRY_MAPPINGS.get(country.lower().strip(), country.strip().title())
            record["country"] = normalized
        return record

class PriceNormalizer(BaseTransformer):
    def __init__(self):
        super().__init__("PriceNormalizer")
    
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        price = record.get("price")
        if price is not None:
            try: 
                record["price"] = float(price)
            except (ValueError, TypeError):
                record["price"] = None
        
        currency = record.get("currency")
        if isinstance(currency, str):
            record["currency"] = currency.upper()
        
        return record


class DateNormalizer(BaseTransformer):
    def __init__(self):
        super().__init__("DateNormalizer")

    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        date_val = record.get("reporting_date")
        if date_val is not None:
            if isinstance(date_val, str):
                parsed = parse_iso_datetime(date_val)
                if parsed:
                    record["reporting_date"] = parsed
                else:
                    try:
                        from datetime import datetime
                        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:  # FIX: was "%d/%m%Y"
                            try:
                                record["reporting_date"] = datetime.strptime(date_val, fmt)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
        return record

class ProductTypeEnricher(BaseTransformer):
    def __init__(self, product_type: ProductType):
        super().__init__("ProductTypeEnricher")
        self.product_type = product_type
    
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        record["product_type"] = self.product_type.value
        return record
class UnitNormalizer(BaseTransformer):
    UNIT_MAPPINGS = {
        "liter": "per_liter",
        "litre": "per_liter",
        "l": "per_liter",
        "kwh": "per_kwh",
        "kw/h": "per_kwh",
        "m3": "per_cubic_meter",
        "cubic meter": "per_cubic_meter",
        "gallon": "per_gallon",
        "gal": "per_gallon" 
    }                     

    def __init__(self): 
        super().__init__("UnitNormalizer")
        
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        unit = record.get("unit")
        if isinstance(unit, str):
            unit_lower = unit.lower().strip()
            record["unit"] = self.UNIT_MAPPINGS.get(unit_lower, unit_lower)
        return record

class MetadataEnricher(BaseTransformer):
    def __init__(self, batch_id: str):
        super().__init__("MetadataEnricher")
        self.batch_id = batch_id
        self.timestamp = utc_now()
    
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        record["batch_id"] = self.batch_id
        if "ingestion_timestamp" not in record:
            record["ingestion_timestamp"] = self.timestamp
        return record


class TransformationPipeline:
    def __init__(self, transformers: Optional[List[BaseTransformer]] = None):
        self.transformers = transformers or []
        self.logger = logger

    def add_transformer(self, transformer: BaseTransformer) -> None: 
        self.transformers.append(transformer)

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:  
        for transformer in self.transformers:
            record = transformer.transform(record)
        return record

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:  
        transformed = []
        for record in records:
            try:
                transformed.append(self.transform_record(record))
            except Exception as e:
                self.logger.warning(
                    "Record transformation failed",
                    error=str(e),
                    record_preview=str(record)[:200]
                )                                   
        return transformed


def create_transformation_pipeline(
    batch_id: str,
    product_type: ProductType
) -> TransformationPipeline:
    pipeline = TransformationPipeline()
    pipeline.add_transformer(CountryNormalizer())
    pipeline.add_transformer(PriceNormalizer())  
    pipeline.add_transformer(DateNormalizer())
    pipeline.add_transformer(ProductTypeEnricher(product_type))
    pipeline.add_transformer(UnitNormalizer())  
    pipeline.add_transformer(MetadataEnricher(batch_id)) 
    return pipeline