from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, validator

class ProductType(str, Enum):
    FUEL = 'fuel'
    ELECTRICITY = 'electricity'
    NATURAL_GAS = 'natural_gas'

class Currency(str, Enum):
    USD = 'USD'
    EUR = 'EUR'
    GBP = 'GBP'
    LOCAL = 'LOCAL'

class EnergyPriceRecord(BaseModel):
    country: str = Field(..., min_length=1, description="Country name")
    product_type: ProductType = Field(..., description="Type of energy product")
    price: float = Field(..., gt=0, description="Price value (must be positive)")
    currency: str = Field(default="USD", min_length=1)
    unit: str = Field(default="per_litre", description="Unit of measurement")
    source: str = Field(default="global_petrol_prices", description="Data source")
    reporting_date: datetime = Field(..., description="Date the price was reported")
    ingestion_timestamp: datetime = Field(default_factory=datetime.utcnow)
    batch_id: str = Field(..., description="Pipeline batch identifier")

    @validator('country')
    def normalize_country(cls, v) -> str:
        return v.strip().title()

    class Config:
        json_encoders = {  # FIX: 
            datetime: lambda v: v.isoformat()
        }    

class RawAPIData(BaseModel):
    batch_id: str 
    source_url: str
    raw_content: str
    format_type: str
    ingestion_timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)

class PipelineRun(BaseModel):
    batch_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_valid: int = 0
    records_invalid: int = 0 
    error_message: Optional[str] = None
    execution_type: str = "incremental"

class DataQualityCheck(BaseModel):
    batch_id: str
    check_name: str
    check_type: str
    passed: bool
    failed_records: int = 0
    total_records: int = 0
    details: Dict[str, Any] = Field(default_factory=dict)
    checked_at: datetime = Field(default_factory=datetime.utcnow)

class PriceReport(BaseModel):
    product_type: ProductType  
    latest_price: float
    currency: str
    unit: str
    country: str
    reporting_date: datetime
    change_24h: Optional[float] = None 
    change_percent_24h: Optional[float] = None

class DataQualitySummary(BaseModel):
    batch_id: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    pass_rate: float
    checks: List[DataQualityCheck]