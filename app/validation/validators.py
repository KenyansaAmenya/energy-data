from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from app.utils.date_utils import parse_iso_datetime
from app.utils.logger import get_logger
from app.utils.models import DataQualityCheck, EnergyPriceRecord

logger = get_logger(__name__)


class ValidationResult:
    def __init__(self):
        self.is_valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, message: str) -> None:
        self.is_valid = False
        self.errors.append(message)
    
    def add_warning(self, message: str) -> None:
        self.warnings.append(message)


class BaseValidator(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = logger
    
    @abstractmethod
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        pass

class NullCheckValidator(BaseValidator):

    REQUIRED_FIELDS = ["country", "price", "product_type", "reporting_date"]
    
    def __init__(self):
        super().__init__("null_check")
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        for field in self.REQUIRED_FIELDS:
            value = record.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                result.add_error(f"Required field '{field}' is null or empty")
        
        return result


class TypeValidator(BaseValidator):
    
    def __init__(self):
        super().__init__("type_validation")
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        price = record.get("price")
        if price is not None:
            try:
                float(price)
            except (ValueError, TypeError):
                result.add_error(f"Field 'price' must be numeric, got: {type(price).__name__}")
        
        date_val = record.get("reporting_date")
        if date_val is not None:
            parsed = parse_iso_datetime(str(date_val)) if isinstance(date_val, str) else date_val
            if parsed is None and not isinstance(date_val, (int, float)):
                result.add_error(f"Field 'reporting_date' must be a valid date")
        
        country = record.get("country")
        if country is not None and not isinstance(country, str):
            result.add_error(f"Field 'country' must be string, got: {type(country).__name__}")
        
        return result


class NegativeValueValidator(BaseValidator):
    
    def __init__(self):
        super().__init__("negative_value_check")
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        price = record.get("price")
        if price is not None:
            try:
                price_float = float(price)
                if price_float <= 0:
                    result.add_error(f"Field 'price' must be positive, got: {price_float}")
                elif price_float > 10000: 
                    result.add_warning(f"Field 'price' seems unusually high: {price_float}")
            except (ValueError, TypeError):
                pass 
        
        return result


class DuplicateValidator(BaseValidator):

    def __init__(self, existing_keys: Optional[set] = None):
        super().__init__("duplicate_check")
        self.existing_keys = existing_keys or set()
        self.seen_keys: set = set()
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        country = str(record.get("country", "")).strip().lower()
        product = str(record.get("product_type", "")).strip().lower()
        date = str(record.get("reporting_date", "")).strip()
        
        business_key = f"{country}:{product}:{date}"
        
        if business_key in self.existing_keys or business_key in self.seen_keys:
            result.add_error(f"Duplicate record detected for key: {business_key}")
        else:
            self.seen_keys.add(business_key)
        
        return result


class SchemaValidator(BaseValidator):
    
    def __init__(self):
        super().__init__("schema_validation")
    
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        
        try:
            EnergyPriceRecord(**record)
        except Exception as e:
            result.add_error(f"Schema validation failed: {str(e)}")
        
        return result

class ValidationPipeline:
    
    def __init__(self, validators: Optional[List[BaseValidator]] = None):
        self.validators = validators or []
        self.logger = logger
    
    def add_validator(self, validator: BaseValidator) -> None:
        self.validators.append(validator)
    
    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        all_errors = []
        
        for validator in self.validators:
            result = validator.validate(record)
            if not result.is_valid:
                all_errors.extend(result.errors)
        
        return len(all_errors) == 0, all_errors
    
    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        batch_id: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[DataQualityCheck]]:
        valid_records = []
        invalid_records = []
        
        validator_stats = {v.name: {"passed": 0, "failed": 0} for v in self.validators}
        
        for idx, record in enumerate(records):
            is_valid, errors = self.validate_record(record)
            
            for validator in self.validators:
                result = validator.validate(record)
                if result.is_valid:
                    validator_stats[validator.name]["passed"] += 1
                else:
                    validator_stats[validator.name]["failed"] += 1
            
            if is_valid:
                valid_records.append(record)
            else:
                record["_validation_errors"] = errors
                record["_record_index"] = idx
                invalid_records.append(record)
        
        quality_checks = []
        for validator in self.validators:
            stats = validator_stats[validator.name]
            total = stats["passed"] + stats["failed"]
            
            quality_checks.append(DataQualityCheck(
                batch_id=batch_id,
                check_name=validator.name,
                check_type=validator.name,
                passed=stats["failed"] == 0,
                failed_records=stats["failed"],
                total_records=total,
                details={
                    "pass_rate": stats["passed"] / total if total > 0 else 1.0,
                    "failure_examples": [
                        r["_validation_errors"] for r in invalid_records[:5]
                    ]
                }
            ))
        
        self.logger.info(
            "Batch validation complete",
            batch_id=batch_id,
            total_records=len(records),
            valid_records=len(valid_records),
            invalid_records=len(invalid_records),
            correlation_id=batch_id
        )
        
        return valid_records, invalid_records, quality_checks


def create_default_pipeline(existing_keys: Optional[set] = None) -> ValidationPipeline:
    pipeline = ValidationPipeline()
    pipeline.add_validator(NullCheckValidator())
    pipeline.add_validator(TypeValidator())
    pipeline.add_validator(NegativeValueValidator())
    pipeline.add_validator(DuplicateValidator(existing_keys))
    pipeline.add_validator(SchemaValidator())
    return pipeline