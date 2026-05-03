import pytest

from app.validation.validators import (
    DuplicateValidator,
    NegativeValueValidator,
    NullCheckValidator,
    TypeValidator,
    ValidationPipeline,
    ValidationResult
)


class TestNullCheckValidator:
    def test_valid_record(self):
        validator = NullCheckValidator()
        record = {
            "country": "USA",
            "price": 1.50,
            "product_type": "fuel",
            "reporting_date": "2024-01-01"
        }
        result = validator.validate(record)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_missing_country(self):
        validator = NullCheckValidator()
        record = {
            "price": 1.50,
            "product_type": "fuel",
            "reporting_date": "2024-01-01"
        }
        result = validator.validate(record)
        assert result.is_valid is False
        assert any("country" in e for e in result.errors)
    
    def test_empty_string(self):
        validator = NullCheckValidator()
        record = {
            "country": "   ",
            "price": 1.50,
            "product_type": "fuel",
            "reporting_date": "2024-01-01"
        }
        result = validator.validate(record)
        assert result.is_valid is False


class TestTypeValidator:
    def test_valid_types(self):
        validator = TypeValidator()
        record = {
            "price": 1.50,
            "reporting_date": "2024-01-01T00:00:00",
            "country": "USA"
        }
        result = validator.validate(record)
        assert result.is_valid is True
    
    def test_invalid_price(self):
        validator = TypeValidator()
        record = {"price": "not_a_number"}
        result = validator.validate(record)
        assert result.is_valid is False
        assert any("numeric" in e for e in result.errors)


class TestNegativeValueValidator:
    def test_positive_price(self):
        validator = NegativeValueValidator()
        result = validator.validate({"price": 1.50})
        assert result.is_valid is True
    
    def test_zero_price(self):
        validator = NegativeValueValidator()
        result = validator.validate({"price": 0})
        assert result.is_valid is False
    
    def test_negative_price(self):
        validator = NegativeValueValidator()
        result = validator.validate({"price": -1.50})
        assert result.is_valid is False


class TestDuplicateValidator:
    def test_no_duplicate(self):
        validator = DuplicateValidator()
        record = {
            "country": "USA",
            "product_type": "fuel",
            "reporting_date": "2024-01-01"
        }
        result = validator.validate(record)
        assert result.is_valid is True
    
    def test_duplicate_detected(self):
        existing = {"usa:fuel:2024-01-01"}
        validator = DuplicateValidator(existing)
        record = {
            "country": "USA",
            "product_type": "fuel",
            "reporting_date": "2024-01-01"
        }
        result = validator.validate(record)
        assert result.is_valid is False


class TestValidationPipeline:
    def test_batch_validation(self):
        pipeline = ValidationPipeline([
            NullCheckValidator(),
            TypeValidator()
        ])
        
        records = [
            {"country": "USA", "price": 1.50, "product_type": "fuel", "reporting_date": "2024-01-01"},
            {"country": None, "price": 1.50, "product_type": "fuel", "reporting_date": "2024-01-01"},
            {"country": "UK", "price": "bad", "product_type": "fuel", "reporting_date": "2024-01-01"}
        ]
        
        valid, invalid, checks = pipeline.validate_batch(records, "test_batch")
        
        assert len(valid) == 1
        assert len(invalid) == 2
        assert len(checks) == 2