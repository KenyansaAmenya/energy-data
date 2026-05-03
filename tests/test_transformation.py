import pytest

from app.transformation.transformers import (
    CountryNormalizer,
    DateNormalizer,
    PriceNormalizer,
    ProductTypeEnricher,
    TransformationPipeline
)
from app.utils.models import ProductType


class TestCountryNormalizer:
    def test_standard_country(self):
        transformer = CountryNormalizer()
        result = transformer.transform({"country": "usa"})
        assert result["country"] == "United States"
    
    def test_unmapped_country(self):
        transformer = CountryNormalizer()
        result = transformer.transform({"country": "germany"})
        assert result["country"] == "Germany"


class TestPriceNormalizer:
    def test_string_price(self):
        transformer = PriceNormalizer()
        result = transformer.transform({"price": "1.50"})
        assert result["price"] == 1.50
    
    def test_invalid_price(self):
        transformer = PriceNormalizer()
        result = transformer.transform({"price": "invalid"})
        assert result["price"] is None
    
    def test_currency_uppercase(self):
        transformer = PriceNormalizer()
        result = transformer.transform({"currency": "usd"})
        assert result["currency"] == "USD"


class TestDateNormalizer:
    def test_iso_date(self):
        transformer = DateNormalizer()
        result = transformer.transform({"reporting_date": "2024-01-15T00:00:00"})
        assert result["reporting_date"].year == 2024
    
    def test_simple_date(self):
        transformer = DateNormalizer()
        result = transformer.transform({"reporting_date": "2024-01-15"})
        assert result["reporting_date"].year == 2024


class TestTransformationPipeline:
    def test_full_pipeline(self):
        pipeline = TransformationPipeline([
            CountryNormalizer(),
            PriceNormalizer(),
            DateNormalizer(),
            ProductTypeEnricher(ProductType.FUEL)
        ])
        
        record = {
            "country": "usa",
            "price": "2.50",
            "reporting_date": "2024-01-15",
            "currency": "usd"
        }
        
        result = pipeline.transform_record(record)
        
        assert result["country"] == "United States"
        assert result["price"] == 2.50
        assert result["product_type"] == "fuel"
        assert result["currency"] == "USD"