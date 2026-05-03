import pytest
from unittest.mock import MagicMock, patch

from app.ingestion.xml_fetcher import XMLFetcher
from app.loaders.mongo_loader import MongoDBLoader
from app.utils.models import ProductType, RawAPIData
from config.settings import APIConfig, MongoConfig, PipelineConfig


class TestIngestionToMongoDB:
    @pytest.fixture
    def mock_db(self):
        return MagicMock()
    
    @pytest.fixture
    def mongo_config(self):
        return MongoConfig(
            uri="mongodb://test:27017/test",
            db_name="test_db"
        )
    
    def test_raw_data_loading(self, mock_db, mongo_config):
        loader = MongoDBLoader(mock_db, mongo_config)
        
        raw_records = [
            {
                "batch_id": "test_001",
                "source_url": "http://test.com",
                "raw_content": "<xml>test</xml>",
                "format_type": "xml",
                "ingestion_timestamp": "2024-01-01T00:00:00"
            }
        ]
        
        # Mock insert_many
        mock_collection = MagicMock()
        mock_collection.insert_many.return_value = MagicMock(inserted_ids=["id1"])
        mock_db.__getitem__.return_value = mock_collection
        
        count = loader.load_raw_data(raw_records)
        assert count == 1
    
    def test_curated_data_upsert(self, mock_db, mongo_config):
        loader = MongoDBLoader(mock_db, mongo_config)
        
        records = [
            {
                "country": "USA",
                "product_type": "fuel",
                "price": 2.50,
                "currency": "USD",
                "unit": "per_liter",
                "reporting_date": "2024-01-01",
                "batch_id": "test_001"
            }
        ]
        
        mock_collection = MagicMock()
        mock_collection.bulk_write.return_value = MagicMock(
            upserted_count=1,
            modified_count=0
        )
        mock_db.__getitem__.return_value = mock_collection
        
        count = loader.load_curated_data(records, "fuel")
        assert count == 1


class TestXMLFetcher:
    def test_parse_valid_xml(self):
        config = APIConfig(
            api_key="test_key",
            gpp_base_url="http://test.com",
            gpp_electricity_url="http://test.com/electricity",
            gpp_gas_url="http://test.com/gas"
        )
        pipeline_config = PipelineConfig(batch_size=100, max_retries=3, retry_delay_seconds=5)
        
        fetcher = XMLFetcher(config, pipeline_config)
        
        xml_content = """<?xml version="1.0"?>
        <data>
            <country>
                <name>USA</name>
                <price>2.50</price>
                <currency>USD</currency>
                <unit>liter</unit>
                <date>2024-01-01</date>
            </country>
        </data>"""
        
        raw_data = RawAPIData(
            batch_id="test_001",
            source_url="http://test.com",
            raw_content=xml_content,
            format_type="xml",
            ingestion_timestamp="2024-01-01T00:00:00"
        )
        
        records = fetcher.parse(raw_data)
        assert len(records) == 1
        assert records[0]["country"] == "USA"
        assert records[0]["price"] == 2.50