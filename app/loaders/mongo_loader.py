from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, UpdateOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from app.utils.date_utils import utc_now
from app.utils.logger import get_logger
from app.utils.models import DataQualityCheck, PipelineRun 
from config.settings import MongoConfig

logger = get_logger(__name__)

class MongoLoader:
    def __init__(self, db: Database, config: MongoConfig):
        self.db = db
        self.config = config
        self.logger = logger
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try: 
            raw_collection = self.db[self.config.raw_collection]
            raw_collection.create_index([("batch_id", ASCENDING)], unique=False)  
            raw_collection.create_index([("ingestion_timestamp", ASCENDING)])  

            curated_collections = [ 
                self.config.fuel_collection,
                self.config.electricity_collection,
                self.config.gas_collection
            ]    

            for collection_name in curated_collections:
                collection = self.db[collection_name]  
                collection.create_index(
                    [("country", ASCENDING), 
                    ("product_type", ASCENDING), 
                    ("reporting_date", ASCENDING)],
                    unique=True,
                    name="business_key_idx"
                )
                collection.create_index([("batch_id", ASCENDING)])  
                collection.create_index([("ingestion_timestamp", ASCENDING)])

            self.db[self.config.pipeline_runs_collection].create_index(
                [("batch_id", ASCENDING)], unique=True
            )    
            self.db[self.config.quality_checks_collection].create_index(
                [("batch_id", ASCENDING), ("check_name", ASCENDING)]
            )

            self.logger.info("MongoDB indexes ensured")
        except Exception as e:
            self.logger.error("Failed to create indexes", error=str(e))
            raise

    def load_raw_data(self, raw_records: List[Dict[str, Any]]) -> int:  
        if not raw_records:  
            return 0

        try:
            collection = self.db[self.config.raw_collection]
            for record in raw_records:
                if "ingestion_timestamp" not in record:
                    record["ingestion_timestamp"] = utc_now()

            result = collection.insert_many(raw_records, ordered=False)

            self.logger.info(
                "Loaded raw data",
                count=len(result.inserted_ids),
                batch_id=raw_records[0].get("batch_id", "unknown")
            )
            return len(result.inserted_ids)

        except Exception as e: 
            self.logger.error("Failed to load raw data", error=str(e))
            raise

    def load_curated_data(
        self,
        records: List[Dict[str, Any]],
        product_type: str
    ) -> int:
        if not records:
            return 0

        collection_name = self._get_collection_for_product(product_type)
        collection = self.db[collection_name]

        operations = []
        for record in records:
            filter_doc = {
                "country": record.get("country"),
                "product_type": record.get("product_type"),
                "reporting_date": record.get("reporting_date")
            }    

            update_doc = {
                "$set": {
                    **{k: v for k, v in record.items() if k not in ["country", "product_type", "reporting_date"]},
                    "last_updated": utc_now()
                },
                "$setOnInsert": {  
                    "created_at": utc_now()
                }
            }

            operations.append(UpdateOne(filter_doc, update_doc, upsert=True))

        try:
            result = collection.bulk_write(operations, ordered=False)

            self.logger.info(
                "Loaded curated data",
                collection=collection_name,
                upserted=result.upserted_count, 
                modified=result.modified_count,
                batch_id=records[0].get("batch_id", "unknown")
            )    
            return result.upserted_count + result.modified_count

        except BulkWriteError as bwe:
            self.logger.warning(
                "Curated load had partial failures",
                error=str(bwe.details)[:500]
            )    
            return len(operations) - len(bwe.details.get("writeErrors", []))
        except Exception as e:
            self.logger.error("Failed to load curated data", error=str(e))
            raise

    def save_pipeline_run(self, run: PipelineRun) -> None:  
        collection = self.db[self.config.pipeline_runs_collection]
        collection.update_one(
            {"batch_id": run.batch_id},
            {"$set": run.dict()},
            upsert=True
        )
        self.logger.info("Saved pipeline run", batch_id=run.batch_id, status=run.status)

    def save_quality_checks(self, checks: List[DataQualityCheck]) -> None:  # FIX: was missing
        if not checks:
            return

        collection = self.db[self.config.quality_checks_collection]
        operations = [
            UpdateOne(
                {"batch_id": check.batch_id, "check_name": check.check_name},
                {"$set": check.dict()},
                upsert=True
            )
            for check in checks
        ]            
        collection.bulk_write(operations)

    def get_existing_business_keys(
        self, 
        product_type: str,
        lookback_days: int = 30
    ) -> set: 
        from datetime import timedelta

        collection_name = self._get_collection_for_product(product_type)
        collection = self.db[collection_name]

        cutoff = utc_now() - timedelta(days=lookback_days)

        cursor = collection.find(
            {"ingestion_timestamp": {"$gte": cutoff}},
            {"country": 1, "product_type": 1, "reporting_date": 1}  # FIX: county → country
        )

        keys = set()
        for doc in cursor:
            key = f"{doc['country']}:{doc['product_type']}:{doc['reporting_date']}"
            keys.add(key)

        return keys

    def _get_collection_for_product(self, product_type: str) -> str:
        mapping = {  
            "fuel": self.config.fuel_collection,
            "electricity": self.config.electricity_collection,
            "natural_gas": self.config.gas_collection
        }      
        collection = mapping.get(product_type) 
        if not collection:
            raise ValueError(f"Unknown product type: {product_type}")
        return collection