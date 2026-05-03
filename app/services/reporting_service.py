from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pymongo.database import Database

from app.utils.date_utils import utc_now
from app.utils.logger import get_logger
from app.utils.models import DataQualitySummary, PriceReport, ProductType
from config.settings import MongoConfig

logger = get_logger(__name__)


class ReportingService:
    
    def __init__(self, db: Database, config: MongoConfig):
        self.db = db
        self.config = config
        self.logger = logger
    
    def get_latest_prices(
        self,
        product_type: Optional[ProductType] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        collections = self._get_target_collections(product_type)
        results = []
        
        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                cursor = collection.find().sort("reporting_date", -1).limit(limit)
                results.extend(list(cursor))
            except Exception as e:
                self.logger.warning(f"Error querying {collection_name}: {e}")
        
        for r in results:
            r.pop("_id", None)
            if "reporting_date" in r and isinstance(r["reporting_date"], datetime):
                r["reporting_date"] = r["reporting_date"].isoformat()
            if "ingestion_timestamp" in r and isinstance(r["ingestion_timestamp"], datetime):
                r["ingestion_timestamp"] = r["ingestion_timestamp"].isoformat()
        
        return results
    
    def get_price_changes_24h(self) -> List[Dict[str, Any]]:
        cutoff = utc_now() - timedelta(hours=24)
        collections = [
            self.config.fuel_collection,
            self.config.electricity_collection,
            self.config.gas_collection
        ]
        
        changes = []
        
        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                
                pipeline = [
                    {"$match": {"ingestion_timestamp": {"$gte": cutoff}}},
                    {"$sort": {"reporting_date": -1}},
                    {
                        "$group": {
                            "_id": {"country": "$country", "product_type": "$product_type"},
                            "latest": {"$first": "$$ROOT"},
                            "previous": {"$last": "$$ROOT"}
                        }
                    }
                ]
                
                for doc in collection.aggregate(pipeline):
                    latest = doc.get("latest", {})
                    previous = doc.get("previous", {})
                    
                    curr_price = latest.get("price")
                    prev_price = previous.get("price")
                    
                    if curr_price and prev_price and prev_price != 0 and curr_price != prev_price:
                        delta = curr_price - prev_price
                        delta_pct = (delta / prev_price) * 100
                        
                        changes.append({
                            "country": doc["_id"]["country"],
                            "product_type": doc["_id"]["product_type"],
                            "current_price": curr_price,
                            "previous_price": prev_price,
                            "currency": latest.get("currency", "USD"),
                            "unit": latest.get("unit", "per_liter"),
                            "change_24h": round(delta, 4),
                            "change_percent_24h": round(delta_pct, 2),
                            "trend": "up" if delta > 0 else "down"
                        })
            except Exception as e:
                self.logger.warning(f"Error computing changes for {collection_name}: {e}")
        
        changes.sort(key=lambda x: abs(x["change_24h"]), reverse=True)
        return changes[:20]
    
    def get_data_quality_summary(self, batch_id: Optional[str] = None) -> Dict[str, Any]:
        collection = self.db[self.config.quality_checks_collection]
        
        query = {}
        if batch_id:
            query["batch_id"] = batch_id
        
        if not batch_id:
            latest = self.db[self.config.pipeline_runs_collection].find_one(
                sort=[("start_time", -1)]
            )
            if latest:
                query["batch_id"] = latest["batch_id"]
        
        try:
            checks = list(collection.find(query))
        except Exception:
            checks = []
        
        if not checks:
            return {
                "batch_id": batch_id or "unknown",
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "pass_rate": 0.0,
                "checks": []
            }
        
        target_batch = checks[0].get("batch_id", "unknown") if checks else "unknown"
        total = len(checks)
        passed = sum(1 for c in checks if c.get("passed", False))
        
        run = self.db[self.config.pipeline_runs_collection].find_one(
            {"batch_id": target_batch}
        )
        
        return {
            "batch_id": target_batch,
            "total_checks": total,
            "passed_checks": passed,
            "failed_checks": total - passed,
            "pass_rate": round(passed / total, 4) if total > 0 else 0.0,
            "records_processed": run.get("records_processed", 0) if run else 0,
            "records_valid": run.get("records_valid", 0) if run else 0,
            "records_invalid": run.get("records_invalid", 0) if run else 0,
            "checks": [
                {
                    "check_name": c.get("check_name", "unknown"),
                    "check_type": c.get("check_type", "unknown"),
                    "passed": c.get("passed", False),
                    "failed_records": c.get("failed_records", 0),
                    "total_records": c.get("total_records", 0)
                }
                for c in checks
            ]
        }
    
    def get_24h_summary(self) -> Dict[str, Any]:
        cutoff = utc_now() - timedelta(hours=24)
        
        # Count records processed in last 24h
        total_processed = 0
        for coll_name in [
            self.config.fuel_collection,
            self.config.electricity_collection,
            self.config.gas_collection
        ]:
            try:
                total_processed += self.db[coll_name].count_documents(
                    {"ingestion_timestamp": {"$gte": cutoff}}
                )
            except Exception:
                pass
        
        latest_prices = self.get_latest_prices(limit=10)
        changes = self.get_price_changes_24h()
        
        return {
            "generated_at": utc_now().isoformat(),
            "period": "24h",
            "total_records_processed_24h": total_processed,
            "latest_prices": latest_prices,
            "top_changes": {
                "increases": [c for c in changes if c["trend"] == "up"][:5],
                "decreases": [c for c in changes if c["trend"] == "down"][:5]
            },
            "data_quality": self.get_data_quality_summary()
        }
    
    def _get_target_collections(
        self,
        product_type: Optional[ProductType]
    ) -> List[str]:
        if product_type:
            mapping = {
                ProductType.FUEL: self.config.fuel_collection,
                ProductType.ELECTRICITY: self.config.electricity_collection,
                ProductType.NATURAL_GAS: self.config.gas_collection
            }
            return [mapping.get(product_type, self.config.fuel_collection)]
        return [
            self.config.fuel_collection,
            self.config.electricity_collection,
            self.config.gas_collection
        ]