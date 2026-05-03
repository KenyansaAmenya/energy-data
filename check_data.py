import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.utils.mongo_client import get_mongo_db
from config.settings import get_config

config = get_config()
db = get_mongo_db(config.mongo)

print(f"Connected to: {config.mongo.db_name}")
print(f"Using URI: {config.mongo.uri[:30]}...")

collections = [
    "raw_api_data",
    "fuel_prices", 
    "electricity_prices",
    "natural_gas_prices",
    "pipeline_runs",
    "data_quality_checks"
]

for coll_name in collections:
    count = db[coll_name].count_documents({})
    print(f"{coll_name}: {count} documents")

# Show latest pipeline run
latest = db["pipeline_runs"].find_one(sort=[("start_time", -1)])
if latest:
    print(f"\nLatest run: {latest['batch_id']}")
    print(f"Status: {latest['status']}")
    print(f"Records: {latest.get('records_processed', 0)} processed, {latest.get('records_valid', 0)} valid")

# Show sample raw data
raw_sample = db["raw_api_data"].find_one()
if raw_sample:
    print(f"\nSample raw data format: {raw_sample.get('format_type')}")
    print(f"Raw content preview: {str(raw_sample.get('raw_content', ''))[:200]}")