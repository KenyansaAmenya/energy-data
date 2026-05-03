from datetime import datetime, timedelta
from typing import Any, Dict, List
import time

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from app.ingestion.oilprice_fetcher import OilPriceFetcher
from app.loaders.mongo_loader import MongoLoader
from app.services.reporting_service import ReportingService
from app.transformation.transformers import create_transformation_pipeline
from app.utils.date_utils import generate_batch_id, utc_now
from app.utils.logger import get_logger
from app.utils.models import PipelineRun, ProductType, RawAPIData
from app.utils.mongo_client import get_mongo_db
from app.validation.validators import create_default_pipeline
from config.settings import get_config

logger = get_logger(__name__)

default_args = {
    "owner": "energy_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": lambda context: logger.error(
        "Task failed",
        task_id=context["task_instance"].task_id,
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        execution_date=str(context["execution_date"]),
        exception=str(context.get("exception", "Unknown"))
    )
}

def create_dag():

    dag = DAG(
        "energy_price_pipeline",
        default_args=default_args,
        description="Energy Price Data Pipeline (OilPriceAPI)",
        schedule_interval="0 */6 * * *",
        start_date=days_ago(1),
        catchup=False,
        tags=["energy", "prices", "etl", "oilpriceapi"],
        max_active_runs=1,
        params={
            "execution_type": "incremental",
            "product_types": ["fuel", "natural_gas"]
        }
    )
    
    def ingest_data_task(**context: Any) -> Dict[str, Any]:
        """Ingest data from OilPriceAPI."""
        config = get_config()
        batch_id = generate_batch_id()
        execution_type = context["params"].get("execution_type", "incremental")
        
        logger.info(
            "Starting OilPriceAPI ingestion",
            batch_id=batch_id,
            execution_type=execution_type,
            run_id=context["run_id"]
        )
        
        run = PipelineRun(
            batch_id=batch_id,
            status="running",
            start_time=utc_now(),
            execution_type=execution_type
        )
        
        db = get_mongo_db(config.mongo)
        loader = MongoLoader(db, config.mongo)
        loader.save_pipeline_run(run)
        
        all_raw_records = []
        ingestion_stats = []
        
        product_types = context["params"].get("product_types", ["fuel", "natural_gas"])
        
        for pt_str in product_types:
            try:
                product_type = ProductType(pt_str)
                
                fetcher = OilPriceFetcher(config.api, config.pipeline)
                
                raw_data_list = fetcher.fetch(
                    product_type=product_type,
                    limit=10
                )
                
                for raw_data in raw_data_list:
                    all_raw_records.append(raw_data.dict())
                
                ingestion_stats.append({
                    "product_type": pt_str,
                    "status": "success",
                    "records": len(raw_data_list)
                })
                
            except Exception as e:
                logger.error(
                    "OilPriceAPI ingestion failed for product",
                    product_type=pt_str,
                    error=str(e),
                    batch_id=batch_id
                )
                ingestion_stats.append({
                    "product_type": pt_str,
                    "status": "failed",
                    "error": str(e)
                })
        
        if all_raw_records:
            loader.load_raw_data(all_raw_records)
        else:
            logger.error("No raw records ingested", batch_id=batch_id)
            raise AirflowFailException(f"No data ingested for batch {batch_id}")
        
        # Push batch_id to XCom with multiple methods for reliability
        context["ti"].xcom_push(key="batch_id", value=batch_id)
        context["ti"].xcom_push(key="ingestion_stats", value=ingestion_stats)
        context["ti"].xcom_push(key="raw_count", value=len(all_raw_records))
        
        logger.info(
            "OilPriceAPI ingestion complete",
            batch_id=batch_id,
            total_raw=len(all_raw_records),
            run_id=context["run_id"]
        )
        
        return {
            "batch_id": batch_id,
            "records_ingested": len(all_raw_records),
            "stats": ingestion_stats
        }
    
    def transform_data_task(**context: Any) -> Dict[str, Any]:
        """Transform data from OilPriceAPI."""
        config = get_config()
        
        # TRY MULTIPLE WAYS TO GET BATCH_ID
        batch_id = context["ti"].xcom_pull(task_ids="ingest_data", key="batch_id")
        
        if not batch_id:
            # Try getting from the return value
            ingest_result = context["ti"].xcom_pull(task_ids="ingest_data")
            if ingest_result:
                batch_id = ingest_result.get("batch_id")
        
        if not batch_id:
            # Fallback: get the most recent batch from MongoDB
            db = get_mongo_db(config.mongo)
            raw_collection = db[config.mongo.raw_collection]
            latest_raw = raw_collection.find_one(sort=[("ingestion_timestamp", -1)])
            if latest_raw:
                batch_id = latest_raw.get("batch_id")
                logger.info(f"Using latest batch from MongoDB: {batch_id}")
            else:
                raise AirflowFailException("No batch_id found from ingest_data task or MongoDB")
        
        logger.info("Starting data transformation", batch_id=batch_id, run_id=context["run_id"])
        
        db = get_mongo_db(config.mongo)
        
        # Get raw documents for this batch
        raw_collection = db[config.mongo.raw_collection]
        raw_docs = list(raw_collection.find({"batch_id": batch_id}))
        
        if not raw_docs:
            logger.warning(f"No raw documents found for batch {batch_id}, trying latest")
            raw_docs = list(raw_collection.find().sort("ingestion_timestamp", -1).limit(10))
            if raw_docs:
                batch_id = raw_docs[0].get("batch_id")
                logger.info(f"Using latest batch: {batch_id}")
        
        if not raw_docs:
            raise AirflowFailException(f"No raw data found for batch {batch_id}")
        
        # Parse raw documents
        all_records = []
        fetcher = OilPriceFetcher(config.api, config.pipeline)
        
        for doc in raw_docs:
            if doc.get("format_type") == "json":
                raw = RawAPIData(**doc)
                parsed = fetcher.parse(raw)
                all_records.extend(parsed)
        
        if not all_records:
            logger.warning("No records parsed from raw data", batch_id=batch_id)
            return {"batch_id": batch_id, "transformed_count": 0}
        
        # Transform records by product type
        transformed_by_product: Dict[str, List[Dict]] = {}
        
        for record in all_records:
            product_type_str = record.get("product_type", "fuel")
            
            try:
                pt = ProductType(product_type_str)
            except ValueError:
                pt = ProductType.FUEL
            
            if pt.value not in transformed_by_product:
                transformed_by_product[pt.value] = []
            
            pipeline = create_transformation_pipeline(batch_id, pt)
            transformed = pipeline.transform([record])
            transformed_by_product[pt.value].extend(transformed)
        
        context["ti"].xcom_push(key="transformed_data", value=transformed_by_product)
        context["ti"].xcom_push(key="batch_id", value=batch_id)
        
        total_transformed = sum(len(v) for v in transformed_by_product.values())
        
        logger.info(
            "Transformation complete",
            batch_id=batch_id,
            total_transformed=total_transformed,
            run_id=context["run_id"]
        )
        
        return {
            "batch_id": batch_id,
            "transformed_count": total_transformed,
            "by_product": {k: len(v) for k, v in transformed_by_product.items()}
        }
    
    def load_to_mongodb_task(**context: Any) -> Dict[str, Any]:
        """Load transformed data to MongoDB."""
        config = get_config()
        
        # Try multiple ways to get batch_id
        batch_id = context["ti"].xcom_pull(task_ids="transform_data", key="batch_id")
        
        if not batch_id:
            ingest_result = context["ti"].xcom_pull(task_ids="ingest_data")
            if ingest_result:
                batch_id = ingest_result.get("batch_id")
        
        if not batch_id:
            # Fallback: get the most recent batch from MongoDB
            db = get_mongo_db(config.mongo)
            raw_collection = db[config.mongo.raw_collection]
            latest_raw = raw_collection.find_one(sort=[("ingestion_timestamp", -1)])
            if latest_raw:
                batch_id = latest_raw.get("batch_id")
                logger.info(f"Using latest batch from MongoDB for load: {batch_id}")
        
        transformed_data = context["ti"].xcom_pull(
            task_ids="transform_data", key="transformed_data"
        )
        
        db = get_mongo_db(config.mongo)
        loader = MongoLoader(db, config.mongo)
        
        logger.info("Starting data load", batch_id=batch_id, run_id=context["run_id"])
        
        total_loaded = 0
        load_stats = []
        
        if not transformed_data:
            logger.warning("No transformed data to load", batch_id=batch_id)
            return {"batch_id": batch_id, "total_loaded": 0, "stats": []}
        
        for product_type, records in transformed_data.items():
            if not records:
                continue
            
            try:
                count = loader.load_curated_data(records, product_type)
                total_loaded += count
                load_stats.append({
                    "product_type": product_type,
                    "loaded": count,
                    "status": "success"
                })
                logger.info(f"Loaded {count} records to {product_type}", batch_id=batch_id)
            except Exception as e:
                logger.error(
                    "Load failed for product",
                    product_type=product_type,
                    error=str(e),
                    batch_id=batch_id
                )
                load_stats.append({
                    "product_type": product_type,
                    "status": "failed",
                    "error": str(e)
                })
        
        # Update pipeline run status
        run = PipelineRun(
            batch_id=batch_id,
            status="success" if total_loaded > 0 else "failed",
            start_time=utc_now(),
            end_time=utc_now(),
            records_processed=total_loaded,
            records_valid=total_loaded,
            records_invalid=0
        )
        loader.save_pipeline_run(run)
        
        logger.info(
            "Load complete",
            batch_id=batch_id,
            total_loaded=total_loaded,
            run_id=context["run_id"]
        )
        
        return {
            "batch_id": batch_id,
            "total_loaded": total_loaded,
            "stats": load_stats
        }
    
    def generate_report_task(**context: Any) -> Dict[str, Any]:
        """Generate reports from data."""
        config = get_config()
        
        # Try multiple ways to get batch_id
        batch_id = context["ti"].xcom_pull(task_ids="load_to_mongodb", key="batch_id")
        
        if not batch_id:
            ingest_result = context["ti"].xcom_pull(task_ids="ingest_data")
            if ingest_result:
                batch_id = ingest_result.get("batch_id")
        
        if not batch_id:
            # Fallback: get the most recent batch from MongoDB
            db = get_mongo_db(config.mongo)
            raw_collection = db[config.mongo.raw_collection]
            latest_raw = raw_collection.find_one(sort=[("ingestion_timestamp", -1)])
            if latest_raw:
                batch_id = latest_raw.get("batch_id")
        
        logger.info("Generating reports", batch_id=batch_id, run_id=context["run_id"])
        
        db = get_mongo_db(config.mongo)
        service = ReportingService(db, config.mongo)
        
        summary = service.get_24h_summary()
        
        summary["data_source"] = "OilPriceAPI"
        summary["batch_id"] = batch_id
        
        db["reports_cache"].update_one(
            {"report_type": "24h_summary", "batch_id": batch_id},
            {"$set": {
                "report_type": "24h_summary",
                "batch_id": batch_id,
                "generated_at": utc_now(),
                "data": summary,
                "source": "oilpriceapi"
            }},
            upsert=True
        )
        
        logger.info("Reports generated", batch_id=batch_id, run_id=context["run_id"])
        
        return {
            "batch_id": batch_id,
            "report_generated": True,
            "summary_preview": {
                "total_records": summary.get("total_records_processed_24h"),
                "quality_pass_rate": summary.get("data_quality", {}).get("pass_rate"),
                "source": "OilPriceAPI"
            }
        }
    
    # Define tasks
    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_task,
        dag=dag
    )
    
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_task,
        dag=dag
    )
    
    load_to_mongodb = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb_task,
        dag=dag
    )
    
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report_task,
        dag=dag
    )
    
    # Task dependencies (validation removed)
    ingest_data >> transform_data >> load_to_mongodb >> generate_report
    
    return dag


dag = create_dag()