# Energy Data Platform

A production-oriented data pipeline for collecting, validating, transforming, storing, and serving energy price data, centered around **OilPriceAPI** ingestion.

The repository combines:
- **Batch ETL orchestration** with Apache Airflow.
- **Data quality and transformation** pipelines in Python.
- **MongoDB-backed storage** for raw and curated datasets.
- **A Flask web/API layer** for dashboard and query endpoints.
- **Containerized local development** with Docker Compose.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Layout](#repository-layout)
- [Data Flow](#data-flow)
- [Core Components](#core-components)
  - [Ingestion](#ingestion)
  - [Validation](#validation)
  - [Transformation](#transformation)
  - [Loaders](#loaders)
  - [Service Layer](#service-layer)
  - [Flask API & Dashboard](#flask-api--dashboard)
  - [Airflow DAG](#airflow-dag)
- [Configuration](#configuration)
- [Environment Variables](#environment-variables)
- [Local Development](#local-development)
- [Run with Docker Compose](#run-with-docker-compose)
- [Running the Flask App](#running-the-flask-app)
- [Running Airflow](#running-airflow)
- [Testing](#testing)
- [Data Model Notes](#data-model-notes)
- [Operational Notes](#operational-notes)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)

---

## Architecture Overview

At a high level, this platform performs a recurring ETL loop:

1. **Ingest** raw energy price data from OilPriceAPI.
2. **Persist raw payloads** to MongoDB for traceability.
3. **Parse + transform** records into normalized structures.
4. **Validate quality constraints** (nulls, types, ranges, duplicates).
5. **Load curated collections** by product type.
6. **Expose analytics endpoints** through Flask API and a dashboard view.

This dual design (batch pipeline + online API) supports both data engineering workflows and application-facing access patterns.

---

## Repository Layout

```text
energy-data/
├── airflow/dags/                   # Airflow DAG definitions
├── api/                            # Deployment entrypoint wrapper
├── app/
│   ├── ingestion/                  # Fetchers and source adapters
│   ├── validation/                 # Validation pipeline + validators
│   ├── transformation/             # Transformation pipeline
│   ├── loaders/                    # MongoDB write adapters
│   ├── services/                   # Business/reporting services
│   └── utils/                      # Models, logging, date, mongo helpers
├── config/                         # App settings from env
├── flask_app/
│   ├── routes/                     # Flask blueprints
│   ├── templates/                  # HTML dashboard
│   └── app.py                      # Flask application factory
├── tests/                          # Unit/integration tests
├── docker-compose.yml              # Full local stack
├── Dockerfile.airflow              # Airflow image
├── Dockerfile.flask                # Flask image
├── requirements.txt                # Python dependencies
└── README.md
```

---

## Data Flow

### 1) Ingestion
- The Airflow DAG triggers `ingest_data`.
- `OilPriceFetcher` pulls product-specific records (e.g., fuel, natural gas).
- Raw responses are wrapped with metadata (`batch_id`, timestamps, source URL, format type).

### 2) Raw Storage
- `MongoLoader` stores raw records in `raw_api_data`.
- Pipeline run metadata is captured in `pipeline_runs`.

### 3) Transform + Parse
- Raw JSON payloads are parsed into record-level structures.
- Transformation pipeline applies country normalization, price normalization, date parsing, and product-type enrichment.

### 4) Quality Validation
- Validation pipeline checks:
  - required fields
  - type correctness
  - non-negative / non-zero values
  - duplicate signature checks

### 5) Curated Storage
- Curated records are written to product-specific collections (e.g. `fuel_prices`, `natural_gas_prices`).

### 6) Serving Layer
- Flask endpoints query MongoDB and expose:
  - stats
  - combined latest prices
  - product-specific prices
  - report data
  - top increases/decreases

---

## Core Components

### Ingestion
Located in `app/ingestion/`.

Important modules:
- `base_fetcher.py`: abstract fetch/parse contract.
- `oilprice_fetcher.py`: OilPriceAPI implementation.
- `xml_fetcher.py` and `excel_fetcher.py`: extensibility adapters for other feed formats.
- `fetcher_factory.py`: factory pattern for selecting fetcher implementations.

### Validation
Located in `app/validation/validators.py`.

Includes composable validators such as:
- `NullCheckValidator`
- `TypeValidator`
- `NegativeValueValidator`
- `DuplicateValidator`
- `ValidationPipeline`

Each validator emits structured results, enabling data quality tracking and robust skip/fail behavior.

### Transformation
Located in `app/transformation/transformers.py`.

Includes:
- `CountryNormalizer`
- `PriceNormalizer`
- `DateNormalizer`
- `ProductTypeEnricher`
- `TransformationPipeline`

The pipeline design makes field-level standardization deterministic and testable.

### Loaders
Located in `app/loaders/mongo_loader.py`.

Responsibilities:
- bulk insert raw records
- upsert curated records
- write pipeline execution metadata
- support data quality checks persistence

### Service Layer
Located in `app/services/reporting_service.py`.

Encapsulates business/reporting logic so API routes remain thin and orchestration-agnostic.

### Flask API & Dashboard
- App factory: `flask_app/app.py`
- API routes: `flask_app/routes/api_routes.py`
- Dashboard routes: `flask_app/routes/dashboard_routes.py`

Primary endpoints:
- `GET /health`
- `GET /api/stats`
- `GET /api/prices?limit=50`
- `GET /api/prices/fuel?limit=50`
- `GET /api/prices/natural-gas?limit=50`
- `GET /api/report`
- `GET /api/changes`
- `GET /` (dashboard HTML)

### Airflow DAG
Defined in `airflow/dags/energy_pipeline_dag.py`.

Key characteristics:
- DAG name: `energy_price_pipeline`
- schedule: every 6 hours (`0 */6 * * *`)
- retries + retry delay configured in `default_args`
- task stages:
  1. ingest
  2. transform
  3. validate
  4. load
  5. reporting/cleanup hooks

(*Task naming may evolve; inspect DAG file for exact operator graph in your current branch.*)

---

## Configuration

Settings are centralized in `config/settings.py` using frozen dataclasses:

- `MongoConfig`
- `APIConfig`
- `PipelineConfig`
- `FlaskConfig`
- `AppConfig`

`get_config()` caches a single config instance and reads from environment variables at runtime.

---

## Environment Variables

Create a `.env` file in repo root. Minimum required variables:

```bash
# Mongo
MONGO_URI=mongodb://admin:admin123@localhost:27017/energy_platform?authSource=admin
MONGO_DB_NAME=energy_platform

# OilPriceAPI
OILPRICE_API_KEY=replace-with-real-key
OILPRICE_BASE_URL=https://api.oilpriceapi.com/v1
OILPRICE_REQUEST_TIMEOUT=30
OILPRICE_MAX_RETRIES=3
OILPRICE_RETRY_DELAY=5

# Flask
FLASK_ENV=development
FLASK_PORT=5000
SECRET_KEY=replace-with-strong-secret

# Pipeline
BATCH_SIZE=1000
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5

# Optional logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

> Note: `config/settings.py` enforces required variables and raises `ValueError` if missing.

---

## Local Development

### 1) Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure environment

```bash
cp .env.example .env  # if available
# or create .env manually
```

### 4) Run tests

```bash
pytest -q
```

---

## Run with Docker Compose

Bring up the complete stack (MongoDB, Postgres, Redis, Airflow init/webserver/scheduler, Flask):

```bash
docker compose up --build
```

Useful service ports:
- Flask API: `http://localhost:5000`
- Airflow UI: `http://localhost:8080`
- MongoDB: `localhost:27017`

To stop:

```bash
docker compose down
```

To remove named volumes as well:

```bash
docker compose down -v
```

---

## Running the Flask App

For direct local execution:

```bash
python flask_app/app.py
```

Or with Flask CLI (if configured):

```bash
flask --app flask_app.app run --host 0.0.0.0 --port 5000
```

Health check example:

```bash
curl http://localhost:5000/health
```

---

## Running Airflow

If running through Docker Compose, Airflow is already provisioned via `airflow-init` and associated services.

Default credentials configured in compose command:
- username: `admin`
- password: `admin`

Open:
- `http://localhost:8080`

Trigger DAG:
- DAG ID: `energy_price_pipeline`

---

## Testing

Test suite is under `tests/` and includes:
- API endpoint behavior
- transformation normalization logic
- validation rules
- environment configuration checks
- ingestion/loader integration-style mocks

Run all tests:

```bash
pytest
```

Run a single file:

```bash
pytest tests/test_transformation.py -q
```

---

## Data Model Notes

Core models live in `app/utils/models.py` and typically include:
- raw API data envelope
- pipeline run metadata
- product type enum
- curated price records
- data quality check structures

Mongo collections (default names from config):
- `raw_api_data`
- `fuel_prices`
- `electricity_prices`
- `natural_gas_prices`
- `pipeline_runs`
- `data_quality_checks`

---

## Operational Notes

- `get_config()` is strict about required env vars; startup will fail fast when config is incomplete.
- Flask app wires Mongo and reporting service during app creation.
- Airflow tasks use XCom and fallback Mongo lookups for batch continuity.
- Logging utilities are centralized in `app/utils/logger.py`.

---

## Troubleshooting

### Missing environment variable errors
If startup raises `ValueError` from `config/settings.py`, verify `.env` keys (especially `MONGO_URI`, `OILPRICE_API_KEY`, `SECRET_KEY`).

### Mongo connection failures
- Confirm Mongo container is healthy: `docker ps`.
- Verify URI authSource and credentials.
- Ensure network/port mapping aligns with where app is running (container vs host).

### Airflow tasks fail with missing batch data
- Confirm ingest task wrote raw records to `raw_api_data`.
- Inspect Airflow task logs.
- Validate XCom keys and fallback retrieval path.

### API returns empty report/prices
- Data may not be ingested yet; trigger DAG manually.
- `/api/report` may return a default placeholder when cache is unavailable.

---

## Future Improvements

- Add `.env.example` with all required keys and comments.
- Add schema migration/versioning strategy for Mongo collections.
- Add API authentication/authorization for production deployment.
- Add observability stack (metrics + tracing + alerting).
- Add CI pipeline (lint, type checks, tests) with quality gates.
- Reconcile legacy tests and naming drift to reflect current ingestion classes.