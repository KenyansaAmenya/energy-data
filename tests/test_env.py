import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

def test_mongo_config():
    """Test MongoDB configuration."""
    print("\n📊 MongoDB Configuration:")
    print("-" * 40)
    
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME", "energy_platform")
    
    if uri:
        # Mask credentials for security
        masked = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', uri)
        print(f"✅ MONGO_URI: {masked[:60]}...")
        print(f"✅ MONGO_DB_NAME: {db_name}")
        return True
    else:
        print("❌ MONGO_URI: Missing from .env")
        return False

def test_oilprice_config():
    """Test OilPriceAPI configuration."""
    print("\n🔌 OilPriceAPI Configuration:")
    print("-" * 40)
    
    api_key = os.getenv("OILPRICE_API_KEY")
    base_url = os.getenv("OILPRICE_BASE_URL", "https://api.oilpriceapi.com/v1")
    timeout = os.getenv("OILPRICE_REQUEST_TIMEOUT", "30")
    
    if not api_key:
        print("❌ OILPRICE_API_KEY: Missing from .env")
        return False
    elif api_key == "your_oilprice_api_key_here":
        print("⚠️ OILPRICE_API_KEY: Still using placeholder - replace with actual API key")
        return False
    elif api_key == "":
        print("❌ OILPRICE_API_KEY: Empty - please add your API key")
        return False
    else:
        print(f"✅ OILPRICE_API_KEY: {api_key[:5]}...{api_key[-4:]}")
        print(f"✅ OILPRICE_BASE_URL: {base_url}")
        print(f"✅ OILPRICE_REQUEST_TIMEOUT: {timeout}")
        return True

def test_eia_config():
    """Test EIA API configuration (optional fallback)."""
    print("\n🔌 EIA API Configuration (Optional):")
    print("-" * 40)
    
    api_key = os.getenv("EIA_API_KEY")
    base_url = os.getenv("EIA_BASE_URL", "https://api.eia.gov/v2")
    timeout = os.getenv("EIA_REQUEST_TIMEOUT", "30")
    max_retries = os.getenv("EIA_MAX_RETRIES", "3")
    retry_delay = os.getenv("EIA_RETRY_DELAY", "5")
    
    if not api_key:
        print("⚠️ EIA_API_KEY: Missing (optional - only needed if using EIA fallback)")
        return True  # Not required since OilPriceAPI is primary
    elif api_key == "your_eia_api_key_here":
        print("⚠️ EIA_API_KEY: Still using placeholder")
        return True
    elif api_key == "":
        print("⚠️ EIA_API_KEY: Empty")
        return True
    else:
        print(f"✅ EIA_API_KEY: {api_key[:5]}...{api_key[-4:]}")
        print(f"✅ EIA_BASE_URL: {base_url}")
        print(f"✅ EIA_REQUEST_TIMEOUT: {timeout}")
        print(f"✅ EIA_MAX_RETRIES: {max_retries}")
        print(f"✅ EIA_RETRY_DELAY: {retry_delay}")
        return True

def test_flask_config():
    """Test Flask configuration."""
    print("\n🌐 Flask Configuration:")
    print("-" * 40)
    
    env = os.getenv("FLASK_ENV", "development")
    port = os.getenv("FLASK_PORT", "5000")
    secret_key = os.getenv("SECRET_KEY")
    
    print(f"✅ FLASK_ENV: {env}")
    print(f"✅ FLASK_PORT: {port}")
    
    if not secret_key:
        print("❌ SECRET_KEY: Missing from .env")
        return False
    elif secret_key == "your-secret-key-here":
        print("⚠️ SECRET_KEY: Using default - change for production")
    else:
        print(f"✅ SECRET_KEY: {'*' * len(secret_key)}")
    return True

def test_airflow_config():
    """Test Airflow configuration."""
    print("\n✈️ Airflow Configuration:")
    print("-" * 40)
    
    executor = os.getenv("AIRFLOW__CORE__EXECUTOR")
    db_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    db_conn_v2 = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    load_examples = os.getenv("AIRFLOW__CORE__LOAD_EXAMPLES")
    
    if not executor:
        print("❌ AIRFLOW__CORE__EXECUTOR: Missing")
        return False
    else:
        print(f"✅ AIRFLOW__CORE__EXECUTOR: {executor}")
    
    if db_conn:
        # Mask password
        masked = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', db_conn)
        print(f"✅ AIRFLOW__CORE__SQL_ALCHEMY_CONN: {masked}")
    else:
        print("⚠️ AIRFLOW__CORE__SQL_ALCHEMY_CONN: Missing (optional)")
    
    if db_conn_v2:
        masked = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', db_conn_v2)
        print(f"✅ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: {masked}")
    else:
        print("⚠️ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: Missing (recommended)")
    
    print(f"✅ AIRFLOW__CORE__LOAD_EXAMPLES: {load_examples}")
    return True

def test_pipeline_config():
    """Test pipeline configuration."""
    print("\n⚙️ Pipeline Configuration:")
    print("-" * 40)
    
    batch_size = os.getenv("BATCH_SIZE", "1000")
    max_retries = os.getenv("MAX_RETRIES", "3")
    retry_delay = os.getenv("RETRY_DELAY_SECONDS", "5")
    
    print(f"✅ BATCH_SIZE: {batch_size}")
    print(f"✅ MAX_RETRIES: {max_retries}")
    print(f"✅ RETRY_DELAY_SECONDS: {retry_delay}")
    return True

def test_logging_config():
    """Test logging configuration."""
    print("\n📝 Logging Configuration:")
    print("-" * 40)
    
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = os.getenv("LOG_FORMAT", "json")
    
    print(f"✅ LOG_LEVEL: {log_level}")
    print(f"✅ LOG_FORMAT: {log_format}")
    return True

def main():
    """Run all configuration tests."""
    print("\n" + "=" * 60)
    print("ENERGY DATA PLATFORM - CONFIGURATION TEST")
    print("=" * 60)
    
    results = []
    
    results.append(("MongoDB", test_mongo_config()))
    results.append(("OilPriceAPI", test_oilprice_config()))
    results.append(("EIA API (Optional)", test_eia_config()))
    results.append(("Flask", test_flask_config()))
    results.append(("Airflow", test_airflow_config()))
    results.append(("Pipeline", test_pipeline_config()))
    results.append(("Logging", test_logging_config()))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    
    if all_passed:
        print("🎉 All configurations are valid!")
        print("✨ Your environment is ready for OilPriceAPI integration!")
        return 0
    else:
        print("⚠️ Some configurations need attention.")
        print("📝 Please fix the failing items above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())