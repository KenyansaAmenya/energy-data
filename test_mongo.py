from config.settings import get_config
from pymongo import MongoClient
import os

# Force the URI
uri = 'mongodb://admin:admin123@localhost:27017/energy_platform?authSource=admin'
print(f'Testing connection to: {uri[:50]}...')

try:
    client = MongoClient(uri)
    db = client['energy_platform']
    print(f'fuel_prices count: {db.fuel_prices.count_documents({})}')
    print(f'natural_gas_prices count: {db.natural_gas_prices.count_documents({})}')
    print('✅ Connection successful!')
except Exception as e:
    print(f'❌ Connection failed: {e}')
