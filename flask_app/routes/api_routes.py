import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask import Blueprint, jsonify, request
from app.utils.mongo_client import get_mongo_db
from config.settings import get_config

api_bp = Blueprint('api', __name__)

def get_db():
    config = get_config()
    return get_mongo_db(config.mongo)

@api_bp.route('/stats')
def get_stats():
    try:
        db = get_db()
        stats = {
            'fuel_prices': db.fuel_prices.count_documents({}),
            'natural_gas_prices': db.natural_gas_prices.count_documents({}),
            'raw_data': db.raw_api_data.count_documents({}),
            'pipeline_runs': db.pipeline_runs.count_documents({}),
            'quality_checks': db.data_quality_checks.count_documents({})
        }
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/prices')
def get_all_prices():
    try:
        db = get_db()
        limit = int(request.args.get('limit', 50))
        
        fuel_prices = list(db.fuel_prices.find({}, {'_id': 0}).sort('reporting_date', -1).limit(limit))
        gas_prices = list(db.natural_gas_prices.find({}, {'_id': 0}).sort('reporting_date', -1).limit(limit))
        
        all_prices = fuel_prices + gas_prices
        all_prices.sort(key=lambda x: x.get('reporting_date', ''), reverse=True)
        
        return jsonify({'status': 'success', 'data': all_prices[:limit]})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

@api_bp.route('/prices/fuel')
def get_fuel_prices():
    try:
        db = get_db()
        limit = int(request.args.get('limit', 50))
        prices = list(db.fuel_prices.find({}, {'_id': 0}).sort('reporting_date', -1).limit(limit))
        return jsonify(prices)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/prices/natural-gas')
def get_gas_prices():
    try:
        db = get_db()
        limit = int(request.args.get('limit', 50))
        prices = list(db.natural_gas_prices.find({}, {'_id': 0}).sort('reporting_date', -1).limit(limit))
        return jsonify(prices)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/report')
def get_report():
    try:
        db = get_db()
        report = db.reports_cache.find_one({}, {'_id': 0})
        if report:
            return jsonify({'status': 'success', 'data': report})
        return jsonify({
            'status': 'success',
            'data': {
                'total_records_processed_24h': 0,
                'data_quality': {'pass_rate': 100, 'passed_checks': 0, 'failed_checks': 0},
                'generated_at': '2026-05-03T00:00:00Z'
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

@api_bp.route('/changes')
def get_changes():
    try:
        db = get_db()
        fuel_prices = list(db.fuel_prices.find({}, {'_id': 0}).sort('reporting_date', -1).limit(100))
        
        increases = []
        decreases = []
        
        for price in fuel_prices:
            change = {
                'country': price.get('country', 'Global'),
                'product_type': price.get('product_type', 'fuel'),
                'commodity': price.get('commodity_code', 'unknown'),
                'price': price.get('price', 0),
                'change_24h': price.get('change_24h', 0),
                'change_percent_24h': price.get('change_percent_24h', 0)
            }
            if change.get('change_24h', 0) > 0:
                increases.append(change)
            elif change.get('change_24h', 0) < 0:
                decreases.append(change)
        
        return jsonify({
            'status': 'success',
            'data': {
                'top_increases': increases[:5],
                'top_decreases': decreases[:5]
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500