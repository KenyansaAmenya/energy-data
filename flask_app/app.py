import os
import sys

from config.settings import get_config
config = get_config()
print(f"DEBUG - Mongo URI: {config.mongo.uri}")
print(f"DEBUG - DB Name: {config.mongo.db_name}")

from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, render_template
from flask_cors import CORS

from app.services.reporting_service import ReportingService
from app.utils.logger import get_logger
from app.utils.mongo_client import MongoDBClient
from config.settings import get_config

logger = get_logger(__name__)


def create_app() -> Flask:

    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static"
    )
    
    config = get_config()
    app.config["SECRET_KEY"] = config.flask.secret_key
    app.config["JSON_SORT_KEYS"] = False
    
    CORS(app)
    
    mongo_client = MongoDBClient(config.mongo)
    
    from flask_app.routes.api_routes import api_bp
    from flask_app.routes.dashboard_routes import dashboard_bp
    
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(dashboard_bp, url_prefix="/")
    
    app.reporting_service = ReportingService(
        mongo_client.get_database(),
        config.mongo
    )
    
    @app.route("/health")
    def health_check() -> Dict[str, Any]:
        db_healthy = mongo_client.health_check()
        return jsonify({
            "status": "healthy" if db_healthy else "degraded",
            "database": "connected" if db_healthy else "disconnected",
            "service": "energy-platform-api"
        })
    
    @app.errorhandler(404)
    def not_found(error: Any) -> tuple:
        return jsonify({
            "error": "Not found",
            "message": "The requested resource does not exist"
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error: Any) -> tuple:
        logger.error("Internal server error", error=str(error))
        return jsonify({
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }), 500
    
    logger.info("Flask application initialized")
    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", 5000)), debug=True)