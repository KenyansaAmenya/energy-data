import pytest
from unittest.mock import MagicMock, patch

from flask_app.app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    
    with app.test_client() as client:
        yield client


class TestHealthEndpoint:
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert "status" in data


class TestPricesEndpoint:
    @patch("flask_app.app.ReportingService")
    def test_get_prices(self, mock_service, client):
        mock_instance = MagicMock()
        mock_instance.get_latest_prices.return_value = [
            {
                "country": "USA",
                "product_type": "fuel",
                "price": 2.50,
                "currency": "USD",
                "reporting_date": "2024-01-01"
            }
        ]
        
        with patch.object(client.application, "reporting_service", mock_instance):
            response = client.get("/api/prices?product_type=fuel&limit=10")
            assert response.status_code == 200
            data = response.get_json()
            assert data["status"] == "success"
            assert len(data["data"]) == 1
    
    def test_invalid_product_type(self, client):
        response = client.get("/api/prices?product_type=invalid")
        assert response.status_code == 400


class TestReportEndpoint:
    @patch("flask_app.app.ReportingService")
    def test_get_report(self, mock_service, client):
        mock_instance = MagicMock()
        mock_instance.get_24h_summary.return_value = {
            "generated_at": "2024-01-01T00:00:00",
            "period": "24h",
            "total_records_processed_24h": 100
        }
        
        with patch.object(client.application, "reporting_service", mock_instance):
            response = client.get("/api/report")
            assert response.status_code == 200
            data = response.get_json()
            assert data["status"] == "success"