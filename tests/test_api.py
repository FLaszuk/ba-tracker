import pytest
from fastapi.testclient import TestClient
from app.api import app

client = TestClient(app)

def test_get_months():
    response = client.get("/api/months")
    assert response.status_code == 200
    data = response.json()
    assert "months" in data
    assert isinstance(data["months"], list)
    # The demo script generates 2025-01 to 2025-12, so there should be at least 1 month
    assert len(data["months"]) > 0

def test_get_kpi():
    # Pobierz pierwszy z dostępnych miesięcy, żeby mieć pewność, że istnieje
    months_resp = client.get("/api/months").json()
    test_month = months_resp["months"][0]
    
    response = client.get(f"/api/kpi/{test_month}")
    assert response.status_code == 200
    data = response.json()
    assert "total_flight_hours" in data
    assert "total_landings" in data
    assert "unique_aircraft" in data
    assert "unique_aircraft_manufacturers" in data
    assert "unique_engine_manufacturers" in data

def test_aircraft_market_share():
    months_resp = client.get("/api/months").json()
    test_month = months_resp["months"][0]
    
    response = client.get(f"/api/aircraft-market-share/{test_month}")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert isinstance(data["data"], list)
    if len(data["data"]) > 0:
        first_item = data["data"][0]
        assert "aircraft_manufacturer" in first_item
        assert "total_hours" in first_item
        assert "market_share_pct" in first_item

def test_top_models():
    months_resp = client.get("/api/months").json()
    test_month = months_resp["months"][0]
    
    response = client.get(f"/api/top-models/{test_month}?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) <= 5
    if len(data["data"]) > 0:
        first_item = data["data"][0]
        assert "aircraft_model" in first_item
        assert "total_landings" in first_item

def test_invalid_month():
    response = client.get("/api/kpi/2099-01")
    # Zakładając, że na ten miesiąc nie ma danych, powinno nam zwrócić błąd 404 lub podobny zapięty w logikę backendu.
    # Upewnijmy się, jak reaguje API
    assert response.status_code == 404
