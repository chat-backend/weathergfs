import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_chat_with_valid_coordinates(monkeypatch):
    # Mock generate_bulletin để giả lập dữ liệu thành công
    def mock_generate_bulletin(region_name, lat, lon):
        return {
            "status": "ok",
            "bulletin": {"text": "Trời nắng đẹp"},
            "current": {"temp": 30},
            "hourly": [],
            "daily": [],
            "alerts": [],
            "source": "open_meteo"
        }
    monkeypatch.setattr("services.chat.generate_bulletin", mock_generate_bulletin)

    response = client.get("/v1/chat?lat=16.0&lon=108.0&region=da%20nang")
    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert "bulletin" in data["data"]

def test_chat_with_invalid_region():
    response = client.get("/v1/chat?region=unknownplace")
    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "error"
    assert "Không tìm thấy địa danh" in data["message"]

def test_chat_generate_bulletin_error(monkeypatch):
    def mock_generate_bulletin(region_name, lat, lon):
        return {"status": "error", "message": "Nguồn dữ liệu lỗi"}
    monkeypatch.setattr("services.chat.generate_bulletin", mock_generate_bulletin)

    response = client.get("/v1/chat?lat=16.0&lon=108.0&region=da%20nang")
    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "error"
    assert "Nguồn dữ liệu lỗi" in data["message"]

def test_chat_system_exception(monkeypatch):
    def mock_generate_bulletin(region_name, lat, lon):
        raise Exception("System crash")
    monkeypatch.setattr("services.chat.generate_bulletin", mock_generate_bulletin)

    response = client.get("/v1/chat?lat=16.0&lon=108.0&region=da%20nang")
    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "error"
    assert "Lỗi hệ thống" in data["message"]