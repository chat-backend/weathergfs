import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from services.chat import router

# Tạo app FastAPI và include router
app = FastAPI()
app.include_router(router)

client = TestClient(app)

def test_chat_weather_with_xa_lanh_ngoc():
    """
    Kiểm tra khi nhập địa danh 'xa lanh ngoc',
    API phải trả về bản tin thời tiết hoặc thông báo lỗi rõ ràng.
    """
    response = client.get("/v1/chat?region=xa lanh ngoc")
    data = response.json()

    print("\n=== Bản tin thời tiết cho xa lanh ngoc ===")
    print(f"Trạng thái: {data['status']}")
    print(f"Thông điệp: {data['message']}")

    if data["status"] == "ok":
        region = data["data"]["region"]
        current = data["data"]["current"]
        bulletin = data["data"]["bulletin"]

        print(f"Địa danh: {region['name']} (lat={region['lat']}, lon={region['lon']})")
        print("Thời tiết hiện tại:")
        for k, v in current.items():
            print(f"  - {k}: {v}")
        print("Bản tin:")
        for k, v in bulletin.items():
            print(f"  - {k}: {v}")
    else:
        print("❌ Không tìm thấy dữ liệu thời tiết")

    # Kiểm tra cơ bản
    assert response.status_code == 200
    assert data["status"] in ["ok", "error"]