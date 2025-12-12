# services/weather_services.py
import json
import requests
import time
from pathlib import Path
from requests.exceptions import Timeout, RequestException
from services.error_handler import handle_service_error


class RegionIndex:
    """
    Lớp quản lý danh sách địa danh từ file GeoJSON.
    Dùng để resolve tên địa danh thành tọa độ (lat/lon).
    """
    def __init__(self, path: str):
        self.path = Path(path)
        self.features = []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.features = data.get("features", [])
        except Exception as e:
            handle_service_error("region_index", "init", e, alert_type="critical")

    def resolve_region(self, name: str):
        """Tìm địa danh theo tên, trả về dict chứa lat/lon nếu có."""
        if not name:
            return None
        for f in self.features:
            props = f.get("properties", {})
            if props.get("name") == name:
                coords = f.get("geometry", {}).get("coordinates", [])
                if coords and len(coords) >= 2:
                    return {
                        "name": name,
                        "lat": coords[1],
                        "lon": coords[0],
                        "source": "geojson"
                    }
        return None


class WeatherService:
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    @staticmethod
    def fetch_forecast(lat: float, lon: float,
                       max_retries: int = 3,
                       timeout: int = 10,
                       backoff_factor: int = 2) -> dict:
        """
        Gọi API Open-Meteo với retry khi timeout + backoff.
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": (
                "temperature_2m,apparent_temperature,dewpoint_2m,precipitation,rain,"
                "precipitation_probability,relative_humidity_2m,windspeed_10m,windgusts_10m,"
                "winddirection_10m,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,"
                "pressure_msl,shortwave_radiation,uv_index"
            ),
            "daily": (
                "temperature_2m_min,temperature_2m_max,precipitation_sum,precipitation_hours,"
                "windspeed_10m_max,windgusts_10m_max,sunrise,sunset,uv_index_max"
            ),
            "current_weather": "true",
            "timezone": "Asia/Ho_Chi_Minh",
            "forecast_days": "10"
        }

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(WeatherService.BASE_URL, params=params, timeout=timeout)
                resp.raise_for_status()
                try:
                    data = resp.json()
                except ValueError as e:
                    handle_service_error("weather_service", "fetch_forecast", e, alert_type="critical")
                    return {
                        "status": "error",
                        "level": "critical",
                        "message": "API trả về dữ liệu không hợp lệ (không phải JSON)",
                        "hint": "Kiểm tra dịch vụ Open-Meteo"
                    }
                return {
                    "status": "ok",
                    "level": "info",
                    "data": data,
                    "message": f"API thành công sau {attempt} lần thử"
                }
            except Timeout:
                handle_service_error("weather_service", "fetch_forecast",
                                     Exception(f"Timeout lần {attempt}/{max_retries} với lat={lat}, lon={lon}"),
                                     alert_type="warning")
                if attempt < max_retries:
                    sleep_time = backoff_factor ** (attempt - 1)
                    time.sleep(sleep_time)
                else:
                    return {
                        "status": "error",
                        "level": "warning",
                        "message": "API timeout sau nhiều lần thử",
                        "hint": "Kiểm tra kết nối mạng hoặc API Open-Meteo"
                    }
            except RequestException as e:
                handle_service_error("weather_service", "fetch_forecast", e, alert_type="critical")
                return {
                    "status": "error",
                    "level": "critical",
                    "message": f"Lỗi khi gọi API: {e}",
                    "hint": "Kiểm tra URL hoặc dịch vụ API"
                }