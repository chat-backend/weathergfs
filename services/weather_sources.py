# services/weather_sources.py
import httpx
from typing import Dict, Any

OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
WINDY_POINT_FORECAST = "https://api.windy.com/api/point-forecast/v2"
WINDY_API_KEY = "YOUR_WINDY_KEY"

async def fetch_openmeteo(lat: float, lon: float) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            OPEN_METEO_FORECAST,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,pressure_msl",
                "hourly": "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,pressure_msl",
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,uv_index_max"
            }
        )
        resp.raise_for_status()
        return resp.json()

async def fetch_windy(lat: float, lon: float) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            WINDY_POINT_FORECAST,
            json={
                "lat": lat,
                "lon": lon,
                "model": "gfs",
                "parameters": ["temp", "wind", "pressure", "rh", "precip"],
                "levels": ["surface"],
                "key": WINDY_API_KEY
            }
        )
        resp.raise_for_status()
        return resp.json()

async def get_weather(lat: float, lon: float) -> Dict[str, Any]:
    """Trả về dữ liệu hợp nhất từ Open-Meteo và Windy: current + daily"""
    om = await fetch_openmeteo(lat, lon)
    wy = await fetch_windy(lat, lon)

    # current từ Open-Meteo
    current = {
        "temperature": om.get("current", {}).get("temperature_2m"),
        "precipitation": om.get("current", {}).get("precipitation"),
        "wind_speed": om.get("current", {}).get("wind_speed_10m"),
        "humidity": om.get("current", {}).get("relative_humidity_2m"),
        "pressure": om.get("current", {}).get("pressure_msl"),
    }

    # daily từ Open-Meteo
    daily = {
        "tmin": om.get("daily", {}).get("temperature_2m_min", [None])[0],
        "tmax": om.get("daily", {}).get("temperature_2m_max", [None])[0],
        "rain_total": om.get("daily", {}).get("precipitation_sum", [None])[0],
        "uv_max": om.get("daily", {}).get("uv_index_max", [None])[0],
    }

    # windy bổ sung (ví dụ nhiệt độ, gió, áp suất)
    windy_data = wy.get("data", {})

    return {
        "openmeteo": {"current": current, "daily": daily},
        "windy": windy_data
    }