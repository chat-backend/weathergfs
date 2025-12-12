# services/app_utils.py
import re
import logging
import requests
import unicodedata
from datetime import datetime
from difflib import get_close_matches
from typing import Optional, Tuple, Dict, Any

from configs.locations import LOCATIONS
from services.error_handler import handle_service_error

logger = logging.getLogger("WeatherService")

# ------------------- NORMALIZE -------------------
def normalize_query(text: str) -> str:
    """Chuẩn hóa chuỗi để so khớp địa danh (bỏ dấu, lowercase, loại ký tự đặc biệt)."""
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join([c for c in text if unicodedata.category(c) != "Mn"])
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split()).strip()

# ------------------- REGION RESOLUTION -------------------
def resolve_region(region: str = None, lat: float = None, lon: float = None) -> Optional[Dict[str, Any]]:
    """Tìm thông tin vùng từ tên địa danh hoặc lat/lon. Chỉ dùng LOCATIONS."""

    # Ưu tiên tọa độ direct source
    if lat is not None and lon is not None:
        try:
            lat = float(lat); lon = float(lon)
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.debug(f"[resolve_region] Direct lat/lon accepted: lat={lat}, lon={lon}")
                return {
                    "name": region or "Unknown region",
                    "lat": lat,
                    "lon": lon,
                    "source": "direct"
                }
            else:
                logger.warning(f"[resolve_region] Lat/lon out of range: lat={lat}, lon={lon}")
        except Exception as e:
            logger.error(f"[resolve_region] Invalid lat/lon: {e}")
        return None

    # Nếu có region, tra cứu trong LOCATIONS
    if region:
        query = normalize_query(region)
        logger.debug(f"[resolve_region] Query normalized: '{region}' -> '{query}'")

        # 1) Exact match theo key
        for key_name, info in LOCATIONS.items():
            if normalize_query(key_name) == query:
                logger.info(f"[resolve_region] Exact match key: '{key_name}'")
                return {
                    "name": key_name,
                    "lat": info.get("lat"),
                    "lon": info.get("lon"),
                    "source": info.get("type", "unknown")
                }

        # 2) Exact match theo alias
        for key_name, info in LOCATIONS.items():
            for alias in info.get("aliases", []):
                if normalize_query(alias) == query:
                    logger.info(f"[resolve_region] Exact match alias: '{alias}' -> '{key_name}'")
                    return {
                        "name": key_name,
                        "lat": info.get("lat"),
                        "lon": info.get("lon"),
                        "source": info.get("type", "unknown")
                    }

        # 3) Fuzzy search trên cả key và alias
        normalized_keys = {normalize_query(k): k for k in LOCATIONS.keys()}
        normalized_alias_map = {}
        for k, info in LOCATIONS.items():
            for a in info.get("aliases", []):
                normalized_alias_map[normalize_query(a)] = k

        fuzzy_pool = list(normalized_keys.keys()) + list(normalized_alias_map.keys())
        matches = get_close_matches(query, fuzzy_pool, n=1, cutoff=0.85)
        if matches:
            best = matches[0]
            key_name = normalized_keys.get(best) or normalized_alias_map.get(best)
            if key_name:
                info = LOCATIONS.get(key_name, {})
                logger.info(f"[resolve_region] Fuzzy match: '{region}' -> '{key_name}'")
                return {
                    "name": key_name,
                    "lat": info.get("lat"),
                    "lon": info.get("lon"),
                    "source": info.get("type", "unknown")
                }
        else:
            logger.warning(f"[resolve_region] No fuzzy match for query='{query}'")

        # 4) Không tìm thấy
        logger.warning(f"[resolve_region] Không tìm thấy địa danh: {region}")
        return {
            "name": region,
            "lat": None,
            "lon": None,
            "source": "not_found"
        }

    # Không có region và không có lat/lon hợp lệ
    logger.warning("[resolve_region] No region and no lat/lon provided")
    return None

# ------------------- WEATHER FETCH -------------------
def fetch_weather_data(lat: float, lon: float, days: int = 10) -> Optional[Dict[str, Any]]:
    """Lấy dữ liệu thời tiết trực tiếp từ Open-Meteo API."""
    try:
        hourly_vars = ",".join([
            "temperature_2m","apparent_temperature","dewpoint_2m",
            "precipitation","precipitation_probability",
            "relative_humidity_2m",
            "windspeed_10m","winddirection_10m","windgusts_10m",
            "cloudcover","cloudcover_low","cloudcover_mid","cloudcover_high",
            "shortwave_radiation","pressure_msl"
        ])
        daily_vars = ",".join([
            "temperature_2m_max","temperature_2m_min","precipitation_sum",
            "windspeed_10m_max","windgusts_10m_max",
            "precipitation_hours","sunrise","sunset"
        ])
        url = (
            "https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&current_weather=true"
            f"&hourly={hourly_vars}"
            f"&daily={daily_vars}"
            f"&timezone=Asia%2FHo_Chi_Minh"
            f"&forecast_days={days}"
        )
        logger.debug(f"[fetch_weather_data] Requesting: {url}")
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            logger.debug("[fetch_weather_data] Response OK")
            return resp.json()
        logger.error(f"[fetch_weather_data] HTTP {resp.status_code} for URL: {url}")
        return None
    except Exception as e:
        logger.error(f"[fetch_weather_data] Exception: {e}")
        return None


# ------------------- WEATHER RESPONSE -------------------
def build_weather_response(region_info: Dict[str, Any], weather_data: Dict[str, Any]) -> Dict[str, Any]:
    """Xây dựng JSON trả về cho endpoint weather/chat, luôn an toàn và thống nhất."""
    try:
        result = {
            "region": region_info.get("name", "Unknown region"),
            "lat": region_info.get("lat"),
            "lon": region_info.get("lon"),
            "source": region_info.get("source", "unknown"),
            "timestamp": datetime.now().isoformat(),
            "weather": weather_data if weather_data else {},
            "error": None
        }

        if result["lat"] is None or result["lon"] is None:
            logger.warning(
                f"[build_weather_response] Missing lat/lon for region='{result['region']}' source='{result['source']}'"
            )

        if not result["weather"]:
            logger.debug(f"[build_weather_response] Empty weather data for region='{result['region']}'")

        logger.info(f"[build_weather_response] Response built for region='{result['region']}'")
        return result

    except Exception as e:
        err = str(e)
        logger.error(f"[build_weather_response] Exception: {err}")
        return {
            "region": region_info.get("name", "Unknown region"),
            "lat": region_info.get("lat"),
            "lon": region_info.get("lon"),
            "source": region_info.get("source", "unknown"),
            "timestamp": datetime.now().isoformat(),
            "weather": {},
            "error": err
        }