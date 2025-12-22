# services/app_utils.py
import re
import logging
import requests
import unicodedata
from datetime import datetime
from difflib import get_close_matches
from typing import Optional, Dict, Any

from vietnam_provinces import PROVINCES
from vietnam_wards import WARDS
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

# ------------------- FALLBACK GEOCODE -------------------
def fallback_geocode(region: str) -> Optional[Dict[str, float]]:
    """Tra cứu tọa độ từ OpenStreetMap Nominatim khi thiếu lat/lon."""
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": region, "format": "json", "limit": 1}
        headers = {"User-Agent": "WeatherGfsApp/1.0"}
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                return {"lat": lat, "lon": lon, "source": "osm"}
        return None
    except Exception as e:
        logger.error(f"[fallback_geocode] Exception: {e}")
        return None

# ------------------- REGION RESOLUTION -------------------
def resolve_region(region: str = None, lat: float = None, lon: float = None) -> Dict[str, Any]:
    """Tìm thông tin vùng từ tên địa danh hoặc lat/lon. Dùng PROVINCES và WARDS, fallback OSM."""

    # Ưu tiên tọa độ trực tiếp
    if lat is not None and lon is not None:
        try:
            lat = float(lat); lon = float(lon)
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return {"name": region or "Unknown region", "lat": lat, "lon": lon, "source": "direct"}
        except Exception as e:
            logger.error(f"[resolve_region] Invalid lat/lon: {e}")
        return {"name": region or "Unknown region", "lat": None, "lon": None, "source": "invalid"}

    if region:
        query = normalize_query(region)

        # Exact match trong PROVINCES
        for province in PROVINCES:
            if normalize_query(province) == query:
                return {"name": province, "lat": None, "lon": None, "source": "province"}

        # Exact match trong WARDS (chỉ xử lý dict)
        for ward in WARDS:
            if not isinstance(ward, dict):
                continue
            try:
                ward_name = f"{ward.get('xa')} {ward.get('ten')} {ward.get('tinh')}"
                if normalize_query(ward_name) == query:
                    info = {"name": ward_name, "lat": ward.get("lat"), "lon": ward.get("lon"), "source": "ward"}
                    # Fallback nếu thiếu tọa độ
                    if info["lat"] is None or info["lon"] is None:
                        osm = fallback_geocode(region)
                        if osm:
                            info["lat"] = osm["lat"]; info["lon"] = osm["lon"]; info["source"] = "osm"
                    return info
            except Exception as e:
                logger.warning(f"[resolve_region] Ward parse error: {e}")
                continue

        # Fuzzy search trên PROVINCES
        matches = get_close_matches(query, [normalize_query(p) for p in PROVINCES], n=1, cutoff=0.85)
        if matches:
            best = matches[0]
            for province in PROVINCES:
                if normalize_query(province) == best:
                    return {"name": province, "lat": None, "lon": None, "source": "province"}

        # Fuzzy search trên WARDS (chỉ xử lý dict)
        ward_names = []
        for w in WARDS:
            if isinstance(w, dict):
                try:
                    ward_names.append(normalize_query(f"{w.get('xa')} {w.get('ten')} {w.get('tinh')}"))
                except Exception as e:
                    logger.warning(f"[resolve_region] Ward normalize error: {e}")
                    continue
        matches = get_close_matches(query, ward_names, n=1, cutoff=0.85)
        if matches:
            best = matches[0]
            for ward in WARDS:
                if not isinstance(ward, dict):
                    continue
                try:
                    ward_name = normalize_query(f"{ward.get('xa')} {ward.get('ten')} {ward.get('tinh')}")
                    if ward_name == best:
                        info = {
                            "name": f"{ward.get('xa')} {ward.get('ten')} {ward.get('tinh')}",
                            "lat": ward.get("lat"),
                            "lon": ward.get("lon"),
                            "source": "ward"
                        }
                        if info["lat"] is None or info["lon"] is None:
                            osm = fallback_geocode(region)
                            if osm:
                                info["lat"] = osm["lat"]; info["lon"] = osm["lon"]; info["source"] = "osm"
                        return info
                except Exception as e:
                    logger.warning(f"[resolve_region] Ward fuzzy error: {e}")
                    continue

        # Không tìm thấy → fallback OSM
        osm = fallback_geocode(region)
        if osm:
            return {"name": region, "lat": osm["lat"], "lon": osm["lon"], "source": "osm"}

        return {"name": region, "lat": None, "lon": None, "source": "not_found"}

    return {"name": "Unknown region", "lat": None, "lon": None, "source": "empty"}

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
            "weather": {},
            "error": None
        }

        # Nếu thiếu tọa độ → trả về lỗi rõ ràng
        if result["lat"] is None or result["lon"] is None:
            msg = f"Không tìm thấy tọa độ cho {result['region']} (source={result['source']})"
            logger.warning(f"[build_weather_response] {msg}")
            result["error"] = msg
            return result

        # Nếu không có dữ liệu thời tiết
        if not weather_data:
            msg = f"Không có dữ liệu thời tiết cho {result['region']}"
            logger.warning(f"[build_weather_response] {msg}")
            result["error"] = msg
            return result

        # Nếu có dữ liệu hợp lệ
        result["weather"] = weather_data
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