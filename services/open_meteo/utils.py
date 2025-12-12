# services/open_meteo/utils.py
import urllib.parse
import pandas as pd
from typing import Any, Dict, List, Optional

# ===== Tiện ích số liệu =====
def safe_round(val: Any, ndigits: int = 2) -> Optional[float]:
    """Làm tròn an toàn, trả None nếu không thể ép float."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        return round(float(val), ndigits)
    except Exception:
        return None

def safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """Ép kiểu an toàn sang float, trả về default nếu lỗi."""
    if val is None:
        return default
    if isinstance(val, float) and pd.isna(val):
        return default
    try:
        return float(val)
    except Exception:
        return default

def _get(h: Dict[str, Any], key: str, idx: int, ndigits: Optional[int] = None) -> Any:
    """Lấy h[key][idx] an toàn, hỗ trợ làm tròn."""
    arr = h.get(key, [])
    if not isinstance(arr, (list, tuple)) or idx is None or idx < 0 or idx >= len(arr):
        return None
    val = arr[idx]
    return safe_round(val, ndigits) if ndigits is not None else val

def kmh_to_ms(val: Any, ndigits: int = 2) -> Optional[float]:
    """Đổi km/h sang m/s, giữ None nếu đầu vào None."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        return safe_round(float(val) / 3.6, ndigits)
    except Exception:
        return None

# ===== Mô tả thời tiết =====
def describe_weather(rain_mm: Optional[float], cloud_pct: Optional[float]) -> str:
    """Sinh mô tả ngắn dựa trên lượng mưa (mm) và độ mây (%)."""
    r = 0.0 if (rain_mm is None or pd.isna(rain_mm)) else float(rain_mm)
    c = 50.0 if (cloud_pct is None or pd.isna(cloud_pct)) else float(cloud_pct)
    if r >= 20: return "Mưa rất to"
    if r >= 10: return "Mưa to"
    if r >= 2:  return "Mưa vừa"
    if r >= 0.2:return "Có mưa"
    if c >= 80: return "Nhiều mây"
    if c >= 50: return "Có mây"
    return "Trời quang"

# ===== Thời gian & chỉ số gần nhất =====
def _nearest_hour_index(times: List[str], current_iso: str) -> int:
    """Tìm index giờ gần nhất với ISO hiện tại; trả -1 nếu không xác định."""
    if not times or not current_iso:
        return -1
    try:
        t_series = pd.to_datetime(pd.Series(times), errors="coerce", utc=True)
        t_current = pd.to_datetime(current_iso, errors="coerce", utc=True)
        if pd.isna(t_current) or t_series.isna().all():
            return -1
        diffs = (t_series - t_current).abs()
        # idxmin trên Series trả index; cần kiểm tra NaT
        valid = diffs.dropna()
        if valid.empty:
            return -1
        idx = int(valid.idxmin())
        return idx
    except Exception:
        return -1

# ===== URL gọi API =====
def build_api_url(lat: float, lon: float, forecast_days: int = 10) -> str:
    """Xây dựng URL Open-Meteo với tham số cần thiết."""
    base = "https://api.open-meteo.com/v1/forecast"
    hourly = ",".join([
        "temperature_2m","apparent_temperature","dewpoint_2m",
        "precipitation","rain","precipitation_probability",
        "relative_humidity_2m",
        "windspeed_10m","windgusts_10m","winddirection_10m",
        "cloudcover","cloudcover_low","cloudcover_mid","cloudcover_high",
        "pressure_msl","shortwave_radiation","uv_index"
    ])
    daily = ",".join([
        "temperature_2m_min","temperature_2m_max",
        "precipitation_sum","precipitation_hours",
        "windspeed_10m_max","windgusts_10m_max",
        "sunrise","sunset","uv_index_max"
    ])
    params = {
        "latitude": safe_round(lat, 6) or lat,
        "longitude": safe_round(lon, 6) or lon,
        "hourly": hourly,
        "daily": daily,
        "current_weather": "true",
        "timezone": "Asia/Ho_Chi_Minh",
        "forecast_days": int(forecast_days),
    }
    return f"{base}?{urllib.parse.urlencode(params)}"

# ===== Trích xuất hourly theo ngày =====
def extract_hourly_for_day(hourly_data: Dict[str, Any], day: str) -> List[Dict[str, Any]]:
    """Lọc danh sách bản ghi hourly theo ngày YYYY-MM-DD."""
    times = hourly_data.get("time") or []
    if not isinstance(times, list):
        return []

    records: List[Dict[str, Any]] = []
    for i, ts in enumerate(times):
        if not isinstance(ts, str) or len(ts) < 10:
            continue
        if ts[:10] == day:
            rec = {
                "ts": ts,
                "temp_c": _get(hourly_data, "temperature_2m", i, 2),
                "apparent_temp_c": _get(hourly_data, "apparent_temperature", i, 2),
                "dewpoint_c": _get(hourly_data, "dewpoint_2m", i, 2),
                "rain_mm": _get(hourly_data, "precipitation", i, 2),
                "precip_prob_pct": _get(hourly_data, "precipitation_probability", i, 0),
                "humidity_pct": _get(hourly_data, "relative_humidity_2m", i, 0),
                # Đồng bộ: chuyển gió từ km/h sang m/s
                "wind_speed_ms": kmh_to_ms(_get(hourly_data, "windspeed_10m", i, 2)),
                "wind_gust_ms": kmh_to_ms(_get(hourly_data, "windgusts_10m", i, 2)),
                "wind_direction": _get(hourly_data, "winddirection_10m", i, 0),
                "cloud_cover_pct": _get(hourly_data, "cloudcover", i, 0),
                "cloud_low_pct": _get(hourly_data, "cloudcover_low", i, 0),
                "cloud_mid_pct": _get(hourly_data, "cloudcover_mid", i, 0),
                "cloud_high_pct": _get(hourly_data, "cloudcover_high", i, 0),
                "mslp_hpa": _get(hourly_data, "pressure_msl", i, 1),
                "solar_radiation_wm2": _get(hourly_data, "shortwave_radiation", i, 1),
                "uv_index": _get(hourly_data, "uv_index", i, 1),
            }
            records.append(rec)
    return records

# ===== Tổng hợp daily từ hourly =====
def aggregate_daily_from_hourly(hourly_records: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """Tổng hợp daily từ các record hourly theo trung bình/cực trị/tổng."""
    if not hourly_records:
        return {}

    def vals(field: str) -> List[float]:
        v = []
        for r in hourly_records:
            x = r.get(field)
            if x is None or (isinstance(x, float) and pd.isna(x)):
                continue
            try:
                v.append(float(x))
            except Exception:
                continue
        return v

    def avg(field: str) -> Optional[float]:
        v = vals(field)
        return round(sum(v)/len(v), 2) if v else None

    def max_val(field: str) -> Optional[float]:
        v = vals(field)
        return round(max(v), 2) if v else None

    def min_val(field: str) -> Optional[float]:
        v = vals(field)
        return round(min(v), 2) if v else None

    def sum_val(field: str) -> Optional[float]:
        v = vals(field)
        return round(sum(v), 2) if v else None

    # Số giờ mưa: tính theo rain_mm > 0
    rain_hours = None
    rv = vals("rain_mm")
    if rv:
        rain_hours = int(sum(1 for x in rv if x > 0))

    return {
        "temp_c": avg("temp_c"),
        "temp_min": min_val("temp_c"),
        "temp_max": max_val("temp_c"),
        "apparent_temp_c": avg("apparent_temp_c"),
        "dewpoint_c": avg("dewpoint_c"),
        "rain_mm": sum_val("rain_mm"),
        "rain_hourly_mm": max_val("rain_mm"),
        "precip_prob_pct": avg("precip_prob_pct"),
        "humidity_pct": avg("humidity_pct"),
        "wind_speed_ms": avg("wind_speed_ms"),
        "wind_gust_ms": max_val("wind_gust_ms"),
        "wind_direction": avg("wind_direction"),
        "cloud_cover_pct": avg("cloud_cover_pct"),
        "mslp_hpa": avg("mslp_hpa"),
        "solar_radiation_wm2": avg("solar_radiation_wm2"),
        "uv_index": max_val("uv_index"),
        "precip_hours": rain_hours,
    }

