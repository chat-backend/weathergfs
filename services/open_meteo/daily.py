# services/open_meteo/daily.py
import pandas as pd
import numpy as np
from .utils import describe_weather, extract_hourly_for_day, aggregate_daily_from_hourly, kmh_to_ms
from services.error_handler import handle_service_error
from .utils import (
    _get,
    safe_round,
    safe_float,
    describe_weather,
    _nearest_hour_index,
    kmh_to_ms,
)

def _get_daily_value(d: dict, key: str, i: int):
    """Helper: lấy giá trị daily JSON theo index, trả về None nếu không hợp lệ.
       Hỗ trợ list, tuple, numpy.ndarray, pandas.Series, pd.Index."""
    arr = d.get(key)
    if arr is None:
        return None
    # Ép kiểu về list nếu là numpy/pandas
    if isinstance(arr, (np.ndarray, pd.Series, pd.Index)):
        arr = arr.tolist()
    if isinstance(arr, (list, tuple)) and i < len(arr):
        return arr[i]
    return None

def parse_daily(data: dict, forecast_days: int = 10) -> pd.DataFrame:
    """Parse dữ liệu daily, ưu tiên lấy trực tiếp từ daily JSON, fallback sang aggregate từ hourly nếu thiếu."""
    try:
        d = data.get("daily", {}) or {}
        times = d.get("time", []) or []
        h = data.get("hourly", {}) or {}
        if not times:
            return pd.DataFrame()

        records = []
        for i, ts in enumerate(times[:forecast_days]):
            # Lấy dữ liệu hourly trong ngày đó
            day_hourly = extract_hourly_for_day(h, ts)
            agg = aggregate_daily_from_hourly(day_hourly)

            # Lấy trực tiếp từ daily JSON (ưu tiên tuyệt đối)
            temp_min = _get_daily_value(d, "temperature_2m_min", i)
            temp_max = _get_daily_value(d, "temperature_2m_max", i)
            rain_mm = _get_daily_value(d, "precipitation_sum", i)
            precip_hours = _get_daily_value(d, "precipitation_hours", i)
            uv_index = _get_daily_value(d, "uv_index_max", i)
            wind_speed_max = _get_daily_value(d, "windspeed_10m_max", i)
            wind_gust_max = _get_daily_value(d, "windgusts_10m_max", i)
            sunrise = _get_daily_value(d, "sunrise", i)
            sunset = _get_daily_value(d, "sunset", i)

            rec = {
                "ts": f"{ts}T00:00",
                "temp_min": temp_min if temp_min is not None else agg.get("temp_min"),
                "temp_max": temp_max if temp_max is not None else agg.get("temp_max"),
                "rain_mm": rain_mm if rain_mm is not None else agg.get("rain_mm"),
                "rain_hourly_mm": agg.get("rain_hourly_mm"),
                "wind_speed_max_ms": kmh_to_ms(wind_speed_max) if wind_speed_max is not None else agg.get("wind_speed_ms"),
                "wind_gust_max_ms": kmh_to_ms(wind_gust_max) if wind_gust_max is not None else agg.get("wind_gust_ms"),
                "precip_hours": precip_hours if precip_hours is not None else agg.get("precip_hours"),
                "sunrise": sunrise,
                "sunset": sunset,
                "uv_index": uv_index if uv_index is not None else agg.get("uv_index"),
                "weather_desc": describe_weather(
                    rain_mm if rain_mm is not None else agg.get("rain_mm"),
                    agg.get("cloud_cover_pct")
                ),
                "source": "open_meteo"
            }

            # Chỉ merge các field bổ sung từ agg nếu chưa có trong rec
            for k, v in agg.items():
                if k not in rec or rec[k] is None:
                    rec[k] = v

            records.append(rec)

        return pd.DataFrame(records)

    except Exception as e:
        handle_service_error("parse_daily", "daily", e, alert_type="data")
        return pd.DataFrame()