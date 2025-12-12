# services/open_meteo/current.py
import pandas as pd
from .utils import (
    _get,
    safe_round,
    safe_float,
    describe_weather,
    _nearest_hour_index,
    kmh_to_ms,
)
from services.error_handler import handle_service_error
from .utils import (
    _get,
    safe_round,
    safe_float,
    describe_weather,
    _nearest_hour_index,
    kmh_to_ms,
)

def parse_current(data: dict) -> pd.DataFrame:
    """
    Parse dữ liệu current_weather kết hợp với hourly để bổ sung trường thiếu.
    Luôn dùng safe_float/safe_round để tránh lỗi khi dữ liệu không hợp lệ.
    """
    try:
        cw = data.get("current_weather", {}) or {}
        h = data.get("hourly", {}) or {}
        times = h.get("time", []) or []
        idx = _nearest_hour_index(times, cw.get("time"))

        if not cw or not cw.get("time"):
            # Trả về DataFrame rỗng nếu thiếu dữ liệu cơ bản
            return pd.DataFrame()

        # Lấy giá trị mưa và mây từ hourly nếu có index hợp lệ
        rain_val_raw = _get(h, "precipitation", idx, 2) if idx >= 0 else None
        rain_val = safe_float(rain_val_raw, None)

        cloud_val_raw = _get(h, "cloudcover", idx, 0) if idx >= 0 else None
        cloud_val = safe_float(cloud_val_raw, None)

        record = {
            "ts": cw.get("time"),
            "temp_c": safe_round(cw.get("temperature"), 2),
            "apparent_temp_c": safe_round(_get(h, "apparent_temperature", idx, 2), 2) if idx >= 0 else None,
            "dewpoint_c": safe_round(_get(h, "dewpoint_2m", idx, 2), 2) if idx >= 0 else None,
            "rain_mm": rain_val,
            "precip_prob_pct": safe_round(_get(h, "precipitation_probability", idx, 0), 0) if idx >= 0 else None,
            "humidity_pct": safe_round(_get(h, "relative_humidity_2m", idx, 0), 0) if idx >= 0 else None,
            "wind_speed_ms": kmh_to_ms(cw.get("windspeed"), 2),
            "wind_gust_ms": kmh_to_ms(_get(h, "windgusts_10m", idx, 2)) if idx >= 0 else None,
            "wind_direction": safe_round(cw.get("winddirection"), 0),
            "cloud_cover_pct": cloud_val,
            "mslp_hpa": safe_round(_get(h, "pressure_msl", idx, 1), 1) if idx >= 0 else None,
            "solar_radiation_wm2": safe_round(_get(h, "shortwave_radiation", idx, 1), 1) if idx >= 0 else None,
            "uv_index": safe_round(_get(h, "uv_index", idx, 1), 1) if idx >= 0 else None,
            "weather_desc": describe_weather(rain_val, cloud_val),
            "source": "open_meteo",
        }

        return pd.DataFrame([record])

    except Exception as e:
        # Nếu có lỗi parse, gọi handle_service_error để log cảnh báo
        handle_service_error("parse_current", "current", e, alert_type="data")
        return pd.DataFrame()