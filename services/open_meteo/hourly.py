# services/open_meteo/hourly.py
import pandas as pd
from .utils import _get, describe_weather, kmh_to_ms
from services.error_handler import handle_service_error
from .utils import (
    _get,
    safe_round,
    safe_float,
    describe_weather,
    _nearest_hour_index,
    kmh_to_ms,
)

def parse_hourly(data: dict, forecast_days: int = 10) -> pd.DataFrame:
    """Parse dữ liệu hourly trong khoảng forecast_days, có fallback cho mưa."""
    try:
        h = data.get("hourly", {}) or {}
        times = h.get("time", []) or []
        if not times:
            return pd.DataFrame()

        records = []
        for i, ts in enumerate(times[:forecast_days * 24]):
            ws_kmh = _get(h, "windspeed_10m", i, 2)
            gust_kmh = _get(h, "windgusts_10m", i, 2)

            # Ưu tiên lấy rain, fallback sang precipitation
            rain_val = _get(h, "rain", i, 2)
            if rain_val is None:
                rain_val = _get(h, "precipitation", i, 2)

            cloud_val = _get(h, "cloudcover", i, 0)

            record = {
                "ts": ts,
                "temp_c": _get(h, "temperature_2m", i, 2),
                "apparent_temp_c": _get(h, "apparent_temperature", i, 2),
                "dewpoint_c": _get(h, "dewpoint_2m", i, 2),
                "rain_mm": rain_val,
                "precip_prob_pct": _get(h, "precipitation_probability", i, 0),
                "humidity_pct": _get(h, "relative_humidity_2m", i, 0),
                "wind_speed_ms": kmh_to_ms(ws_kmh, 2),
                "wind_direction": _get(h, "winddirection_10m", i, 0),
                "wind_gust_ms": kmh_to_ms(gust_kmh, 2),
                "cloud_cover_pct": cloud_val,
                "cloud_low_pct": _get(h, "cloudcover_low", i, 0),
                "cloud_mid_pct": _get(h, "cloudcover_mid", i, 0),
                "cloud_high_pct": _get(h, "cloudcover_high", i, 0),
                "mslp_hpa": _get(h, "pressure_msl", i, 1),
                "solar_radiation_wm2": _get(h, "shortwave_radiation", i, 1),
                "uv_index": _get(h, "uv_index", i, 1),
                "weather_desc": describe_weather(rain_val, cloud_val),
                "source": "open_meteo"
            }
            records.append(record)

        return pd.DataFrame(records)

    except Exception as e:
        handle_service_error("parse_hourly", "hourly", e, alert_type="data")
        return pd.DataFrame()