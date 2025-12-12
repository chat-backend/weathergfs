# services/open_meteo/open_meteo.py
import logging
import time
import requests
import pandas as pd
from datetime import timedelta

from services import config
from services.error_handler import handle_service_error
from services.open_meteo.utils import build_api_url
from services.open_meteo.report import generate_weather_report
from services.open_meteo.current import parse_current
from services.open_meteo.hourly import parse_hourly
from services.open_meteo.daily import parse_daily

# Logger chung
logger = logging.getLogger("WeatherService")

# ===== Hàm gọi API với retry và headers =====
def fetch_forecast(lat: float, lon: float) -> dict:
    """Gọi Open-Meteo API, trả về dict JSON hoặc {} nếu lỗi."""
    url = build_api_url(lat, lon, config.FORECAST_DAYS)
    headers = {
        "User-Agent": config.API_USER_AGENT,
        "Accept": "application/json"
    }

    for attempt in range(config.MAX_RETRIES):
        try:
            logger.info(f"[fetch_forecast] Call API: {url} (attempt {attempt+1})")
            resp = requests.get(url, headers=headers, timeout=config.API_TIMEOUT)

            # Nếu server lỗi 5xx thì retry
            if resp.status_code >= 500:
                logger.warning(f"[fetch_forecast] Server error {resp.status_code}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(1.5 * (2 ** attempt))
                    continue

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError("Invalid JSON response")
            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            logger.error(f"[fetch_forecast] Network error: {e}", exc_info=True)
            if attempt < config.MAX_RETRIES - 1:
                time.sleep(1.5 * (2 ** attempt))
                continue
            handle_service_error("fetch_forecast", "network", e, alert_type="network")
            return {}

        except requests.RequestException as e:
            logger.error(f"[fetch_forecast] HTTP error: {e}", exc_info=True)
            handle_service_error("fetch_forecast", "http_error", e, alert_type="network")
            return {}

        except Exception as e:
            logger.exception("[fetch_forecast] Unexpected error", exc_info=True)
            handle_service_error("fetch_forecast", "unexpected", e, alert_type="data")
            return {}

    return {}

# ===== Hàm đọc cache theo section =====
def read_cache(lat: float, lon: float, section: str = None) -> pd.DataFrame:
    """Đọc dữ liệu từ API và parse theo section (current/hourly/daily)."""
    data = fetch_forecast(lat, lon)
    if not data:
        return pd.DataFrame()

    try:
        if section == "current":
            return parse_current(data)
        elif section == "hourly":
            return parse_hourly(data, forecast_days=config.FORECAST_DAYS)
        elif section == "daily":
            return parse_daily(data, forecast_days=config.FORECAST_DAYS)
        else:
            logger.warning("[read_cache] Section không xác định, trả về raw JSON")
            return pd.DataFrame([{"raw_json": data}])
    except Exception as e:
        handle_service_error("read_cache", section or "unknown", e, alert_type="data")
        return pd.DataFrame()

# ===== Hàm tổng hợp summary =====
def get_cache_summary(lat: float, lon: float) -> dict:
    """Tạo báo cáo tổng hợp về dữ liệu current/hourly/daily."""
    summary = {}
    for section in ["current", "hourly", "daily"]:
        try:
            df = read_cache(lat, lon, section=section)
            if df.empty:
                summary[section] = {"record_count": 0, "is_empty": True, "sample": None}
            else:
                sample = df.iloc[0].to_dict()
                stats = {}
                if "temp_c" in df.columns:
                    stats["temp_min"] = float(df["temp_c"].min())
                    stats["temp_max"] = float(df["temp_c"].max())
                    stats["temp_avg"] = float(df["temp_c"].mean())
                if "rain_mm" in df.columns:
                    stats["rain_total_mm"] = float(df["rain_mm"].sum())
                    stats["rain_hours"] = int((df["rain_mm"] > 0).sum())
                if "uv_index" in df.columns:
                    stats["uv_max"] = float(df["uv_index"].max())
                summary[section] = {
                    "record_count": len(df),
                    "is_empty": False,
                    "sample": sample,
                    "stats": stats
                }
        except Exception as e:
            summary[section] = {
                "record_count": 0,
                "is_empty": True,
                "sample": None,
                "error": str(e),
            }
    return summary

# ===== Hàm cộng dồn mưa 24h =====
def sum_rain_next_24h(df_hourly: pd.DataFrame, now_iso: str):
    """
    Cộng dồn mưa từ thời điểm now đến 24h tới.
    Ưu tiên rain_mm, fallback showers, cuối cùng precipitation.
    """
    if df_hourly is None or df_hourly.empty or not now_iso:
        return None

    now = pd.to_datetime(now_iso)
    end = now + timedelta(hours=24)

    mask = (pd.to_datetime(df_hourly["ts"]) >= now) & (pd.to_datetime(df_hourly["ts"]) < end)
    window = df_hourly.loc[mask]
    if window.empty:
        return None

    rain_vals = None
    if "rain_mm" in window.columns:
        rain_vals = window["rain_mm"]
    if rain_vals is None or rain_vals.isna().all():
        if "showers" in window.columns:
            rain_vals = window["showers"]
    if rain_vals is None or rain_vals.isna().all():
        if "precipitation" in window.columns:
            rain_vals = window["precipitation"]

    return round(float(rain_vals.fillna(0).sum()), 2) if rain_vals is not None else None


# ===== Hàm kiểm tra consistency daily vs hourly =====
def check_daily_hourly_consistency(daily_mm, hourly_sum_mm, ts_day):
    """So sánh tổng mưa daily từ API với tổng hourly để log cảnh báo nếu lệch nhiều."""
    if daily_mm is None or hourly_sum_mm is None:
        return
    if daily_mm == 0 and hourly_sum_mm == 0:
        return

    err = abs(daily_mm - hourly_sum_mm) / max(daily_mm, hourly_sum_mm)
    if err > 0.15:
        logger.warning(
            f"[rain] Lệch tổng mưa ngày {ts_day}: daily={daily_mm} vs hourly={hourly_sum_mm} (err={err:.0%})"
        )


# ===== Export rõ ràng =====
__all__ = [
    "fetch_forecast",
    "read_cache",
    "get_cache_summary",
    "sum_rain_next_24h",
    "check_daily_hourly_consistency",
]