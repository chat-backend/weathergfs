# services/storm_alert.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
import feedparser

# Ngưỡng dấu hiệu áp thấp/bão
LOW_PRESSURE_FORMATION = 1000   # hPa (áp thấp hình thành)
STORM_PRESSURE_ALERT = 990      # hPa (áp suất thấp bất thường)
STORM_WIND_ALERT = 17           # m/s ~ cấp gió bão
STORM_WIND_EXTREME = 25         # m/s ~ gió bão rất mạnh
STORM_RAIN_ALERT = 100          # mm/ngày (mưa cực lớn)

# URL mặc định của NCHMF (có thể thay đổi theo trang RSS/HTML thực tế)
NCHMF_URL = "https://www.nchmf.gov.vn/"
NCHMF_RSS = "https://www.nchmf.gov.vn/rss/bao-canh-bao.xml"

def _fmt_date(ts_val) -> str:
    """Định dạng ngày dd/mm từ ts hoặc ts_local."""
    try:
        return pd.to_datetime(ts_val).strftime("%d/%m")
    except Exception:
        return str(ts_val)

def classify_wind(wind: float) -> str:
    """Phân loại cấp gió và giật theo thang đơn giản."""
    if wind >= STORM_WIND_EXTREME:
        return f"💨 Gió bão rất mạnh {wind:.1f} m/s (cấp ≥ 10, giật trên cấp 12)"
    elif wind >= STORM_WIND_ALERT:
        return f"💨 Gió bão {wind:.1f} m/s (cấp 8–9, giật cấp 10–11)"
    elif wind >= 10:
        return f"💨 Gió mạnh {wind:.1f} m/s"
    return ""

def fetch_nchmf_alerts_html(url=NCHMF_URL):
    """Lấy cảnh báo từ HTML trang NCHMF."""
    alerts = []
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.news-item, li"):
            title = item.get_text(strip=True)
            if any(k in title.lower() for k in ["cảnh báo", "bão", "áp thấp"]):
                alerts.append(title)
    except Exception as e:
        alerts.append(f"Lỗi khi lấy dữ liệu NCHMF HTML: {e}")
    return alerts

def fetch_nchmf_alerts_rss(rss_url=NCHMF_RSS):
    """Lấy cảnh báo từ RSS feed của NCHMF."""
    alerts = []
    try:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            title = entry.title
            if any(k in title.lower() for k in ["cảnh báo", "bão", "áp thấp"]):
                alerts.append(title)
    except Exception as e:
        alerts.append(f"Lỗi khi lấy RSS NCHMF: {e}")
    return alerts

def check_storm_alert(current: dict, daily_df: pd.DataFrame, official_alerts=None) -> str:
    """Khẳng định trạng thái áp thấp/bão dựa trên:
    - Cảnh báo chính thức từ NCHMF (ưu tiên nếu có)
    - Dữ liệu nội bộ: áp suất, gió, mưa
    """
    # 1) Nếu có cảnh báo chính thức từ NCHMF
    if official_alerts:
        return "🌀 CẢNH BÁO CHÍNH THỨC NCHMF:\n- " + "\n- ".join(official_alerts)

    signals = []

    # 2) Áp suất
    pressure = current.get("mslp_hpa") if current.get("mslp_hpa") is not None else current.get("mslp")
    if isinstance(pressure, (int, float)):
        if pressure <= STORM_PRESSURE_ALERT:
            signals.append(f"📉 Áp suất rất thấp {pressure:.0f} hPa → tâm bão hình thành")
        elif pressure <= LOW_PRESSURE_FORMATION:
            signals.append(f"📉 Áp suất thấp {pressure:.0f} hPa → hiện tượng áp thấp đang hình thành")

    # 3) Gió
    wind = 0.0
    wind_raw = current.get("wind_speed_ms")
    try:
        wind = float(wind_raw) if wind_raw is not None else 0.0
    except (TypeError, ValueError):
        pass
    wind_signal = classify_wind(wind)
    if wind_signal:
        signals.append(wind_signal)

    # 4) Mưa theo ngày
    heavy_rain_detected = False
    if isinstance(daily_df, pd.DataFrame) and not daily_df.empty and "rain_mm" in daily_df.columns:
        heavy_rain_days = daily_df[daily_df["rain_mm"] >= STORM_RAIN_ALERT]
        for _, row in heavy_rain_days.iterrows():
            date_txt = _fmt_date(row.get("ts_local") or row.get("ts"))
            rain_val = row.get("rain_mm", 0.0)
            signals.append(f"🌧️ {date_txt}: mưa cực lớn {rain_val:.1f} mm")
            heavy_rain_detected = True

    # 5) Phân loại khẳng định
    if not signals:
        return "✅ Không có dấu hiệu áp thấp hay bão."

    has_pressure = any("Áp suất" in s for s in signals)
    has_wind = any("Gió" in s for s in signals)

    # Bổ sung: nếu có mưa cực lớn + gió mạnh (≥17 m/s) thì coi là bão
    if heavy_rain_detected and wind >= STORM_WIND_ALERT:
        return "🌀 BÃO đã hình thành (mưa cực lớn + gió mạnh):\n• " + "\n• ".join(signals)

    if wind >= STORM_WIND_EXTREME:
        return "🌀 BÃO đã hình thành:\n• " + "\n• ".join(signals)
    if has_pressure and has_wind:
        return "🌀 BÃO đã hình thành:\n• " + "\n• ".join(signals)
    if has_pressure and not has_wind:
        return "🌪️ ÁP THẤP nhiệt đới:\n• " + "\n• ".join(signals)

    return "⚠️ Nguy cơ bão:\n• " + "\n• ".join(signals)