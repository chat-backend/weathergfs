# services/unusual_alert.py
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
from services.utils import TEMP_EXTREME, WIND_EXTREME, RAIN_EXTREME, PRESSURE_LOW

# Danh sách hiện tượng bất thường cần cảnh báo
UNUSUAL_EVENTS = [
    "sấm sét","dông tố","mưa đá","lốc xoáy","mưa axit","sương mù dày đặc",
    "hiện tượng kỳ lạ","động đất","sóng thần","núi lửa","bão cát","khói bụi",
    "bầu trời xuất hiện vật lạ","ánh sáng bất thường","mưa thiên thạch",
    "sương muối","hạn hán cực đoan","cháy rừng",
]

# Nguồn ngoài (ví dụ NCHMF, USGS, NASA)
NCHMF_RSS = "https://www.nchmf.gov.vn/rss/canh-bao-bat-thuong.xml"
NCHMF_URL = "https://www.nchmf.gov.vn/"

def fetch_unusual_alerts_html(url=NCHMF_URL):
    alerts = []
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.news-item, li"):
            title = item.get_text(strip=True)
            if any(ev in title.lower() for ev in UNUSUAL_EVENTS):
                alerts.append(title)
    except Exception as e:
        alerts.append(f"Lỗi khi lấy dữ liệu HTML: {e}")
    return alerts

def fetch_unusual_alerts_rss(rss_url=NCHMF_RSS):
    alerts = []
    try:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            title = entry.title
            if any(ev in title.lower() for ev in UNUSUAL_EVENTS):
                alerts.append(title)
    except Exception as e:
        alerts.append(f"Lỗi khi lấy RSS: {e}")
    return alerts

def check_unusual_alert(current: dict, hourly_df: pd.DataFrame, daily_df: pd.DataFrame, official_alerts=None) -> str:
    """
    Khẳng định hiện tượng bất thường dựa trên:
    - Cảnh báo chính thức từ nguồn ngoài (ưu tiên nếu có)
    - Dữ liệu nội bộ: nhiệt độ, gió, mưa, áp suất, mô tả thời tiết
    """
    # 1) Nếu có cảnh báo chính thức
    if official_alerts:
        return "⚠️ CẢNH BÁO CHÍNH THỨC:\n- " + "\n- ".join(official_alerts)

    alerts = []

    # 2) Kiểm tra số liệu hiện tại
    temp = current.get("temp_c")
    wind = current.get("wind_speed_ms")
    rain = current.get("rain_mm")
    pressure = current.get("mslp_hpa")

    if isinstance(temp, (int, float)) and temp >= TEMP_EXTREME:
        alerts.append(f"🌡️ Nhiệt độ bất thường {temp:.1f}°C ≥ {TEMP_EXTREME}°C")
    if isinstance(wind, (int, float)) and wind >= WIND_EXTREME:
        alerts.append(f"💨 Gió cực mạnh {wind:.1f} m/s ≥ {WIND_EXTREME} m/s")
    if isinstance(rain, (int, float)) and rain >= RAIN_EXTREME:
        alerts.append(f"🌧️ Mưa cực lớn {rain:.1f} mm ≥ {RAIN_EXTREME} mm/ngày")
    if isinstance(pressure, (int, float)) and pressure <= PRESSURE_LOW:
        alerts.append(f"📉 Áp suất bất thường {pressure:.0f} hPa ≤ {PRESSURE_LOW} hPa")

    # 3) Kiểm tra mô tả thời tiết hiện tại
    desc = str(current.get("weather_desc", "")).lower()
    for event in UNUSUAL_EVENTS:
        if event in desc:
            alerts.append(f"⚠️ Hiện tượng bất thường phát hiện: {event.capitalize()}")

    # 4) Kiểm tra dữ liệu theo giờ/ngày (tương tự storm_alert)
    for df, label in [(hourly_df, "theo giờ"), (daily_df, "theo ngày")]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                desc_row = str(row.get("weather_desc", "")).lower()
                ts_val = row.get("ts_local") or row.get("ts")
                ts = pd.to_datetime(ts_val, errors="coerce")
                ts_str = ts.strftime("%d/%m %H:%M") if ts is not pd.NaT else str(ts_val)
                for event in UNUSUAL_EVENTS:
                    if event in desc_row:
                        alerts.append(f"⚠️ {label} {ts_str}: dự báo có {event}")
                if "temp_c" in row and row["temp_c"] >= TEMP_EXTREME:
                    alerts.append(f"🌡️ {label} {ts_str}: nhiệt độ bất thường {row['temp_c']:.1f}°C")
                if "wind_speed_ms" in row and row["wind_speed_ms"] >= WIND_EXTREME:
                    alerts.append(f"💨 {label} {ts_str}: gió cực mạnh {row['wind_speed_ms']:.1f} m/s")
                if "rain_mm" in row and row["rain_mm"] >= RAIN_EXTREME:
                    alerts.append(f"🌧️ {label} {ts_str}: mưa cực lớn {row['rain_mm']:.1f} mm")

    if not alerts:
        return "✅ Không phát hiện hiện tượng bất thường."
    return "\n".join(alerts)