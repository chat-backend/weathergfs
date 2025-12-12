# services/query_dispatcher.py
import re
import pandas as pd

# ===== Hàm con: Hiện tại =====
def generate_current_conditions(current: dict) -> list[str]:
    """Sinh bản tin tình hình hiện tại."""
    msgs = []
    if not current:
        return ["⚠️ Không có dữ liệu quan trắc hiện tại."]
    temp = current.get("temp_c")
    wind = current.get("wind_speed_ms")
    rain = current.get("rain_mm")
    desc = current.get("weather_desc", "")
    msgs.append(f"🌡️ Nhiệt độ: {temp}°C")
    msgs.append(f"💨 Gió: {wind} m/s")
    msgs.append(f"🌧️ Lượng mưa: {rain} mm")
    msgs.append(f"💡 Nhận định: {desc}")
    return msgs

# ===== Hàm con: Tổng quan hôm nay =====
def generate_daily_overview(hourly_df: pd.DataFrame, today, hum=None, rain_24h=None) -> list[str]:
    """Sinh bản tin tổng quan trong ngày."""
    msgs = []
    if hourly_df.empty:
        return ["⚠️ Không có dữ liệu hourly để tạo tổng quan trong ngày."]
    temp_min = hourly_df["temp_c"].min()
    temp_max = hourly_df["temp_c"].max()
    msgs.append(f"🌡️ Biên độ nhiệt hôm nay: {temp_min:.1f}°C – {temp_max:.1f}°C")
    if hum is not None:
        msgs.append(f"💧 Độ ẩm trung bình: {hum}%")
    if rain_24h is not None:
        msgs.append(f"🌧️ Lượng mưa trong ngày: {rain_24h} mm")
    return msgs

# ===== Hàm con: Dự báo 24h =====
def generate_hourly_forecast(hourly_df: pd.DataFrame, total_rain_24h=None, rain_service_data=None) -> list[str]:
    """Sinh bản tin dự báo theo giờ trong 24h tới."""
    msgs = []
    if hourly_df.empty:
        return ["⚠️ Không có dữ liệu hourly để hiển thị dự báo 24h."]
    for _, row in hourly_df.head(24).iterrows():
        ts = row.get("ts_local") or row.get("ts")
        temp = row.get("temp_c")
        rain = row.get("rain_mm")
        msgs.append(f"🕒 {ts}: {temp}°C, mưa {rain} mm")
    if total_rain_24h is not None:
        msgs.append(f"🌧️ Tổng lượng mưa 24h: {total_rain_24h} mm")
    return msgs

# ===== Hàm con: Xu hướng 10 ngày =====
def generate_trend_10days(hourly_df: pd.DataFrame, today, rain_10d=None):
    """Sinh bản tin xu hướng 10 ngày."""
    msgs = []
    if hourly_df.empty:
        return ["⚠️ Không có dữ liệu hourly để phân tích xu hướng 10 ngày."], None, {}
    daily_temp = hourly_df.groupby(hourly_df["ts_local"].dt.date)["temp_c"].mean()
    for d, t in daily_temp.head(10).items():
        msgs.append(f"📅 {d}: Nhiệt độ TB {t:.1f}°C")
    if rain_10d:
        msgs.append(f"🌧️ Lượng mưa 10 ngày: {rain_10d}")
    return msgs, daily_temp, {}

# ===== Hàm chính: Phân loại theo khung giờ =====
def dispatch_weather_query(user_query: str, current: dict, hourly_df, daily_df, today, rain_summary=None):
    """
    Phân loại yêu cầu người dùng theo khung thời gian:
      - 'hiện tại' → tình hình hiện tại
      - 'hôm nay' → tổng quan trong ngày
      - '24h' → dự báo theo giờ trong 24h tới
      - '10 ngày' → xu hướng 10 ngày
    """
    query = user_query.lower()
    bulletin = []

    if re.search(r"hiện tại", query):
        bulletin.extend(generate_current_conditions(current))

    elif re.search(r"hôm nay", query):
        bulletin.extend(generate_daily_overview(hourly_df, today,
                                                hum=current.get("humidity_pct"),
                                                rain_24h=current.get("rain_24h")))

    elif re.search(r"24h", query) or re.search(r"24 giờ", query):
        bulletin.extend(generate_hourly_forecast(hourly_df,
                                                 total_rain_24h=(rain_summary.get("24h") if rain_summary else current.get("rain_24h")),
                                                 rain_service_data={"hourly": rain_summary.get("hourly", [])} if rain_summary else {}))

    elif re.search(r"10 ngày", query) or re.search(r"mười ngày", query):
        trend_msgs, _, _ = generate_trend_10days(hourly_df, today, rain_10d=current.get("rain_10d"))
        bulletin.extend(trend_msgs)

    else:
        bulletin.extend(generate_daily_overview(hourly_df, today,
                                                hum=current.get("humidity_pct"),
                                                rain_24h=current.get("rain_24h")))

    return "\n".join(bulletin)