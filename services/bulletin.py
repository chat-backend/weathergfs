# services/bulletin.py
import logging
import pandas as pd
from datetime import datetime
from pytz import timezone

# Import dịch vụ Open-Meteo
from services.open_meteo.open_meteo import read_cache
from services.rain_openmeteo import get_precipitation_summary, get_precipitation_current

# Import các module con để sinh bản tin
from services.current_conditions import generate_current_conditions
from services.daily_overview import generate_daily_overview
from services.hourly_forecast import generate_hourly_forecast
from services.trend_10days import generate_trend_10days, _ensure_ts_local
from services.storm_alert import check_storm_alert
from services.unusual_alert import check_unusual_alert

from services.utils import (
    safe_float,
    choose_weather_icon,
    _safe_df_records,
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Múi giờ mặc định Việt Nam (ICT, UTC+7)
ICT = timezone("Asia/Bangkok")

# ===== Hàm 2: Sinh bản tin đầy đủ từ tọa độ =====
def generate_bulletin(region_name: str, lat: float, lon: float):
    """Điều phối sinh bản tin thời tiết từ dữ liệu Open-Meteo (chuẩn ICT UTC+7)."""
    try:
        now_local = datetime.now(ICT)
        today = now_local.date()

        # 1. Đọc dữ liệu từ cache
        current_df = read_cache(lat, lon, "current")
        hourly_df = read_cache(lat, lon, "hourly")
        daily_df = read_cache(lat, lon, "daily")

        current_df = current_df if isinstance(current_df, pd.DataFrame) else pd.DataFrame()
        hourly_df = hourly_df if isinstance(hourly_df, pd.DataFrame) else pd.DataFrame()
        daily_df = daily_df if isinstance(daily_df, pd.DataFrame) else pd.DataFrame()

        if current_df.empty and hourly_df.empty and daily_df.empty:
            return {
                "status": "error",
                "message": "Không có dữ liệu từ nguồn",
                "hint": "Kiểm tra kết nối API Open-Meteo",
                "note": "Thiếu current/hourly/daily"
            }

        # 2. Chuẩn hóa thời gian về ICT
        hourly_df = _ensure_ts_local(hourly_df) if not hourly_df.empty else hourly_df
        daily_df = _ensure_ts_local(daily_df) if not daily_df.empty else daily_df

        # Current an toàn
        current = current_df.iloc[0].to_dict() if not current_df.empty else {}
        bulletin = []
        alerts_list = []
        rain_summary = {"current": 0.0, "24h": 0.0, "hourly": [], "10d": []}

        # 3. Lượng mưa (chuẩn ICT)
        try:
            rain_summary = get_precipitation_summary(lat, lon)
            current["rain_now"] = safe_float(get_precipitation_current(lat, lon), 0.0)
            current["rain_mm"] = safe_float(rain_summary.get("current"), current.get("rain_mm", 0.0))
            current["rain_24h"] = safe_float(rain_summary.get("24h"), 0.0)
            current["rain_10d"] = rain_summary.get("10d", [])
        except Exception as e:
            logger.warning(f"Lỗi lấy lượng mưa: {e}")
            current["rain_now"] = 0.0
            current["rain_mm"] = safe_float(current.get("rain_mm"), 0.0)
            current["rain_24h"] = 0.0
            current["rain_10d"] = []
            rain_summary = {"current": current["rain_mm"], "24h": 0.0, "hourly": [], "10d": []}

        # 4. Header
        src_name = current.get("source", "open_meteo")
        desc = current.get("weather_desc", "")
        temp_val = current.get("temp_c")
        wind_spd = current.get("wind_speed_ms")
        rain_val_header = safe_float(current.get("rain_now"), current.get("rain_mm", 0.0))

        header_icon = choose_weather_icon(desc, temp=temp_val, wind=wind_spd, rain=rain_val_header)
        bulletin.extend([
            f"{header_icon}✨ BẢN TIN DỰ BÁO THỜI TIẾT — {region_name}",
            f"📅 Ngày: {today.strftime('%d/%m/%Y')}",
            f"🕒 Cập nhật lúc: {now_local.strftime('%H:%M %z')}",
            f"📡 Nguồn dữ liệu: {src_name}",
            ""
        ])

        # 5. Hiện tại
        if current:
            bulletin.extend(generate_current_conditions(current))
        else:
            bulletin.append("⚠️ Không có dữ liệu quan trắc hiện tại.")

        # 6. Tổng quan trong ngày
        if not hourly_df.empty:
            bulletin.extend(
                generate_daily_overview(
                    hourly_df,
                    today,
                    hum=current.get("humidity_pct"),
                    rain_24h=rain_summary.get("24h")
                )
            )
        else:
            bulletin.append("⚠️ Không có dữ liệu hourly để tạo tổng quan trong ngày.")

        # 7. Dự báo 24h
        if not hourly_df.empty:
            hourly_msgs = generate_hourly_forecast(
                hourly_df,
                total_rain_24h=rain_summary.get("24h"),
                rain_service_data={"hourly": rain_summary.get("hourly", [])}
            )
            bulletin.extend(hourly_msgs)
        else:
            bulletin.append("⚠️ Không có dữ liệu hourly để hiển thị dự báo theo giờ.")

        # 8. Xu hướng 10 ngày
        if not hourly_df.empty:
            trend_msgs, dfd_10, stats = generate_trend_10days(
                hourly_df,
                today,
                rain_10d=current.get("rain_10d")
            )
            bulletin.extend(trend_msgs)
        else:
            bulletin.append("⚠️ Không có dữ liệu hourly để phân tích xu hướng 10 ngày.")
            stats = {}

        # 9. Cảnh báo bão
        try:
            storm_alerts = check_storm_alert(current, daily_df)
            bulletin.append("🚨 Cảnh báo bão:\n" + storm_alerts)
            alerts_list.append(storm_alerts)
        except Exception as e:
            logger.warning(f"Lỗi khi tạo cảnh báo bão: {e}")
            storm_alerts = "⚠️ Không thể xác định trạng thái bão."
            bulletin.append("🚨 Cảnh báo bão:\n" + storm_alerts)
            alerts_list.append(storm_alerts)

        # 10. Cảnh báo bất thường
        try:
            unusual_alerts = check_unusual_alert(current, hourly_df, daily_df)
            bulletin.append("⚠️ Cảnh báo hiện tượng bất thường:\n" + unusual_alerts)
            alerts_list.append(unusual_alerts)
        except Exception as e:
            logger.warning(f"Lỗi khi tạo cảnh báo bất thường: {e}")
            unusual_alerts = "⚠️ Không thể xác định hiện tượng bất thường."
            bulletin.append("⚠️ Cảnh báo hiện tượng bất thường:\n" + unusual_alerts)
            alerts_list.append(unusual_alerts)

        # 11. Kết quả trả về
        return {
            "status": "ok",
            "source": src_name,
            "bulletin": {
                "summary": f"Tổng quan: {desc or 'Thời tiết ôn hòa'}",
                "text": "\n".join(bulletin),
                "updated_at": now_local.isoformat()
            },
            "current": current,
            "hourly": _safe_df_records(hourly_df),
            "daily": _safe_df_records(daily_df),
            "alerts": alerts_list,
            "rain": rain_summary,
            "trend_stats": stats
        }

    except Exception as e:
        logger.error(f"Lỗi hệ thống khi sinh bản tin: {e}")
        return {
            "status": "error",
            "message": f"Lỗi hệ thống khi sinh bản tin: {e}",
            "hint": "Kiểm tra kết nối API Open-Meteo và dữ liệu trả về",
            "note": "generate_bulletin đã bắt exception và trả về error JSON",
        }