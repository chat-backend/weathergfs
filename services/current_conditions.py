# services/current_conditions.py
import pandas as pd
from datetime import datetime

from services.utils import (
    safe_float,
    choose_weather_icon,
    fmt_unit,
    generate_comment,
)
from services.meteorology import compute_all_metrics
from services.wind import compute_wind_metrics, wind_alert


# ===== Tiện ích hiển thị =====
def wind_direction_8dir(deg):
    """Quy đổi độ sang hướng gió 8 hướng (N, NE, E, SE, S, SW, W, NW)."""
    if deg is None or not isinstance(deg, (int, float)) or pd.isna(deg):
        return "Không rõ"
    dirs = [
        "Bắc (N)", "Đông Bắc (NE)", "Đông (E)", "Đông Nam (SE)",
        "Nam (S)", "Tây Nam (SW)", "Tây (W)", "Tây Bắc (NW)"
    ]
    idx = int((deg + 22.5) % 360 / 45)
    return dirs[idx]


def format_source(src):
    """Map nguồn dữ liệu thành tên hiển thị đẹp, có fallback an toàn."""
    src_map = {
        "open_meteo": "Open‑Meteo (GFS/ICON/ECMWF)",
        "open_meteo_source": "Open‑Meteo (GFS/ICON/ECMWF)",
        "cache": "Cache nội bộ WeatherGfs",
        "aggregate_hourly": "Tổng hợp từ hourly",
    }
    return src_map.get(str(src), str(src)) if src is not None else "Open‑Meteo (GFS/ICON/ECMWF)"


def format_time(ts):
    """Định dạng thời gian quan trắc ISO → DD/MM/YYYY HH:MM (local)."""
    try:
        ts_str = str(ts)
        if ts_str.endswith("Z"):
            ts_str = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(ts)


def generate_current_conditions(current: dict) -> list:
    """
    Hiển thị tình hình hiện tại: thông số tức thời, nhận định thời tiết (không sinh cảnh báo tại đây).
    """
    bulletin = []

    if not current:
        bulletin.append("=== 🌍 TÌNH HÌNH HIỆN TẠI ===")
        bulletin.append("⚠️ Không có dữ liệu quan trắc hiện tại.")
        return bulletin

    # Lấy dữ liệu từ JSON
    temp_c    = current.get("temp_c")
    hum       = current.get("humidity_pct")
    wind      = current.get("wind_speed_ms")
    wind_dir  = current.get("wind_direction")

    rain_now  = safe_float(current.get("rain_now"), None)
    rain      = rain_now if rain_now is not None else safe_float(current.get("rain_mm"), 0.0)

    desc      = current.get("weather_desc", "Không rõ")
    uv_idx    = current.get("uv_index")
    cloud     = current.get("cloud_cover_pct")
    mslp      = current.get("mslp_hpa")
    solar     = current.get("solar_radiation_wm2")
    ts        = current.get("ts")

    icon_now = choose_weather_icon(desc, temp=temp_c, wind=wind, rain=rain)

    # ===== Thông số cơ bản =====
    bulletin.append("=== 🌍 TÌNH HÌNH HIỆN TẠI ===")
    bulletin.append("📊 Thông số cơ bản")
    bulletin.append(f"{icon_now} Nhiệt độ: {fmt_unit(temp_c, '°C')} | {desc}")

    # Chỉ số khí tượng
    metrics = compute_all_metrics(temp_c, wind, hum, cloud)
    if metrics["realfeel"] is not None:
        bulletin.append(f"🌡️ Cảm giác thực tế (RealFeel): {metrics['realfeel']:.1f}°C")
    if metrics["heat_index"] is not None:
        bulletin.append(f"🔥 Chỉ số oi bức (Heat Index): {metrics['heat_index']:.1f}°C")

    # Lượng mưa tức thời
    bulletin.append(f"🌧️ Lượng mưa hiện tại (ICT): {fmt_unit(rain, 'mm')}")

    # Gió
    wind_metrics = compute_wind_metrics(wind, current.get("wind_gust_ms"))
    wind_val = f"{wind_metrics['wind_speed_ms']:.1f} m/s"
    if wind_metrics["gust_speed_ms"] is not None:
        wind_val += f" (giật {wind_metrics['gust_speed_ms']:.1f} m/s)"
    wind_val += f" — Cấp Beaufort: {wind_metrics['beaufort_scale']} ({wind_metrics['description']})"
    if isinstance(wind_dir, (int, float)) and pd.notnull(wind_dir):
        wind_val += f" — Hướng: {wind_direction_8dir(wind_dir)}"
    bulletin.append(f"💨 Gió: {wind_val}")

    bft_note = wind_alert(wind_metrics["beaufort_scale"])
    if bft_note:
        bulletin.append(bft_note)

    # Độ ẩm
    hum_str = f"{int(hum)}%" if isinstance(hum, (int, float)) and pd.notnull(hum) else "-"
    bulletin.append(f"💧 Độ ẩm trung bình: {hum_str}")

    # Mây
    cloud_str = f"{int(cloud)}%" if isinstance(cloud, (int, float)) and pd.notnull(cloud) else "-"
    bulletin.append(f"☁️ Mây che phủ: {cloud_str}")

    # Áp suất
    bulletin.append(f"📈 Áp suất: {fmt_unit(mslp, 'hPa')}")

    # Bức xạ mặt trời
    bulletin.append(f"🔆 Bức xạ mặt trời: {fmt_unit(solar, 'W/m²')}")

    # UV
    if isinstance(uv_idx, (int, float)) and pd.notnull(uv_idx):
        bulletin.append(f"☀️ Chỉ số tia cực tím (UV Index): {uv_idx:.1f}")
    if isinstance(solar, (int, float)) and pd.notnull(solar) and solar == 0.0:
        bulletin.append("ℹ️ Ghi chú: Ban đêm nên bức xạ mặt trời bằng 0.")

    # Thời gian quan trắc
    if ts:
        bulletin.append(f"🕒 Thời gian quan trắc: {format_time(ts)}")

    # Nhận định tổng hợp
    has_any_data = any([
        temp_c is not None and pd.notnull(temp_c),
        rain is not None and pd.notnull(rain),
        wind is not None and pd.notnull(wind),
        hum is not None and pd.notnull(hum),
        cloud is not None and pd.notnull(cloud),
        mslp is not None and pd.notnull(mslp),
        solar is not None and pd.notnull(solar),
        uv_idx is not None and pd.notnull(uv_idx),
    ])
    if has_any_data:
        bulletin.append(
            generate_comment(
                desc=desc, temp=temp_c, rain=rain, wind=wind,
                hum=hum, cloud=cloud, mslp=mslp, solar=solar, uv_index=uv_idx
            )
        )

    bulletin.append("")
    return bulletin