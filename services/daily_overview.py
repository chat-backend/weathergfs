# services/daily_overview.py
import pandas as pd
from services.utils import (
    safe_float,
    _fmt_mm,
    _fmt_hum,
    _fmt_wind,
    fmt_unit,
    HEAT_ALERT,
    COLD_ALERT,
    WIND_ALERT,
    RAIN_ALERT,
    UV_ALERT,
    SOLAR_ALERT,
    generate_comment,
)
from services.meteorology import compute_all_metrics
from services.wind import compute_wind_metrics, wind_alert


def generate_daily_overview(hourly_df, today, hum=None, rain_24h=None):
    bulletin = []

    # Lọc dữ liệu theo ngày
    if not hourly_df.empty and "ts_local" in hourly_df.columns:
        today_hours = hourly_df[hourly_df["ts_local"].dt.date == today]
    else:
        today_hours = hourly_df

    # Khởi tạo biến mặc định
    total_rain = max_wind = avg_wind = 0.0
    avg_temp = min_temp = max_temp = None
    desc_day = "Thời tiết ôn hòa"
    uv_idx = sea_pressure = surface_pressure = solar_radiation = None

    # ===== Ưu tiên lấy lượng mưa từ rain_openmeteo =====
    if isinstance(rain_24h, (int, float)):
        total_rain = float(rain_24h)
    elif not today_hours.empty and "precipitation_mm" in today_hours:
        total_rain = float(today_hours["precipitation_mm"].sum())
    elif not today_hours.empty and "rain_mm" in today_hours:
        total_rain = float(today_hours["rain_mm"].sum())
    else:
        total_rain = 0.0

    # Tính toán thống kê khác
    if not today_hours.empty:
        if "wind_speed_ms" in today_hours:
            max_wind = float(today_hours["wind_speed_ms"].max())
            avg_wind = float(today_hours["wind_speed_ms"].mean())
        if "temp_c" in today_hours:
            avg_temp = float(today_hours["temp_c"].mean())
            max_temp = float(today_hours["temp_c"].max())
            min_temp = float(today_hours["temp_c"].min())
        if "weather_desc" in today_hours and not today_hours["weather_desc"].isna().all():
            mode_vals = today_hours["weather_desc"].mode()
            if not mode_vals.empty:
                desc_day = str(mode_vals.iloc[0])
        if "uv_index" in today_hours and not today_hours["uv_index"].isna().all():
            uv_idx = float(today_hours["uv_index"].mean())
        if "humidity_pct" in today_hours and hum is None:
            hum = float(today_hours["humidity_pct"].mean())
        if "mslp_hpa" in today_hours and not today_hours["mslp_hpa"].isna().all():
            sea_pressure = float(today_hours["mslp_hpa"].mean())
        if "surface_pressure_hpa" in today_hours and not today_hours["surface_pressure_hpa"].isna().all():
            surface_pressure = float(today_hours["surface_pressure_hpa"].mean())
        if "solar_radiation_wm2" in today_hours and not today_hours["solar_radiation_wm2"].isna().all():
            solar_radiation = float(today_hours["solar_radiation_wm2"].mean())

    # ===== Thông số cơ bản =====
    bulletin.append("=== 📅 TỔNG QUAN TRONG NGÀY ===")
    bulletin.append(f"🌡️ Nhiệt độ trung bình: {fmt_unit(avg_temp, '°C')}")
    if min_temp is not None and max_temp is not None:
        bulletin.append(f"🌡️ Biên độ nhiệt: {min_temp:.1f}°C – {max_temp:.1f}°C")

    # ✅ Tính toán chỉ số khí tượng từ meteorology.py
    metrics = compute_all_metrics(avg_temp, avg_wind, hum, None)
    if metrics["realfeel"] is not None:
        bulletin.append(f"🌡️ Cảm giác thực tế (RealFeel): {metrics['realfeel']:.1f}°C")
    if metrics["heat_index"] is not None:
        bulletin.append(f"🔥 Chỉ số oi bức (Heat Index): {metrics['heat_index']:.1f}°C")

    bulletin.append(f"🌧️ Lượng mưa trong ngày: {fmt_unit(total_rain, 'mm')}")

    # ✅ Tính toán chỉ số gió bằng module wind.py
    wind_metrics = compute_wind_metrics(max_wind, None)
    wind_val = f"{wind_metrics['wind_speed_ms']:.1f} m/s"
    wind_val += f" — Cấp Beaufort: {wind_metrics['beaufort_scale']} ({wind_metrics['description']})"
    bulletin.append(f"💨 Gió mạnh nhất: {wind_val}")

    if hum is not None:
        bulletin.append(f"💧 Độ ẩm trung bình: {fmt_unit(hum, '%')}")
    if sea_pressure is not None:
        bulletin.append(f"📈 Áp suất mặt biển trung bình: {fmt_unit(sea_pressure, 'hPa')}")
    if surface_pressure is not None:
        bulletin.append(f"📈 Áp suất mặt đất trung bình: {fmt_unit(surface_pressure, 'hPa')}")
    if solar_radiation is not None:
        bulletin.append(f"🔆 Bức xạ mặt trời trung bình: {fmt_unit(solar_radiation, 'W/m²')}")
    if uv_idx is not None:
        bulletin.append(f"☀️ Chỉ số UV trung bình: {uv_idx:.1f}")

    # ===== Cảnh báo trong ngày =====
    alerts = []
    if not today_hours.empty:
        if total_rain > RAIN_ALERT:
            alerts.append(f"🌧️ Mưa lớn ({total_rain:.1f} mm), nguy cơ ngập úng.")
        if max_wind > WIND_ALERT:
            alerts.append(f"💨 Gió mạnh nhất {max_wind:.1f} m/s, cần chú ý an toàn.")
        if max_temp is not None and max_temp >= HEAT_ALERT:
            alerts.append(f"🔥 Nắng nóng gay gắt, nhiệt độ cao nhất {max_temp:.1f}°C.")
        if min_temp is not None and min_temp <= COLD_ALERT:
            alerts.append(f"❄️ Trời lạnh, nhiệt độ thấp nhất {min_temp:.1f}°C, cần giữ ấm.")
        if uv_idx is not None and uv_idx >= UV_ALERT:
            alerts.append(
                f"☀️ Chỉ số UV cao ({uv_idx:.1f}), nguy cơ tổn thương da và mắt. "
                "Nên mặc áo dài tay, đội mũ rộng vành, dùng kem chống nắng và hạn chế ra ngoài trời nắng."
            )
        if metrics["heat_index"] is not None and metrics["heat_index"] >= 40.0:
            alerts.append(
                f"🔥 Chỉ số oi bức (Heat Index): {metrics['heat_index']:.1f}°C — nguy cơ sốc nhiệt. "
                "Hạn chế hoạt động ngoài trời, uống đủ nước và nghỉ ngơi trong bóng râm."
            )
        if metrics["realfeel"] is not None and metrics["realfeel"] <= 10.0:
            alerts.append(
                f"❄️ Cảm giác thực tế (RealFeel): {metrics['realfeel']:.1f}°C — nguy cơ cảm lạnh. "
                "Nên mặc ấm, hạn chế ở ngoài trời lâu."
            )

    if alerts:
        bulletin.append("\n🚨 Cảnh báo trong ngày")
        bulletin.extend(alerts)

    # ===== Nhận định tổng quan =====
    overview_comment = generate_comment(
        desc_day, avg_temp, total_rain, avg_wind, hum,
        mslp=sea_pressure, solar=solar_radiation, uv_index=uv_idx
    )
    bulletin.append(overview_comment)

    # 📌 Tóm tắt cuối bản tin
    summary_parts = []
    if total_rain > RAIN_ALERT:
        summary_parts.append("mưa lớn")
    if max_wind > WIND_ALERT:
        summary_parts.append("gió mạnh")
    if max_temp is not None and max_temp >= HEAT_ALERT:
        summary_parts.append("nắng nóng gay gắt")
    if min_temp is not None and min_temp <= COLD_ALERT:
        summary_parts.append("trời lạnh")
    if uv_idx is not None and uv_idx >= UV_ALERT:
        summary_parts.append("UV cao")

    if summary_parts:
        bulletin.append("")
        bulletin.append("📌 Tóm tắt: " + ", ".join(summary_parts))

    bulletin.append("")
    return bulletin