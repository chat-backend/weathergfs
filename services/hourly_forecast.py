# services/hourly_forecast.py
import pandas as pd

from services.utils import (
    safe_float,
    choose_weather_icon,
    _fmt_mm,
    _fmt_hum,
    _fmt_wind,
    fmt_unit,
    HEAT_ALERT,
    COLD_ALERT,
    WIND_ALERT,
    UV_ALERT,
    SOLAR_ALERT,
)
from services.meteorology import compute_all_metrics


def _normalize_rain_row(row: pd.Series) -> float:
    """Chuẩn hóa lượng mưa theo giờ: ưu tiên precipitation_mm, fallback cộng rain+snow+showers."""
    if row is None or row.empty:
        return 0.0
    if "precipitation_mm" in row and pd.notnull(row.get("precipitation_mm")):
        return safe_float(row.get("precipitation_mm"), 0.0)
    rain = safe_float(row.get("rain_mm"), 0.0)
    snow = safe_float(row.get("snowfall_mm"), 0.0)
    showers = safe_float(row.get("showers_mm"), 0.0)
    return max(0.0, rain + snow + showers)


def generate_hourly_forecast(
    hourly_df: pd.DataFrame,
    total_rain_24h: float = None,
    rain_service_data: dict = None,
):
    """
    Sinh bản tin dự báo theo giờ (24h).
    - Ưu tiên dùng mưa theo giờ từ rain_service_data['hourly'] (Open-Meteo).
    - Nếu không có rain_service_data, fallback dùng dữ liệu mưa trong dataframe (precipitation_mm / rain+snow+showers).
    - Tổng 24h: nếu có rain_service_data thì tính từ nó; nếu không, tính từ dataframe; nếu truyền total_rain_24h thì dùng để hiển thị.
    """

    bulletin = []

    if hourly_df is None or hourly_df.empty:
        return ["❌ Không có dữ liệu hourly từ nguồn"]

    bulletin.append("=== 🕒 DỰ BÁO THEO GIỜ (24h) ===")

    # Sắp xếp theo thời gian
    dfh_ict = hourly_df.sort_values("ts_local") if "ts_local" in hourly_df.columns else hourly_df.sort_values("ts")
    next_24 = dfh_ict.head(24)

    # Mưa theo giờ từ dịch vụ mưa (Open-Meteo)
    rain_hourly_vals = []
    if isinstance(rain_service_data, dict):
        rain_hourly_vals = rain_service_data.get("hourly", []) or []

    # Các thống kê
    rain_hours = strong_wind_hours = cold_hours = hot_hours = uv_high_hours = solar_high_hours = 0
    realfeel_cold_hours = heat_index_high_hours = 0

    for i, (idx, row) in enumerate(next_24.iterrows()):
        # Thời gian
        ts_txt = "-"
        if "ts_local" in next_24.columns and pd.notnull(row.get("ts_local")):
            ts_txt = row["ts_local"].strftime("%H:%M")
        elif row.get("ts"):
            ts_txt = pd.to_datetime(row.get("ts"), errors="coerce").strftime("%H:%M")

        # Các biến khí tượng
        tval    = row.get("temp_c")
        desc_h  = row.get("weather_desc", "Không rõ")
        wind_h  = safe_float(row.get("wind_speed_ms"), 0.0)
        hum_h   = row.get("humidity_pct")
        cloud_h = row.get("cloud_cover_pct")
        uv_h    = row.get("uv_index")
        mslp_h  = row.get("mslp_hpa")
        solar_h = row.get("solar_radiation_wm2")

        # Lượng mưa theo giờ: ưu tiên từ rain_service_data, fallback từ dataframe
        if rain_hourly_vals and i < len(rain_hourly_vals):
            rain_h = safe_float(rain_hourly_vals[i], 0.0)
        else:
            rain_h = _normalize_rain_row(row)

        # ✅ Truyền đúng lượng mưa vào choose_weather_icon
        icon = choose_weather_icon(desc_h, temp=tval, wind=wind_h, rain=rain_h)

        # Tính toán chỉ số khí tượng
        metrics = compute_all_metrics(tval, wind_h, hum_h, cloud_h)
        realfeel_txt = f"{metrics['realfeel']:.1f}°C" if metrics["realfeel"] is not None else "-"
        heat_index_txt = f"{metrics['heat_index']:.1f}°C" if metrics["heat_index"] is not None else "-"

        uv_txt = f"{safe_float(uv_h, None):.1f}" if pd.notnull(uv_h) else "-"
        cloud_txt = f"{safe_float(cloud_h, None):.0f}%" if pd.notnull(cloud_h) else "-"

        line = (
            f"{ts_txt} → {icon} {fmt_unit(tval, '°C')} | {desc_h} | "
            f"{_fmt_mm(rain_h)} (mưa) | {_fmt_wind(wind_h)} (gió) | "
            f"{_fmt_hum(hum_h)} | ☁️ {cloud_txt} | "
            f"📈 {fmt_unit(mslp_h, 'hPa')} | 🔆 {fmt_unit(solar_h, 'W/m²')} | "
            f"🌞 UV: {uv_txt} | 🌡️ RealFeel: {realfeel_txt} | 🔥 Heat Index: {heat_index_txt}"
        )
        bulletin.append(line)

        # Thống kê theo giờ
        if rain_h > 0: rain_hours += 1
        if wind_h >= WIND_ALERT: strong_wind_hours += 1
        if isinstance(tval, (int, float)) and pd.notnull(tval):
            if tval <= COLD_ALERT: cold_hours += 1
            if tval >= HEAT_ALERT: hot_hours += 1
        if isinstance(uv_h, (int, float)) and uv_h >= UV_ALERT: uv_high_hours += 1
        if isinstance(solar_h, (int, float)) and solar_h >= SOLAR_ALERT: solar_high_hours += 1

        if metrics["realfeel"] is not None and metrics["realfeel"] <= 10.0:
            realfeel_cold_hours += 1
        if metrics["heat_index"] is not None and metrics["heat_index"] >= 40.0:
            heat_index_high_hours += 1

        # Nếu là dòng cuối cùng thì chèn thống kê ngay sau đó
        if i == len(next_24) - 1:
            if rain_hourly_vals:
                # ✅ Dùng trực tiếp tổng lượng mưa 24h từ API
                total_rain_24h_calc = float(sum(rain_hourly_vals))
            else:
                # Fallback: tính từ dataframe theo ts_local (UTC+7)
                total_rain_24h_calc = float(next_24.apply(_normalize_rain_row, axis=1).sum())

            # Nếu truyền total_rain_24h (tham số cũ), dùng để hiển thị; nếu không dùng giá trị tính được
            total_display = fmt_unit(
                total_rain_24h if isinstance(total_rain_24h, (int, float)) else total_rain_24h_calc,
                'mm'
            )

            bulletin.append(
                f"📊 Thống kê 24h: {rain_hours} giờ có mưa | Tổng lượng mưa: {total_display} | "
                f"{strong_wind_hours} giờ gió mạnh | {cold_hours} giờ lạnh | {hot_hours} giờ nắng nóng | "
                f"{uv_high_hours} giờ UV cao | {solar_high_hours} giờ bức xạ mạnh | "
                f"{realfeel_cold_hours} giờ RealFeel lạnh | {heat_index_high_hours} giờ Heat Index cao"
            )

    return bulletin