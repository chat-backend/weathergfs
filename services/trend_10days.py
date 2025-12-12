# services/trend_10days.py
import pandas as pd
from pytz import timezone

from services.utils import (
    safe_float, choose_weather_icon, _fmt_mm, _fmt_hum, fmt_unit,
    HEAT_ALERT, COLD_ALERT, WIND_ALERT, RAIN_ALERT, UV_ALERT
)
from services.meteorology import compute_all_metrics

# Múi giờ ICT
ICT = timezone("Asia/Bangkok")

# ==== Phần đầu: tiện ích chuẩn hóa ====
def _ensure_ts_local(df: pd.DataFrame) -> pd.DataFrame:
    """Đảm bảo có cột ts_local dạng timezone-aware ICT."""
    if "ts_local" in df.columns:
        s = pd.to_datetime(df["ts_local"], errors="coerce")
        if getattr(s.dt, "tz", None) is None:
            s = s.dt.tz_localize("UTC")
        df = df.copy()
        df["ts_local"] = s.dt.tz_convert(ICT)
        return df
    elif "ts" in df.columns:
        s = pd.to_datetime(df["ts"], errors="coerce")
        if getattr(s.dt, "tz", None) is None:
            s = s.dt.tz_localize("UTC")
        df = df.copy()
        df["ts_local"] = s.dt.tz_convert(ICT)
        return df
    else:
        return df

def _ensure_precip_column(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo cột chuẩn 'precipitation_mm' từ dữ liệu hourly."""
    df = df.copy()
    if "precipitation_mm" in df.columns:
        df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce").fillna(0.0)
        return df
    for col in ("rain_mm", "snowfall_mm", "showers_mm"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["precipitation_mm"] = (df["rain_mm"] + df["snowfall_mm"] + df["showers_mm"]).clip(lower=0.0)
    return df

def _validate_hourly_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Đảm bảo đầy đủ cột numeric để tổng hợp daily không ra NaN."""
    df = _ensure_precip_column(df).copy()
    needed = ["temp_c", "precipitation_mm", "wind_speed_ms", "humidity_pct",
              "cloud_cover_pct", "mslp_hpa", "solar_radiation_wm2", "uv_index"]
    defaults = {"temp_c": pd.NA, "precipitation_mm": 0.0, "wind_speed_ms": pd.NA,
                "humidity_pct": pd.NA, "cloud_cover_pct": pd.NA, "mslp_hpa": pd.NA,
                "solar_radiation_wm2": pd.NA, "uv_index": pd.NA}
    for col in needed:
        if col not in df.columns:
            df[col] = defaults[col]
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "weather_desc" not in df.columns:
        df["weather_desc"] = "Không rõ"
    return df

# ==== Phần giữa: tổng hợp daily & sinh bản tin chi tiết ====
def aggregate_daily_from_hourly(hourly_df: pd.DataFrame, start_ts_ict: pd.Timestamp, days: int = 10) -> pd.DataFrame:
    """Tổng hợp dữ liệu theo giờ thành daily cho '10 ngày tới'."""
    if hourly_df is None or hourly_df.empty:
        return pd.DataFrame()
    df = _ensure_ts_local(hourly_df).copy()
    df = _validate_hourly_columns(df)
    if "ts_local" not in df.columns:
        return pd.DataFrame()
    if start_ts_ict.tzinfo is None:
        start_ts_ict = ICT.localize(pd.Timestamp(start_ts_ict))
    else:
        start_ts_ict = start_ts_ict.tz_convert(ICT)
    end_ts_ict = start_ts_ict + pd.Timedelta(days=days)
    df_10 = df[(df["ts_local"] >= start_ts_ict) & (df["ts_local"] < end_ts_ict)].copy()
    if df_10.empty:
        return pd.DataFrame()
    df_10["date"] = df_10["ts_local"].dt.date
    df_10["hour"] = df_10["ts_local"].dt.hour
    daytime = df_10[(df_10["hour"] >= 6) & (df_10["hour"] <= 18)]
    agg_all = df_10.groupby("date").agg({
        "temp_c": ["min", "max"],
        "precipitation_mm": "sum",
        "wind_speed_ms": "mean",
        "humidity_pct": "mean",
        "cloud_cover_pct": "mean",
        "mslp_hpa": "mean",
        "solar_radiation_wm2": "mean",
        "uv_index": "mean",
        "weather_desc": lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "Không rõ"
    })
    agg_all.columns = [
        "temp_min", "temp_max", "precipitation_mm", "wind_speed_ms", "humidity_pct",
        "cloud_cover_pct", "mslp_hpa", "solar_radiation_wm2_fallback",
        "uv_index_fallback", "weather_desc"
    ]
    agg_all = agg_all.reset_index()
    if not daytime.empty:
        agg_day = daytime.groupby("date").agg({
            "solar_radiation_wm2": "mean", "uv_index": "max"
        }).reset_index().rename(columns={
            "solar_radiation_wm2": "solar_radiation_wm2_day", "uv_index": "uv_index_day"
        })
        agg_all = agg_all.merge(agg_day, on="date", how="left")
    else:
        agg_all["solar_radiation_wm2_day"] = pd.NA
        agg_all["uv_index_day"] = pd.NA
    agg_all["solar_radiation_wm2"] = agg_all["solar_radiation_wm2_day"].where(
        pd.notnull(agg_all["solar_radiation_wm2_day"]), agg_all["solar_radiation_wm2_fallback"]
    )
    agg_all["uv_index"] = agg_all["uv_index_day"].where(
        pd.notnull(agg_all["uv_index_day"]), agg_all["uv_index_fallback"]
    )
    agg_all["wind_avg_ms"] = agg_all["wind_speed_ms"]
    agg_all["ts_local"] = pd.to_datetime(agg_all["date"]).map(lambda d: pd.Timestamp(d).tz_localize(ICT))
    agg_all["source"] = "aggregate_hourly"
    return agg_all.sort_values("ts_local").head(days)


def generate_trend_10days(
    hourly_df: pd.DataFrame,
    today_date: pd.Timestamp,
    rain_10d: list[dict] = None,   # ✅ list các dict {"date":..., "precipitation":...}
    start_from_now: bool = True
):
    """Sinh bản tin xu hướng 10 ngày từ hourly_df, đồng bộ với rain_service và meteorology."""
    bulletin, dfd_10 = [], pd.DataFrame()
    if hourly_df is None or hourly_df.empty: 
        return [], dfd_10, {}

    if "ts_local" not in hourly_df.columns or hourly_df["ts_local"].isna().all(): 
        return [], dfd_10, {}

    base_ts = pd.to_datetime(today_date)
    if start_from_now:
        now_ict = pd.Timestamp.now(tz=ICT)
        min_src = hourly_df["ts_local"].min()
        start_ts_ict = max(now_ict.floor("h"), min_src)
    else:
        start_ts_ict = ICT.localize(pd.Timestamp(base_ts.date())) if base_ts.tzinfo is None else base_ts.tz_convert(ICT).normalize()

    daily_df = aggregate_daily_from_hourly(hourly_df, start_ts_ict, days=10)
    if daily_df.empty or len(daily_df) < 3: 
        return [], dfd_10, {}

    bulletin.append("=== 📅 XU HƯỚNG 10 NGÀY TỚI ===")

    # Thống kê
    stats = {
        "rain_days": 0, "heavy_rain_days": 0, "sunny_days": 0,
        "cold_days": 0, "windy_days": 0, "uv_high_days": 0,
        "realfeel_cold_days": 0, "heat_index_high_days": 0,
    }

    dfd_10 = daily_df.copy()
    for i, row in enumerate(dfd_10.itertuples()):
        date_txt = row.ts_local.strftime("%d/%m")
        temp_txt = f"{row.temp_min:.1f}°C – {row.temp_max:.1f}°C" if pd.notnull(row.temp_min) and pd.notnull(row.temp_max) else "-"

        # ✅ Dùng đúng cột precipitation_mm
        rain_total = safe_float(row.precipitation_mm, None)
        # ✅ Override bằng dữ liệu rain_10d từ rain_openmeteo nếu có
        if rain_10d and i < len(rain_10d):
            rain_total = safe_float(rain_10d[i].get("precipitation"), rain_total)

        wind_avg = safe_float(row.wind_avg_ms, None)
        hum_d = safe_float(row.humidity_pct, None)
        uv_d = safe_float(getattr(row, "uv_index", None), None)
        cloud_d = safe_float(row.cloud_cover_pct, None)
        mslp_d = safe_float(row.mslp_hpa, None)
        solar_d = safe_float(row.solar_radiation_wm2, None)
        desc_d = row.weather_desc

        metrics = compute_all_metrics(row.temp_max, wind_avg, hum_d, cloud_d)
        realfeel_txt = f"{metrics['realfeel']:.1f}°C" if metrics["realfeel"] is not None else "-"
        heat_index_txt = f"{metrics['heat_index']:.1f}°C" if metrics["heat_index"] is not None else "-"

        icon = choose_weather_icon(desc_d, temp=row.temp_max, wind=wind_avg, rain=rain_total)
        cloud_txt = f"{cloud_d:.0f}%" if pd.notnull(cloud_d) else "-"

        bulletin.append(
            f"{icon} {date_txt} → 🌡️ {temp_txt} | 🌡️ RealFeel: {realfeel_txt} | 🔥 Heat Index: {heat_index_txt} | "
            f"{_fmt_mm(rain_total)} | 💨 {fmt_unit(wind_avg,'m/s')} | {_fmt_hum(hum_d)} | "
            f"☁️ {cloud_txt} | 📈 {fmt_unit(mslp_d,'hPa')} | 🔆 {fmt_unit(solar_d,'W/m²')} | 🌞 UV: {fmt_unit(uv_d,'')}"
        )

        # Cộng dồn thống kê
        if isinstance(rain_total, (int, float)) and pd.notnull(rain_total):
            if rain_total > 0: stats["rain_days"] += 1
            if rain_total >= RAIN_ALERT: stats["heavy_rain_days"] += 1
        if isinstance(row.temp_max, (int, float)) and pd.notnull(row.temp_max) and row.temp_max >= HEAT_ALERT:
            stats["sunny_days"] += 1
        if isinstance(row.temp_min, (int, float)) and pd.notnull(row.temp_min) and row.temp_min <= COLD_ALERT:
            stats["cold_days"] += 1
        if isinstance(wind_avg, (int, float)) and pd.notnull(wind_avg) and wind_avg >= WIND_ALERT:
            stats["windy_days"] += 1
        if isinstance(uv_d, (int, float)) and pd.notnull(uv_d) and uv_d >= UV_ALERT:
            stats["uv_high_days"] += 1
        if metrics["realfeel"] is not None and metrics["realfeel"] <= 10.0:
            stats["realfeel_cold_days"] += 1
        if metrics["heat_index"] is not None and metrics["heat_index"] >= 40.0:
            stats["heat_index_high_days"] += 1

    # 👉 Thống kê tổng hợp
    bulletin.append("")
    bulletin.append(
        f"📊 Thống kê 10 ngày tới: 🌧️ {stats['rain_days']} ngày có mưa | 🌧️ {stats['heavy_rain_days']} ngày mưa lớn | "
        f"☀️ {stats['sunny_days']} ngày nắng nóng | ❄️ {stats['cold_days']} ngày lạnh | 💨 {stats['windy_days']} ngày gió mạnh | "
        f"🌞 {stats['uv_high_days']} ngày UV cao | ❄️ {stats['realfeel_cold_days']} ngày RealFeel lạnh | 🔥 {stats['heat_index_high_days']} ngày Heat Index cao"
    )

    # 📌 Tóm tắt xu hướng cuối bản tin
    summary = format_trend_summary(stats)
    bulletin.append("")
    bulletin.append(summary)

    return bulletin, dfd_10, stats


def summarize_trend_stats(stats: dict) -> dict:
    """Trả về dict thống kê 10 ngày (dùng cho JSON/API)."""
    return stats or {}


def format_trend_summary(stats: dict) -> str:
    """Tạo chuỗi tóm tắt xu hướng từ dict thống kê."""
    if not stats:
        return ""  # Không in ra gì nếu không có dữ liệu
    parts = []
    if stats.get("heavy_rain_days", 0) > 0: parts.append("mưa lớn")
    if stats.get("sunny_days", 0) > 0: parts.append("nắng nóng")
    if stats.get("cold_days", 0) > 0: parts.append("trời lạnh")
    if stats.get("windy_days", 0) > 0: parts.append("gió mạnh")
    if stats.get("uv_high_days", 0) > 0: parts.append("UV cao")
    if stats.get("realfeel_cold_days", 0) > 0: parts.append("RealFeel lạnh")
    if stats.get("heat_index_high_days", 0) > 0: parts.append("Heat Index cao")

    return "📌 Xu hướng: " + ", ".join(parts) if parts else ""