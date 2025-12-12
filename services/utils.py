# services/utils.py
import logging
import pandas as pd
from pytz import timezone

logger = logging.getLogger("WeatherUtils")

# ===== Ngưỡng cảnh báo (chuẩn Việt Nam) =====
HEAT_ALERT = 32.0         # Nắng nóng (≥32°C đã oi bức)
RAIN_ALERT = 10.0         # Mưa lớn (≥10 mm/ngày dễ gây ngập úng)
WIND_ALERT = 8.0          # Gió mạnh (≥8 m/s bắt đầu gây nguy hiểm ngoài trời)
HUMIDITY_HIGH = 80.0      # Độ ẩm cao (≥80% gây oi bức, dễ cảm lạnh)
HUMIDITY_LOW = 40.0       # Độ ẩm thấp (≤40% gây khô da)
COLD_ALERT = 20.0         # Trời lạnh (≤20°C đã lạnh với người VN)
MSLP_HIGH = 1020.0        # Áp suất cao
MSLP_LOW = 1008.0         # Áp suất thấp
SOLAR_ALERT = 600.0       # Bức xạ mặt trời mạnh (≥600 W/m²)
UV_ALERT = 4.0            # UV cao (≥4 đã cần cảnh báo ở VN)

# ===== Ngưỡng cảnh báo bất thường =====
TEMP_EXTREME = 40.0       # °C, nhiệt độ cực cao
WIND_EXTREME = 25.0       # m/s, gió cực mạnh
RAIN_EXTREME = 100.0      # mm/ngày, mưa cực lớn
PRESSURE_LOW = 990.0      # hPa, áp suất thấp bất thường

ALERT_DESCRIPTIONS = {
    "HEAT_ALERT": "🔥 Nắng nóng oi bức nguy hiểm",
    "RAIN_ALERT": "🌧️ Mưa lớn dễ gây ngập úng",
    "WIND_ALERT": "💨 Gió mạnh, nguy hiểm ngoài trời",
    "HUMIDITY_HIGH": "💧 Độ ẩm cao bất thường, dễ gây cảm lạnh",
    "HUMIDITY_LOW": "🏜️ Độ ẩm thấp bất thường, dễ gây khô da",
    "COLD_ALERT": "❄️ Trời lạnh bất thường",
    "MSLP_HIGH": "📈 Áp suất cao bất thường",
    "MSLP_LOW": "📉 Áp suất thấp bất thường",
    "SOLAR_ALERT": "🔆 Bức xạ mặt trời mạnh",
    "UV_ALERT": "☀️ Chỉ số UV cao",
}

# Múi giờ ICT
ICT = timezone("Asia/Bangkok")

# ===== Hàm tiện ích =====
def safe_float(val, default=0.0):
    """Chuyển đổi sang float an toàn, nếu lỗi thì trả về default."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def _safe_df_records(df: pd.DataFrame) -> list[dict]:
    """Chuyển DataFrame thành list[dict] an toàn."""
    if df is None or df.empty:
        return []
    try:
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Lỗi khi chuyển DataFrame sang records: {e}")
        return []

def _ensure_ts_local(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa DataFrame để có cột ts_local dạng timezone-aware ICT."""
    if df is None or df.empty:
        return df

    if "ts_local" in df.columns:
        s = pd.to_datetime(df["ts_local"], errors="coerce")
    elif "ts" in df.columns:
        s = pd.to_datetime(df["ts"], errors="coerce")
    else:
        return df

    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize("UTC")
    df = df.copy()
    df["ts_local"] = s.dt.tz_convert(ICT)
    return df

def fmt_unit(val, unit=""):
    """Định dạng giá trị với đơn vị, xử lý None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    return f"{val:.1f}{unit}"

def _fmt_temp(val):
    """Định dạng nhiệt độ với emoji."""
    return f"🌡️ {val:.1f}°C" if val is not None and pd.notnull(val) else "-"

def _fmt_mm(val):
    """Định dạng lượng mưa mm với emoji."""
    return f"🌧️ {val:.1f} mm" if val is not None and pd.notnull(val) else "-"

def _fmt_wind(val):
    """Định dạng tốc độ gió m/s với emoji."""
    return f"💨 {val:.1f} m/s" if val is not None and pd.notnull(val) else "-"

def _fmt_hum(val):
    """Định dạng độ ẩm % với emoji."""
    return f"💧 {val:.0f}%" if val is not None and pd.notnull(val) else "-"

# ===== Chọn icon thời tiết =====
def choose_weather_icon(desc, temp=None, wind=None, rain=None):
    """Chọn emoji icon dựa trên mô tả thời tiết và dữ liệu."""
    desc = str(desc).lower() if desc else ""
    if "mưa" in desc or (rain is not None and rain > 0):
        return "🌧️"
    if "bão" in desc:
        return "⛈️"
    if "sương" in desc or "mù" in desc:
        return "🌫️"
    if "nhiều mây" in desc or "cloud" in desc:
        return "☁️"
    if "clear" in desc or "trời quang" in desc:
        return "🌤️"
    if "nắng" in desc or (temp is not None and temp >= HEAT_ALERT):
        return "☀️"
    if "lạnh" in desc or (temp is not None and temp <= COLD_ALERT):
        return "❄️"
    if wind is not None and wind >= WIND_ALERT:
        return "💨"
    return "🌤️"

# ===== Mô tả lượng mưa =====
def describe_rain(rain_mm: float) -> str:
    """Mô tả lượng mưa theo mm (chế độ Việt Nam)."""
    if rain_mm is None or pd.isna(rain_mm):
        return "Không rõ dữ liệu mưa"
    if rain_mm == 0:
        return "Không có mưa"
    if rain_mm < 1:
        return "Mưa rất nhẹ"
    if rain_mm < 3:
        return "Mưa nhẹ"
    if rain_mm < 10:
        return "Mưa vừa"
    return "Mưa lớn"   # ≥ 10 mm

# ===== Sinh nhận định =====
def generate_comment(
    desc=None, temp=None, rain=None, wind=None,
    hum=None, cloud=None, mslp=None, solar=None, uv_index=None
):
    """Sinh nhận định tự động từ dữ liệu thời tiết đầy đủ (ngưỡng chế độ Việt Nam)."""
    comments = []

    # Mưa
    if rain is not None and pd.notnull(rain):
        if rain >= RAIN_ALERT:
            comments.append(f"🌧️ Mưa lớn ({rain:.1f} mm), dễ gây ngập úng")
        elif rain >= 3:
            comments.append(f"🌧️ Mưa vừa ({rain:.1f} mm)")
        elif rain > 0:
            comments.append(f"🌧️ Mưa nhẹ ({rain:.1f} mm)")

    # Nhiệt độ
    if temp is not None and pd.notnull(temp):
        if temp >= HEAT_ALERT:
            comments.append(f"🔥 Nắng nóng ({temp:.1f}°C), cần đề phòng sốc nhiệt")
        elif temp <= COLD_ALERT:
            comments.append(f"❄️ Trời lạnh ({temp:.1f}°C), chú ý giữ ấm")

    # Gió
    if wind is not None and pd.notnull(wind):
        if wind >= WIND_ALERT:
            comments.append(f"💨 Gió mạnh ({wind:.1f} m/s), hạn chế hoạt động ngoài trời")
        elif wind > 0:
            comments.append(f"💨 Có gió nhẹ ({wind:.1f} m/s)")

    # Độ ẩm
    if hum is not None and pd.notnull(hum):
        if hum >= HUMIDITY_HIGH:
            comments.append(f"💧 Độ ẩm cao ({hum:.0f}%), dễ gây cảm lạnh")
        elif hum <= HUMIDITY_LOW:
            comments.append(f"🏜️ Độ ẩm thấp ({hum:.0f}%), dễ gây khô da")

        # Mây
    if cloud is not None and pd.notnull(cloud):
        if cloud >= 80:
            comments.append(f"☁️ Nhiều mây ({cloud:.0f}%), trời âm u")
        elif cloud <= 20:
            comments.append(f"☀️ Ít mây ({cloud:.0f}%), trời quang đãng")

    # Áp suất
    if mslp is not None and pd.notnull(mslp):
        if mslp < MSLP_LOW:
            comments.append(f"📉 Áp suất thấp ({mslp:.1f} hPa), dễ hình thành thời tiết xấu")
        elif mslp > MSLP_HIGH:
            comments.append(f"📈 Áp suất cao ({mslp:.1f} hPa), trời ổn định")

    # Bức xạ mặt trời
    if solar is not None and pd.notnull(solar):
        if solar > SOLAR_ALERT:
            comments.append(f"☀️ Bức xạ mặt trời mạnh ({solar:.1f} W/m²), nên tránh nắng gắt")

    # UV
    if uv_index is not None and pd.notnull(uv_index):
        if uv_index >= UV_ALERT:
            comments.append(f"🔆 Chỉ số UV cao ({uv_index:.1f}), cần bảo vệ da")

    return "💡 Nhận định thời tiết:\n- " + "\n- ".join(comments) if comments else "💡 Nhận định: Không có hiện tượng nổi bật."


# ===== Sinh cảnh báo =====
def detect_alerts(current: dict) -> list[str]:
    """Sinh danh sách cảnh báo dựa trên dữ liệu hiện tại (ngưỡng chế độ Việt Nam)."""
    alerts = []

    temp = safe_float(current.get("temp_c"), default=None)
    wind = safe_float(current.get("wind_speed_ms"), default=None)
    rain = safe_float(current.get("rain_24h"), default=None)
    uv = safe_float(current.get("uv_index"), default=None)
    solar = safe_float(current.get("solar_wm2"), default=None)
    mslp = safe_float(current.get("mslp"), default=None)
    hum = safe_float(current.get("humidity_pct"), default=None)

    # Nhiệt độ
    if temp is not None and temp >= HEAT_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['HEAT_ALERT']} ({temp:.1f}°C)")
    if temp is not None and temp <= COLD_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['COLD_ALERT']} ({temp:.1f}°C)")

    # Gió
    if wind is not None and wind >= WIND_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['WIND_ALERT']} ({wind:.1f} m/s)")

    # Mưa
    if rain is not None and rain >= RAIN_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['RAIN_ALERT']} ({rain:.1f} mm)")

    # UV
    if uv is not None and uv >= UV_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['UV_ALERT']} ({uv:.1f})")

    # Bức xạ mặt trời
    if solar is not None and solar >= SOLAR_ALERT:
        alerts.append(f"{ALERT_DESCRIPTIONS['SOLAR_ALERT']} ({solar:.1f} W/m²)")

    # Áp suất
    if mslp is not None and mslp < MSLP_LOW:
        alerts.append(f"{ALERT_DESCRIPTIONS['MSLP_LOW']} ({mslp:.1f} hPa)")
    if mslp is not None and mslp > MSLP_HIGH:
        alerts.append(f"{ALERT_DESCRIPTIONS['MSLP_HIGH']} ({mslp:.1f} hPa)")

    # Độ ẩm
    if hum is not None and hum >= HUMIDITY_HIGH:
        alerts.append(f"{ALERT_DESCRIPTIONS['HUMIDITY_HIGH']} ({hum:.0f}%)")
    if hum is not None and hum <= HUMIDITY_LOW:
        alerts.append(f"{ALERT_DESCRIPTIONS['HUMIDITY_LOW']} ({hum:.0f}%)")

    return alerts