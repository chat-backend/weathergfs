# services/wind.py
import pandas as pd
from services.utils import safe_float

def compute_wind_metrics(wind_speed_ms: float, gust_speed_ms: float = None) -> dict:
    """
    Tính toán các chỉ số gió:
      - Tốc độ gió trung bình (m/s)
      - Cấp gió Beaufort
      - Gió giật (nếu có)
    """
    wind_speed = safe_float(wind_speed_ms, 0.0)
    gust_speed = safe_float(gust_speed_ms, None)

    # Quy đổi sang cấp Beaufort
    beaufort_scale = _wind_to_beaufort(wind_speed)

    return {
        "wind_speed_ms": wind_speed,
        "gust_speed_ms": gust_speed,
        "beaufort_scale": beaufort_scale,
        "description": _beaufort_description(beaufort_scale)
    }

def _wind_to_beaufort(speed_ms: float) -> int:
    """
    Quy đổi tốc độ gió (m/s) sang cấp Beaufort.
    """
    thresholds = [0.3, 1.5, 3.3, 5.5, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6]
    for i, th in enumerate(thresholds):
        if speed_ms <= th:
            return i
    return 12  # >= 32.7 m/s

def _beaufort_description(scale: int) -> str:
    """
    Mô tả gió theo thang Beaufort.
    """
    descriptions = {
        0: "Lặng gió",
        1: "Gió rất nhẹ",
        2: "Gió nhẹ",
        3: "Gió vừa",
        4: "Gió khá mạnh",
        5: "Gió mạnh",
        6: "Gió rất mạnh",
        7: "Gió to",
        8: "Gió bão nhỏ",
        9: "Gió bão vừa",
        10: "Gió bão lớn",
        11: "Gió bão rất lớn",
        12: "Cuồng phong"
    }
    return descriptions.get(scale, "Không rõ")

# ✅ Hàm bổ sung: trả về cảnh báo gió theo cấp Beaufort
def wind_alert(beaufort_scale: int) -> str | None:
    """
    Trả về cảnh báo gió theo cấp Beaufort.
    Ví dụ: "Cấp 6 – Gió rất mạnh, nguy cơ nguy hiểm ngoài trời".
    """
    desc = _beaufort_description(beaufort_scale)
    if beaufort_scale >= 6:
        return f"💨 Cấp {beaufort_scale} – {desc}, nguy cơ nguy hiểm ngoài trời."
    elif beaufort_scale >= 4:
        return f"💨 Cấp {beaufort_scale} – {desc}, cần chú ý khi di chuyển ngoài trời."
    elif beaufort_scale >= 2:
        return f"💨 Cấp {beaufort_scale} – {desc}, gió nhẹ, ít ảnh hưởng."
    else:
        return None