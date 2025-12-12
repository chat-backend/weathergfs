# services/meteorology.py
import pandas as pd

from services.utils import safe_float

def compute_realfeel(temp_c: float, wind_ms: float) -> float | None:
    """
    Tính toán cảm giác thực tế (RealFeel).
    Công thức đơn giản: RealFeel = Nhiệt độ - 0.6 * tốc độ gió.
    """
    if isinstance(temp_c, (int, float)) and pd.notnull(temp_c) \
       and isinstance(wind_ms, (int, float)) and pd.notnull(wind_ms):
        return temp_c - 0.6 * wind_ms
    return None

def compute_heat_index(temp_c: float, humidity_pct: float, wind_ms: float = 0.0) -> float | None:
    """
    Tính toán chỉ số oi bức (Heat Index).
    Công thức tham khảo NOAA:
    HI = T + 0.33*RH - 0.7*WS - 4.0
    """
    if isinstance(temp_c, (int, float)) and pd.notnull(temp_c) \
       and isinstance(humidity_pct, (int, float)) and pd.notnull(humidity_pct) \
       and temp_c >= 27 and humidity_pct >= 40:
        return temp_c + 0.33 * humidity_pct - 0.7 * (wind_ms or 0) - 4.0
    return None

def compute_realfeel_night(temp_c: float, wind_ms: float, cloud_cover_pct: float) -> float | None:
    """
    Tính RealFeel ban đêm (có xét thêm mây che phủ).
    Công thức tham khảo: RealFeelNight = T - 0.6*WS + (CloudCover/100)*2
    """
    if isinstance(temp_c, (int, float)) and pd.notnull(temp_c) \
       and isinstance(wind_ms, (int, float)) and pd.notnull(wind_ms):
        adj = (cloud_cover_pct/100.0)*2 if isinstance(cloud_cover_pct, (int, float)) else 0
        return temp_c - 0.6 * wind_ms + adj
    return None

def compute_all_metrics(temp_c: float, wind_ms: float, humidity_pct: float, cloud_cover_pct: float = None) -> dict:
    """
    Trả về tất cả các chỉ số khí tượng cần thiết.
    """
    return {
        "realfeel": compute_realfeel(temp_c, wind_ms),
        "heat_index": compute_heat_index(temp_c, humidity_pct, wind_ms),
        "realfeel_night": compute_realfeel_night(temp_c, wind_ms, cloud_cover_pct),
    }