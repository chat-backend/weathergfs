# services/rain_openmeteo.py
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any
from pytz import timezone

logger = logging.getLogger(__name__)

ICT = timezone("Asia/Bangkok")
BASE_URL = "https://api.open-meteo.com/v1/forecast"


def _hour_key(iso_str: str) -> str:
    """
    Chuẩn hóa chuỗi thời gian ISO thành khóa theo giờ: 'YYYY-MM-DDTHH:00'.
    - Dùng để so khớp bền vững giữa thời gian hệ thống (có offset) và thời gian API (có/không offset).
    - Ví dụ: '2025-12-12T13:00+07:00' hoặc '2025-12-12T13:00' -> '2025-12-12T13:00'
    """
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%Y-%m-%dT%H:00")
    except Exception:
        # Fallback: cắt thủ công theo vị trí giờ nếu định dạng lạ
        # Hỗ trợ chuỗi dạng 'YYYY-MM-DDTHH:MM...' -> lấy đến phút
        if "T" in iso_str and len(iso_str) >= 16:
            return iso_str[:16]
        return iso_str


def _closest_index_iso(times: List[str], target_iso: str) -> int:
    """
    Tìm index có thời gian gần nhất với target_iso trong danh sách ISO-8601.
    So sánh theo datetime.parse (đầy đủ offset nếu có). Nếu danh sách rỗng, trả về 0.
    """
    if not times:
        return 0
    try:
        target_dt = datetime.fromisoformat(target_iso)
    except Exception:
        # Nếu target_iso parse lỗi, thử theo khóa giờ đầu tiên
        target_dt = None

    # Nếu parse được, dùng khoảng cách thời gian thực
    if target_dt is not None:
        try:
            return min(
                range(len(times)),
                key=lambda i: abs(datetime.fromisoformat(times[i]) - target_dt)
            )
        except Exception:
            pass

    # Fallback: so khớp theo khóa giờ
    tkey = _hour_key(target_iso)
    # Nếu có khóa giờ trùng khớp, ưu tiên index đầu tiên trùng
    for i, ts in enumerate(times):
        if _hour_key(ts) == tkey:
            return i
    # Nếu không trùng, trả về 0
    return 0


def _build_now_iso_local_hour() -> str:
    """
    Tạo chuỗi thời gian hiện tại theo ICT, làm tròn về giờ:
    - ISO có offset (ví dụ: '2025-12-12T13:00+07:00')
    """
    return datetime.now(ICT).replace(minute=0, second=0, microsecond=0).isoformat()


def get_precipitation_current(lat: float, lon: float) -> float:
    """
    Lấy lượng mưa tại đúng giờ hiện tại (theo timezone=auto).
    Nếu không có điểm đúng giờ, lấy điểm gần nhất theo thời gian.
    """
    try:
        url = (
            f"{BASE_URL}?latitude={lat}&longitude={lon}"
            f"&hourly=precipitation&forecast_days=2&timezone=auto"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data: Dict[str, Any] = resp.json()

        times = data.get("hourly", {}).get("time", []) or []
        precip = data.get("hourly", {}).get("precipitation", []) or []

        if not times or not precip:
            return 0.0

        # Khóa giờ hiện tại theo ICT
        now_iso = _build_now_iso_local_hour()
        now_key = _hour_key(now_iso)

        # Ưu tiên khớp theo khóa giờ (bền vững với offset)
        idx = None
        for i, ts in enumerate(times):
            if _hour_key(ts) == now_key:
                idx = i
                break

        # Nếu không khớp, dùng index gần nhất
        if idx is None:
            idx = _closest_index_iso(times, now_iso)

        if idx < 0 or idx >= len(precip):
            return 0.0

        return float(precip[idx])
    except Exception as e:
        logger.error(f"Open-Meteo current error: {e}")
        return 0.0


def get_precipitation_24h(lat: float, lon: float) -> dict:
    """
    Lấy lượng mưa theo giờ trong 24h tới, luôn đủ 24 giá trị bắt đầu từ giờ hiện tại.
    - Nếu giờ hiện tại không khớp, bắt đầu từ điểm gần nhất.
    - Nếu dữ liệu thiếu, bổ sung 0.0 cho đủ 24.
    Trả về:
      {
        "hourly": [24 giá trị float],
        "total_24h": tổng lượng mưa 24h (float)
      }
    """
    try:
        url = (
            f"{BASE_URL}?latitude={lat}&longitude={lon}"
            f"&hourly=precipitation&forecast_days=2&timezone=auto"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data: Dict[str, Any] = resp.json()

        times = data.get("hourly", {}).get("time", []) or []
        precip = data.get("hourly", {}).get("precipitation", []) or []

        if not times or not precip:
            hourly = [0.0] * 24
            return {"hourly": hourly, "total_24h": float(sum(hourly))}

        # Khóa giờ hiện tại theo ICT
        now_iso = _build_now_iso_local_hour()
        now_key = _hour_key(now_iso)

        # Tìm vị trí bắt đầu theo khóa giờ
        start_idx = None
        for i, ts in enumerate(times):
            if _hour_key(ts) == now_key:
                start_idx = i
                break

        if start_idx is None:
            start_idx = _closest_index_iso(times, now_iso)

        # Cắt 24 phần tử liên tiếp từ vị trí bắt đầu
        hourly_slice = precip[start_idx:start_idx + 24]

        # Đảm bảo đủ 24 phần tử
        hourly = [float(x) for x in hourly_slice]
        if len(hourly) < 24:
            hourly.extend([0.0] * (24 - len(hourly)))

        total_24h = float(sum(hourly))
        return {"hourly": hourly, "total_24h": total_24h}
    except Exception as e:
        logger.error(f"Open-Meteo 24h error: {e}")
        hourly = [0.0] * 24
        return {"hourly": hourly, "total_24h": float(sum(hourly))}


def get_precipitation_10d(lat: float, lon: float) -> List[dict]:
    """
    Lấy lượng mưa theo ngày trong 10 ngày tới, luôn đủ 10 ngày.
    - Nếu dữ liệu thiếu, bổ sung ngày None với lượng mưa 0.0.
    Trả về: danh sách 10 phần tử dạng {"date": str|None, "precipitation": float}
    """
    try:
        url = (
            f"{BASE_URL}?latitude={lat}&longitude={lon}"
            f"&daily=precipitation_sum&forecast_days=10&timezone=auto"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data: Dict[str, Any] = resp.json()

        times = data.get("daily", {}).get("time", []) or []
        precip = data.get("daily", {}).get("precipitation_sum", []) or []

        forecast: List[dict] = []
        for i in range(10):
            if i < len(times) and i < len(precip):
                forecast.append(
                    {"date": times[i], "precipitation": float(precip[i])}
                )
            else:
                forecast.append({"date": None, "precipitation": 0.0})

        return forecast
    except Exception as e:
        logger.error(f"Open-Meteo 10d error: {e}")
        return [{"date": None, "precipitation": 0.0} for _ in range(10)]


def get_precipitation_summary(lat: float, lon: float) -> dict:
    """
    Tổng hợp dữ liệu lượng mưa cho 3 thời điểm:
    - current: lượng mưa hiện tại (float)
    - 24h: tổng lượng mưa 24 giờ tới (float)
    - hourly: mảng 24 giá trị mưa theo giờ (list[float])
    - 10d: mảng 10 ngày tới (list[{"date": str|None, "precipitation": float}])
    - error: None nếu thành công, hoặc chuỗi mô tả lỗi
    """
    try:
        current = get_precipitation_current(lat, lon)
        summary_24h = get_precipitation_24h(lat, lon)
        next_10days = get_precipitation_10d(lat, lon)

        return {
            "current": current,
            "24h": summary_24h["total_24h"],
            "hourly": summary_24h["hourly"],
            "10d": next_10days,
            "error": None,
        }
    except Exception as e:
        logger.error(f"Open-Meteo summary error: {e}")
        return {
            "current": 0.0,
            "24h": 0.0,
            "hourly": [0.0] * 24,
            "10d": [{"date": None, "precipitation": 0.0} for _ in range(10)],
            "error": str(e),
        }