# services/chat.py
import logging
import math
from typing import Optional
from fastapi import APIRouter, Query

from services.app_utils import resolve_region
from services.bulletin import generate_bulletin
from services.error_handler import handle_service_error

logger = logging.getLogger("WeatherAPI")

router = APIRouter()


def clean_nan(obj):
    """Đệ quy thay thế NaN bằng None để JSON hợp lệ."""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


@router.get("/v1/chat", tags=["Weather Services"])
def chat(
    region: Optional[str] = Query(None, description="Tên địa danh"),
    lat: Optional[float] = Query(None, description="Vĩ độ"),
    lon: Optional[float] = Query(None, description="Kinh độ"),
    group_hours: bool = Query(False, description="Tùy chọn hiển thị theo nhóm giờ")
):
    """
    Tạo bản tin thời tiết theo hướng direct source.
    - Ưu tiên lat/lon nếu được cung cấp.
    - Nếu không có lat/lon, thử resolve theo tên từ state (regions/wards).
    """
    logger.info(f"/v1/chat region={region} lat={lat} lon={lon} group_hours={group_hours}")

    # Ưu tiên tọa độ, fallback sang tìm theo tên
    region_info = resolve_region(region=region, lat=lat, lon=lon)

    # Kiểm tra chặt chẽ lat/lon
    if (
        not region_info
        or "lat" not in region_info
        or "lon" not in region_info
        or region_info.get("lat") is None
        or region_info.get("lon") is None
    ):
        from services.notify import notify_api
        hint = "Cung cấp tên địa danh hợp lệ trong dữ liệu hoặc truyền lat/lon trực tiếp"
        result = notify_api("chat_request", "error", "Không tìm thấy địa danh hoặc tọa độ", hint)
        return {
            "status": result.get("status", "error"),
            "message": result.get("message", ""),
            "data": clean_nan({
                "ts": result.get("ts"),
                "action": result.get("action"),
                "hint": result.get("hint"),
                "fix_suggestion": result.get("fix_suggestion"),
            }),
        }

    try:
        # Gọi trực tiếp generate_bulletin với lat/lon
        bulletin_result = generate_bulletin(
            region_name=region_info.get("name") or (region or "Khu vực"),
            lat=region_info["lat"],
            lon=region_info["lon"],
        )

        if bulletin_result and bulletin_result.get("status") == "ok":
            return {
                "status": "ok",
                "message": "Bản tin thời tiết trực tiếp từ nguồn",
                "data": clean_nan({
                    "region": region_info,
                    "bulletin": bulletin_result.get("bulletin", {}),
                    "current": bulletin_result.get("current", {}),
                    "hourly": bulletin_result.get("hourly", []),
                    "daily": bulletin_result.get("daily", []),
                    "alerts": bulletin_result.get("alerts", []),
                    "source": bulletin_result.get("source", "open_meteo"),
                    "options": {"group_hours": group_hours},
                }),
            }
        else:
            # Thông điệp lỗi chi tiết hơn
            if bulletin_result is None:
                error_msg = "Không có dữ liệu từ nguồn (bulletin_result=None)"
            elif bulletin_result.get("status") == "error":
                error_msg = f"Bulletin lỗi dữ liệu: {bulletin_result.get('message', 'Không rõ nguyên nhân')}"
            else:
                error_msg = "Bulletin lỗi dữ liệu không xác định"

            handle_service_error("chat_route", "generate_bulletin", Exception(error_msg), alert_type="data")
            from services.notify import notify_api
            result = notify_api("chat_request", "error", error_msg, "Kiểm tra kết nối API hoặc logic generate_bulletin")
            return {
                "status": result.get("status", "error"),
                "message": result.get("message", ""),
                "data": clean_nan({
                    "ts": result.get("ts"),
                    "action": result.get("action"),
                    "hint": result.get("hint"),
                    "fix_suggestion": result.get("fix_suggestion"),
                }),
            }

    except Exception as e:
        logger.exception("Error in /v1/chat")
        handle_service_error("chat_route", "chat", e, alert_type="system")
        from services.notify import notify_api
        result = notify_api(
            "chat_request", "error",
            f"Lỗi hệ thống khi sinh bản tin: {e}",
            "Quản lý cần kiểm tra kết nối API và logic xử lý"
        )
        return {
            "status": result.get("status", "error"),
            "message": result.get("message", ""),
            "data": clean_nan({
                "ts": result.get("ts"),
                "action": result.get("action"),
                "hint": result.get("hint"),
                "fix_suggestion": result.get("fix_suggestion"),
            }),
        }


