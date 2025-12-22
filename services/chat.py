# services/chat.py
import logging
import math
from typing import Optional
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.app_utils import resolve_region
from services.bulletin import generate_bulletin
from services.error_handler import handle_service_error

logger = logging.getLogger("WeatherAPI")

router = APIRouter()


import datetime
try:
    import pandas as pd
except ImportError:
    pd = None

def clean_nan(obj):
    """Đệ quy thay thế NaN bằng None và convert object sang JSON hợp lệ."""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    elif pd is not None and isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


@router.get("/v1/chat", tags=["Weather Services"])
def chat(
    region: Optional[str] = Query(None, description="Tên địa danh"),
    lat: Optional[float] = Query(None, description="Vĩ độ"),
    lon: Optional[float] = Query(None, description="Kinh độ"),
    group_hours: bool = Query(False, description="Tùy chọn hiển thị theo nhóm giờ")
):
    try:
        logger.info(f"/v1/chat region={region} lat={lat} lon={lon} group_hours={group_hours}")

        # 1) Resolve region safely
        try:
            region_info = resolve_region(region=region, lat=lat, lon=lon)
        except Exception as re:
            logger.exception("[chat] resolve_region raised")
            handle_service_error("chat_route", "resolve_region", re, alert_type="system")
            return JSONResponse(
                status_code=200,
                content={
                    "status": "error",
                    "message": f"Không thể xử lý địa danh: {re}",
                    "data": clean_nan({"hint": "Thử nhập tên khác hoặc truyền lat/lon trực tiếp"}),
                },
            )

        # 2) Invalid coordinates → error 200
        if not region_info or region_info.get("lat") is None or region_info.get("lon") is None:
            hint = "Cung cấp tên địa danh hợp lệ trong dữ liệu hoặc truyền lat/lon trực tiếp"
            message = "Không tìm thấy địa danh hoặc tọa độ"
            data = {"hint": hint}

            try:
                from services.notify import notify_api
                result = notify_api("chat_request", "error", message, hint)
                if isinstance(result, dict):
                    message = result.get("message", message)
                    data = {
                        "ts": result.get("ts"),
                        "action": result.get("action"),
                        "hint": result.get("hint", hint),
                        "fix_suggestion": result.get("fix_suggestion"),
                    }
            except Exception as ne:
                logger.warning(f"[chat] notify_api error (invalid region): {ne}")
                data = {"hint": hint}

            return JSONResponse(
                status_code=200,
                content={"status": "error", "message": message, "data": clean_nan(data)},
            )

        # 3) Generate bulletin safely
        try:
            bulletin_result = generate_bulletin(
                region_name=region_info.get("name") or (region or "Khu vực"),
                lat=region_info["lat"],
                lon=region_info["lon"],
            )
        except Exception as ge:
            logger.exception("[chat] generate_bulletin raised")
            handle_service_error("chat_route", "generate_bulletin", ge, alert_type="system")
            msg = f"Lỗi hệ thống khi sinh bản tin: {ge}"
            data = {"hint": "Quản lý cần kiểm tra kết nối API và logic xử lý"}
            try:
                from services.notify import notify_api
                result = notify_api("chat_request", "error", msg, data["hint"])
                if isinstance(result, dict):
                    msg = result.get("message", msg)
                    data = {
                        "ts": result.get("ts"),
                        "action": result.get("action"),
                        "hint": result.get("hint", data["hint"]),
                        "fix_suggestion": result.get("fix_suggestion"),
                    }
            except Exception as ne:
                logger.warning(f"[chat] notify_api error (system gen): {ne}")
                data = {"hint": "Quản lý cần kiểm tra kết nối API và logic xử lý"}

            return JSONResponse(status_code=200, content={"status": "error", "message": msg, "data": clean_nan(data)})

        # 4) Success path
        if bulletin_result and bulletin_result.get("status") == "ok":
            return JSONResponse(
                status_code=200,
                content={
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
                },
            )

        # 5) Data error from source → 200
        error_msg = "Không có dữ liệu từ nguồn" if bulletin_result is None else \
                    bulletin_result.get("message", "Bulletin lỗi dữ liệu không xác định")
        handle_service_error("chat_route", "generate_bulletin", Exception(error_msg), alert_type="data")

        message = error_msg
        data = {"hint": "Kiểm tra kết nối API hoặc logic generate_bulletin"}
        try:
            from services.notify import notify_api
            result = notify_api("chat_request", "error", error_msg, data["hint"])
            if isinstance(result, dict):
                message = result.get("message", error_msg)
                data = {
                    "ts": result.get("ts"),
                    "action": result.get("action"),
                    "hint": result.get("hint", data["hint"]),
                    "fix_suggestion": result.get("fix_suggestion"),
                }
        except Exception as ne:
            logger.warning(f"[chat] notify_api error (bulletin): {ne}")
            data = {"hint": "Kiểm tra kết nối API hoặc logic generate_bulletin"}

        return JSONResponse(status_code=200, content={"status": "error", "message": message, "data": clean_nan(data)})

    except Exception as e:
        # Outer guard — bất cứ lỗi nào chưa lường trước cũng trả 200
        logger.exception("[chat] Outer exception")
        handle_service_error("chat_route", "chat_outer", e, alert_type="system")
        safe_message = f"Lỗi hệ thống không xác định: {e}"
        safe_data = {"hint": "Vui lòng thử lại hoặc cung cấp lat/lon trực tiếp"}
        try:
            from services.notify import notify_api
            result = notify_api("chat_request", "error", safe_message, safe_data["hint"])
            if isinstance(result, dict):
                safe_message = result.get("message", safe_message)
                safe_data = {
                    "ts": result.get("ts"),
                    "action": result.get("action"),
                    "hint": result.get("hint", safe_data.get("hint")),
                    "fix_suggestion": result.get("fix_suggestion"),
                }
        except Exception as ne:
            logger.warning(f"[chat] notify_api error (outer): {ne}")
            safe_data = {"hint": "Vui lòng thử lại hoặc cung cấp lat/lon trực tiếp"}

        return JSONResponse(status_code=200, content={"status": "error", "message": safe_message, "data": clean_nan(safe_data)})