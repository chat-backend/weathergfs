# services/error_handler.py
import traceback
import logging
from typing import Optional
from services.notify import (
    notify_api,
    system_alert,
    data_alert,
    connection_alert,
    security_alert,
    config_alert,
)

# Logger riêng cho error handler
logger = logging.getLogger("WeatherGfs")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

def handle_service_error(level: str, message: str):
    print(f"[{level.upper()}] {message}")

def handle_service_error(
    service: str,
    context: str,
    e: Exception,
    alert_type: str = "system",
    extra_info: Optional[dict] = None,
) -> dict:
    """
    Hàm chuẩn xử lý lỗi cho tất cả service.

    Args:
        service (str): Tên service (vd: 'gfs_current', 'weather_task', 'gfs_manager').
        context (str): Ngữ cảnh lỗi (vd: 'fetch_current', 'extract_weather_data').
        e (Exception): Exception bắt được.
        alert_type (str): Loại alert ('system', 'data', 'connection', 'security', 'config').
        extra_info (dict, optional): Thông tin bổ sung để quản lý dễ phân tích.

    Returns:
        dict: Thông tin lỗi đã chuẩn hóa, gồm status, message, detail, alert.
    """

    # Tạo thông điệp lỗi chi tiết
    msg = f"[{service}] Lỗi tại {context}: {str(e)}"
    detail = traceback.format_exc()

    # Log lỗi vào hệ thống
    logger.error("❌ %s\nChi tiết:\n%s", msg, detail)

    # Luôn gửi notify_api để ghi nhận lỗi
    notify_api(service, "error", msg, hint=context)

    # Gửi alert phù hợp theo loại lỗi
    if alert_type == "system":
        alert = system_alert(msg, severity="error", context=extra_info)
    elif alert_type == "data":
        alert = data_alert(msg, context=extra_info)
    elif alert_type == "connection":
        alert = connection_alert(service, str(e))
    elif alert_type == "security":
        alert = security_alert(msg, context=extra_info)
    elif alert_type == "config":
        alert = config_alert(context, str(e))
    else:
        # Nếu loại alert không hợp lệ, fallback về system
        alert = system_alert(
            f"⚠️ UNKNOWN ALERT TYPE ({alert_type}): {msg}", severity="error", context=extra_info
        )

    return {
        "status": "error",
        "service": service,
        "context": context,
        "message": msg,
        "detail": detail,
        "alert": alert,
    }