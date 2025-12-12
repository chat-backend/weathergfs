# services/notify.py
import logging
import json
from typing import List, Dict, Union
from datetime import datetime, timezone
from fastapi import APIRouter

from services.config import (
    check_resources,
    check_api_connection,
    CPU_THRESHOLD,
    RAM_THRESHOLD,
    DISK_THRESHOLD,
)

# =========================
# Router để tích hợp vào FastAPI
# =========================
router = APIRouter()

# =========================
# Cấu hình logger riêng cho notify
# =========================
logger = logging.getLogger("notify")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def _current_ts() -> str:
    """Trả về timestamp chuẩn UTC với format rõ ràng."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# =========================
# 1. Notify cho API endpoints
# =========================
def notify_api(action: str, status: str, detail: str = "", hint: str = "") -> Dict:
    ts = _current_ts()
    if status == "ok":
        logger.info(f"✅ {action} thành công. {detail}")
    else:
        logger.error(f"❌ {action} thất bại. {detail} | Hướng dẫn: {hint}")

    return {
        "ts": ts,
        "status": status,
        "action": action,
        "message": detail,
        "hint": hint,
        "fix_suggestion": (
            "Xem lại tham số đầu vào, kiểm tra cache, hoặc khởi động lại service."
            if status == "error"
            else ""
        ),
    }

# =========================
# 2. Cảnh báo hệ thống tổng hợp
# =========================
def system_alert(message: str, severity: str = "error", context: Union[Dict, None] = None) -> Dict:
    ts = _current_ts()
    log_map = {
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
        "critical": logger.critical,
    }
    log_func = log_map.get(severity, logger.error)
    log_func(f"[{severity.upper()}] {message}")

    guidance = {
        "info": "Theo dõi thêm, không cần hành động ngay.",
        "warning": "Kiểm tra cấu hình hoặc dữ liệu liên quan.",
        "error": "Xem lại log chi tiết, thử khởi động lại service.",
        "critical": "Liên hệ quản trị hệ thống ngay lập tức.",
    }
    fix_suggestion = {
        "info": "Không cần chỉnh sửa, chỉ cần giám sát.",
        "warning": "Xem lại file cấu hình, dữ liệu đầu vào, hoặc kết nối mạng.",
        "error": "Kiểm tra log, khởi động lại service, hoặc kiểm tra cache.",
        "critical": "Dừng hệ thống, liên hệ quản trị viên để xử lý khẩn cấp.",
    }

    return {
        "status": "system_alert",
        "alert": {
            "ts": ts,
            "type": "system",
            "severity": severity,
            "message": message,
            "context": context or {},
            "guidance": guidance.get(severity, "Theo dõi thêm."),
            "fix_suggestion": fix_suggestion.get(severity, "Chưa có hướng dẫn cụ thể."),
        },
    }

# =========================
# 3. Cảnh báo chuyên biệt
# =========================
def startup_alert(service_name: str) -> Dict:
    return system_alert(f"Service '{service_name}' đã khởi động.", severity="info")

def shutdown_alert(service_name: str) -> Dict:
    return system_alert(f"Service '{service_name}' đã dừng hoạt động.", severity="info")

def connection_alert(service: str, error: str) -> Dict:
    return system_alert(
        f"Không kết nối được tới {service}: {error}",
        severity="error",
        context={"service": service, "error": error},
    )

def data_alert(issue: str, context: Dict = None) -> Dict:
    return system_alert(
        f"Dữ liệu bất thường: {issue}",
        severity="warning",
        context=context or {},
    )

def performance_alert(duration: float, threshold: float = 2.0) -> Dict:
    if duration > threshold:
        return system_alert(
            f"Thời gian phản hồi {duration:.2f}s vượt ngưỡng {threshold:.2f}s",
            severity="warning",
            context={"duration": duration, "threshold": threshold},
        )
    return {"status": "ok"}

def security_alert(issue: str, context: Dict = None) -> Dict:
    return system_alert(
        f"Vấn đề bảo mật: {issue}",
        severity="critical",
        context=context or {},
    )

def config_alert(param: str, error: str) -> Dict:
    return system_alert(
        f"Lỗi cấu hình {param}: {error}",
        severity="error",
        context={"param": param, "error": error},
    )

def resource_alert(resource: str, usage: float, threshold: float) -> Dict:
    severity = "warning" if usage > threshold else "info"
    return system_alert(
        f"Tài nguyên {resource} đang sử dụng {usage:.2f}% (ngưỡng {threshold:.2f}%)",
        severity=severity,
        context={"resource": resource, "usage": usage, "threshold": threshold},
    )

# =========================
# 4. Endpoint cảnh báo tổng hợp
# =========================
@router.get("/v1/alerts/all", tags=["Notify"])
def alerts_all():
    """Tổng hợp cảnh báo hệ thống: CPU/RAM/Disk, API."""
    resources = check_resources()
    api_status = check_api_connection()

    alerts = []

    # CPU/RAM/Disk
    for res in ["cpu", "ram", "disk"]:
        usage = resources.get(res)
        threshold = {"cpu": CPU_THRESHOLD, "ram": RAM_THRESHOLD, "disk": DISK_THRESHOLD}[res]
        alerts.append(resource_alert(res.upper(), usage, threshold))

    # API
    alerts.append({
        "type": "api",
        "status": api_status.get("status"),
        "message": api_status.get("message")
    })

    return {
        "status": "ok",
        "detail": "Tổng hợp cảnh báo hệ thống",
        "alerts": alerts
    }

@router.get("/v1/alerts/resources", tags=["Notify"])
def alerts_resources():
    """Báo cáo chi tiết tình trạng CPU/RAM/Disk."""
    resources = check_resources()
    alerts = []

    # CPU/RAM/Disk chi tiết
    for res in ["cpu", "ram", "disk"]:
        usage = resources.get(res)
        threshold = {"cpu": CPU_THRESHOLD, "ram": RAM_THRESHOLD, "disk": DISK_THRESHOLD}[res]
        alerts.append(resource_alert(res.upper(), usage, threshold))

    return {
        "status": "ok" if not resources["alerts"] else "warning",
        "detail": "Báo cáo chi tiết CPU/RAM/Disk",
        "resources": {
            "cpu_usage_pct": resources["cpu"],
            "ram_usage_pct": resources["ram"],
            "disk_usage_pct": resources["disk"],
        },
        "alerts": alerts,
        "ts": _current_ts()
    }