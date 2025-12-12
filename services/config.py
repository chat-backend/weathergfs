# services/config.py
import os
import json
import psutil
from dotenv import load_dotenv
import requests
from typing import Dict, Any

# 1. Load biến môi trường từ .env
load_dotenv()

# =========================
# 2. Các hằng số cấu hình chính
# =========================
# Vị trí mặc định (chỉ lấy current, hourly, daily)
OPENMETEO_ENABLED: bool = os.getenv("OPENMETEO_ENABLED", "true").lower() == "true"
DEFAULT_CURRENT: bool = os.getenv("DEFAULT_CURRENT", "true").lower() == "true"
DEFAULT_HOURS: int = int(os.getenv("DEFAULT_HOURS", "24"))
DEFAULT_DAYS: int = int(os.getenv("DEFAULT_DAYS", "10"))

# Logging & App
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
PORT: int = int(os.getenv("PORT", "8000"))
SERVICE_NAME: str = os.getenv("SERVICE_NAME", "weathervn")
ENV: str = os.getenv("ENV", "production")
TZ: str = os.getenv("TZ", "Asia/Ho_Chi_Minh")

# API Config
FORECAST_DAYS: int = int(os.getenv("FORECAST_DAYS", "10"))
API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "20"))
API_USER_AGENT: str = os.getenv("API_USER_AGENT", "WeatherVietnam/1.0 (+https://example.local)")
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
API_TZ: str = os.getenv("API_TZ", "Asia/Ho_Chi_Minh")

# Monitoring thresholds
CPU_THRESHOLD: float = float(os.getenv("CPU_THRESHOLD", "80.0"))
RAM_THRESHOLD: float = float(os.getenv("RAM_THRESHOLD", "80.0"))
DISK_THRESHOLD: float = float(os.getenv("DISK_THRESHOLD", "90.0"))

# =========================
# 3. Hàm tiện ích chung
# =========================
def to_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)

def format_size(bytes_val: int) -> str:
    if bytes_val < 1024**2:
        return f"{bytes_val / 1024:.2f} KB"
    elif bytes_val < 1024**3:
        return f"{bytes_val / (1024**2):.2f} MB"
    else:
        return f"{bytes_val / (1024**3):.2f} GB"

# =========================
# 4. Kiểm tra tài nguyên hệ thống
# =========================
def check_resources(cpu_threshold: float = CPU_THRESHOLD,
                    ram_threshold: float = RAM_THRESHOLD,
                    disk_threshold: float = DISK_THRESHOLD) -> Dict[str, Any]:
    cpu_usage = psutil.cpu_percent(interval=1)
    ram_usage = psutil.virtual_memory().percent
    disk_usage = psutil.disk_usage('/').percent

    alerts = []
    if cpu_usage > cpu_threshold:
        alerts.append({"type": "CPU", "usage": cpu_usage})
    if ram_usage > ram_threshold:
        alerts.append({"type": "RAM", "usage": ram_usage})
    if disk_usage > disk_threshold:
        alerts.append({"type": "Disk", "usage": disk_usage})

    return {"cpu": cpu_usage, "ram": ram_usage, "disk": disk_usage, "alerts": alerts}

# =========================
# 5. Kiểm tra kết nối mạng/API
# =========================
def check_api_connection(url: str = "https://api.open-meteo.com/v1/forecast") -> Dict[str, str]:
    try:
        headers = {"User-Agent": API_USER_AGENT, "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        if resp.status_code == 200:
            return {"status": "ok", "message": "Kết nối thành công."}
        else:
            return {"status": "error", "message": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =========================
# 6. Trả về config
# =========================
def get_config() -> Dict[str, Any]:
    return {
        "OPENMETEO_ENABLED": OPENMETEO_ENABLED,
        "DEFAULT_CURRENT": DEFAULT_CURRENT,
        "DEFAULT_HOURS": DEFAULT_HOURS,
        "DEFAULT_DAYS": DEFAULT_DAYS,
        "LOG_LEVEL": LOG_LEVEL,
        "PORT": PORT,
        "SERVICE_NAME": SERVICE_NAME,
        "ENV": ENV,
        "TZ": TZ,
        "API": {
            "FORECAST_DAYS": FORECAST_DAYS,
            "API_TIMEOUT": API_TIMEOUT,
            "API_USER_AGENT": API_USER_AGENT,
            "MAX_RETRIES": MAX_RETRIES,
            "API_TZ": API_TZ,
        },
        "THRESHOLDS": {
            "CPU_THRESHOLD": CPU_THRESHOLD,
            "RAM_THRESHOLD": RAM_THRESHOLD,
            "DISK_THRESHOLD": DISK_THRESHOLD,
        }
    }