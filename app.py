# app.py
import os
import json
import math
import logging
from pathlib import Path
from datetime import datetime, timezone
from starlette.requests import Request
from fastapi import FastAPI, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from dotenv import load_dotenv

from services.state import state
from configs.locations import LOCATIONS
from services.weather_services import RegionIndex, WeatherService
from services.app_utils import resolve_region, fetch_weather_data, build_weather_response
from services.open_meteo.open_meteo import fetch_forecast, read_cache, get_cache_summary
from services.error_handler import handle_service_error
from services.notify import router as notify_router
from services.notify import notify_api
from services.config import get_config, check_resources, check_api_connection  
from services.chat import router as chat_router

# ==============================
# Logging setup
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("WeatherAPI")

# ==============================
# Load environment variables
# ==============================
load_dotenv()

# ==============================
# FastAPI app initialization
# ==============================
app = FastAPI(
    title="Weather Vietnam",
    description="Weather data aggregation service for Vietnam regions (Open-Meteo API)",
    version="1.0.0",
)

# ==============================
# Middleware: Wrap response & sanitize NaN
# ==============================
def clean_nan(obj):
    """Đệ quy thay thế NaN bằng None để JSON hợp lệ."""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

class ResponseWrapperMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            if isinstance(response, JSONResponse):
                try:
                    body = json.loads(response.body.decode())
                except Exception:
                    return response
                body = clean_nan(body)
                if isinstance(body, dict) and {"status", "message", "data"} <= body.keys():
                    return JSONResponse(content=body, status_code=response.status_code)
                return JSONResponse(
                    content={"status": "ok", "message": "Thành công", "data": body},
                    status_code=response.status_code
                )
            return response
        except Exception as e:
            return JSONResponse(
                content={"status": "error", "message": str(e), "data": {}},
                status_code=500
            )

app.add_middleware(ResponseWrapperMiddleware)

# ==============================
# CORS setup
# ==============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# Include Routers
# ==============================
app.include_router(notify_router, tags=["Notify"])
app.include_router(chat_router, tags=["Weather Services"])

# ==============================
# Region data initialization (using LOCATIONS only)
# ==============================
from configs.locations import LOCATIONS

# Đồng bộ state với LOCATIONS để các service khác có thể truy cập
state["locations"] = LOCATIONS

logger.info(f"✅ Loaded {len(LOCATIONS)} locations (34 provinces + 94 wards of Đà Nẵng)")

# ==============================
# Startup & Shutdown events
# ==============================
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Application startup: hệ thống khởi động theo hướng gọi trực tiếp Open-Meteo...")
    try:
        # Kiểm tra cấu hình và kết nối API
        check_resources()
        check_api_connection()
        logger.info("✅ Kiểm tra tài nguyên và kết nối API thành công")
    except Exception as e:
        logger.error("❌ Lỗi khi kiểm tra tài nguyên/kết nối API: %s", e)

# ==============================
# Router & Endpoints (Direct Source)
# ==============================
router = APIRouter()

@router.get("/v1/ping", tags=["Weather Services"])
def ping():
    from services.notify import notify_api
    result = notify_api("ping", "ok", "Weather API is alive", "")
    return {
        "status": result.get("status", "ok"),
        "message": result.get("message", ""),
        "data": {
            "ts": result.get("ts"),
            "action": result.get("action"),
            "hint": result.get("hint"),
            "fix_suggestion": result.get("fix_suggestion"),
        },
    }

@router.get("/v1/weather", tags=["Weather Services"])
def get_weather(
    region: str = Query(None, description="Tên địa danh"),
    lat: float = Query(None, description="Vĩ độ"),
    lon: float = Query(None, description="Kinh độ")
):
    """
    Lấy dữ liệu thời tiết trực tiếp từ Open-Meteo theo region hoặc lat/lon.
    """
    logger.info(f"/v1/weather region={region} lat={lat} lon={lon}")
    region_info = resolve_region(region=region, lat=lat, lon=lon)

    # Nếu không tìm thấy region hoặc tọa độ
    if (not region_info or
        region_info.get("lat") is None or
        region_info.get("lon") is None):
        result = notify_api(
            "weather_request", "error",
            "Không tìm thấy địa danh hoặc tọa độ",
            "Cung cấp region hợp lệ hoặc truyền lat/lon trực tiếp"
        )
        return {
            "status": result.get("status", "error"),
            "message": result.get("message", ""),
            "data": {
                "ts": result.get("ts"),
                "action": result.get("action"),
                "hint": result.get("hint"),
                "fix_suggestion": result.get("fix_suggestion"),
            },
        }

    try:
        # Lấy dữ liệu thời tiết từ Open-Meteo
        data = fetch_weather_data(region_info["lat"], region_info["lon"])
        return {
            "status": "ok",
            "message": "Dữ liệu thời tiết đã được lấy thành công",
            "data": build_weather_response(region_info, data)
        }
    except Exception as e:
        logger.exception("Error in /v1/weather")
        # Sử dụng handle_service_error để chuẩn hóa lỗi
        error_info = handle_service_error(
            service="weather_service",
            context="get_weather",
            e=e,
            alert_type="connection",   # lỗi kết nối API
            extra_info={"region": region, "lat": lat, "lon": lon}
        )
        return JSONResponse(content=error_info, status_code=500)

@router.get("/health", tags=["Weather Services"])
def health():
    """
    Kiểm tra tình trạng hệ thống: tài nguyên và kết nối API.
    """
    try:
        resources = check_resources()
        api_status = check_api_connection()
        return {
            "status": "ok" if not resources["alerts"] else "warning",
            "message": "Báo cáo tình trạng hệ thống",
            "data": {
                "config": get_config(),
                "resources": resources,
                "checks": {"api_connection": api_status},
                "system_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                "app_version": "1.0.0",
            }
        }
    except Exception as e:
        handle_service_error("health_route", "health", e, alert_type="system")
        from services.notify import notify_api
        result = notify_api("health_check", "error", str(e), "Kiểm tra cấu hình hệ thống")
        return {
            "status": result.get("status", "error"),
            "message": result.get("message", ""),
            "data": {
                "ts": result.get("ts"),
                "action": result.get("action"),
                "hint": result.get("hint"),
                "fix_suggestion": result.get("fix_suggestion"),
            },
        }

@router.get("/v1/weather_summary", tags=["Weather Services"])
def get_weather_summary(
    lat: float = Query(..., description="Vĩ độ"),
    lon: float = Query(..., description="Kinh độ")
):
    """
    Trả về summary dữ liệu thời tiết trực tiếp cho một địa điểm (direct source mode).
    """
    try:
        # Lấy dữ liệu trực tiếp từ Open-Meteo
        data = fetch_weather_data(lat, lon)

        # Trích xuất dữ liệu từ current_weather
        current = data.get("current_weather", {}) if data else {}

        summary = {
            "location": {"lat": lat, "lon": lon},
            "current": {
                "temperature": current.get("temperature"),
                "windspeed": current.get("windspeed"),
                "winddirection": current.get("winddirection"),
                "weathercode": current.get("weathercode"),
                "time": current.get("time"),
            },
            "meta": {
                "source": "Open-Meteo API",
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            }
        }

        return {
            "status": "ok",
            "message": "Summary dữ liệu thời tiết",
            "data": summary
        }
    except Exception as e:
        logger.exception("Error in /v1/weather_summary")
        error_info = handle_service_error(
            service="weather_service",
            context="get_weather_summary",
            e=e,
            alert_type="connection",
            extra_info={"lat": lat, "lon": lon}
        )
        return JSONResponse(content=error_info, status_code=500)


@router.get("/version", tags=["System"])
def version_info():
    """
    Trả về thông tin version, build time, và cấu hình hệ thống.
    """
    try:
        config = get_config()
        return {
            "status": "ok",
            "message": "Thông tin phiên bản và cấu hình hệ thống",
            "data": {
                "app_version": "1.0.0",
                "build_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                "config": config,
            }
        }
    except Exception as e:
        error_info = handle_service_error(
            service="system_route",
            context="version_info",
            e=e,
            alert_type="system"
        )
        return JSONResponse(content=error_info, status_code=500)


@app.get("/", tags=["Root"])
def root():
    """
    Endpoint gốc: xác nhận API đang chạy ở chế độ direct source.
    """
    return {
        "status": "ok",
        "message": "Weather Service API đang chạy (direct source mode)",
        "data": {
            "system_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "app_version": "1.0.0",
        }
    }

# ==============================
# Gắn router vào app
# ==============================
app.include_router(router)
# chat_router đã include ở trên với tags=["Weather Services"], không lặp lại ở đây