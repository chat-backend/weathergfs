# services/open_meteo/report.py
from typing import Dict, List

def _format_current(current: Dict) -> List[str]:
    """Định dạng phần thời tiết hiện tại."""
    lines = []
    if current and not current.get("is_empty", True):
        sample = current.get("sample") or {}
        temp = sample.get("temp_c")
        desc = sample.get("weather_desc") or "không rõ"
        humidity = sample.get("humidity_pct")
        wind = sample.get("wind_speed_ms")
        wind_dir = sample.get("wind_direction")
        uv = sample.get("uv_index")

        lines.append("⏰ Thời tiết hiện tại:")
        lines.append(f"- Nhiệt độ: {round(temp)}°C, trời {desc}" if temp is not None else f"- Nhiệt độ: chưa có dữ liệu, trời {desc}")
        lines.append(f"- Độ ẩm: {humidity}%" if humidity is not None else "- Độ ẩm: chưa có dữ liệu")
        if wind is not None:
            if wind_dir is not None:
                lines.append(f"- Gió: {wind} m/s, hướng {wind_dir}°")
            else:
                lines.append(f"- Gió: {wind} m/s")
        else:
            lines.append("- Gió: chưa có dữ liệu")
        lines.append(f"- Chỉ số UV: {uv}" if uv is not None else "- Chỉ số UV: chưa có dữ liệu")
    return lines

def _format_hourly(hourly: Dict) -> List[str]:
    """Định dạng phần dự báo 24h tới."""
    lines = []
    if hourly and not hourly.get("is_empty", True):
        stats = hourly.get("stats") or {}
        lines.append("\n🌦️ Dự báo 24 giờ tới:")
        if stats.get("temp_min") is not None and stats.get("temp_max") is not None:
            lines.append(f"- Nhiệt độ dao động {stats['temp_min']}–{stats['temp_max']}°C")
        if stats.get("rain_total_mm") is not None:
            lines.append(f"- Tổng lượng mưa dự kiến: {stats['rain_total_mm']} mm")
        if stats.get("rain_hours") is not None:
            lines.append(f"- Số giờ có mưa: {stats['rain_hours']} giờ")
        if stats.get("uv_max") is not None:
            lines.append(f"- Chỉ số UV cao nhất: {stats['uv_max']}")
    return lines

def _format_daily(daily: Dict) -> List[str]:
    """Định dạng phần xu hướng 10 ngày tới."""
    lines = []
    if daily and not daily.get("is_empty", True):
        stats = daily.get("stats") or {}
        lines.append("\n📅 Xu hướng 10 ngày tới:")
        if stats.get("temp_avg") is not None:
            lines.append(f"- Nhiệt độ trung bình: {stats['temp_avg']}°C")
        if stats.get("temp_min") is not None and stats.get("temp_max") is not None:
            lines.append(f"- Nhiệt độ thấp nhất: {stats['temp_min']}°C, cao nhất: {stats['temp_max']}°C")
        if stats.get("rain_total_mm") is not None:
            lines.append(f"- Tổng lượng mưa: {stats['rain_total_mm']} mm")
        if stats.get("rain_hours") is not None:
            lines.append(f"- Số giờ có mưa: {stats['rain_hours']} giờ")
        if stats.get("uv_max") is not None:
            lines.append(f"- Chỉ số UV cao nhất: {stats['uv_max']}")
        sample = daily.get("sample") or {}
        if sample.get("sunrise") and sample.get("sunset"):
            lines.append(f"- Mặt trời mọc: {sample['sunrise']}, lặn: {sample['sunset']}")
    return lines

def generate_weather_report(summary: Dict) -> str:
    """
    Sinh bản tin thời tiết thân thiện từ summary của get_cache_summary.
    """
    if not summary:
        return "⚠️ Không có dữ liệu thời tiết, vui lòng thử lại sau."

    lines: List[str] = []
    lines.extend(_format_current(summary.get("current", {})))
    lines.extend(_format_hourly(summary.get("hourly", {})))
    lines.extend(_format_daily(summary.get("daily", {})))

    return "\n".join(lines)