import pytest
from services import bulletin

def test_generate_bulletin_with_region():
    """
    Kiểm tra hàm generate_bulletin với một địa danh cụ thể.
    Ví dụ: 'xã lãnh ngọc' (lat/lon đã được ánh xạ).
    """
    region_name = "xã lãnh ngọc"
    lat = 15.422626
    lon = 108.260143

    result = bulletin.generate_bulletin(region_name, lat, lon)

    print("\n=== Kết quả generate_bulletin cho", region_name, "===")
    print("Status:", result.get("status"))
    print("Source:", result.get("source"))
    print("Summary:", result.get("bulletin", {}).get("summary"))
    print("Text:\n", result.get("bulletin", {}).get("text"))

    print("\n--- Current ---")
    for k, v in result.get("current", {}).items():
        print(f"  {k}: {v}")

    print("\n--- Hourly ---")
    hourly = result.get("hourly", [])
    if hourly:
        print(f"  Có {len(hourly)} bản ghi hourly")
        print("  Ví dụ:", hourly[0])
    else:
        print("  ⚠️ Không có dữ liệu hourly")

    print("\n--- Daily ---")
    daily = result.get("daily", [])
    if daily:
        print(f"  Có {len(daily)} bản ghi daily")
        print("  Ví dụ:", daily[0])
    else:
        print("  ⚠️ Không có dữ liệu daily")

    print("\n--- Alerts ---")
    for alert in result.get("alerts", []):
        print("  -", alert)

    # Kiểm tra cơ bản
    assert result["status"] in ["ok", "error"]