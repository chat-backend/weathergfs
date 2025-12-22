import pytest
from services import app_utils

def test_resolve_region_with_valid_province():
    """
    Kiểm tra khi nhập tên tỉnh/thành phố có trong PROVINCES.
    Ví dụ: 'Đà Nẵng' hoặc 'Da Nang' phải được nhận diện là province hoặc fallback osm.
    """
    result = app_utils.resolve_region(region="Đà Nẵng")
    assert result["source"] in ["province", "osm"]
    assert "Đà Nẵng" in result["name"] or "Da Nang" in result["name"]

def test_resolve_region_with_valid_ward():
    """
    Kiểm tra khi nhập tên xã/phường có trong WARDS.
    Ví dụ: 'Hòa Khánh' phải được nhận diện là ward.
    """
    result = app_utils.resolve_region(region="Hòa Khánh")
    assert result["source"] in ["ward", "osm"]
    assert "Hòa Khánh" in result["name"]

def test_resolve_region_with_invalid_region():
    """
    Kiểm tra khi nhập địa danh không tồn tại.
    Ví dụ: 'unknownplace' phải trả về source = not_found hoặc fallback osm.
    """
    result = app_utils.resolve_region(region="unknownplace")
    assert result["source"] in ["not_found", "osm"]
    # Nếu not_found thì lat/lon phải None
    if result["source"] == "not_found":
        assert result["lat"] is None and result["lon"] is None

def test_resolve_region_with_direct_coordinates():
    """
    Kiểm tra khi truyền trực tiếp lat/lon hợp lệ.
    Ví dụ: lat=16.0, lon=108.0 phải trả về source = direct.
    """
    result = app_utils.resolve_region(region="TestRegion", lat=16.0, lon=108.0)
    assert result["source"] == "direct"
    assert result["lat"] == 16.0
    assert result["lon"] == 108.0
    assert result["name"] == "TestRegion"

def test_resolve_region_with_empty_input():
    """
    Kiểm tra khi không truyền region và không có lat/lon.
    Phải trả về source = empty.
    """
    result = app_utils.resolve_region()
    assert result["source"] == "empty"
    assert result["lat"] is None
    assert result["lon"] is None
    assert result["name"] == "Unknown region"