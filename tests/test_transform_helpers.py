import pytest
from etl.transform_helpers import (
    categorize_city, categorize_province,
    standardize_phone, infer_missing_province,
    standardize_species, standardize_unit
)


# ---------------------------
# Test: categorize_city
# ---------------------------
@pytest.mark.parametrize("value,expected", [
    ("city a", "City A"),
    ("City B", "City B"),
    ("random", "Unknown"),
])
def test_categorize_city(value, expected):
    assert categorize_city(value) == expected

# ---------------------------
# Test: categorize_province
# ---------------------------
@pytest.mark.parametrize("value,expected", [
    ("ontario", "ON"),
    ("BC", "BC"),
    (None, None),
    ("unknown", None),
])
def test_categorize_province(value, expected):
    assert categorize_province(value) == expected

# ---------------------------
# Test: standardize_phone
# ---------------------------
@pytest.mark.parametrize("value,expected", [
    ("(204) 555-1234", "204-555-1234"),
    ("12045551234", "204-555-1234"),
    ("204555123", None)
])
def test_standardize_phone(value, expected):
    assert standardize_phone(value) == expected

# ---------------------------
# Test: infer_missing_province
# ---------------------------
def test_infer_missing_province_known_area_code():
    row = {"province": "", "owner_phone": "204-555-1234"}
    assert infer_missing_province(row) == "MB"

def test_infer_missing_province_existing_value():
    row = {"province": "on", "owner_phone": "204-555-1234"}
    assert infer_missing_province(row) == "ON"

def test_infer_missing_province_unknown_code():
    row = {"province": "", "owner_phone": "999-123-4567"}
    assert infer_missing_province(row) == "Add area code to dict"

# ---------------------------
# Test: standardize_species
# ---------------------------
@pytest.mark.parametrize("val,expected", [
    ("Dog", "dog"),
    ("c", "cat"),
    ("unknown", None)
])
def test_standardize_species(val, expected):
    assert standardize_species(val) == expected

# ---------------------------
# Test: standardize_unit
# ---------------------------
@pytest.mark.parametrize("val,expected", [
    ("A", "A"),
    (" b ", "B"),
    ("12", None),
    ("", None)
])
def test_standardize_unit(val, expected):
    assert standardize_unit(val) == expected
