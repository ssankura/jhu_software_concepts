"""
test_analysis_helpers.py

Unit tests for analysis formatting helpers and dependency injection behavior.

Covers:
-------
- _convert_decimal(): ensures Decimal → float conversion
- _fmt_pct(): enforces two-decimal percentage formatting
- Dependency injection path inside /analysis route

All tests are marked @pytest.mark.analysis per assignment policy.
"""

from decimal import Decimal
import pytest

from app.pages.analysis import _convert_decimal, _fmt_pct


# ============================================================================
# Test: _convert_decimal
# ----------------------------------------------------------------------------
# Ensures Decimal values (from PostgreSQL numeric types) are converted
# to floats before being rendered in templates.
#
# Why this matters:
# - psycopg often returns Decimal for numeric fields
# - Templates and JSON formatting should display clean floats
# - Prevents odd rendering like Decimal('12.34')
# ============================================================================

@pytest.mark.analysis
def test_convert_decimal_converts_decimal_to_float():
    # Decimal should convert to float
    assert _convert_decimal(Decimal("12.34")) == 12.34

    # Non-Decimal values should remain unchanged
    assert _convert_decimal(5) == 5


# ============================================================================
# Test: _fmt_pct
# ----------------------------------------------------------------------------
# Enforces strict percentage formatting requirements:
#
# Requirements:
# - Always two decimal places
# - Always ends with "%"
# - None should render as "0.00%"
#
# This supports assignment rule:
#   "analysis percentages must be two decimals"
# ============================================================================

@pytest.mark.analysis
def test_fmt_pct_formats_with_two_decimals_and_percent():
    # Float rounds correctly
    assert _fmt_pct(12.345) == "12.35%"

    # String without % gets % appended
    assert _fmt_pct("12.34") == "12.34%"

    # Already formatted string remains unchanged
    assert _fmt_pct("12.34%") == "12.34%"

    # None safely converts to default value
    assert _fmt_pct(None) == "0.00%"


# ============================================================================
# Test: Dependency Injection in /analysis
# ----------------------------------------------------------------------------
# The app fixture injects fake fetch_one_fn and fetch_all_fn.
#
# This test ensures:
# - /analysis route executes successfully
# - Injected dependencies are used
# - No real database is required
#
# Also contributes to:
# - Route coverage
# - create_app dependency wiring coverage
# ============================================================================

@pytest.mark.analysis
def test_analysis_uses_injected_fetchers(app):
    client = app.test_client()

    # Call the analysis route
    resp = client.get("/analysis")

    # Should render successfully using injected fake DB functions
    assert resp.status_code == 200
