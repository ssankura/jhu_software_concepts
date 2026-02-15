from decimal import Decimal
import pytest

from app.pages.analysis import _convert_decimal, _fmt_pct


@pytest.mark.analysis
def test_convert_decimal_converts_decimal_to_float():
    assert _convert_decimal(Decimal("12.34")) == 12.34
    assert _convert_decimal(5) == 5


@pytest.mark.analysis
def test_fmt_pct_formats_with_two_decimals_and_percent():
    assert _fmt_pct(12.345) == "12.35%"
    assert _fmt_pct("12.34") == "12.34%"
    assert _fmt_pct("12.34%") == "12.34%"
    assert _fmt_pct(None) == "0.00%"


@pytest.mark.analysis
def test_analysis_uses_injected_fetchers(app):
    # app fixture already injects fetch_one_fn/fetch_all_fn.
    # This test simply calls the view function to execute that path.
    client = app.test_client()
    resp = client.get("/analysis")
    assert resp.status_code == 200
