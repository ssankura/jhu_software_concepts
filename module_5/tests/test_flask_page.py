"""
test_flask_page.py

Web-layer tests for Module 4.

Covers:
-------
- Application factory registers required routes
- GET /analysis renders successfully
- Required UI components are present
- Stable selectors exist for button testing

Marked with @pytest.mark.web per assignment policy.
"""

import pytest
from bs4 import BeautifulSoup

# ============================================================================
# Test: Required Routes Exist
# ----------------------------------------------------------------------------
# Requirement:
#   The Flask app must expose:
#     - "/"
#     - "/analysis"
#     - "/pull-data"
#     - "/update-analysis"
#
# This test ensures:
#   - Blueprint registration works
#   - create_app() properly wires routes
#   - No route was accidentally removed
# ============================================================================

@pytest.mark.web
def test_create_app_has_required_routes(app):

    # Extract all registered URL rules
    rules = {r.rule for r in app.url_map.iter_rules()}

    # Ensure required endpoints exist
    assert "/" in rules
    assert "/analysis" in rules
    assert "/pull-data" in rules
    assert "/update-analysis" in rules


# ============================================================================
# Test: GET /analysis Page Rendering
# ----------------------------------------------------------------------------
# Requirement:
#   - GET /analysis must return 200
#   - Page must contain:
#       - "Analysis" text
#       - At least one "Answer:" label
#       - Pull Data button (with stable test selector)
#       - Update Analysis button (with stable test selector)
#
# Why string matching is acceptable:
#   These are static UI elements guaranteed by template design.
#   For deeper structural checks, BeautifulSoup could be used,
#   but string presence satisfies assignment requirements.
# ============================================================================




@pytest.mark.web
def test_get_analysis_renders_required_components(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Required page text
    assert "Analysis" in text
    assert "Answer:" in text

    # Required buttons (stable selectors)
    pull_btn = soup.select_one('[data-testid="pull-data-btn"]')
    assert pull_btn is not None

    update_btn = soup.select_one('[data-testid="update-analysis-btn"]')
    assert update_btn is not None
