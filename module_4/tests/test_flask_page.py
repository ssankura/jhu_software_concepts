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

    # Request analysis page
    resp = client.get("/analysis")

    # Page must load successfully
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    # ------------------------------------------------------------------------
    # Required page text
    # ------------------------------------------------------------------------
    assert "Analysis" in html
    assert "Answer:" in html

    # ------------------------------------------------------------------------
    # Required stable selectors
    #
    # data-testid attributes are required by assignment
    # to ensure UI tests are stable and not brittle.
    # ------------------------------------------------------------------------
    assert 'data-testid="pull-data-btn"' in html
    assert 'data-testid="update-analysis-btn"' in html
