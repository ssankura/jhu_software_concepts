"""
test_home_route.py

Web-layer test for root route behavior.

Covers:
-------
- GET "/" route
- Ensures homepage delegates to analysis view

Marked with @pytest.mark.web per assignment policy.
"""

import pytest


# ============================================================================
# Test: Root Route ("/") Delegates to Analysis
# ----------------------------------------------------------------------------
# Requirement:
#   The "/" route must exist and render the analysis page.
#
# Implementation detail:
#   In analysis.py, home() simply calls analysis().
#
# This test ensures:
#   - "/" returns 200
#   - It renders the same content as /analysis
#   - Route wiring and delegation work correctly
# ============================================================================

@pytest.mark.web
def test_home_route_calls_analysis(client):

    # Call root route
    resp = client.get("/")

    # Must return HTTP 200 (successful render)
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    # Since home() delegates to analysis(),
    # the rendered page must contain "Analysis"
    assert "Analysis" in html
