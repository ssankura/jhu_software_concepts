"""
test_analysis_format.py

Tests related to analysis rendering and formatting requirements.

Covers:
-------
- Presence of "Answer:" labels in the rendered HTML
- Percentage formatting with exactly two decimal places

These tests satisfy Module 4 requirements under:

    @pytest.mark.analysis
"""

import re
import pytest
from bs4 import BeautifulSoup


# ============================================================================
# Regex for Percentage Validation
# ----------------------------------------------------------------------------
# Matches:
#   12.34%
#   39.28%
#   100.00%
#
# Requirements:
# - Must have exactly two decimal places
# - Must end with %
#
# Does NOT match:
#   12.3%
#   12%
#   12.345%
# ============================================================================

PCT_RE = re.compile(r"\d+\.\d{2}%")


# ============================================================================
# Test: "Answer:" Label Present
# ----------------------------------------------------------------------------
# Assignment Requirement:
#   "Analysis outputs are labeled and percentages are shown with two decimals"
#
# Every analysis card must include the literal label "Answer:".
# This ensures UI consistency and grading compliance.
# ============================================================================

@pytest.mark.analysis
def test_page_includes_answer_labels(client):
    resp = client.get("/analysis")

    # Page must load successfully
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    # Verify that at least one "Answer:" label exists
    # (Multiple expected in full page)
    assert "Answer:" in html


# ============================================================================
# Test: Percentages Render with Two Decimals
# ----------------------------------------------------------------------------
# Validates that percentages are formatted correctly (e.g., 39.28%).
#
# Backend responsibility:
#   _fmt_pct() in analysis.py ensures two-decimal formatting.
#
# This test ensures UI reflects that formatting rule.
# ============================================================================



@pytest.mark.analysis
def test_percentages_render_with_two_decimals(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Look for at least one percentage like 39.28%
    assert re.search(r"\d+\.\d{2}%", text), f"No two-decimal percent found in: {text}"

