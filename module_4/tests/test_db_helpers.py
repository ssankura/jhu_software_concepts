"""
test_db_config.py

Tests related to database configuration and environment handling.

Covers:
-------
- get_database_url() behavior when DATABASE_URL is missing

Ensures:
--------
- The application fails fast with a clear error
- No silent fallback behavior
- Proper environment validation

Marked with @pytest.mark.db per assignment policy.
"""

import os
import pytest

from app.db import get_database_url


# ============================================================================
# Test: DATABASE_URL Missing
# ----------------------------------------------------------------------------
# Requirement:
#   Application must rely on DATABASE_URL environment variable.
#
# This test ensures:
# - If DATABASE_URL is not set
# - get_database_url() raises ValueError
#
# Why monkeypatch?
#   We remove DATABASE_URL temporarily in a controlled way
#   without affecting the real system environment.
# ============================================================================

@pytest.mark.db
def test_get_database_url_raises_when_missing(monkeypatch):

    # Remove DATABASE_URL from environment for this test
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Function must raise ValueError if variable is missing
    with pytest.raises(ValueError):
        get_database_url()
