"""
test_db_insert.py

Database-layer tests for Module 4.

Covers:
-------
- Insert behavior via POST /pull-data
- Required schema fields
- Idempotency (no duplicate rows on repeated pulls)
- Query-layer correctness (query_applicants_as_dicts)

Marked with @pytest.mark.db per assignment policy.
"""

import pytest
import psycopg


# ============================================================================
# Test: Insert on Pull
# ----------------------------------------------------------------------------
# Requirement:
#   After POST /pull-data:
#     - New rows must exist in PostgreSQL
#     - Required fields must be non-null
#
# Flow:
#   1. Verify table is empty
#   2. Trigger /pull-data
#   3. Verify rows were inserted
#   4. Validate required fields
# ============================================================================

@pytest.mark.db
def test_insert_on_pull_writes_rows(database_url, client_db):

    # --------------------------
    # Before: Table is empty
    # --------------------------
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            before = cur.fetchone()[0]

    assert before == 0

    # --------------------------
    # Action: Trigger pull
    # --------------------------
    resp = client_db.post("/pull-data")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    # --------------------------
    # After: Rows should exist
    # --------------------------
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            after = cur.fetchone()[0]

            cur.execute("SELECT url, status FROM applicants ORDER BY url ASC;")
            rows = cur.fetchall()

    # Must have inserted at least one row
    assert after >= 1

    # Required non-null field checks:
    # - url must exist (enforced by UNIQUE + NOT NULL policy)
    # - status must not be None (we insert it explicitly)
    assert all(r[0] for r in rows)              # url non-null
    assert all(r[1] is not None for r in rows)  # status not null


# ============================================================================
# Test: Idempotency (Duplicate Pulls)
# ----------------------------------------------------------------------------
# Requirement:
#   Duplicate pulls must NOT duplicate rows.
#
# Implementation:
#   - UNIQUE constraint on url
#   - ON CONFLICT DO NOTHING in loader
#
# This test ensures:
#   Two pulls → still only unique rows present.
# ============================================================================

@pytest.mark.db
def test_idempotency_duplicate_pulls_do_not_duplicate(database_url, client_db):

    # First pull
    r1 = client_db.post("/pull-data")
    assert r1.status_code == 200

    # Second pull (same deterministic fake scraper rows)
    r2 = client_db.post("/pull-data")
    assert r2.status_code == 200

    # Validate count remains equal to number of unique URLs
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            count = cur.fetchone()[0]

    # We inject exactly 2 unique URLs via fake scraper
    assert count == 2


# ============================================================================
# Test: Query Function Returns Expected Keys
# ----------------------------------------------------------------------------
# Requirement:
#   Query layer must return dictionary with required schema fields.
#
# Validates:
#   - query_applicants_as_dicts returns list of dicts
#   - Required keys are present
#   - Data path works end-to-end
# ============================================================================

@pytest.mark.db
def test_query_function_returns_expected_keys(client_db):

    # Ensure DB has data
    resp = client_db.post("/pull-data")
    assert resp.status_code == 200

    from query_data import query_applicants_as_dicts

    rows = query_applicants_as_dicts(limit=5)

    # Basic shape validation
    assert isinstance(rows, list)
    assert len(rows) >= 1

    required_keys = {
        "url", "term", "status", "us_or_international",
        "gpa", "gre", "gre_v", "gre_aw",
        "degree", "program",
        "llm_generated_program", "llm_generated_university"
    }

    # Ensure required fields are present in returned dict
    assert required_keys.issubset(rows[0].keys())
