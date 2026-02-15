"""
test_integration_end_to_end.py

Full integration test for Module 4.

Covers:
-------
- Real PostgreSQL insert
- POST /pull-data
- POST /update-analysis
- GET /analysis render
- Percentage formatting presence
- Idempotency via UNIQUE(url)

Marked with @pytest.mark.integration per assignment policy.

This test exercises the entire stack:

    Flask Route
        ↓
    Dependency Injection
        ↓
    Real DB Loader (psycopg)
        ↓
    PostgreSQL
        ↓
    Real fetch_one / fetch_all
        ↓
    HTML Rendering
"""

import pytest
import psycopg


# ============================================================================
# End-to-End Flow
# ----------------------------------------------------------------------------
# Steps:
#   1. Pull → inserts rows into real database
#   2. Update-analysis → returns ok
#   3. Render /analysis → shows results
#   4. Pull again → verify uniqueness constraint prevents duplicates
# ============================================================================

@pytest.mark.integration
def test_end_to_end_pull_update_render(database_url, db_clean):
    """
    End-to-end validation of full workflow.

    Verifies:
    - pull inserts rows into real DB
    - update-analysis returns ok
    - analysis page renders correctly
    - percentage formatting exists
    - duplicate pulls do not duplicate rows
    """

    from app import create_app
    from app.db import fetch_one, fetch_all

    # ------------------------------------------------------------------------
    # Deterministic dataset for integration test
    # These rows mimic real scraper output
    # ------------------------------------------------------------------------
    rows = [
        {
            "url": "https://example.com/integration/1",
            "term": "Fall 2026",
            "status": "Accepted",
            "us_or_international": "International",
            "gpa": 3.9,
            "gre": 320,
            "gre_v": 165,
            "gre_aw": 4.0,
            "degree": "PhD",
            "program": "Computer Science",
            "llm_generated_program": "Computer Science",
            "llm_generated_university": "Stanford University",
        },
        {
            "url": "https://example.com/integration/2",
            "term": "Fall 2026",
            "status": "Rejected",
            "us_or_international": "American",
            "gpa": 3.7,
            "gre": 315,
            "gre_v": 160,
            "gre_aw": 3.5,
            "degree": "Masters",
            "program": "Computer Science",
            "llm_generated_program": "Computer Science",
            "llm_generated_university": "Johns Hopkins University",
        },
    ]

    # ------------------------------------------------------------------------
    # Fake scraper returns deterministic rows
    # ------------------------------------------------------------------------
    def fake_scraper():
        return list(rows)

    # ------------------------------------------------------------------------
    # Real DB loader for integration test
    # Uses ON CONFLICT DO NOTHING to enforce idempotency
    # ------------------------------------------------------------------------
    def db_loader(in_rows):
        insert_sql = """
        INSERT INTO applicants (
            url, term, status, us_or_international, gpa, gre, gre_v, gre_aw,
            degree, program, llm_generated_program, llm_generated_university
        )
        VALUES (
            %(url)s, %(term)s, %(status)s, %(us_or_international)s, %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s,
            %(degree)s, %(program)s, %(llm_generated_program)s, %(llm_generated_university)s
        )
        ON CONFLICT (url) DO NOTHING;
        """

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                for r in in_rows:
                    cur.execute(insert_sql, r)
            conn.commit()

        return len(in_rows)

    # No-op update-analysis (we only validate route behavior)
    def fake_update_analysis():
        return None

    # ------------------------------------------------------------------------
    # Create app wired to:
    # - Fake scraper
    # - Real DB loader
    # - Real DB fetch functions
    # ------------------------------------------------------------------------
    app = create_app(
        test_config={"TESTING": True},
        deps={
            "scraper_fn": fake_scraper,
            "loader_fn": db_loader,
            "update_analysis_fn": fake_update_analysis,
            "fetch_one_fn": fetch_one,
            "fetch_all_fn": fetch_all,
        },
    )

    client = app.test_client()

    # ------------------------------------------------------------------------
    # Step 1: Pull Data
    # ------------------------------------------------------------------------
    r1 = client.post("/pull-data")
    assert r1.status_code == 200
    assert r1.get_json()["ok"] is True

    # ------------------------------------------------------------------------
    # Step 2: Update Analysis
    # ------------------------------------------------------------------------
    r2 = client.post("/update-analysis")
    assert r2.status_code == 200
    assert r2.get_json()["ok"] is True

    # ------------------------------------------------------------------------
    # Step 3: Render Analysis Page
    # ------------------------------------------------------------------------
    r3 = client.get("/analysis")
    assert r3.status_code == 200

    html = r3.data.decode("utf-8")

    # Core UI requirements
    assert "Analysis" in html
    assert "Answer:" in html

    # Formatting requirement:
    # At least one percent must appear (two-decimal formatting tested elsewhere)
    assert "%" in html

    # ------------------------------------------------------------------------
    # Step 4: Idempotency Validation
    # Pull again with identical rows
    # ------------------------------------------------------------------------
    r4 = client.post("/pull-data")
    assert r4.status_code == 200

    # Count only integration test rows
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM applicants
                WHERE url LIKE 'https://example.com/integration/%';
            """)
            count = cur.fetchone()[0]

    # Should remain 2 due to UNIQUE(url)
    assert count == 2
