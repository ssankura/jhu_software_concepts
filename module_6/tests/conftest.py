"""
conftest.py

Pytest configuration and shared fixtures for Module 4 test suite.

Responsibilities:
-----------------
- Make src/ importable (so "app", "pull_data", etc. resolve correctly)
- Provide fake dependency injection for web/unit tests
- Provide real PostgreSQL-backed fixtures for db/integration tests
- Ensure database isolation between tests
- Enforce uniqueness policy (url UNIQUE)

Test Architecture:
------------------
We separate tests into two layers:

1) Unit/Web tests:
   - Use fake scraper, fake loader, fake query functions
   - Fast, deterministic, no DB required

2) DB/Integration tests:
   - Use real PostgreSQL connection
   - Insert deterministic rows
   - Validate schema, idempotency, uniqueness
"""

import sys
from pathlib import Path
import os
import psycopg
import pytest


# ============================================================================
# Ensure src/ is importable
# ----------------------------------------------------------------------------
# This allows imports like:
#     from app import create_app
#
# Without modifying PYTHONPATH manually.
# Required for GitHub Actions and local pytest runs.
# ============================================================================

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from app import create_app


# ============================================================================
# Fake Query Functions (Unit/Web tests)
# ----------------------------------------------------------------------------
# These simulate database responses so web tests do not require PostgreSQL.
# Keeps tests fast and deterministic.
# ============================================================================

def fake_fetch_one(sql: str):
    """
    Simulate fetch_one behavior.

    Logic:
    - COUNT queries return 1
    - Percent/ROUND queries return 12.34
    - Otherwise return 3.9 (used for averages)
    """
    if "COUNT" in sql:
        return 1

    if "100.0" in sql or "ROUND" in sql:
        return 12.34

    return 3.9


def fake_fetch_all(sql: str):
    """
    Simulate fetch_all behavior.

    Supports:
    - Q3 average metrics (returns one row with 4 values)
    - Top programs table (returns two rows)
    """
    if "AVG" in sql and "FROM applicants" in sql:
        return [(3.9, 320, 165, 4.0)]

    return [("Computer Science", 10), ("Data Science", 7)]


# ============================================================================
# Fake Rows Fixture
# ----------------------------------------------------------------------------
# Provides deterministic rows for pull-data button tests.
# Structure only needs to minimally satisfy loader contract.
# ============================================================================

@pytest.fixture()
def fake_rows():
    return [
        {"url": "https://example.com/r/1", "term": "Fall 2026", "status": "Accepted"},
        {"url": "https://example.com/r/2", "term": "Fall 2026", "status": "Rejected"},
    ]


# ============================================================================
# Flask App (Unit/Web version)
# ----------------------------------------------------------------------------
# Uses dependency injection to replace:
# - scraper
# - loader
# - update_analysis
# - DB query functions
#
# No real database is touched.
# ============================================================================

@pytest.fixture()
def app(fake_rows):

    def fake_scraper():
        return list(fake_rows)

    def fake_loader(rows):
        # Pretend all rows were inserted
        return len(rows)

    def fake_update_analysis():
        return None

    app = create_app(
        test_config={"TESTING": True},
        deps={
            "scraper_fn": fake_scraper,
            "loader_fn": fake_loader,
            "update_analysis_fn": fake_update_analysis,
            "fetch_one_fn": fake_fetch_one,
            "fetch_all_fn": fake_fetch_all
        },
    )
    return app


@pytest.fixture()
def client(app):
    """
    Standard Flask test client (unit/web tests).
    """
    return app.test_client()


# ============================================================================
# Flask App (Real DB version)
# ----------------------------------------------------------------------------
# Used for:
# - db tests
# - integration tests
#
# Injects:
# - deterministic scraper
# - real PostgreSQL loader
# - real fetch_one/fetch_all
# ============================================================================

@pytest.fixture()
def app_db(database_url, db_clean):
    """
    Flask app wired to a real PostgreSQL loader.

    Ensures:
    - Rows are actually inserted
    - Schema constraints are respected
    - Idempotency via UNIQUE(url) works
    """

    rows = [
        {
            "url": "https://example.com/r/1",
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
            "url": "https://example.com/r/2",
            "term": "Fall 2025",
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

    def fake_scraper():
        return list(rows)

    def db_loader(in_rows):
        """
        Real DB insert logic for tests.

        Uses ON CONFLICT DO NOTHING to enforce idempotency.
        Asserts required non-null fields (e.g., url).
        """
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
                    assert r.get("url"), "url is required"
                    cur.execute(insert_sql, r)
            conn.commit()

        return len(in_rows)

    def fake_update_analysis():
        return None

    from app.db import fetch_one, fetch_all

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
    return app


@pytest.fixture()
def client_db(app_db):
    """
    Flask test client backed by real PostgreSQL.
    """
    return app_db.test_client()


# ============================================================================
# Database Fixtures
# ----------------------------------------------------------------------------
# Session-scoped fixtures reduce setup cost.
# Function-scoped cleanup ensures test isolation.
# ============================================================================

@pytest.fixture(scope="session")
def database_url():
    """
    Require DATABASE_URL to be set for DB tests.
    """
    url = os.environ.get("DATABASE_URL")
    assert url, "DATABASE_URL must be set for db tests"
    return url


@pytest.fixture(scope="session")
def db_init_schema(database_url):
    """
    Create applicants table if missing.

    Uniqueness policy:
        url is UNIQUE (idempotency requirement).
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS applicants (
        id BIGSERIAL PRIMARY KEY,
        url TEXT NOT NULL UNIQUE,
        term TEXT,
        status TEXT,
        us_or_international TEXT,
        gpa DOUBLE PRECISION,
        gre DOUBLE PRECISION,
        gre_v DOUBLE PRECISION,
        gre_aw DOUBLE PRECISION,
        degree TEXT,
        program TEXT,
        llm_generated_program TEXT,
        llm_generated_university TEXT
    );
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


@pytest.fixture()
def db_clean(database_url, db_init_schema):
    """
    Truncate applicants table before each DB test.

    Ensures:
    - Clean slate per test
    - No cross-test contamination
    - Deterministic row counts
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY;")
        conn.commit()
