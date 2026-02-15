import sys
from pathlib import Path
import os
import psycopg

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import pytest
from app import create_app

def fake_fetch_one(sql: str):
    # counts
    if "COUNT" in sql:
        return 1
    # percentages
    if "100.0" in sql or "ROUND" in sql:
        return 12.34
    # averages
    return 3.9

def fake_fetch_all(sql: str):
    # q3 averages query expects one row with 4 values
    if "AVG" in sql and "FROM applicants" in sql:
        return [(3.9, 320, 165, 4.0)]
    # top programs table
    return [("Computer Science", 10), ("Data Science", 7)]


@pytest.fixture()
def fake_rows():
    # Minimal fake record list (shape doesn't matter yet, used in buttons tests later)
    return [
        {"url": "https://example.com/r/1", "term": "Fall 2026", "status": "Accepted"},
        {"url": "https://example.com/r/2", "term": "Fall 2026", "status": "Rejected"},
    ]


@pytest.fixture()
def app(fake_rows):
    def fake_scraper():
        return list(fake_rows)

    def fake_loader(rows):
        # return how many rows would be inserted
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
    return app.test_client()


@pytest.fixture()
def app_db(database_url, db_clean):
    """
    Flask app wired to a real DB loader for db tests.
    Scraper returns deterministic rows; loader inserts into Postgres.
    """
    from app import create_app

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
        import psycopg

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
                    # required non-null fields (enforced by schema)
                    assert r.get("url"), "url is required"
                    cur.execute(insert_sql, r)
            conn.commit()

        return len(in_rows)

    # For db tests we can still fake analysis update (no-op)
    def fake_update_analysis():
        return None

    # Also inject DB query functions for /analysis rendering during db/integration tests
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
    return app_db.test_client()


@pytest.fixture(scope="session")
def database_url():
    url = os.environ.get("DATABASE_URL")
    assert url, "DATABASE_URL must be set for db tests"
    return url


@pytest.fixture(scope="session")
def db_init_schema(database_url):
    """
    Create the applicants table if it doesn't exist.
    Uniqueness policy: url is UNIQUE.
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
    Truncate applicants before each db test.
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY;")
        conn.commit()