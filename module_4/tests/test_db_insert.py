import pytest
import psycopg


@pytest.mark.db
def test_insert_on_pull_writes_rows(database_url, client_db):
    # before: empty
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            before = cur.fetchone()[0]
    assert before == 0

    # action: pull
    resp = client_db.post("/pull-data")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    # after: rows exist
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            after = cur.fetchone()[0]
            cur.execute("SELECT url, status FROM applicants ORDER BY url ASC;")
            rows = cur.fetchall()

    assert after >= 1
    # required non-null fields
    assert all(r[0] for r in rows)  # url non-null
    assert all(r[1] is not None for r in rows)  # status not null (we insert it)


@pytest.mark.db
def test_idempotency_duplicate_pulls_do_not_duplicate(database_url, client_db):
    r1 = client_db.post("/pull-data")
    assert r1.status_code == 200
    r2 = client_db.post("/pull-data")
    assert r2.status_code == 200

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            count = cur.fetchone()[0]

    # We inject exactly 2 unique URLs in scraper; uniqueness is on url
    assert count == 2


@pytest.mark.db
def test_query_function_returns_expected_keys(client_db):
    # ensure data exists
    resp = client_db.post("/pull-data")
    assert resp.status_code == 200

    from query_data import query_applicants_as_dicts

    rows = query_applicants_as_dicts(limit=5)
    assert isinstance(rows, list)
    assert len(rows) >= 1

    required_keys = {
        "url", "term", "status", "us_or_international", "gpa", "gre", "gre_v", "gre_aw",
        "degree", "program", "llm_generated_program", "llm_generated_university"
    }
    assert required_keys.issubset(rows[0].keys())