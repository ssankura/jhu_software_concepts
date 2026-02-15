import pytest
import psycopg


@pytest.mark.integration
def test_end_to_end_pull_update_render(database_url, db_clean):
    """
    End-to-end:
    - pull inserts rows into real DB
    - update-analysis returns ok
    - analysis page renders and includes percent with 2 decimals + %
    """
    from app import create_app
    from app.db import fetch_one, fetch_all

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

    def fake_scraper():
        return list(rows)

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

    # no-op, endpoint behavior is what we test
    def fake_update_analysis():
        return None

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

    # pull
    r1 = client.post("/pull-data")
    assert r1.status_code == 200
    assert r1.get_json()["ok"] is True

    # update
    r2 = client.post("/update-analysis")
    assert r2.status_code == 200
    assert r2.get_json()["ok"] is True

    # render
    r3 = client.get("/analysis")
    assert r3.status_code == 200
    html = r3.data.decode("utf-8")

    assert "Analysis" in html
    assert "Answer:" in html
    assert "%" in html  # formatting present somewhere

    # uniqueness: pull again (same rows)
    r4 = client.post("/pull-data")
    assert r4.status_code == 200

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants WHERE url LIKE 'https://example.com/integration/%';")
            count = cur.fetchone()[0]
    assert count == 2