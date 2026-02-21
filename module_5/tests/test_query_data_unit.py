# Tests for query_data module functionality

import os
import pytest
from decimal import Decimal
import runpy


# Test that get_connection raises ValueError when DATABASE_URL is missing
@pytest.mark.db
def test_get_connection_raises_when_missing(monkeypatch):
    import query_data
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError):
        query_data.get_connection()


# Test Decimal conversion helper
@pytest.mark.db
def test_clean_value_decimal():
    import query_data
    assert query_data._clean_value(Decimal("1.23")) == 1.23
    assert query_data._clean_value("x") == "x"


# Test run_query multi-row, single-row, and no-result branches
@pytest.mark.db
def test_run_query_multi_and_single_branches(capsys):
    import query_data

    class Cursor:
        def __init__(self):
            self.executed = []
            self._fetchone = (Decimal("2.50"),)
            self._fetchall = [(Decimal("1.0"), "a"), (Decimal("2.0"), "b")]

        def execute(self, sql):
            self.executed.append(sql)

        def fetchall(self):
            return self._fetchall

        def fetchone(self):
            return self._fetchone

    cur = Cursor()

    # multi-row branch
    query_data.run_query(cur, "t", "SQL", multi=True)
    out = capsys.readouterr().out
    assert "1.0 a" in out
    assert "2.0 b" in out

    # single value branch
    cur._fetchone = (Decimal("9.99"),)
    query_data.run_query(cur, "t", "SQL2", label="Val")
    out = capsys.readouterr().out
    assert "Val: 9.99" in out

    # no results branch
    cur._fetchone = None
    query_data.run_query(cur, "t", "SQL3")
    out = capsys.readouterr().out
    assert "No results" in out


# Test query_applicants_as_dicts with injected fetcher
@pytest.mark.db
def test_query_applicants_as_dicts_injected_fetcher():
    import query_data


    def fake_fetch_all(stmt, params=None):
        # stmt is now a psycopg SQL object, so we only assert on params
        assert params == (1,)
        return [("u", "t", "s", "International", 4.0, 320, 160, 4.5, "MS", "P", "LP", "LU")]


    rows = query_data.query_applicants_as_dicts(limit=1, fetch_all_fn=fake_fetch_all)
    assert rows[0]["url"] == "u"
    assert "llm_generated_university" in rows[0]


# Test that main() calls run_query
@pytest.mark.db
def test_main_calls_run_query(monkeypatch):
    import query_data

    calls = {"n": 0}

    def fake_run_query(*args, **kwargs):
        calls["n"] += 1
        return None

    class Cursor:
        def execute(self, sql): pass
        def fetchall(self): return []
        def fetchone(self): return (1,)

    class Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def cursor(self):
            class CM:
                def __enter__(self2): return Cursor()
                def __exit__(self2, *a): return False
            return CM()

    monkeypatch.setattr(query_data, "get_connection", lambda: Conn())
    monkeypatch.setattr(query_data, "run_query", fake_run_query)

    query_data.main()
    assert calls["n"] > 0


# Duplicate test: ensure get_connection raises when DATABASE_URL missing
@pytest.mark.db
def test_get_connection_raises_when_database_url_missing(monkeypatch):
    import query_data
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        query_data.get_connection()


# Duplicate Decimal conversion test
@pytest.mark.db
def test_clean_value_converts_decimal():
    import query_data
    from decimal import Decimal

    assert query_data._clean_value(Decimal("12.34")) == 12.34
    assert query_data._clean_value("x") == "x"


# Test dict conversion structure and key presence
@pytest.mark.db
def test_query_applicants_as_dicts_uses_fetch_all_fn():
    import query_data

    fake_rows = [
        ("u1", "Fall 2026", "Accepted", "International", 3.9, 330, 165, 4.5, "MS", "CS", "CS", "MIT"),
        ("u2", "Fall 2025", "Rejected", "American", None, None, None, None, "PhD", "EE", "EE", "Stanford"),
    ]

    def fake_fetch_all(stmt, params=None):
        assert params == (2,)
        return fake_rows

    out = query_data.query_applicants_as_dicts(limit=2, fetch_all_fn=fake_fetch_all)
    assert len(out) == 2

    expected_keys = {
        "url", "term", "status", "us_or_international", "gpa", "gre", "gre_v", "gre_aw",
        "degree", "program", "llm_generated_program", "llm_generated_university"
    }
    assert set(out[0].keys()) == expected_keys
    assert out[0]["url"] == "u1"
    assert out[1]["llm_generated_university"] == "Stanford"


# Test labeled single-value output formatting
@pytest.mark.db
def test_run_query_single_value_with_label(monkeypatch, capsys):
    import query_data

    class Cur:
        def execute(self, sql):
            self.sql = sql
        def fetchone(self):
            return (123,)

    query_data.run_query(Cur(), title="t", sql_query="SELECT 1", label="Applicant count")
    captured = capsys.readouterr().out
    assert "Applicant count: 123" in captured


# Test multi-row formatting
@pytest.mark.db
def test_run_query_multi_row(monkeypatch, capsys):
    import query_data
    from decimal import Decimal

    class Cur:
        def execute(self, sql):
            self.sql = sql
        def fetchall(self):
            return [
                ("Fall 2026", Decimal("10")),
                ("Fall 2025", Decimal("5")),
            ]

    query_data.run_query(Cur(), title="t", sql_query="SELECT x", multi=True)
    out = capsys.readouterr().out
    assert "Fall 2026 10.0" in out
    assert "Fall 2025 5.0" in out


# Test multi-value labeled formatting
@pytest.mark.db
def test_run_query_multi_value_with_labels(capsys):
    import query_data
    from decimal import Decimal

    class Cur:
        def execute(self, sql):
            self.sql = sql
        def fetchone(self):
            return (Decimal("3.111"), Decimal("320.0"))

    query_data.run_query(
        Cur(),
        title="t",
        sql_query="SELECT avg(gpa), avg(gre)",
        multi_labels=["Average GPA", "Average GRE"],
    )
    out = capsys.readouterr().out
    assert "Average GPA:" in out
    assert "Average GRE:" in out


# Test no-results message
@pytest.mark.db
def test_run_query_no_results_prints_message(capsys):
    import query_data

    class Cur:
        def execute(self, sql):
            self.sql = sql
        def fetchone(self):
            return None

    query_data.run_query(Cur(), title="t", sql_query="SELECT nothing")
    out = capsys.readouterr().out
    assert "No results" in out


# Test successful get_connection path
@pytest.mark.db
def test_get_connection_success(monkeypatch):
    import query_data

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")

    called = {}

    def fake_connect(url):
        called["url"] = url
        class DummyConn:
            def close(self): ...
        return DummyConn()

    monkeypatch.setattr(query_data.psycopg, "connect", fake_connect)

    conn = query_data.get_connection()
    assert called["url"] == "postgresql://example"
    conn.close()


# Test single-value without label branch
@pytest.mark.db
def test_run_query_single_value_no_label(capsys):
    import query_data

    class Cur:
        def execute(self, sql): ...
        def fetchone(self):
            return (777,)

    query_data.run_query(Cur(), title="t", sql_query="SELECT 1", label=None)
    out = capsys.readouterr().out
    assert "777" in out


# Test multi-value fallback branch when labels length mismatch
@pytest.mark.db
def test_run_query_multi_value_without_matching_labels_hits_else_branch(capsys):
    import query_data

    class Cur:
        def execute(self, sql): ...
        def fetchone(self):
            return (1, 2, 3)

    query_data.run_query(Cur(), title="t", sql_query="SELECT 1,2,3", multi_labels=["A", "B"])
    out = capsys.readouterr().out
    assert "1 2 3" in out


# Test default fetch_all_fn import branch
@pytest.mark.db
def test_query_applicants_as_dicts_default_fetch_all_fn(monkeypatch):
    import query_data

    fake_rows = [
        ("u1", "Fall 2026", "Accepted", "International", 3.9, 330, 165, 4.5, "MS", "CS", "CS", "MIT"),
    ]

    monkeypatch.setattr(query_data, "default_fetch_all_fn", lambda stmt, params=None: fake_rows)

    out = query_data.query_applicants_as_dicts(limit=1, fetch_all_fn=None)
    assert out[0]["url"] == "u1"
    assert out[0]["llm_generated_university"] == "MIT"


# Test __main__ guard execution safely
@pytest.mark.db
def test_query_data_module_main_guard_line(monkeypatch):
    import query_data

    class DummyCursor:
        def execute(self, sql): ...
        def fetchone(self): return (1,)
        def fetchall(self): return [("Fall 2026", 1)]
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class DummyConn:
        def cursor(self): return DummyCursor()
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(query_data, "get_connection", lambda: DummyConn())

    runpy.run_module("query_data", run_name="__main__")


@pytest.mark.db
def test_clamp_limit_defaults_when_invalid():
    import query_data
    assert query_data._clamp_limit("abc") == 10
    assert query_data._clamp_limit(None) == 10