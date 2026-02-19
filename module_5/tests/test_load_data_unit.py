"""
test_load_data_unit.py

Unit tests for load_data.py (ETL/DB loader utilities).

Goals:
------
- Validate cleaning helpers (clean_text/clean_float/clean_date)
- Validate record mapping logic (map_record accepts multiple upstream key variants)
- Validate JSON loader behavior and error handling
- Validate table creation SQL is executed
- Validate load_data() behavior without real PostgreSQL by using test doubles
- Cover batch-insert behavior:
    - skipping records missing URL
    - flushing final batch
- Validate CLI entrypoint (main()) correctly parses args and calls load_data()

All tests are marked @pytest.mark.db because they relate to the data/DB layer.
"""

import json
import pytest


# ============================================================================
# Cleaning Helpers
# ----------------------------------------------------------------------------
# These helpers normalize raw JSON values before DB insertion.
# They are critical for:
# - avoiding null/blank string noise
# - safe numeric conversion
# - robust date parsing across common formats
# ============================================================================

@pytest.mark.db
def test_clean_text_and_float_and_date():
    import load_data

    # -------------------- clean_text --------------------
    assert load_data.clean_text("  hi ") == "hi"
    assert load_data.clean_text("   ") is None
    assert load_data.clean_text(None) is None

    # -------------------- clean_float -------------------
    assert load_data.clean_float("3.14") == 3.14
    assert load_data.clean_float("x") is None
    assert load_data.clean_float(None) is None

    # -------------------- clean_date --------------------
    assert load_data.clean_date("February 01, 2026").year == 2026
    assert load_data.clean_date("2026-02-01").month == 2
    assert load_data.clean_date("bad") is None
    assert load_data.clean_date("") is None
    assert load_data.clean_date(None) is None


# ============================================================================
# Record Mapping Robustness
# ----------------------------------------------------------------------------
# map_record() accepts multiple possible keys because the pipeline evolves:
# - scrape output keys may change
# - cleaning/standardization may rename fields
# This test ensures compatibility with multiple upstream variants.
# ============================================================================

@pytest.mark.db
def test_map_record_accepts_multiple_keys():
    import load_data

    rec = {
        "overview_url": "http://x",               # alternate URL key
        "program_name_clean": "CS",               # LLM/cleaned program key
        "university_clean": "U",                  # LLM/cleaned university key
        "international_or_american": "International",
        "gre_score": "320",                       # alternate GRE key
    }

    out = load_data.map_record(rec)

    assert out["url"] == "http://x"
    assert out["llm_generated_program"] == "CS"
    assert out["llm_generated_university"] == "U"
    assert out["gre"] == 320.0


# ============================================================================
# JSON Loader Validation
# ----------------------------------------------------------------------------
# load_json() must read a JSON file containing a LIST of dict records.
# If the JSON root object is not a list, we raise ValueError.
# ============================================================================

@pytest.mark.db
def test_load_json_requires_list(tmp_path):
    import load_data

    p = tmp_path / "x.json"
    p.write_text(json.dumps({"a": 1}), encoding="utf-8")

    with pytest.raises(ValueError):
        load_data.load_json(str(p))


# ============================================================================
# DDL Coverage: create_table()
# ----------------------------------------------------------------------------
# create_table() must execute:
# - CREATE TABLE IF NOT EXISTS applicants
# - CREATE UNIQUE INDEX IF NOT EXISTS applicants_url_uniq
#
# We use a minimal connection test double that only records SQL strings.
# This keeps the test fast and avoids needing a real DB.
# ============================================================================

@pytest.mark.db
def test_create_table_executes_sql(monkeypatch):
    import load_data

    executed = []

    class Conn:
        def execute(self, sql):
            executed.append(sql)

    load_data.create_table(Conn())

    assert any("CREATE TABLE IF NOT EXISTS applicants" in s for s in executed)
    assert any("CREATE UNIQUE INDEX IF NOT EXISTS" in s for s in executed)


# ============================================================================
# Error Handling: Missing DATABASE_URL
# ----------------------------------------------------------------------------
# load_data() must fail fast if DATABASE_URL is not set.
# This is required for portability and CI usage.
# ============================================================================

@pytest.mark.db
def test_load_data_raises_without_database_url(monkeypatch, tmp_path):
    import load_data

    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Prevent deep file work; we only want to test env validation branch.
    monkeypatch.setattr(load_data, "load_json", lambda p: [])

    with pytest.raises(RuntimeError):
        load_data.load_data(str(tmp_path / "x.json"))


# ============================================================================
# Insert Behavior + Skip Missing URL
# ----------------------------------------------------------------------------
# Requirements:
# - URL is required for dedupe (UNIQUE(url))
# - Records missing URL must be skipped (not inserted)
#
# This test uses a fully mocked psycopg connection so no real DB is required.
# It also forces batch_size=1 to exercise batch commit path immediately.
# ============================================================================

@pytest.mark.db
def test_load_data_inserts_and_skips_missing_url(monkeypatch, tmp_path):
    import load_data

    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    # Two records: second missing url -> should be skipped
    data = [
        {"url": "http://a", "program_name": "P", "applicant_status": "Accepted"},
        {"program_name": "NOURL"},
    ]
    monkeypatch.setattr(load_data, "load_json", lambda p: data)

    executed = {"creates": 0, "inserts": 0, "commits": 0}

    class Cur:
        def executemany(self, sql, params_seq):
            # load_data uses executemany for batch inserts
            if "INSERT INTO applicants" in sql:
                executed["inserts"] += len(list(params_seq))

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            # create_table() calls conn.execute for DDL
            executed["creates"] += 1

            # load_data ends with:
            # conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
            class Result:
                def fetchone(self_inner):
                    return (executed["inserts"],)

            if "SELECT COUNT(*) FROM applicants" in sql:
                return Result()

            return None

        def commit(self):
            executed["commits"] += 1

        def cursor(self):
            class CM:
                def __enter__(self2):
                    return Cur()

                def __exit__(self2, *a):
                    return False

            return CM()

    # Patch psycopg.connect to return our fake connection object
    monkeypatch.setattr(load_data.psycopg, "connect", lambda url: Conn())

    # Small batch size forces insert branch quickly
    load_data.load_data(str(tmp_path / "x.json"), batch_size=1)

    assert executed["creates"] >= 1
    assert executed["inserts"] == 1      # only first record inserted
    assert executed["commits"] >= 1


# ============================================================================
# Final Batch Flush Path
# ----------------------------------------------------------------------------
# When batch_size is large and file has fewer rows, load_data() must still:
# - insert the remaining rows in the final "if batch:" block
# - commit exactly once for that final batch
# ============================================================================

@pytest.mark.db
def test_load_data_flushes_final_batch(monkeypatch, tmp_path):
    import load_data

    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    # Only 1 record; batch_size large -> triggers final flush path
    data = [{"url": "http://only", "program_name": "P", "applicant_status": "Accepted"}]
    monkeypatch.setattr(load_data, "load_json", lambda p: data)

    executed = {"inserts": 0, "commits": 0}

    class Cur:
        def executemany(self, sql, params_seq):
            if "INSERT INTO applicants" in sql:
                executed["inserts"] += len(list(params_seq))

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            class Result:
                def fetchone(self_inner):
                    return (executed["inserts"],)

            if "SELECT COUNT(*) FROM applicants" in sql:
                return Result()

            return None

        def commit(self):
            executed["commits"] += 1

        def cursor(self):
            class CM:
                def __enter__(self2):
                    return Cur()

                def __exit__(self2, *a):
                    return False

            return CM()

    monkeypatch.setattr(load_data.psycopg, "connect", lambda url: Conn())

    load_data.load_data(str(tmp_path / "x.json"), batch_size=100)

    assert executed["inserts"] == 1
    assert executed["commits"] == 1


# ============================================================================
# Duplicate Missing DATABASE_URL Test (Coverage Safety)
# ----------------------------------------------------------------------------
# This appears redundant, but it often exists to ensure:
# - the RuntimeError branch is fully covered even if earlier tests change
# If you want, you can remove it if coverage stays 100%.
# ============================================================================

@pytest.mark.db
def test_load_data_raises_when_database_url_missing(monkeypatch, tmp_path):
    import load_data

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(load_data, "load_json", lambda p: [])

    with pytest.raises(RuntimeError):
        load_data.load_data(str(tmp_path / "x.json"))


# ============================================================================
# load_json Filtering Behavior
# ----------------------------------------------------------------------------
# load_json() must:
# - accept JSON lists
# - filter out non-dict items defensively
# This prevents crashes if scraped JSON contains unexpected artifacts.
# ============================================================================

@pytest.mark.db
def test_load_json_filters_only_dicts(tmp_path):
    import load_data

    p = tmp_path / "data.json"
    payload = [{"a": 1}, "x", 123, {"b": 2}, ["not", "dict"]]
    p.write_text(json.dumps(payload), encoding="utf-8")

    records = load_data.load_json(str(p))
    assert records == [{"a": 1}, {"b": 2}]


# ============================================================================
# CLI Entrypoint: main()
# ----------------------------------------------------------------------------
# main() uses argparse to parse:
#   --file
#   --batch-size
#
# This test validates:
# - argument parsing works
# - main() calls load_data() with expected values
#
# We patch sys.argv to simulate CLI input.
# ============================================================================

import sys

@pytest.mark.db
def test_main_parses_args_and_calls_load_data(monkeypatch):
    import load_data

    called = {}

    def fake_load_data(file, batch_size=1000):
        called["file"] = file
        called["batch_size"] = batch_size

    # Patch the function main() will call
    monkeypatch.setattr(load_data, "load_data", fake_load_data)

    # Patch argv for argparse
    monkeypatch.setattr(
        sys,
        "argv",
        ["load_data.py", "--file", "my.json", "--batch-size", "42"],
    )

    load_data.main()

    assert called == {"file": "my.json", "batch_size": 42}
