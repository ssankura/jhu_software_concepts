"""
test_db_py_coverage.py

Additional unit tests to ensure 100% coverage of src/app/db.py.

These tests specifically target:
- DB_PORT default handling
- Missing DB_* environment variable error path
- The branch in fetch_all() where no params are provided
"""

import pytest
import app.db as db


def test_db_url_builds_with_default_port(monkeypatch):
    """
    Verify that DB_PORT defaults to 5432 when not set.

    Covers:
    - Branch where DATABASE_URL is not defined
    - DB_PORT fallback to default value
    """
    # Ensure legacy DATABASE_URL is not set
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Provide required DB_* variables except DB_PORT
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.delenv("DB_PORT", raising=False)  # triggers default port logic
    monkeypatch.setenv("DB_NAME", "gradcafe")
    monkeypatch.setenv("DB_USER", "graduser")
    monkeypatch.setenv("DB_PASSWORD", "grad123")

    # Expected default port = 5432
    assert db.get_database_url() == (
        "postgresql://graduser:grad123@localhost:5432/gradcafe"
    )


def test_db_url_missing_parts_lists_missing_keys(monkeypatch):
    """
    Verify that missing DB_* variables raise a ValueError.

    Covers:
    - Construction of missing variable list
    - ValueError branch in get_database_url()
    """
    # Remove all DB configuration variables
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)

    with pytest.raises(ValueError) as exc:
        db.get_database_url()

    msg = str(exc.value)

    # Ensure error message mentions missing configuration
    assert "Missing:" in msg
    # Confirm at least one expected variable appears
    assert any(key in msg for key in [
        "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"
    ])


def test_fetch_all_without_params_branch(monkeypatch):
    """
    Verify fetch_all() executes correctly when params=None.

    Covers:
    - Branch where cursor.execute() is called without parameters
    - No real database connection required (mocked psycopg.connect)
    """
    # Provide a dummy DATABASE_URL so get_database_url() succeeds
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/n")

    # Fake cursor to simulate psycopg behavior
    class FakeCursor:
        def execute(self, query):
            # This ensures we cover the "no params" branch
            self.executed_query = query

        def fetchall(self):
            return [("row",)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    # Fake connection object
    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    # Monkeypatch psycopg.connect to avoid real DB usage
    monkeypatch.setattr(db.psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    # Should return mocked row
    assert db.fetch_all("SELECT 1") == [("row",)]


def test_fetch_one_with_params_branch(monkeypatch):
    """
    Covers fetch_one() branch where params are provided (cursor.execute(query, params)).
    No real DB connection needed.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/n")

    class FakeCursor:
        def execute(self, query, params):
            # If we reach here, we covered the "params provided" branch.
            assert query == "SELECT %s"
            assert params == ("x",)

        def fetchone(self):
            return (999,)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(db.psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    assert db.fetch_one("SELECT %s", params=("x",)) == 999


def test_fetch_all_with_params_branch(monkeypatch):
    """
    Covers fetch_all() branch where params are provided (cursor.execute(query, params)).
    No real DB connection needed.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/n")

    class FakeCursor:
        def execute(self, query, params):
            assert query == "SELECT %s"
            assert params == ("y",)

        def fetchall(self):
            return [("ok",)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(db.psycopg, "connect", lambda *_args, **_kwargs: FakeConn())

    assert db.fetch_all("SELECT %s", params=("y",)) == [("ok",)]
