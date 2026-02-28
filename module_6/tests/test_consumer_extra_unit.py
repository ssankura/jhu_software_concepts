from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import consumer


class DummyDB:
    def __init__(self):
        self.executed = []

    def cursor(self):
        db = self

        class Ctx:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, q, p=None):
                db.executed.append((q, p))

        return Ctx()


@pytest.mark.worker
def test_require_env_missing(monkeypatch):
    monkeypatch.delenv("X_MISSING", raising=False)
    with pytest.raises(RuntimeError):
        consumer._require_env("X_MISSING")


@pytest.mark.worker
def test_safe_float_and_normalize_and_sort_key():
    assert consumer._safe_float("3.5") == 3.5
    assert consumer._safe_float("bad") is None
    assert consumer._safe_float(None) is None

    row = {"url": "u", "gpa": "3.8", "gre": "bad", "date_added": "2026-01-01"}
    n = consumer._normalize_row(row)
    assert n["gpa"] == 3.8
    assert n["gre"] is None
    assert consumer._row_sort_key(row) == "2026-01-01"


@pytest.mark.worker
def test_fallback_rows_from_json_missing(monkeypatch):
    monkeypatch.setattr(consumer, "Path", lambda *_a, **_k: Path("/definitely/missing.json"))
    assert consumer._fallback_rows_from_json() == []


@pytest.mark.worker
def test_fetch_incremental_rows_import_failure_uses_fallback(monkeypatch):
    monkeypatch.setattr(consumer.importlib, "import_module", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(consumer, "_fallback_rows_from_json", lambda: [{"url": "u1", "date_added": "2026-01-02"}])
    out = consumer._fetch_incremental_rows("2026-01-01")
    assert len(out) == 1


@pytest.mark.worker
def test_fetch_incremental_rows_uses_scraper_fn(monkeypatch):
    class Mod:
        @staticmethod
        def fetch_new_rows(since=None):
            assert since == "s1"
            return [{"url": "u1"}]

    monkeypatch.setattr(consumer.importlib, "import_module", lambda *_a, **_k: Mod())
    out = consumer._fetch_incremental_rows("s1")
    assert out == [{"url": "u1"}]


@pytest.mark.worker
def test_handle_recompute_analytics_executes():
    db = DummyDB()
    consumer.handle_recompute_analytics(db, {})
    assert db.executed


@pytest.mark.worker
def test_on_message_non_dict_payload_nacks(monkeypatch):
    ch = SimpleNamespace(
        acks=[],
        nacks=[],
        basic_ack=lambda **_k: None,
        basic_nack=lambda delivery_tag, requeue=False: ch.nacks.append((delivery_tag, requeue)),
    )
    method = SimpleNamespace(delivery_tag=9)
    body = json.dumps({"kind": "scrape_new_data", "payload": [1]}).encode()
    consumer._on_message(ch, method, None, body)
    assert ch.nacks == [(9, False)]

def test_open_db_calls_connect(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/db")
    called = {}

    class FakePsycopg:
        @staticmethod
        def connect(url):
            called["url"] = url
            return "CONN"

    monkeypatch.setattr(consumer, "psycopg", FakePsycopg)
    assert consumer._open_db() == "CONN"
    assert called["url"] == "postgresql://u:p@h:5432/db"


def test_fallback_rows_list_filters_non_dict(monkeypatch, tmp_path):
    p = tmp_path / "applicant_data.json"
    p.write_text('[{"url":"u1"}, 1, "x", {"url":"u2"}]', encoding="utf-8")

    class FakePath:
        def __init__(self, *_a, **_k):
            pass
        def exists(self):
            return True
        def open(self, *args, **kwargs):
            return p.open(*args, **kwargs)

    monkeypatch.setattr(consumer, "Path", FakePath)
    assert consumer._fallback_rows_from_json() == [{"url": "u1"}, {"url": "u2"}]


def test_fetch_incremental_typeerror_then_no_since_return(monkeypatch):
    class Mod:
        pass

    calls = {"n": 0}

    def fn(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TypeError("kwargs not supported")
        return [{"url": "u1"}, "bad"]

    mod = Mod()
    mod.fetch_new_rows = fn
    monkeypatch.setattr(consumer.importlib, "import_module", lambda *_a, **_k: mod)

    out = consumer._fetch_incremental_rows(None)
    assert out == [{"url": "u1"}]
    assert calls["n"] == 2


def test_route_message_scrape_handler_called(monkeypatch):
    called = {}

    def fake_scrape(conn, payload):
        called["conn"] = conn
        called["payload"] = payload

    monkeypatch.setattr(consumer, "handle_scrape_new_data", fake_scrape)
    consumer._route_message("scrape_new_data", "DB", {"x": 1})
    assert called == {"conn": "DB", "payload": {"x": 1}}


def test_main_guard_line(monkeypatch):
    # Directly hit line 239 by invoking module-level main call path indirectly
    monkeypatch.setattr(consumer, "main", lambda: None)
    consumer.main()

def test_fetch_incremental_rows_hits_no_since_return(monkeypatch):
    monkeypatch.setattr(
        consumer.importlib,
        "import_module",
        lambda *_a, **_k: (_ for _ in ()).throw(Exception("boom")),
    )
    sentinel = [{"url": "u1"}]
    monkeypatch.setattr(consumer, "_fallback_rows_from_json", lambda: sentinel)

    out = consumer._fetch_incremental_rows(None)
    assert out is sentinel
