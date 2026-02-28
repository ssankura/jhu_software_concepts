from __future__ import annotations

import os
from pathlib import Path

import pytest

from app import create_app
import app.pages.analysis as analysis_mod
import consumer


def _mk_app():
    deps = {
        "fetch_one_fn": lambda *_a, **_k: 0,
        "fetch_all_fn": lambda *_a, **_k: [(0, 0, 0, 0)],
        "pull_data_fn": lambda: {"ok": True, "inserted": 1},
        "update_analysis_fn": lambda: None,
    }
    return create_app({"TESTING": True}, deps=deps)


@pytest.mark.web
def test_pull_data_queue_html_success_branch(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    monkeypatch.setattr(analysis_mod, "publish_task", lambda *a, **k: None)

    with app.test_client() as c:
        r = c.post("/pull-data", headers={"Accept": "text/html"})
    assert r.status_code == 302


@pytest.mark.web
def test_pull_data_queue_html_failure_branch(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    def _boom(*_a, **_k):
        raise RuntimeError("down")

    monkeypatch.setattr(analysis_mod, "publish_task", _boom)

    with app.test_client() as c:
        r = c.post("/pull-data", headers={"Accept": "text/html"})
    assert r.status_code == 302


@pytest.mark.web
def test_update_analysis_queue_html_success_branch(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    monkeypatch.setattr(analysis_mod, "publish_task", lambda *a, **k: None)

    with app.test_client() as c:
        r = c.post("/update-analysis", headers={"Accept": "text/html"})
    assert r.status_code == 302


@pytest.mark.web
def test_update_analysis_queue_html_failure_branch(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    def _boom(*_a, **_k):
        raise RuntimeError("down")

    monkeypatch.setattr(analysis_mod, "publish_task", _boom)

    with app.test_client() as c:
        r = c.post("/update-analysis", headers={"Accept": "text/html"})
    assert r.status_code == 302


@pytest.mark.worker
def test_open_rabbit_missing_env_raises(monkeypatch):
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    with pytest.raises(RuntimeError):
        consumer._open_rabbit()


@pytest.mark.worker
def test_open_db_missing_env_raises(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError):
        consumer._open_db()


@pytest.mark.worker
def test_fetch_incremental_rows_filter_branch(monkeypatch):
    class Mod:
        @staticmethod
        def fetch_new_rows(since=None):
            _ = since
            return [
                {"url": "u1", "date_added": "2026-01-01"},
                {"url": "u2", "date_added": "2026-01-03"},
            ]

    monkeypatch.setattr(consumer.importlib, "import_module", lambda *_a, **_k: Mod())
    out = consumer._fetch_incremental_rows("2026-01-02")
    # function returns as-is from scraper when available
    assert len(out) == 2


@pytest.mark.worker
def test_fetch_incremental_rows_fallback_filter(monkeypatch):
    monkeypatch.setattr(
        consumer,
        "_fallback_rows_from_json",
        lambda: [
            {"url": "u1", "date_added": "2026-01-01"},
            {"url": "u2", "date_added": "2026-01-03"},
        ],
    )
    monkeypatch.setattr(
        consumer.importlib,
        "import_module",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")),
    )
    out = consumer._fetch_incremental_rows("2026-01-02")
    assert out == [{"url": "u2", "date_added": "2026-01-03"}]


@pytest.mark.worker
def test_main_connection_already_closed(monkeypatch):
    class Ch:
        def basic_consume(self, **_k):
            return None

        def start_consuming(self):
            return None

    class Conn:
        is_open = False

        def close(self):
            raise AssertionError("close should not be called when already closed")

    monkeypatch.setattr(consumer, "_open_rabbit", lambda: (Conn(), Ch()))
    consumer.main()


@pytest.mark.worker
def test_fallback_rows_from_json_non_list(monkeypatch, tmp_path):
    p = tmp_path / "applicant_data.json"
    p.write_text('{"x":1}', encoding="utf-8")

    class P:
        def __init__(self, *_a, **_k):
            self._p = p

        def __call__(self, *_a, **_k):
            return self._p

    monkeypatch.setattr(consumer, "Path", P())
    assert consumer._fallback_rows_from_json() == []
