from __future__ import annotations

import pytest

from app import create_app


def _mk_app():
    deps = {
        "fetch_one_fn": lambda *_args, **_kwargs: 0,
        "fetch_all_fn": lambda *_args, **_kwargs: [(0, 0, 0, 0)],
        "pull_data_fn": lambda: {"ok": True, "inserted": 1},
        "update_analysis_fn": lambda: None,
    }
    return create_app({"TESTING": True}, deps=deps)


@pytest.mark.web
def test_pull_data_queue_success(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    import app.pages.analysis as analysis_mod
    monkeypatch.setattr(analysis_mod, "publish_task", lambda *a, **k: None)

    with app.test_client() as c:
        r = c.post("/pull-data", headers={"Accept": "application/json"})
    assert r.status_code == 202
    assert r.get_json()["kind"] == "scrape_new_data"


@pytest.mark.web
def test_pull_data_queue_failure(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    import app.pages.analysis as analysis_mod

    def _boom(*_a, **_k):
        raise RuntimeError("queue down")

    monkeypatch.setattr(analysis_mod, "publish_task", _boom)

    with app.test_client() as c:
        r = c.post("/pull-data", headers={"Accept": "application/json"})
    assert r.status_code == 503


@pytest.mark.web
def test_update_analysis_queue_success(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    import app.pages.analysis as analysis_mod
    monkeypatch.setattr(analysis_mod, "publish_task", lambda *a, **k: None)

    with app.test_client() as c:
        r = c.post("/update-analysis", headers={"Accept": "application/json"})
    assert r.status_code == 202
    assert r.get_json()["kind"] == "recompute_analytics"


@pytest.mark.web
def test_update_analysis_queue_failure(monkeypatch):
    app = _mk_app()
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    import app.pages.analysis as analysis_mod

    def _boom(*_a, **_k):
        raise RuntimeError("queue down")

    monkeypatch.setattr(analysis_mod, "publish_task", _boom)

    with app.test_client() as c:
        r = c.post("/update-analysis", headers={"Accept": "application/json"})
    assert r.status_code == 503
