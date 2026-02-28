"""
Unit tests for src/worker/consumer.py
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import consumer


class FakeChannel:
    def __init__(self):
        self.acks = []
        self.nacks = []
        self.qos = None
        self.consume_args = None
        self.started = False
        self.declares = []

    def exchange_declare(self, **kwargs):
        self.declares.append(("exchange", kwargs))

    def queue_declare(self, **kwargs):
        self.declares.append(("queue", kwargs))

    def queue_bind(self, **kwargs):
        self.declares.append(("bind", kwargs))

    def basic_qos(self, **kwargs):
        self.qos = kwargs

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=False):
        self.nacks.append((delivery_tag, requeue))

    def basic_consume(self, **kwargs):
        self.consume_args = kwargs

    def start_consuming(self):
        self.started = True


class FakeRabbitConn:
    def __init__(self, ch):
        self._ch = ch
        self.is_open = True
        self.closed = False

    def channel(self):
        return self._ch

    def close(self):
        self.closed = True
        self.is_open = False


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.conn.executed.append((query, params))

    def executemany(self, query, seq):
        self.conn.executed_many.append((query, list(seq)))

    def fetchone(self):
        return self.conn.fetchone_value


class FakeDBConn:
    def __init__(self, fetchone_value=None):
        self.fetchone_value = fetchone_value
        self.executed = []
        self.executed_many = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.mark.worker
def test_open_rabbit_declares_and_sets_prefetch(monkeypatch):
    ch = FakeChannel()
    conn = FakeRabbitConn(ch)

    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    class FakePika:
        @staticmethod
        def URLParameters(url):
            return {"url": url}

        @staticmethod
        def BlockingConnection(params):
            assert params["url"].startswith("amqp://")
            return conn

    monkeypatch.setattr(consumer, "pika", FakePika)

    got_conn, got_ch = consumer._open_rabbit()
    assert got_conn is conn
    assert got_ch is ch
    assert ch.qos == {"prefetch_count": 1}


@pytest.mark.worker
def test_on_message_invalid_json_nacks(monkeypatch):
    ch = FakeChannel()
    method = SimpleNamespace(delivery_tag=11)

    consumer._on_message(ch, method, None, b"{not-json")
    assert ch.acks == []
    assert ch.nacks == [(11, False)]


@pytest.mark.worker
def test_on_message_success_ack_after_commit(monkeypatch):
    ch = FakeChannel()
    method = SimpleNamespace(delivery_tag=22)
    db = FakeDBConn()

    monkeypatch.setattr(consumer, "_open_db", lambda: db)

    called = {"route": 0}

    def fake_route(kind, conn, payload):
        called["route"] += 1
        assert kind == "scrape_new_data"
        assert conn is db
        assert payload == {"x": 1}

    monkeypatch.setattr(consumer, "_route_message", fake_route)

    msg = json.dumps({"kind": "scrape_new_data", "payload": {"x": 1}}).encode("utf-8")
    consumer._on_message(ch, method, None, msg)

    assert called["route"] == 1
    assert db.commits == 1
    assert db.rollbacks == 0
    assert db.closed is True
    assert ch.acks == [22]
    assert ch.nacks == []


@pytest.mark.worker
def test_on_message_route_failure_rolls_back_and_nacks(monkeypatch):
    ch = FakeChannel()
    method = SimpleNamespace(delivery_tag=33)
    db = FakeDBConn()

    monkeypatch.setattr(consumer, "_open_db", lambda: db)

    def fake_route(kind, conn, payload):
        _ = (kind, conn, payload)
        raise RuntimeError("boom")

    monkeypatch.setattr(consumer, "_route_message", fake_route)

    msg = json.dumps({"kind": "recompute_analytics", "payload": {}}).encode("utf-8")
    consumer._on_message(ch, method, None, msg)

    assert db.commits == 0
    assert db.rollbacks == 1
    assert db.closed is True
    assert ch.acks == []
    assert ch.nacks == [(33, False)]


@pytest.mark.worker
def test_route_message_unknown_kind_raises():
    db = FakeDBConn()
    with pytest.raises(ValueError, match="Unknown task kind"):
        consumer._route_message("unknown_kind", db, {})


@pytest.mark.worker
def test_get_and_set_watermark(monkeypatch):
    db = FakeDBConn(fetchone_value=("2026-01-01",))
    got = consumer._get_watermark(db, "src1")
    assert got == "2026-01-01"

    consumer._set_watermark(db, "src1", "2026-01-02")
    assert db.executed, "expected execute calls for upsert watermark"


@pytest.mark.worker
def test_handle_scrape_new_data_inserts_and_updates_watermark(monkeypatch):
    db = FakeDBConn(fetchone_value=None)

    rows = [
        {"url": "u1", "date_added": "2026-01-01", "program": "P1"},
        {"url": "u2", "date_added": "2026-01-02", "program": "P2"},
    ]
    monkeypatch.setattr(consumer, "_fetch_incremental_rows", lambda since: rows)

    consumer.handle_scrape_new_data(db, payload={})

    assert len(db.executed_many) == 1
    query, values = db.executed_many[0]
    assert "ON CONFLICT (url) DO NOTHING" in query
    assert len(values) == 2
    assert any("ingestion_watermarks" in q for q, _ in db.executed)


@pytest.mark.worker
def test_handle_scrape_new_data_no_rows_is_noop(monkeypatch):
    db = FakeDBConn(fetchone_value=None)
    monkeypatch.setattr(consumer, "_fetch_incremental_rows", lambda since: [])

    consumer.handle_scrape_new_data(db, payload={})
    assert db.executed_many == []


@pytest.mark.worker
def test_main_sets_consume_and_closes(monkeypatch):
    ch = FakeChannel()
    conn = FakeRabbitConn(ch)

    monkeypatch.setattr(consumer, "_open_rabbit", lambda: (conn, ch))
    consumer.main()

    assert ch.consume_args is not None
    assert ch.consume_args["queue"] == consumer.QUEUE
    assert ch.started is True
    assert conn.closed is True
