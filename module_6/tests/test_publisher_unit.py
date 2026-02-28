"""
Unit tests for src/web/publisher.py
"""

from __future__ import annotations

import json

import pytest

import publisher


class _FakeChannel:
    def __init__(self):
        self.calls = []
        self.publish_kwargs = None

    def exchange_declare(self, **kwargs):
        self.calls.append(("exchange_declare", kwargs))

    def queue_declare(self, **kwargs):
        self.calls.append(("queue_declare", kwargs))

    def queue_bind(self, **kwargs):
        self.calls.append(("queue_bind", kwargs))

    def basic_publish(self, **kwargs):
        self.calls.append(("basic_publish", kwargs))
        self.publish_kwargs = kwargs


class _FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.closed = False

    def channel(self):
        return self._channel

    def close(self):
        self.closed = True


@pytest.mark.web
def test_open_channel_declares_durable_entities(monkeypatch):
    fake_channel = _FakeChannel()
    fake_conn = _FakeConnection(fake_channel)

    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    class _FakePikaModule:
        @staticmethod
        def URLParameters(url):
            return {"url": url}

        @staticmethod
        def BlockingConnection(params):
            assert params["url"].startswith("amqp://")
            return fake_conn

    monkeypatch.setattr(publisher, "pika", _FakePikaModule)

    conn, ch = publisher._open_channel()

    assert conn is fake_conn
    assert ch is fake_channel

    assert ("exchange_declare", {
        "exchange": publisher.EXCHANGE,
        "exchange_type": "direct",
        "durable": True,
    }) in fake_channel.calls

    assert ("queue_declare", {
        "queue": publisher.QUEUE,
        "durable": True,
    }) in fake_channel.calls

    assert ("queue_bind", {
        "exchange": publisher.EXCHANGE,
        "queue": publisher.QUEUE,
        "routing_key": publisher.ROUTING_KEY,
    }) in fake_channel.calls


@pytest.mark.web
def test_publish_task_sends_persistent_message_and_closes_connection(monkeypatch):
    fake_channel = _FakeChannel()
    fake_conn = _FakeConnection(fake_channel)

    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    class _FakeBasicProperties:
        def __init__(self, **kwargs):
            self.delivery_mode = kwargs.get("delivery_mode")
            self.headers = kwargs.get("headers")
            self.content_type = kwargs.get("content_type")

    class _FakePikaModule:
        BasicProperties = _FakeBasicProperties

        @staticmethod
        def URLParameters(url):
            return {"url": url}

        @staticmethod
        def BlockingConnection(params):
            _ = params
            return fake_conn

    monkeypatch.setattr(publisher, "pika", _FakePikaModule)

    publisher.publish_task("scrape_new_data", payload={"since": "x"}, headers={"h": "v"})

    assert fake_conn.closed is True
    assert fake_channel.publish_kwargs is not None

    kwargs = fake_channel.publish_kwargs
    assert kwargs["exchange"] == publisher.EXCHANGE
    assert kwargs["routing_key"] == publisher.ROUTING_KEY
    assert kwargs["mandatory"] is False

    body = json.loads(kwargs["body"].decode("utf-8"))
    assert body["kind"] == "scrape_new_data"
    assert body["payload"] == {"since": "x"}
    assert "ts" in body

    props = kwargs["properties"]
    assert props.delivery_mode == 2
    assert props.headers == {"h": "v"}
    assert props.content_type == "application/json"


@pytest.mark.web
def test_publish_task_closes_connection_on_publish_error(monkeypatch):
    class _ErrorChannel(_FakeChannel):
        def basic_publish(self, **kwargs):
            _ = kwargs
            raise RuntimeError("publish failed")

    fake_channel = _ErrorChannel()
    fake_conn = _FakeConnection(fake_channel)

    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

    class _FakeBasicProperties:
        def __init__(self, **kwargs):
            _ = kwargs

    class _FakePikaModule:
        BasicProperties = _FakeBasicProperties

        @staticmethod
        def URLParameters(url):
            return {"url": url}

        @staticmethod
        def BlockingConnection(params):
            _ = params
            return fake_conn

    monkeypatch.setattr(publisher, "pika", _FakePikaModule)

    with pytest.raises(RuntimeError, match="publish failed"):
        publisher.publish_task("recompute_analytics")

    assert fake_conn.closed is True
