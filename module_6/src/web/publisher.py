from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pika

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"


def _open_channel():
    url = os.environ["RABBITMQ_URL"]
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    return conn, ch


def publish_task(kind: str, payload: dict | None = None, headers: dict | None = None) -> None:
    body = json.dumps(
        {
            "kind": kind,
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": payload or {},
        },
        separators=(",", ":"),
    ).encode("utf-8")

    conn, ch = _open_channel()
    try:
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers=headers or {},
                content_type="application/json",
            ),
            mandatory=False,
        )
    finally:
        conn.close()
