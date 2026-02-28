"""
RabbitMQ publisher utilities for enqueueing background tasks from the web service.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pika

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"


def _open_channel():
    """
    Open a RabbitMQ channel and ensure durable exchange/queue/binding exist.

    Returns:
        tuple: (connection, channel)
    """
    url = os.environ["RABBITMQ_URL"]
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()

    # Durable entities survive broker restarts and allow persistent task delivery.
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    return conn, ch


def publish_task(
    kind: str,
    payload: dict | None = None,
    headers: dict | None = None,
) -> None:
    """
    Publish a persistent task message to RabbitMQ.

    Args:
        kind: Task type (for example: "scrape_new_data", "recompute_analytics").
        payload: Optional JSON-serializable task payload.
        headers: Optional AMQP message headers.

    Raises:
        Exception: Propagates broker publish/connection errors to caller.
    """
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
                delivery_mode=2,  # persistent message
                headers=headers or {},
                content_type="application/json",
            ),
            mandatory=False,
        )
    finally:
        conn.close()
