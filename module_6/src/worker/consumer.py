from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
from typing import Any

import pika
import psycopg
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"
DEFAULT_SOURCE = "applicant_data_json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _open_rabbit():
    url = _require_env("RABBITMQ_URL")
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    ch.basic_qos(prefetch_count=1)
    return conn, ch


def _open_db():
    db_url = _require_env("DATABASE_URL")
    return psycopg.connect(db_url)


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "program": row.get("program"),
        "comments": row.get("comments"),
        "date_added": row.get("date_added"),
        "url": row.get("url"),
        "status": row.get("status"),
        "term": row.get("term"),
        "us_or_international": row.get("us_or_international"),
        "gpa": _safe_float(row.get("gpa")),
        "gre": _safe_float(row.get("gre")),
        "gre_v": _safe_float(row.get("gre_v")),
        "gre_aw": _safe_float(row.get("gre_aw")),
        "degree": row.get("degree"),
        "llm_generated_program": row.get("llm_generated_program"),
        "llm_generated_university": row.get("llm_generated_university"),
    }


def _row_sort_key(row: dict[str, Any]) -> str:
    # Text key is fine with TEXT watermark; prefer date_added, fallback url.
    return str(row.get("date_added") or row.get("url") or "")


def _fallback_rows_from_json() -> list[dict[str, Any]]:
    data_path = Path("/data/applicant_data.json")
    if not data_path.exists():
        logger.warning("No /data/applicant_data.json found; returning empty batch.")
        return []
    with data_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def _fetch_incremental_rows(since: str | None) -> list[dict[str, Any]]:
    """
    Try existing scraper hooks first; if not available, fallback to /data JSON.
    """
    try:
        mod = importlib.import_module("etl.incremental_scraper")
        for name in ("fetch_new_rows", "scrape_new_data", "run_incremental", "get_new_rows"):
            fn = getattr(mod, name, None)
            if callable(fn):
                try:
                    rows = fn(since=since)
                except TypeError:
                    rows = fn()
                if isinstance(rows, list):
                    return [r for r in rows if isinstance(r, dict)]
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Failed calling incremental scraper; using JSON fallback")

    rows = _fallback_rows_from_json()
    if not since:
        return rows

    filtered = []
    for row in rows:
        if _row_sort_key(row) > since:
            filtered.append(row)
    return filtered


def _get_watermark(conn: psycopg.Connection, source: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_seen FROM ingestion_watermarks WHERE source = %s LIMIT 1;",
            (source,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _set_watermark(conn: psycopg.Connection, source: str, last_seen: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_watermarks (source, last_seen, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (source)
            DO UPDATE SET last_seen = EXCLUDED.last_seen, updated_at = now();
            """,
            (source, last_seen),
        )


def handle_scrape_new_data(conn: psycopg.Connection, payload: dict[str, Any]) -> None:
    source = str(payload.get("source") or DEFAULT_SOURCE)
    since = payload.get("since") or _get_watermark(conn, source)

    rows = _fetch_incremental_rows(since)
    if not rows:
        logger.info("No new rows for source=%s since=%s", source, since)
        return

    rows = sorted(rows, key=_row_sort_key)
    normalized = [_normalize_row(r) for r in rows if r.get("url")]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO applicants (
                program, comments, date_added, url, status, term, us_or_international,
                gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
            ) VALUES (
                %(program)s, %(comments)s, %(date_added)s, %(url)s, %(status)s, %(term)s, %(us_or_international)s,
                %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s, %(degree)s, %(llm_generated_program)s, %(llm_generated_university)s
            )
            ON CONFLICT (url) DO NOTHING;
            """,
            normalized,
        )

    max_seen = _row_sort_key(rows[-1])
    if max_seen:
        _set_watermark(conn, source, max_seen)

    logger.info("Scrape task processed: rows=%s source=%s watermark=%s", len(rows), source, max_seen)


def handle_recompute_analytics(conn: psycopg.Connection, payload: dict[str, Any]) -> None:
    _ = payload
    # If you add materialized views, refresh them here.
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
    logger.info("Recompute analytics task processed")


def _route_message(kind: str, conn: psycopg.Connection, payload: dict[str, Any]) -> None:
    task_map = {
        "scrape_new_data": handle_scrape_new_data,
        "recompute_analytics": handle_recompute_analytics,
    }
    if kind not in task_map:
        raise ValueError(f"Unknown task kind: {kind}")
    task_map[kind](conn, payload)


def _on_message(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    _ = properties
    try:
        msg = json.loads(body.decode("utf-8"))
        kind = msg["kind"]
        payload = msg.get("payload", {}) or {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Invalid message, dropping")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    conn = _open_db()
    try:
        _route_message(kind, conn, payload)
        conn.commit()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Task failed kind=%s; rolling back", kind)
        conn.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        conn.close()


def main():
    conn, ch = _open_rabbit()
    logger.info("Worker consuming queue=%s", QUEUE)
    ch.basic_consume(queue=QUEUE, on_message_callback=_on_message, auto_ack=False)
    try:
        ch.start_consuming()
    finally:
        if conn.is_open:
            conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()
