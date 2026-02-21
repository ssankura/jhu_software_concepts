"""
app/db.py

Database helper module for PostgreSQL connectivity using psycopg.

Responsibilities:
- Build DB connection from environment variables
- Provide small query helpers
"""

from __future__ import annotations

import os
from typing import Any, Optional, Sequence

import psycopg


def get_database_url() -> str:
    """
    Return a PostgreSQL connection URL.

    Supported configuration (in priority order):
    1) DATABASE_URL (backward compatible)
    2) DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (Module 5 requirement)

    Raises:
        ValueError: if required environment variables are missing.
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": password,
    }.items() if not v]

    if missing:
        raise ValueError(
            "Database environment is not configured.\n"
            "Set DATABASE_URL, or set: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD "
            "(optional DB_PORT).\n"
            f"Missing: {', '.join(missing)}"
        )

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def fetch_one(sql_query: str, params: Optional[Sequence[Any]] = None) -> Any:
    """
    Execute a query and return the first column of the first row.
    """
    with psycopg.connect(get_database_url()) as connection:
        with connection.cursor() as cursor:
            if params is None:
                cursor.execute(sql_query)
            else:
                cursor.execute(sql_query, params)
            row = cursor.fetchone()
            return row[0] if row else None


def fetch_all(sql_query: str, params: Optional[Sequence[Any]] = None) -> list[tuple]:
    """
    Execute a query and return all rows.
    """
    with psycopg.connect(get_database_url()) as connection:
        with connection.cursor() as cursor:
            if params is None:
                cursor.execute(sql_query)
            else:
                cursor.execute(sql_query, params)
            return cursor.fetchall()
