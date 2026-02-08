"""
app/db.py

Database helper module for PostgreSQL connectivity using psycopg.

Responsibilities:
----------------
• Retrieve database connection string
• Execute SQL queries
• Provide reusable helper functions for Flask routes
"""

import os
import psycopg


def get_database_url() -> str:
    """
    Retrieve DATABASE_URL environment variable.

    Returns:
        str: PostgreSQL connection string

    Raises:
        ValueError: If DATABASE_URL is not set
    """
    db_url = os.environ.get("DATABASE_URL")

    if not db_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set.\n"
            "Example:\n"
            'export DATABASE_URL="postgresql://username:password@localhost:5432/gradcafe"'
        )

    return db_url


def fetch_one(sql_query: str):
    """
    Execute SQL query and return first column of first row.

    Args:
        sql_query (str): SQL query string

    Returns:
        Any: Single value result or None if no result
    """

    with psycopg.connect(get_database_url()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)

            row = cursor.fetchone()

            return row[0] if row else None


def fetch_all(sql_query: str):
    """
    Execute SQL query and return all rows.

    Args:
        sql_query (str): SQL query string

    Returns:
        list[tuple]: Query result rows
    """

    with psycopg.connect(get_database_url()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)

            return cursor.fetchall()
