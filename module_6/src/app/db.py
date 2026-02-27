"""Compatibility shim for `from app.db import ...` imports."""

from web.app.db import (
    get_database_url,
    fetch_one,
    fetch_all,
)

__all__ = [
    "get_database_url",
    "fetch_one",
    "fetch_all",
]