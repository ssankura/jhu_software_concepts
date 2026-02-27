"""Compatibility shim for `from app.pages.analysis import ...`."""

from web.app.pages.analysis import _convert_decimal, _fmt_pct

__all__ = ["_convert_decimal", "_fmt_pct"]
