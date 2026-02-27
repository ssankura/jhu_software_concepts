"""
Compatibility shim.

Module 5 code/tests imported `app.*`.
Module 6 moved the Flask package to `web.app.*`.

This shim keeps old imports working without changing every test file.
"""
from web.app import create_app  # re-export

__all__ = ["create_app"]
