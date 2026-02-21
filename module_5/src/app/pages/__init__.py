"""
app/pages/__init__.py

This module defines the Blueprint responsible for controlling all webpage routes.
It acts as the "sub-pages" module for organizing the application's web pages.

Blueprints allow us to separate route logic into smaller, maintainable components
instead of placing all routes inside a single file.
"""

from flask import Blueprint

# Create Blueprint for all page routes
pages_bp = Blueprint(
    "pages",     # Blueprint name
    __name__     # Import reference name
)

# Import route modules so Flask registers them with the Blueprint
from app.pages import analysis  # pylint: disable=wrong-import-position  # noqa: F401
