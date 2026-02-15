"""
app/__init__.py

Flask application factory for Module 4.

Key requirements:
- Expose create_app(...) factory
- Provide dependency injection hooks for scraper/loader/update-analysis
- Register routes via blueprint
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from flask import Flask
from app.db import fetch_one, fetch_all



ScraperFn = Callable[[], list[dict]]
LoaderFn = Callable[[list[dict]], int]
UpdateAnalysisFn = Callable[[], None]


@dataclass
class BusyState:
    """Observable busy flag; backed by lock file in pull_state.py for UI disabling."""
    pass


def create_app(test_config: Optional[dict] = None, deps: Optional[dict] = None) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "dev"  # OK for local; do NOT put secrets here for production

    if test_config:
        app.config.update(test_config)

    deps = deps or {}

    # Injectables (tests will override these)
    scraper_fn: ScraperFn = deps.get("scraper_fn") or (lambda: [])
    loader_fn: LoaderFn = deps.get("loader_fn") or (lambda rows: 0)
    update_analysis_fn: UpdateAnalysisFn = deps.get("update_analysis_fn") or (lambda: None)
    fetch_one_fn = deps.get("fetch_one_fn") or fetch_one
    fetch_all_fn = deps.get("fetch_all_fn") or fetch_all

    app.extensions["deps"] = {
        "scraper_fn": scraper_fn,
        "loader_fn": loader_fn,
        "update_analysis_fn": update_analysis_fn,
        "fetch_one_fn": fetch_one_fn,
        "fetch_all_fn": fetch_all_fn,
    }

    # Register blueprint routes
    from app.pages import pages_bp
    app.register_blueprint(pages_bp)

    return app
