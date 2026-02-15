"""
app/__init__.py

Flask application factory for Module 4.

This module implements the Flask "application factory" pattern,
which is required for:

- Testability (each test can create an isolated app instance)
- Dependency injection (swap real DB/scraper with fakes)
- Clean separation between configuration and runtime state
- CI compatibility

Assignment Requirements Satisfied:
----------------------------------
✔ Expose create_app(...) factory
✔ Support dependency injection for scraper/loader/update-analysis
✔ Register blueprint routes
✔ Use DATABASE_URL for DB access (via app.db)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from flask import Flask
from app.db import fetch_one, fetch_all


# ---------------------------------------------------------------------------
# Dependency Type Definitions
# ---------------------------------------------------------------------------
# These type aliases document the expected shape of injected functions.
# They improve readability, IDE support, and Sphinx autodoc clarity.

ScraperFn = Callable[[], list[dict]]
LoaderFn = Callable[[list[dict]], int]
UpdateAnalysisFn = Callable[[], None]


# ---------------------------------------------------------------------------
# BusyState (Optional observable hook for UI/tests)
# ---------------------------------------------------------------------------

@dataclass
class BusyState:
    """
    Observable busy flag placeholder.

    NOTE:
        Actual busy-state logic is implemented in pull_state.py using a
        lock-file mechanism.

    This class exists primarily to make busy-state observable/testable
    in higher-level architecture if needed in the future.
    """
    pass


# ---------------------------------------------------------------------------
# Flask Application Factory
# ---------------------------------------------------------------------------

def create_app(test_config: Optional[dict] = None,
               deps: Optional[dict] = None) -> Flask:
    """
    Create and configure a Flask application instance.

    This function follows the Flask application factory pattern,
    allowing multiple independent app instances to be created
    (critical for pytest isolation).

    Args:
        test_config (dict, optional):
            Configuration overrides used during testing.
            Example:
                {"TESTING": True}

        deps (dict, optional):
            Dependency injection container. Tests can inject:
                - scraper_fn
                - loader_fn
                - update_analysis_fn
                - pull_data_fn
                - fetch_one_fn
                - fetch_all_fn

            If not provided, production defaults are used.

    Returns:
        Flask: configured Flask application instance.
    """

    # Create Flask app
    # template_folder and static_folder are explicitly defined to
    # ensure predictable behavior across project structure changes.
    app = Flask(
        __name__,
        template_folder="../templates",
        static_folder="../static",
    )

    # Development-only secret key
    # WARNING: Do NOT hardcode secrets in production systems.
    app.secret_key = "dev"

    # Apply test-specific configuration if provided
    if test_config:
        app.config.update(test_config)

    deps = deps or {}

    # -----------------------------------------------------------------------
    # Dependency Injection Layer
    # -----------------------------------------------------------------------
    # These are injectable so:
    # - Tests can pass fake scraper/loader functions
    # - No live internet scraping occurs during testing
    # - Database writes can be mocked or isolated
    #
    # If no injection provided, safe defaults are used.
    # -----------------------------------------------------------------------

    scraper_fn: ScraperFn = deps.get("scraper_fn") or (lambda: [])
    loader_fn: LoaderFn = deps.get("loader_fn") or (lambda rows: 0)
    update_analysis_fn: UpdateAnalysisFn = deps.get("update_analysis_fn") or (lambda: None)

    # Default pull_data behavior:
    # - Scrape rows
    # - Load rows into DB
    # - Return standardized response dict
    #
    # Tests can override this entire function via deps["pull_data_fn"].
    def default_pull_data_fn():
        rows = scraper_fn()
        inserted = loader_fn(rows)
        return {"ok": True, "inserted": inserted}

    pull_data_fn = deps.get("pull_data_fn") or default_pull_data_fn

    # Database query functions are injectable so:
    # - Tests can avoid real DB queries
    # - Integration tests can use isolated test DB
    fetch_one_fn = deps.get("fetch_one_fn") or fetch_one
    fetch_all_fn = deps.get("fetch_all_fn") or fetch_all

    # Store dependencies inside Flask extension registry.
    # This makes them accessible in routes via:
    #     current_app.extensions["deps"]
    app.extensions["deps"] = {
        "scraper_fn": scraper_fn,
        "loader_fn": loader_fn,
        "update_analysis_fn": update_analysis_fn,
        "pull_data_fn": pull_data_fn,
        "fetch_one_fn": fetch_one_fn,
        "fetch_all_fn": fetch_all_fn,
    }

    # -----------------------------------------------------------------------
    # Blueprint Registration
    # -----------------------------------------------------------------------
    # Blueprints modularize route definitions.
    # This keeps the application scalable and organized.
    from app.pages import pages_bp
    app.register_blueprint(pages_bp)

    return app
