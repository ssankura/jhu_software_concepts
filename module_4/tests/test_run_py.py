"""
test_run_py.py

Tests for run.py (Flask entry point).

Covers:
- App object exposure
- __main__ guard execution
- Ensures no real Flask server starts during testing
"""

import pytest
import runpy


# ============================================================================
# App Exposure Test
# ============================================================================

@pytest.mark.web
def test_run_exposes_app():
    """
    Ensures run.py exposes a valid Flask app instance
    at module import time.

    This validates that:

        app = create_app()

    executed successfully at import.
    """
    import run
    assert run.app is not None


# ============================================================================
# __main__ Guard Coverage Test
# ============================================================================

@pytest.mark.integration
def test_run_py_main_guard_does_not_start_real_server(monkeypatch):
    """
    Covers the bottom guard in run.py:

        if __name__ == "__main__":
            app.run(...)

    We execute run.py as "__main__" using runpy,
    but patch create_app() and app.run() so:

    - No real Flask server starts
    - We can assert expected host/port/debug values
    """

    called = {"create_app": 0, "run": 0}

    # ----------------------------------------------------------------
    # Fake Flask App
    # ----------------------------------------------------------------
    class FakeApp:
        def run(self, host=None, port=None, debug=None):
            called["run"] += 1

            # Ensure expected configuration from run.py
            assert host == "127.0.0.1"
            assert port == 5000
            assert debug is True

    # ----------------------------------------------------------------
    # Fake create_app() factory
    # ----------------------------------------------------------------
    def fake_create_app():
        called["create_app"] += 1
        return FakeApp()

    # Patch app.create_app BEFORE executing run.py as __main__
    import app
    monkeypatch.setattr(app, "create_app", fake_create_app)

    # Execute run.py as if launched from command line
    runpy.run_module("run", run_name="__main__")

    # Ensure both create_app() and app.run() were invoked exactly once
    assert called["create_app"] == 1
    assert called["run"] == 1
