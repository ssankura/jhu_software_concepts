import pytest
import runpy



@pytest.mark.web
def test_run_exposes_app():
    import run
    assert run.app is not None


@pytest.mark.integration
def test_run_py_main_guard_does_not_start_real_server(monkeypatch):
    """
    Execute src/run.py as __main__ to cover the bottom guard line,
    but patch create_app() and app.run() so no real server starts.
    """
    called = {"create_app": 0, "run": 0}

    class FakeApp:
        def run(self, host=None, port=None, debug=None):
            called["run"] += 1
            # Assert the expected values used in run.py
            assert host == "127.0.0.1"
            assert port == 5000
            assert debug is True

    def fake_create_app():
        called["create_app"] += 1
        return FakeApp()

    # Patch app.create_app BEFORE running run.py
    import app
    monkeypatch.setattr(app, "create_app", fake_create_app)

    # Now execute src/run.py as __main__
    runpy.run_module("run", run_name="__main__")

    assert called["create_app"] == 1
    assert called["run"] == 1
