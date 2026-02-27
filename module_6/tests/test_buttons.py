"""
test_buttons.py

Tests for:
- POST /pull-data
- POST /update-analysis
- Busy-state gating behavior
- HTML vs JSON response paths
- Error handling
- Non-dict return normalization

All tests are marked @pytest.mark.buttons per assignment policy.
"""

import pytest


# ============================================================================
# Test: POST /pull-data (JSON path)
# ----------------------------------------------------------------------------
# Validates:
# - Returns 200 when not busy
# - Triggers injected loader
# - Returns correct JSON payload
#
# Covers:
# - pull_data_fn injection
# - JSON response path
# - Inserted count propagation
# ============================================================================

@pytest.mark.buttons
def test_post_pull_data_triggers_loader(client):
    resp = client.post("/pull-data")

    assert resp.status_code == 200
    data = resp.get_json()

    assert data["ok"] is True

    # From conftest fake_loader:
    # inserted == len(fake_rows) == 2
    assert data["inserted"] == 2

    # ------------------------------------------------------------------------
    # HTML response path
    #
    # When Accept header prefers text/html,
    # the route should redirect and flash a success message.
    # ------------------------------------------------------------------------
    resp2 = client.post(
        "/pull-data",
        headers={"Accept": "text/html"},
        follow_redirects=True,
    )

    assert resp2.status_code == 200
    html = resp2.data.decode("utf-8")

    assert "Pull Data completed" in html


# ============================================================================
# Test: POST /update-analysis (Not Busy)
# ----------------------------------------------------------------------------
# Validates:
# - Returns 200
# - Returns {"ok": True}
# - No busy-state interference
# ============================================================================

@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(client):
    resp = client.post("/update-analysis")

    assert resp.status_code == 200
    data = resp.get_json()

    assert data["ok"] is True


# ============================================================================
# Test: Busy Gating Behavior
# ----------------------------------------------------------------------------
# Requirement:
#   When a pull is in progress:
#     - POST /update-analysis returns 409
#     - POST /pull-data returns 409
#
# Implementation:
#   Monkeypatch pull_state._LOCK_FILE to simulate active lock.
#   Avoids sleep() (explicitly forbidden in assignment).
# ============================================================================

@pytest.mark.buttons
def test_busy_gating_blocks_update_and_pull(client, tmp_path, monkeypatch):
    from app.pages import pull_state

    # Create temporary lock file to simulate running state
    lockfile = tmp_path / ".pull_data.lock"
    lockfile.write_text("running\n", encoding="utf-8")

    # Override lock path in pull_state module
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    # update-analysis should be blocked
    r1 = client.post("/update-analysis")
    assert r1.status_code == 409
    assert r1.get_json() == {"busy": True}

    # pull-data should also be blocked
    r2 = client.post("/pull-data")
    assert r2.status_code == 409
    assert r2.get_json() == {"busy": True}


# ============================================================================
# Test: HTML Success Redirect Path
# ----------------------------------------------------------------------------
# Validates:
# - HTML Accept header returns redirect (302/303)
# - Redirect location ends with /analysis
# - Success path executes cleanly
# ============================================================================

@pytest.mark.buttons
def test_post_pull_data_html_success_redirects_to_analysis():
    from app import create_app

    def ok_pull_data():
        return {"ok": True, "inserted": 5}

    # Minimal fetchers required so /analysis can render if redirect followed
    def fake_fetch_one(_sql):
        return 0

    def fake_fetch_all(_sql):
        return [(0, 0, 0, 0)]

    app = create_app(
        test_config={"TESTING": True},
        deps={
            "pull_data_fn": ok_pull_data,
            "fetch_one_fn": fake_fetch_one,
            "fetch_all_fn": fake_fetch_all,
        },
    )

    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "text/html"})

    assert resp.status_code in (302, 303)
    assert resp.headers["Location"].endswith("/analysis")


# ============================================================================
# Test: HTML Failure Redirect Path
# ----------------------------------------------------------------------------
# Validates:
# - Exceptions in pull_data_fn do not crash app
# - HTML path still redirects
# - Error handling logic is covered
# ============================================================================

@pytest.mark.buttons
def test_post_pull_data_html_failure_redirects_to_analysis():
    from app import create_app

    def bad_pull_data():
        raise RuntimeError("boom")

    def fake_fetch_one(_sql):
        return 0

    def fake_fetch_all(_sql):
        return [(0, 0, 0, 0)]

    app = create_app(
        test_config={"TESTING": True},
        deps={
            "pull_data_fn": bad_pull_data,
            "fetch_one_fn": fake_fetch_one,
            "fetch_all_fn": fake_fetch_all,
        },
    )

    client = app.test_client()

    resp = client.post("/pull-data", headers={"Accept": "text/html"})

    assert resp.status_code in (302, 303)
    assert resp.headers["Location"].endswith("/analysis")


# ============================================================================
# Test: Non-dict Return Normalization
# ----------------------------------------------------------------------------
# Requirement:
#   pull_data_fn may return non-dict values.
#
# Implementation:
#   Route must convert integer return values into:
#       {"ok": True, "inserted": <value>}
#
# Ensures robustness and full coverage of normalization branch.
# ============================================================================

@pytest.mark.buttons
def test_post_pull_data_converts_non_dict_return_to_json_dict():
    from app import create_app

    def pull_data_returns_int():
        return 7

    def fake_fetch_one(_sql):
        return 0

    def fake_fetch_all(_sql):
        return [(0, 0, 0, 0)]

    app = create_app(
        test_config={"TESTING": True},
        deps={
            "pull_data_fn": pull_data_returns_int,
            "fetch_one_fn": fake_fetch_one,
            "fetch_all_fn": fake_fetch_all,
        },
    )

    client = app.test_client()

    # Default Accept header prefers JSON in tests
    resp = client.post("/pull-data")

    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "inserted": 7}


# negative test
@pytest.mark.buttons
def test_pull_data_returns_500_on_loader_error(client):
    # Make pull_data_fn raise an exception via dependency injection
    app = client.application
    app.extensions["deps"]["pull_data_fn"] = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

    resp = client.post("/pull-data", headers={"Accept": "application/json"})
    assert resp.status_code == 500
    data = resp.get_json()
    assert data["ok"] is False
    assert "boom" in data["error"]
