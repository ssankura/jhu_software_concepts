import pytest


@pytest.mark.buttons
def test_post_pull_data_triggers_loader(client):
    resp = client.post("/pull-data")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # from our conftest fake_loader: inserted == len(fake_rows) == 2
    assert data["inserted"] == 2

    # --- NEW: HTML path (covers redirect + flash lines) ---
    resp2 = client.post(
        "/pull-data",
        headers={"Accept": "text/html"},
        follow_redirects=True,
    )
    assert resp2.status_code == 200
    html = resp2.data.decode("utf-8")
    assert "Pull Data completed" in html


@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(client):
    resp = client.post("/update-analysis")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True


@pytest.mark.buttons
def test_busy_gating_blocks_update_and_pull(client, tmp_path, monkeypatch):
    # Force busy by pointing pull_state lock file to a temp file and creating it.
    from app.pages import pull_state

    lockfile = tmp_path / ".pull_data.lock"
    lockfile.write_text("running\n", encoding="utf-8")
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    # When busy, update-analysis returns 409
    r1 = client.post("/update-analysis")
    assert r1.status_code == 409
    assert r1.get_json() == {"busy": True}

    # When busy, pull-data returns 409
    r2 = client.post("/pull-data")
    assert r2.status_code == 409
    assert r2.get_json() == {"busy": True}

import pytest


@pytest.mark.buttons
def test_post_pull_data_html_success_redirects_to_analysis():
    from app import create_app

    def ok_pull_data():
        return {"ok": True, "inserted": 5}

    # Minimal fetchers so /analysis can render if redirect is followed
    def fake_fetch_one(_sql):
        return 0

    def fake_fetch_all(_sql):
        # q3_avgs expects a row with 4 numeric-ish values
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

@pytest.mark.buttons
def test_post_pull_data_converts_non_dict_return_to_json_dict():
    from app import create_app

    # Return an int (NOT a dict) so analysis.py converts it to {"ok": True, "inserted": <int>}
    def pull_data_returns_int():
        return 7

    # These aren't used by /pull-data JSON response, but create_app expects deps keys sometimes
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

    resp = client.post("/pull-data")  # default Accept prefers JSON in tests
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "inserted": 7}
