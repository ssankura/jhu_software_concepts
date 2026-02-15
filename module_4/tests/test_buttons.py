import pytest


@pytest.mark.buttons
def test_post_pull_data_triggers_loader(client):
    resp = client.post("/pull-data")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # from our conftest fake_loader: inserted == len(fake_rows) == 2
    assert data["inserted"] == 2


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