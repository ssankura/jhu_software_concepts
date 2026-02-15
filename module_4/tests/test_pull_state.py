import pytest


@pytest.mark.buttons
def test_pull_state_start_and_stop(tmp_path, monkeypatch):
    from app.pages import pull_state

    lockfile = tmp_path / ".pull_data.lock"
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    assert pull_state.is_running() is False

    pull_state.start()
    assert pull_state.is_running() is True

    pull_state.stop()
    assert pull_state.is_running() is False


@pytest.mark.buttons
def test_pull_state_stop_when_missing_does_not_crash(tmp_path, monkeypatch):
    from app.pages import pull_state

    lockfile = tmp_path / ".pull_data.lock"
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    # ensure missing
    assert lockfile.exists() is False

    # should not raise (covers FileNotFoundError branch)
    pull_state.stop()
    assert pull_state.is_running() is False