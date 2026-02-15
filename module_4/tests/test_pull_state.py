"""
test_pull_state.py

Unit tests for pull_state.py.

Purpose:
--------
Validate the lock-file based busy-state mechanism used to prevent
simultaneous "Pull Data" executions.

These tests ensure:
- Lock file creation works
- Lock file removal works
- Missing lock file does not raise errors
- is_running() reflects correct state

Marked with @pytest.mark.buttons because busy-state gating
directly affects button endpoint behavior.
"""

import pytest


# ============================================================================
# Test: start() creates lock file and stop() removes it
# ----------------------------------------------------------------------------
# Validates:
# - is_running() returns False initially
# - start() creates lock file
# - is_running() returns True after start()
# - stop() removes lock file
# - is_running() returns False after stop()
#
# Uses monkeypatch to redirect the lock file path to a temporary location
# so the real project lock file is never touched.
# ============================================================================

@pytest.mark.buttons
def test_pull_state_start_and_stop(tmp_path, monkeypatch):
    from app.pages import pull_state

    # Redirect lock file path to temp directory
    lockfile = tmp_path / ".pull_data.lock"
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    # Initially no lock file → not running
    assert pull_state.is_running() is False

    # start() should create the lock file
    pull_state.start()
    assert pull_state.is_running() is True

    # stop() should remove the lock file
    pull_state.stop()
    assert pull_state.is_running() is False


# ============================================================================
# Test: stop() does not crash if lock file missing
# ----------------------------------------------------------------------------
# Covers FileNotFoundError branch in stop():
#
#   try:
#       _LOCK_FILE.unlink()
#   except FileNotFoundError:
#       pass
#
# Ensures:
# - Calling stop() when lock file doesn't exist is safe
# - No exception is raised
# - System remains in non-running state
# ============================================================================

@pytest.mark.buttons
def test_pull_state_stop_when_missing_does_not_crash(tmp_path, monkeypatch):
    from app.pages import pull_state

    lockfile = tmp_path / ".pull_data.lock"
    monkeypatch.setattr(pull_state, "_LOCK_FILE", lockfile)

    # Ensure file does not exist
    assert lockfile.exists() is False

    # stop() should NOT raise an exception
    pull_state.stop()

    # System should remain not running
    assert pull_state.is_running() is False
