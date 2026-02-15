"""
pull_state.py

A tiny lock-file based mechanism to prevent multiple simultaneous "Pull Data" runs.
Using a file works across Flask reloads and is easy to reason about.
"""

from pathlib import Path

_LOCK_FILE = Path(__file__).resolve().parents[2] / ".pull_data.lock"


def is_running() -> bool:
    """Return True if Pull Data is currently running."""
    return _LOCK_FILE.exists()


def start() -> None:
    """Create the lock file to indicate Pull Data is running."""
    _LOCK_FILE.write_text("running\n", encoding="utf-8")


def stop() -> None:
    """Remove the lock file (ignore if missing)."""
    try:
        _LOCK_FILE.unlink()
    except FileNotFoundError:
        pass
