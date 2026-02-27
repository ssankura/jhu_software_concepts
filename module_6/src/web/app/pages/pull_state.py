"""
pull_state.py

Provides a lightweight, file-based busy-state mechanism to prevent
multiple simultaneous "Pull Data" executions.

Why a lock file?
----------------
- Works across Flask reloads (unlike in-memory flags).
- Survives separate processes (important in development / CI).
- Easy to reason about and test.
- Does not require external services (e.g., Redis).

Behavior:
---------
If the lock file exists → a pull is considered "running".
If the lock file does not exist → system is idle.
"""

import sys
from pathlib import Path

# --- Path setup: allow autodoc to import modules from /src ---
ROOT = Path(__file__).resolve().parents[2]   # module_4/
SRC = ROOT / "src"                          # module_4/src
sys.path.insert(0, str(SRC))

# ---------------------------------------------------------------------------
# Lock File Location
# ---------------------------------------------------------------------------
# The lock file is placed at the project root (two directories above this file).
# This ensures:
# - It is shared across the entire Flask app
# - It works regardless of which module imports pull_state
# - It is stable during tests and CI runs
#
# Example location:
#   module_4/.pull_data.lock
#
_LOCK_FILE = Path(__file__).resolve().parents[2] / ".pull_data.lock"


# ---------------------------------------------------------------------------
# Busy-State Inspection
# ---------------------------------------------------------------------------

def is_running() -> bool:
    """
    Determine whether a Pull Data operation is currently in progress.

    Returns:
        bool: True if the lock file exists (busy), False otherwise.

    Usage:
        - Called before executing /pull-data
        - Called before executing /update-analysis
        - Used in templates to disable UI buttons when busy
    """
    return _LOCK_FILE.exists()


# ---------------------------------------------------------------------------
# Busy-State Activation
# ---------------------------------------------------------------------------

def start() -> None:
    """
    Activate busy state by creating the lock file.

    This function:
        - Creates a simple text file at the project root.
        - Indicates that a pull operation is in progress.

    Important:
        Must always be paired with `stop()` inside a try/finally
        block to prevent permanent lock state if errors occur.
    """
    _LOCK_FILE.write_text("running\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Busy-State Deactivation
# ---------------------------------------------------------------------------

def stop() -> None:
    """
    Deactivate busy state by removing the lock file.

    Behavior:
        - If the file exists → delete it.
        - If the file does not exist → silently ignore.

    Why ignore FileNotFoundError?
        Prevents crashes if:
        - stop() is called twice
        - Lock file was manually removed
        - Tests clean up before stop() executes
    """
    try:
        _LOCK_FILE.unlink()
    except FileNotFoundError:
        # Safe to ignore: lock already cleared
        pass
