"""
test_pull_data_main.py

Unit test for pull_data.py when executed as a script.

Goal:
-----
Verify that running pull_data as a module via:

    python -m pull_data

correctly raises SystemExit with the expected exit code.

Why this matters:
-----------------
pull_data.py ends with:

    if __name__ == "__main__":
        raise SystemExit(main())

This means:
- The script does NOT return normally.
- It always exits via SystemExit.
- The exit code equals whatever main() returns.

In the test environment:
- .venv/bin/python does not exist.
- Therefore main() returns 99.
- That value becomes the SystemExit exit code.

This test ensures:
- __main__ execution path is covered.
- Exit behavior matches design.
- The failure branch (missing venv python) is validated.
"""

import runpy
import pytest


# ============================================================================
# Test: Running pull_data as __main__ raises SystemExit
# ----------------------------------------------------------------------------
# We use runpy.run_module(..., run_name="__main__") to simulate:
#
#     python pull_data.py
#
# Instead of importing the module normally.
#
# Expected behavior:
#   - main() runs
#   - returns 99 (venv python missing)
#   - SystemExit(99) is raised
# ============================================================================

@pytest.mark.db
def test_pull_data___main___raises_systemexit():

    # Expect SystemExit because pull_data.py calls:
    # raise SystemExit(main())
    with pytest.raises(SystemExit) as ex:
        runpy.run_module("pull_data", run_name="__main__")

    # Exit code should match main() return value (99 in test env)
    assert ex.value.code == 99
