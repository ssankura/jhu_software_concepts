"""
test_pull_data_unit.py

Full branch coverage tests for pull_data.py.

These tests validate:
- _run() helper behavior
- All early-return branches inside main()
- File existence checks
- Environment variable validation
- Proper return-code propagation
- Lock release (stop()) on success

All tests are marked with @pytest.mark.db per assignment requirements.
"""

import os
from pathlib import Path
import pytest


# ================================================================
# Test: _run() returns subprocess returncode
# ---------------------------------------------------------------
# Ensures:
# - subprocess.run is called with expected arguments
# - returncode is propagated correctly
# - capture_output and text flags are True
# ================================================================

@pytest.mark.db
def test_run_returns_returncode_and_logs(monkeypatch, tmp_path):
    import pull_data

    # Fake subprocess.run result object
    class Proc:
        returncode = 7
        stdout = "hello"
        stderr = "oops"

    def fake_run(cmd, cwd=None, capture_output=None, text=None):
        # Validate parameters passed to subprocess
        assert isinstance(cmd, list)
        assert str(tmp_path) in str(cwd)
        assert capture_output is True
        assert text is True
        return Proc()

    # Replace subprocess.run with fake version
    monkeypatch.setattr(pull_data.subprocess, "run", fake_run)

    rc = pull_data._run(["echo", "x"], cwd=tmp_path, step_name="step")
    assert rc == 7


# ================================================================
# Test: main() returns 99 when venv python missing
# ---------------------------------------------------------------
# First guard in main():
#   if not venv_python.exists(): return 99
#
# We simulate missing .venv/bin/python by redirecting __file__
# to a temporary directory with no virtual environment.
# ================================================================

@pytest.mark.db
def test_pull_data_main_returns_99_when_venv_missing(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    rc = pull_data.main()
    assert rc == 99


# ================================================================
# Test: main() returns 2 when DATABASE_URL missing
# ---------------------------------------------------------------
# After venv check passes, main() validates environment variable.
# If missing, it returns 2.
# ================================================================

@pytest.mark.db
def test_pull_data_main_returns_2_when_database_url_missing(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))

    # Create fake venv python so first check passes
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("#!/bin/sh\necho hi\n", encoding="utf-8")

    monkeypatch.delenv("DATABASE_URL", raising=False)

    rc = pull_data.main()
    assert rc == 2


# ================================================================
# Test: scrape step failure propagates return code
# ---------------------------------------------------------------
# If scrape (_run) returns non-zero,
# main() immediately returns that code.
# ================================================================

@pytest.mark.db
def test_pull_data_main_scrape_failure_returns_rc(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    # Simulate scrape failure
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: 13)

    rc = pull_data.main()
    assert rc == 13


# ================================================================
# Test: scrape succeeds but JSON missing -> return 5
# ---------------------------------------------------------------
# If applicant_data.json is not produced,
# main() returns 5.
# ================================================================

@pytest.mark.db
def test_pull_data_main_missing_scraped_json_returns_5(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    # Scrape step succeeds but file not created
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: 0)

    (tmp_path / "module_2").mkdir(parents=True, exist_ok=True)

    rc = pull_data.main()
    assert rc == 5


# ================================================================
# Test: clean step failure propagates code
# ================================================================

@pytest.mark.db
def test_pull_data_main_clean_failure_returns_rc(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    module_2 = tmp_path / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    # scrape ok, clean fails
    seq = iter([0, 21])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 21


# ================================================================
# Test: standardize step failure propagates code
# ================================================================

@pytest.mark.db
def test_pull_data_main_standardize_failure_returns_rc(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    module_2 = tmp_path / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    seq = iter([0, 0, 31])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 31


# ================================================================
# Test: missing LLM JSON returns 6
# ================================================================

@pytest.mark.db
def test_pull_data_main_missing_llm_json_returns_6(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    module_2 = tmp_path / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    seq = iter([0, 0, 0])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 6


# ================================================================
# Test: load step failure propagates return code
# ================================================================

@pytest.mark.db
def test_pull_data_main_load_failure_returns_rc(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    module_2 = tmp_path / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (module_2 / "applicant_data_final.json").write_text("[]", encoding="utf-8")

    seq = iter([0, 0, 0, 44])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 44


# ================================================================
# Test: full success path returns 0 and releases lock
# ---------------------------------------------------------------
# Ensures:
# - All subprocess steps succeed
# - main() returns 0
# - stop() is called exactly once (lock released)
# ================================================================

@pytest.mark.db
def test_pull_data_main_success_returns_0_and_calls_stop(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    module_2 = tmp_path / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (module_2 / "applicant_data_final.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: 0)

    stopped = {"called": 0}
    monkeypatch.setattr(
        pull_data,
        "stop",
        lambda: stopped.__setitem__("called", stopped["called"] + 1),
    )

    rc = pull_data.main()
    assert rc == 0
    assert stopped["called"] == 1
