import os
from pathlib import Path

import pytest


@pytest.mark.db
def test_run_returns_returncode_and_logs(monkeypatch, tmp_path):
    import pull_data

    # Fake subprocess.run result
    class Proc:
        returncode = 7
        stdout = "hello"
        stderr = "oops"

    def fake_run(cmd, cwd=None, capture_output=None, text=None):
        assert isinstance(cmd, list)
        assert str(tmp_path) in str(cwd)
        assert capture_output is True
        assert text is True
        return Proc()

    monkeypatch.setattr(pull_data.subprocess, "run", fake_run)

    rc = pull_data._run(["echo", "x"], cwd=tmp_path, step_name="step")
    assert rc == 7


@pytest.mark.db
def test_pull_data_main_returns_99_when_venv_missing(monkeypatch, tmp_path):
    import pull_data

    # Force module_3_dir to tmp_path by patching __file__
    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))

    # Ensure DATABASE_URL exists so we don't fail earlier
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    # venv python should be missing: tmp_path/.venv/bin/python does not exist
    rc = pull_data.main()
    assert rc == 99


@pytest.mark.db
def test_pull_data_main_returns_2_when_database_url_missing(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))

    # Create fake venv python so we pass venv existence check
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("#!/bin/sh\necho hi\n", encoding="utf-8")

    monkeypatch.delenv("DATABASE_URL", raising=False)

    rc = pull_data.main()
    assert rc == 2


@pytest.mark.db
def test_pull_data_main_scrape_failure_returns_rc(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    # Create fake venv python
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    # First step fails
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: 13)

    rc = pull_data.main()
    assert rc == 13


@pytest.mark.db
def test_pull_data_main_missing_scraped_json_returns_5(monkeypatch, tmp_path):
    import pull_data

    monkeypatch.setattr(pull_data, "__file__", str(tmp_path / "pull_data.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake")

    # venv exists
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("x", encoding="utf-8")

    # scrape succeeds but output file not created
    calls = {"n": 0}

    def fake_run(*a, **k):
        calls["n"] += 1
        return 0  # scrape success

    monkeypatch.setattr(pull_data, "_run", fake_run)

    # Ensure module_2 dir exists so paths resolve
    (tmp_path / "module_2").mkdir(parents=True, exist_ok=True)

    rc = pull_data.main()
    assert rc == 5
    assert calls["n"] == 1


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

    # Create scraped_json so it passes existence check after scrape
    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    # scrape ok, clean fails
    seq = iter([0, 21])

    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 21


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

    # scrape ok, clean ok, standardize fails
    seq = iter([0, 0, 31])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 31


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

    # scrape ok, clean ok, standardize ok, but applicant_data_final.json missing
    seq = iter([0, 0, 0])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 6


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

    # scrape ok, clean ok, standardize ok, load fails
    seq = iter([0, 0, 0, 44])
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: next(seq))

    rc = pull_data.main()
    assert rc == 44


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

    # All steps succeed
    monkeypatch.setattr(pull_data, "_run", lambda *a, **k: 0)

    stopped = {"called": 0}
    monkeypatch.setattr(pull_data, "stop", lambda: stopped.__setitem__("called", stopped["called"] + 1))

    rc = pull_data.main()
    assert rc == 0
    assert stopped["called"] == 1
