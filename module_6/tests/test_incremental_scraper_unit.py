from __future__ import annotations

import types
from pathlib import Path

import pytest

import etl.incremental_scraper as inc


class _Proc:
    def __init__(self, rc=0, out="", err=""):
        self.returncode = rc
        self.stdout = out
        self.stderr = err


@pytest.mark.worker
def test_run_logs_stdout_and_stderr(monkeypatch, tmp_path):
    calls = {}

    def fake_run(cmd, cwd, capture_output, text, check):
        calls["cmd"] = cmd
        calls["cwd"] = cwd
        assert capture_output is True
        assert text is True
        assert check is False
        return _Proc(rc=7, out="ok-out", err="bad-err")

    monkeypatch.setattr(inc.subprocess, "run", fake_run)

    rc = inc._run(["python", "x.py"], tmp_path, "x.py")
    assert rc == 7
    assert calls["cmd"] == ["python", "x.py"]
    assert calls["cwd"] == str(tmp_path)


@pytest.mark.worker
def test_main_returns_99_when_venv_missing(monkeypatch, tmp_path):
    # fake __file__ parent = tmp_path, so .venv/bin/python doesn't exist
    monkeypatch.setattr(inc, "__file__", str(tmp_path / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    rc = inc.main()
    assert rc == 99


@pytest.mark.worker
def test_main_returns_2_when_db_url_missing(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.delenv("DATABASE_URL", raising=False)

    rc = inc.main()
    assert rc == 2


@pytest.mark.worker
def test_main_scrape_failure_returns_code(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")

    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: 3)
    rc = inc.main()
    assert rc == 3


@pytest.mark.worker
def test_main_missing_scraped_json_returns_5(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: 0)

    rc = inc.main()
    assert rc == 5


@pytest.mark.worker
def test_main_clean_failure_returns_code(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    scraped = module_2 / "applicant_data.json"
    scraped.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")

    seq = iter([0, 4])  # scrape ok, clean fail
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: next(seq))

    rc = inc.main()
    assert rc == 4


@pytest.mark.worker
def test_main_std_merge_failure_returns_code(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")

    seq = iter([0, 0, 9])  # scrape ok, clean ok, std_merge fail
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: next(seq))

    rc = inc.main()
    assert rc == 9


@pytest.mark.worker
def test_main_missing_llm_json_returns_6(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: 0)

    rc = inc.main()
    assert rc == 6


@pytest.mark.worker
def test_main_load_failure_returns_code(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (module_2 / "applicant_data_final.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")

    seq = iter([0, 0, 0, 11])  # load fails
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: next(seq))

    rc = inc.main()
    assert rc == 11


@pytest.mark.worker
def test_main_success_returns_0(monkeypatch, tmp_path):
    module_3 = tmp_path
    module_2 = module_3 / "module_2"
    module_2.mkdir(parents=True, exist_ok=True)
    venv_py = module_3 / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True, exist_ok=True)
    venv_py.write_text("", encoding="utf-8")

    (module_2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (module_2 / "applicant_data_final.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(inc, "__file__", str(module_3 / "incremental_scraper.py"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(inc, "_run", lambda *_a, **_k: 0)

    rc = inc.main()
    assert rc == 0
