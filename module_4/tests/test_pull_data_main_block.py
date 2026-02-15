import runpy
import pytest


@pytest.mark.db
def test_pull_data___main___raises_systemexit():
    # Running as __main__ calls SystemExit(main()).
    # In this test environment, pull_data.main() returns 99
    # because src/.venv/bin/python does not exist.
    with pytest.raises(SystemExit) as ex:
        runpy.run_module("pull_data", run_name="__main__")

    assert ex.value.code == 99
