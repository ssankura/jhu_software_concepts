import os
import pytest

from app.db import get_database_url


@pytest.mark.db
def test_get_database_url_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError):
        get_database_url()