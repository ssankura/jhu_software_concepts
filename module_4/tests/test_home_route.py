import pytest


@pytest.mark.web
def test_home_route_calls_analysis(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "Analysis" in html