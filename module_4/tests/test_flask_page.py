import pytest


@pytest.mark.web
def test_create_app_has_required_routes(app):
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/" in rules
    assert "/analysis" in rules
    assert "/pull-data" in rules
    assert "/update-analysis" in rules


@pytest.mark.web
def test_get_analysis_renders_required_components(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200

    html = resp.data.decode("utf-8")

    # Required page text
    assert "Analysis" in html
    assert "Answer:" in html

    # Required buttons (stable selectors)
    assert 'data-testid="pull-data-btn"' in html
    assert 'data-testid="update-analysis-btn"' in html