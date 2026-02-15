import re
import pytest

PCT_RE = re.compile(r"\d+\.\d{2}%")


@pytest.mark.analysis
def test_page_includes_answer_labels(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "Answer:" in html


@pytest.mark.analysis
def test_percentages_render_with_two_decimals(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")

    print(html[html.find("Percent International"):html.find("Percent International") + 200])

    # Must contain at least one two-decimal percentage like 12.34%
    assert PCT_RE.search(html), "Expected a percentage formatted with two decimals (e.g., 39.28%)"