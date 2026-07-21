import pytest

GET_ROUTES = [
    "/",
    "/dashboard",
    "/connections",
    "/objects",
    "/maintenance",
    "/vacuum",
    "/gpcopy",
]

REDIRECT_ROUTES = [
    "/skew",
    "/reorganize",
]


@pytest.mark.parametrize("path", GET_ROUTES)
def test_get_route_returns_200(client, path):
    resp = client.get(path)
    assert resp.status_code == 200


@pytest.mark.parametrize("path", REDIRECT_ROUTES)
def test_legacy_route_redirects_to_maintenance(client, path):
    resp = client.get(path)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/maintenance")


def test_bootstrap_loaded_exactly_once(client):
    html = client.get("/").get_data(as_text=True)
    assert html.count("bootstrap.bundle.min.js") == 1


def test_chartjs_only_on_charting_pages(client):
    dash = client.get("/dashboard").get_data(as_text=True)
    maint = client.get("/maintenance").get_data(as_text=True)
    conns = client.get("/connections").get_data(as_text=True)
    assert "chart.js" in dash
    assert "chart.js" in maint
    assert "chart.js" not in conns


def test_single_session_limits_function(client):
    html = client.get("/dashboard").get_data(as_text=True)
    assert html.count("async function loadSessionLimitsStats(") == 1


def test_openpyxl_is_declared():
    reqs = open("requirements.txt", encoding="utf-8").read().lower()
    assert "openpyxl" in reqs
