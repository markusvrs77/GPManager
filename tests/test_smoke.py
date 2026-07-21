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
    assert "chart.umd.min.js" in dash
    assert "chart.umd.min.js" in maint
    assert "chart.umd.min.js" not in conns


CDN_HOSTS = ["cdn.jsdelivr.net", "fonts.googleapis.com", "fonts.gstatic.com",
             "unpkg.com", "cdnjs.cloudflare.com"]


@pytest.mark.parametrize("path", GET_ROUTES)
def test_no_external_cdn_references(client, path):
    """Offline / RHEL target: all assets must be self-hosted."""
    html = client.get(path).get_data(as_text=True)
    for host in CDN_HOSTS:
        assert host not in html, f"{host} referenced on {path}"


def test_single_session_limits_function(client):
    html = client.get("/dashboard").get_data(as_text=True)
    assert html.count("async function loadSessionLimitsStats(") == 1


def test_openpyxl_is_declared():
    reqs = open("requirements.txt", encoding="utf-8").read().lower()
    assert "openpyxl" in reqs
