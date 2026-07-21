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
