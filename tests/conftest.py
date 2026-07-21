import pytest

from app import app as flask_app
from db import init_db


@pytest.fixture(scope="session", autouse=True)
def _init_metadata_db():
    # init_db() runs only under __main__ in app.py, so tests must init explicitly.
    init_db()


@pytest.fixture
def client():
    flask_app.config.update(TESTING=True)
    with flask_app.test_client() as c:
        yield c
