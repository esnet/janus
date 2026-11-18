import pytest
from janus.app import app, init
from janus.settings import cfg
from janus.api.db import DBLayer
from janus.api.profile import ProfileManager
from janus.api.manager import ServiceManager
from janus import settings
import os
import base64


@pytest.fixture(scope="session", autouse=True)
def setup_janus():
    # Setup minimal config for testing
    db_path = "./test_db.json"
    profiles_path = "janus/config/profiles"

    if os.path.exists(db_path):
        os.remove(db_path)

    db = DBLayer(path=db_path)
    pm = ProfileManager(db, profiles_path)
    sm = ServiceManager(db)
    cfg.setdb(db, pm, sm)
    cfg._controller = True
    cfg._agent = True

    # Initialize the app with blueprints
    init(app)

    yield

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def auth_header():
    return {
        "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode("ascii")
    }


def test_openapi_spec(client):
    """Test that the OpenAPI spec is generated correctly."""
    response = client.get("/openapi/openapi.json")
    assert response.status_code == 200
    spec = response.get_json()
    assert spec["openapi"].startswith("3.1")
    paths = spec["paths"].keys()
    assert any("/active" in p for p in paths)


def test_active_sessions_empty(client, auth_header):
    """Test getting active sessions when empty."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/active"

    response = client.get(url, headers=auth_header)
    assert response.status_code == 200
    assert response.get_json() == []


def test_agent_node_info(client):
    """Test getting agent node info."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/agent/node"

    response = client.get(url)
    assert response.status_code == 200
    data = response.get_json()
    assert "cpu" in data
    assert "mem" in data


def test_get_token(client, auth_header):
    """Test getting a JWT token."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/token"

    response = client.post(url, headers=auth_header)
    assert response.status_code == 200
    data = response.get_json()
    assert "access_token" in data
    return data["access_token"]


def test_check_token(client, auth_header):
    """Test checking a JWT token."""
    token = test_get_token(client, auth_header)
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/token"

    headers = {"Authorization": f"Bearer {token}"}
    response = client.get(url, headers=headers)
    assert response.status_code == 200
    assert response.get_json() == {"logged_in_as": "admin"}


def test_get_nodes(client, auth_header):
    """Test getting nodes."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/nodes"

    response = client.get(url, headers=auth_header)
    assert response.status_code == 200
    assert isinstance(response.get_json(), list)


def test_get_images(client, auth_header):
    """Test getting images."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/images"

    response = client.get(url, headers=auth_header)
    assert response.status_code == 200
    assert isinstance(response.get_json(), list)


def test_get_profiles(client, auth_header):
    """Test getting profiles."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/profiles"

    response = client.get(url, headers=auth_header)
    assert response.status_code == 200
    assert isinstance(response.get_json(), list)


def test_get_profiles_host(client, auth_header):
    """Test getting profiles for host resource."""
    prefix = getattr(settings, "API_PREFIX", "") or ""
    url = prefix + "/janus/controller/profiles/host"

    response = client.get(url, headers=auth_header)
    assert response.status_code == 200
    assert isinstance(response.get_json(), list)
