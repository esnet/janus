import pytest
import requests
import urllib3
import uuid
import os
from xprocess import ProcessStarter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pytest.fixture(scope="module")
def Controller(xprocess):
    # Use a unique DB file for this module to avoid conflicts
    db_file = f"/tmp/janus_test_{uuid.uuid4()}.db"

    class Starter(ProcessStarter):
        pattern = "Running on"
        timeout = 20
        # command to start process
        # Explicitly bind to 127.0.0.1
        args = [
            "janus-ctrl",
            "-b",
            "127.0.0.1",
            "-C",
            "--ssl",
            "--dryrun",
            "-P",
            "/tmp",
            "-db",
            db_file,
        ]

    # ensure process is running and return its logfile
    xprocess.ensure("myserver", Starter)
    yield
    # clean up whole process tree afterwards
    xprocess.getinfo("myserver").terminate()

    # Cleanup DB file
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except Exception:
            pass


auth = ("admin", "admin")
headers = {"Content-type": "application/json"}


def test_get_images(Controller):
    res = requests.get(
        "https://127.0.0.1:5000/api/janus/controller/images", auth=auth, verify=False
    )
    assert res.status_code == 200


def test_get_profiles(Controller):
    res = requests.get(
        "https://127.0.0.1:5000/api/janus/controller/profiles", auth=auth, verify=False
    )
    assert res.status_code == 200


def test_get_nodes(Controller):
    res = requests.get(
        "https://127.0.0.1:5000/api/janus/controller/nodes", auth=auth, verify=False
    )
    assert res.status_code == 200


def test_get_sessions(Controller):
    res = requests.get(
        "https://127.0.0.1:5000/api/janus/controller/active", auth=auth, verify=False
    )
    assert res.status_code == 200


def test_create_profile(Controller):
    pytest.shared = str(uuid.uuid4())
    body = {"settings": {}}
    # Restore clean URL
    res = requests.post(
        f"https://127.0.0.1:5000/api/janus/controller/profiles/host/{pytest.shared}",
        json=body,
        headers=headers,
        auth=auth,
        verify=False,
    )
    assert res.status_code == 200


def test_delete_profile(Controller):
    # Restore clean URL
    res = requests.delete(
        f"https://127.0.0.1:5000/api/janus/controller/profiles/host/{pytest.shared}",
        auth=auth,
        verify=False,
    )
    assert res.status_code == 204 or res.status_code == 200
