import pytest
import requests
import urllib3
import json
import uuid
from xprocess import ProcessStarter
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pytest.fixture(scope="module")
def Controller(xprocess):
    class Starter(ProcessStarter):
        pattern = "Serving"
        timeout = 5
        # command to start process
        args = ['janus-ctrl', '-C', '--ssl', '--dryrun', '-P', '/tmp', '-db', '/tmp/janus.db']

    # ensure process is running and return its logfile
    logfile = xprocess.ensure("myserver", Starter)
    yield
    # clean up whole process tree afterwards
    xprocess.getinfo("myserver").terminate()

def pytest_namespace():
    return {'shared': None}

auth = ('admin', 'admin')
headers = {'Content-type': 'application/json'}

def test_get_images(Controller):
    res = requests.get('https://localhost:5000/api/janus/controller/images', auth=auth, verify=False)
    assert res.status_code == 200

def test_get_profiles(Controller):
    res = requests.get('https://localhost:5000/api/janus/controller/profiles', auth=auth, verify=False)
    assert res.status_code == 200

def test_get_nodes(Controller):
    res = requests.get('https://localhost:5000/api/janus/controller/nodes?refresh=true', auth=auth, verify=False)
    assert res.status_code == 200

def test_get_sessions(Controller):
    res = requests.get('https://localhost:5000/api/janus/controller/active', auth=auth, verify=False)
    assert res.status_code == 200

def test_create_profile(Controller):
    pytest.shared = uuid.uuid4()
    body = {"settings": {}}
    res = requests.post(f'https://localhost:5000/api/janus/controller/profiles/host/{pytest.shared}', json=body, headers=headers,
                        auth=auth, verify=False)
    assert res.status_code == 200

#def test_create_node(Controller):
#    pytest.shared = uuid.uuid4()
#    body = {"": {}}
#    res = requests.post(f'https://localhost:5000/api/janus/controller/nodes/{pytest.node}', json=body, headers=headers,
#                        auth=auth, verify=False)
#    assert res.status_code == 200

#def test_create_session(Controller):
#    body = {"instances": ["local"], "profile": f"{pytest.shared}", "image": "dtnaas/tools"}
#    res = requests.post('https://localhost:5000/api/janus/controller/create', json=body, headers=headers,
#                        auth=auth, verify=False)
#    print (res.text)
#    assert res.status_code == 200
#    js = json.loads(res.text)
#    pytest.shared = list(js.keys())[0]

#def test_delete_session(Controller):
#    res = requests.delete(f'https://localhost:5000/api/janus/controller/active/{pytest.shared}?force=true', auth=auth, verify=False)
#    assert res.status_code == 204

def test_delete_profile(Controller):
    res = requests.delete(f'https://localhost:5000/api/janus/controller/profiles/host/{pytest.shared}', auth=auth, verify=False)
    assert res.status_code == 204
