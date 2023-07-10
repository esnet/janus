import pytest
import requests
import urllib3
import json
import uuid
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def pytest_namespace():
    return {'shared': None}

auth = ('admin', 'admin')
headers = {'Content-type': 'application/json'}

def test_get_images():
   res = requests.get('https://localhost:5000/api/janus/controller/images', auth=auth, verify=False)
   assert res.status_code == 200

def test_get_profiles():
   res = requests.get('https://localhost:5000/api/janus/controller/profiles', auth=auth, verify=False)
   assert res.status_code == 200

def test_get_nodes():
   res = requests.get('https://localhost:5000/api/janus/controller/nodes?refresh=txrue', auth=auth, verify=False)
   assert res.status_code == 200

def test_create_profile():
   pytest.shared = uuid.uuid4()
   body = {"settings": {}}
   res = requests.post(f'https://localhost:5000/api/janus/controller/profiles/{pytest.shared}', json=body, headers=headers,
                       auth=auth, verify=False)
   assert res.status_code == 200

def test_delete_profile():
   res = requests.delete(f'https://localhost:5000/api/janus/controller/profiles/{pytest.shared}', auth=auth, verify=False)
   assert res.status_code == 204

def test_create_session():
   body = {"instances": ["local"], "profile": "default", "image": "dtnaas/tools"}
   res = requests.post('https://localhost:5000/api/janus/controller/create', json=body, headers=headers,
                       auth=auth, verify=False)
   assert res.status_code == 200
   js = json.loads(res.text)
   pytest.shared = list(js.keys())[0]

def test_delete_session():
   res = requests.delete(f'https://localhost:5000/api/janus/controller/active/{pytest.shared}?force=true', auth=auth, verify=False)
   assert res.status_code == 204
