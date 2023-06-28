import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_get_images():
   res = requests.get('https://localhost:5001/api/janus/controller/images', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 200

def test_get_profiles():
   res = requests.get('https://localhost:5001/api/janus/controller/profiles', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 200

def test_get_nodes():
   res = requests.get('https://localhost:5001/api/janus/controller/nodes', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 200

def test_delete_profile():
   res = requests.delete('https://localhost:5001/api/janus/controller/profiles/test12', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 204

def test_delete_session1():
   res = requests.delete('https://localhost:5001/api/janus/controller/active/3?force=true', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 204

def test_delete_session2():
   res = requests.delete('https://localhost:5001/api/janus/controller/active/4?force=true', auth=('admin', 'admin'), verify=False)
   assert res.status_code == 204