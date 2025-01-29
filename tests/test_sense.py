import logging.config
import os
from configparser import ConfigParser

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.lib.sense import SENSEMetaManager
from janus.lib.sense_api_handler import SENSEApiHandler
from janus.lib.sense_utils import SenseUtils
from janus.settings import cfg, SUPPORTED_IMAGES

logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)


class SENSEFakeTasks:
    fakeit = True

    # noinspection PyUnusedLocal
    @staticmethod
    def fake_handle_instance_tasks():
        atask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "k8s-gen5-01.sdsc.optiputer.net",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": "10.251.88.241/28",
                        "portName": "vlan.1786",  # "portName": "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": "10.251.88.242/28",
                        "portName": "vlan.1786",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "atask",
                    "uuid": "atask"
                }},
            'uuid': "atask"
        }

        btask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "k8s-gen5-01.sdsc.optiputer.net",
                        "vlan": 1787,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1787",  # "portName": "?name?",
                        "principals": [
                            "aessiari@lbl.gov",
                            "test_user@lbl.gov",
                            "test_user2@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 1787,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1787",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "binstance",
                    "uuid": "binstance"
                }},
            'uuid': "btask"
        }

        ctask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "k8s-gen5-01.sdsc.optiputer.net",
                        "vlan": 1790,
                        "bw": 1000,
                        "ip": "10.251.88.4/28",
                        "portName": "vlan.1790",  # "portName": "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 1791,
                        "bw": 1000,
                        "ip": "10.251.88.6/28",
                        "portName": "?name?",  # "vlan.1791",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "cinstance",
                    "uuid": "cinstance"
                }},
            'uuid': "ctask"
        }

        dtask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "k8s-gen5-01.sdsc.optiputer.net",
                        "vlan": 1788,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1788",  # "portName": "?name?",
                        "principals": [
                            "test_user@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 1789,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1789",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "dinstance",
                    "uuid": "dinstance"
                }},
            'uuid': "dtask"
        }

        etask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "k8s-gen5-01.sdsc.optiputer.net",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1786",  # "portName": "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": "10.251.88.242/28",
                        "portName": "vlan.1786",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "einstance",
                    "uuid": "einstance"
                }},
            'uuid': "etask"
        }

        # atask, dtask and btask worked

        ftask = {
            'config': {
                "command": "handle-sense-instance",
                "targets": [
                    {
                        "name": "mac-en0",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": None,
                        "portName": "vlan.1786",  # "portName": "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    },
                    {
                        "name": "k8s-gen5-02.sdsc.optiputer.net",
                        "vlan": 3911,
                        "bw": 1000,
                        "ip": "10.251.88.242/28",
                        "portName": "vlan.1786",  # "?name?",
                        "principals": [
                            "aessiari@lbl.gov"
                        ]
                    }
                ],
                "context": {
                    "alias": "finstance",
                    "uuid": "finstance"
                }},
            'uuid': "ftask"
        }

        # [ctask, dtask]  two vlans with ip and without ip
        return [ctask, dtask]  # [atask]  # [atask, btask, ctask, dtask, etask]  # [ftask]

    @staticmethod
    def fake_terminate_instance_tasks():
        tasks = list()
        import copy

        for task in SENSEFakeTasks.fake_handle_instance_tasks():
            task = copy.deepcopy(task)
            task['config']['command'] = "instance-termination-notice"
            task['config']['targets'] = []
            task['uuid'] = task['uuid'] + '-' + 'terminate'
            tasks.append(task)

        return tasks


class FakeSENSEApiHandler(SENSEApiHandler):
    def __init__(self):
        super().__init__()

    def retrieve_tasks(self, assigned, status):
        if SENSEFakeTasks.fakeit:
            tasks = list()
            # tasks.extend(SENSEFakeTasks.fake_handle_instance_tasks())
            tasks.extend(SENSEFakeTasks.fake_terminate_instance_tasks())
            return tasks

        return super().retrieve_tasks(assigned, status)

    def _update_task(self, data, **kwargs):
        if SENSEFakeTasks.fakeit:
            log.info(f'faking updating task attempts:{data}:{kwargs}')
            return False

        return super()._update_task(data, **kwargs)


class TestSenseWorkflow:
    def __init__(self, database, config_file, node_name_filter=None):
        db = DBLayer(path=database)

        pm = ProfileManager(db, None)
        sm = ServiceManager(db)
        cfg.setdb(db, pm, sm)
        self.node_name_filter = node_name_filter or list()
        parser = ConfigParser(allow_no_value=True)
        parser.read(config_file)

        config = parser['JANUS']
        cfg.PORTAINER_URI = str(config.get('PORTAINER_URI', None))
        cfg.PORTAINER_WS = str(config.get('PORTAINER_WS', None))
        cfg.PORTAINER_USER = str(config.get('PORTAINER_USER', None))
        cfg.PORTAINER_PASSWORD = str(config.get('PORTAINER_PASSWORD', None))
        vssl = str(config.get('PORTAINER_VERIFY_SSL', 'True'))
        if vssl == 'False':
            cfg.PORTAINER_VERIFY_SSL = False
            import urllib3
            urllib3.disable_warnings()
        else:
            cfg.PORTAINER_VERIFY_SSL = True

        sense_properties = SenseUtils.parse_from_config(cfg=cfg, parser=parser)
        self.mngr = SENSEMetaManager(cfg, sense_properties, sense_api_handler=FakeSENSEApiHandler())

        # self.mngr = SENSEMetaManager(cfg, sense_properties)

        if cfg.sense_metadata:
            cfg.plugins.append(self.mngr)

    def init(self):
        image_table = self.mngr.image_table

        if not self.mngr.db.all(image_table):
            for img in SUPPORTED_IMAGES:
                self.mngr.save_image({"name": img})

        node_table = self.mngr.nodes_table

        if self.mngr.db.all(node_table):
            log.info(f"Nodes already in db .... returning")
            return

        kube_api = KubernetesApi()
        clusters = kube_api.get_nodes(refresh=True)
        assert len(clusters) == 1
        cluster = clusters[0]

        if self.node_name_filter:
            filtered_nodes = list()

            for node in cluster['cluster_nodes']:
                if node['name'] in self.node_name_filter:
                    filtered_nodes.append(node)

            cluster['cluster_nodes'] = filtered_nodes
            cluster['users'] = list()

        cluster['allocated_ports'] = list()

        self.mngr.db.upsert(node_table, cluster, 'name', cluster['name'])
        log.info(f"saved nodes to db from cluster={cluster['name']}")

    def run(self):
        for plugin in self.mngr.cfg.plugins:
            plugin.run()


'''
  # CREATE DB with nodes
  > rm db-test-sense.json
  > python test_sense.py 
  > cat db-test-sense.json | jq .nodes
  
  # create sense instance, create tasks, and run test again
  # this should handle the tasks and create a host profile ...
  > cd tests
  > python test_sense.py 
  > cat db-test-sense.json | jq .host
  > cat db-test-sense.json | jq .sense_instance
  
  # For now cancel sense instance and run test again 
  # this should delete the tasks and delete the host profile and the sense instance in db ....
  > python test_sense.py 
  > cat db-test-sense.json | jq .host
  > cat db-test-sense.json | jq .sense_instance
'''
if __name__ == '__main__':
    db_file_name = 'db-test-sense.json'
    janus_conf_path = 'janus-sense-test.conf'
    filter_nodes = False

    if filter_nodes:
        # urn:ogf:network:nrp-nautilus.io:2020:node-2-8.sdsc.optiputer.net
        endpoints = ['k8s-gen5-01.sdsc.optiputer.net',
                     'k8s-gen5-02.sdsc.optiputer.net',
                     'losa4-nrp-01.cenic.net',
                     'k8s-3090-01.clemson.edu',
                     'node-2-8.sdsc.optiputer.net']
    else:
        endpoints = None

    tsw = TestSenseWorkflow(
        database=os.path.join(os.getcwd(), db_file_name),
        config_file=os.path.join(os.getcwd(), janus_conf_path),
        node_name_filter=endpoints
    )

    tsw.init()
    tsw.run()
