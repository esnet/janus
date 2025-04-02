import json
import logging
from configparser import ConfigParser

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.lib.sense import SENSEMetaManager
from janus.lib.sense_api_handler import SENSEApiHandler
from janus.lib.sense_utils import SenseUtils
from janus.settings import cfg, SUPPORTED_IMAGES

log = logging.getLogger(__name__)


class GeneratorDone(Exception):
    pass


class BaseScript:
    def __init__(self):
        self.context = {"alias": "fake", "uuid": "base"}
        self.principals = ["aessiari@lbl.gov"]

        self.nodes = ['k8s-gen5-01.sdsc.optiputer.net', 'k8s-gen5-02.sdsc.optiputer.net']
        # self.nodes = ['sandie-3.ultralight.org', 'sandie-5.ultralight.org']
        self.ips = ['10.251.88.241/28', '10.251.88.242/28']

    def script(self):
        tasks = list()
        tasks.append(self.atask1(3910))   # target1
        tasks.append(self.atask2(3910))   # target2 with same vlan
        tasks.append(self.empty_task())
        tasks.append(self.atask3(3910))   # target3 and target2 with same vlan
        tasks.append(self.atask4(3910))   # change the order
        tasks.append(self.empty_task())
        tasks.append(self.terminate_task())
        return tasks

    @staticmethod
    def _create_target(name, vlan, ip, principals):
        target = {
            "name": name,
            "vlan": vlan,
            "ip": ip,
            "principals": principals
        }

        return target

    def _create_template(self, uuid, command):
        template = {
            'config': {
                "command": command,
                "targets": [],
                "context": self.context
            },
            'uuid': uuid
        }

        return template

    def atask1(self, vlan) -> list:
        task = self._create_template("atask1", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin'])
        ]
        return [task]

    def atask2(self, vlan):
        task = self._create_template("atask2", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def atask3(self, vlan):
        task = self._create_template("atask3", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin']),
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def atask4(self, vlan):
        task = self._create_template("atask4", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals),
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin']),
        ]

        return [task]

    def empty_task(self):
        task = self._create_template("atask5", "handle-sense-instance")
        task['config']['targets'] = []
        return [task]

    def terminate_task(self):
        task = self._create_template("atask_terminate", "instance-termination-notice")
        return [task]


class ComplexScript(BaseScript):
    def __init__(self):
        super().__init__()
        self.context = {"alias": "fake", "uuid": "cplex"}

    def script(self):
        tasks = list()
        tasks.append(self.ptask1(3910))  # target1
        tasks.append(self.ptask2(3910))  # target2 with same vlan
        tasks.append(self.ptask1(3910))  # REPLAY

        tasks.append(self.ptask1(3912))  # two vlans
        tasks.append(self.ptask1(3912))  # replay

        tasks.append(self.ptask1(3909))
        tasks.append(self.ptask2(3909))  # back to one vlan
        tasks.append(self.ptask2(3909))  # REPLAY

        tasks.append(self.ptask3(3915, 3916))  # NEW VLANS AND IPS

        tasks.append(self.ptask4(3915, 3916))  # NO IPS

        tasks.append(self.terminate_task())
        return tasks

    def ptask1(self, vlan) -> list:
        task = self._create_template("ptask1", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin'])
        ]
        return [task]

    def ptask2(self, vlan):
        task = self._create_template("ptask2", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def ptask3(self, vlan1, vlan2):
        task = self._create_template("ptask3", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, self.ips[0], self.principals),
            self._create_target(self.nodes[1], vlan2, self.ips[1], ['extra_user1'])
        ]

        return [task]

    def ptask4(self, vlan1, vlan2):
        task = self._create_template("ptask4", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, None, []),
            self._create_target(self.nodes[1], vlan2, None, ['extra_user2'])
        ]

        return [task]

    def terminate_task(self):
        task = self._create_template("ptask_terminate", "instance-termination-notice")
        return [task]


class TaskGenerator:
    def __init__(self, script=None):
        script = script or BaseScript()
        self.tasks = script.script()

    def generate(self):
        for i in self.tasks:
            yield i

        raise GeneratorDone()


class NoopSENSEApiHandler(SENSEApiHandler):
    def __init__(self):
        super().__init__('noop_url')

    def retrieve_tasks(self, assigned, status):
        pass

    def post_metadata(self, metadata, domain, name):
        return False


class FakeSENSEApiHandler(SENSEApiHandler):
    def __init__(self, gen):
        super().__init__('fake_url')
        self.gen = gen.generate()
        self.last_task = []
        self.task_state_map = dict()
        self.counter = 0

    def post_metadata(self, metadata, domain, name):
        return True

    def retrieve_tasks(self, assigned, status):
        try:
            for task in self.gen:
                self.last_task = task
                task[0]['uuid'] = str(self.counter) + '-' + task[0]['uuid']
                self.counter += 1
                return task
        except GeneratorDone as e:
            assert len(self.task_state_map) == self.counter
            raise e

    def _update_task(self, data, **kwargs):
        assert 'url' in data
        assert 'targets' in data
        assert 'message' in data
        assert 'uuid' in kwargs
        assert 'state' in kwargs

        if kwargs['uuid'] not in self.task_state_map:
            self.task_state_map[kwargs['uuid']] = kwargs['state']
        else:
            self.task_state_map[kwargs['uuid']] += ',' + kwargs['state']

        import json

        log.debug(f'faking updating task attempts:{json.dumps(data, indent=2)}:{kwargs}')
        return True


def create_sense_meta_manager(database, config_file, sense_api_handler=None):
    db = DBLayer(path=database)
    pm = ProfileManager(db, None)
    sm = ServiceManager(db)
    cfg.setdb(db, pm, sm)
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
    return SENSEMetaManager(cfg, sense_properties, sense_api_handler=sense_api_handler)


def load_images_if_needed(db, image_table):
    if not db.all(image_table):
        for img in SUPPORTED_IMAGES:
            db.upsert(image_table, img, 'name', img['name'])


def load_nodes_if_needed(db, node_table, node_name_filter):
    if not db.all(node_table):
        log.info(f"Loading nodes ....")
        kube_api = KubernetesApi()
        clusters = kube_api.get_nodes(refresh=True)

        for cluster in clusters:
            if node_name_filter:
                filtered_nodes = list()

                for node in cluster['cluster_nodes']:
                    if node['name'] in node_name_filter:
                        filtered_nodes.append(node)

                cluster['cluster_nodes'] = filtered_nodes
                cluster['users'] = list()

            cluster['allocated_ports'] = list()
            db.upsert(node_table, cluster, 'name', cluster['name'])

        cluster_names = [cluster['name'] for cluster in clusters]
        log.info(f"saved nodes to db from cluster={cluster_names}")


def dump_janus_sessions(janus_sessions):
    janus_session_summaries = []

    for janus_session in janus_sessions:
        service_info = SenseUtils.get_service_info(janus_session)
        janus_session_summaries.append(dict(id=janus_session['id'], service_info=service_info))

    print(f"JanusSessionSummaries:", json.dumps(janus_session_summaries, indent=2))
