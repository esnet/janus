import json
import shutil
from configparser import ConfigParser


from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.lib.sense import SENSEMetaManager
from janus.lib.sense_api_handler import SENSEApiHandler
from janus.lib.sense_utils import SenseUtils
from janus.settings import cfg, SUPPORTED_IMAGES

ENDPOINTS_FILTER = ['k8s-gen5-01.sdsc.optiputer.net',
                    'k8s-gen5-02.sdsc.optiputer.net',
                    'losa4-nrp-01.cenic.net',
                    'k8s-3090-01.clemson.edu',
                    'node-2-8.sdsc.optiputer.net']
DB_FILE_NAME = 'db-test-sense.json'
JANUS_CONF_TEST_FILE = 'janus-sense-test.conf'
JANUS_LOGGING_CONF_TEST_FILE = 'janus-sense-logging-test.conf'
_LOGGER = None


def _init_logger():
    import logging.config
    import os

    # logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
    logging_conf_path = os.path.normpath(os.path.join(os.getcwd(), JANUS_LOGGING_CONF_TEST_FILE))

    if not os.path.exists(logging_conf_path):
        logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), JANUS_LOGGING_CONF_TEST_FILE))

    logging.config.fileConfig(logging_conf_path)
    return logging.getLogger('logger_janus')


def get_logger():
    global _LOGGER

    if not _LOGGER:
        _LOGGER = _init_logger()

    return _LOGGER


log = get_logger()


def get_db_file_path():
    import os

    db_path = os.path.normpath(os.path.join(os.getcwd(), DB_FILE_NAME))

    if not os.path.exists(db_path):
        special_db_path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'db-test-sense-special.json'))

        shutil.copyfile(special_db_path, db_path, follow_symlinks=False)

    if not os.path.exists(db_path):
        raise Exception(f"db file not found ....{DB_FILE_NAME}")

    return db_path


def get_janus_conf_file_path():
    import os

    conf_path = os.path.normpath(os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE))

    if not os.path.exists(conf_path):
        conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), JANUS_CONF_TEST_FILE))

    if not os.path.exists(conf_path):
        raise Exception("db file not found ....")

    return conf_path


class GeneratorDone(Exception):
    pass


class SimpleScript:
    def __init__(self, prefix):
        self.prefix = prefix
        self.context = {"alias": f"fake-alias-{self.prefix}", "uuid": f"fake-uuid-{self.prefix}"}
        self.principals = ["aessiari@lbl.gov"]
        self.nodes = ['k8s-gen5-01.sdsc.optiputer.net', 'k8s-gen5-02.sdsc.optiputer.net']
        self.ips = ['10.251.88.241/28', '10.251.88.242/28']

    def script(self):
        tasks = list()
        tasks.append(self.stask(3910))   # target1
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

    def empty_task(self):
        task = self._create_template(f"{self.prefix}-empty", "handle-sense-instance")
        task['config']['targets'] = []
        return [task]

    def terminate_task(self):
        task = self._create_template(f"{self.prefix}-terminate", "instance-termination-notice")
        return [task]

    def stask(self, vlan):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, None, ['admin']),
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]


class BaseScript(SimpleScript):
    def __init__(self, prefix):
        super().__init__(prefix)

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

    def atask1(self, vlan) -> list:
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin'])
        ]
        return [task]

    def atask2(self, vlan):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def atask3(self, vlan):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin']),
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def atask4(self, vlan):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals),
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin']),
        ]

        return [task]


class ComplexScript(SimpleScript):
    def __init__(self, prefix):
        super().__init__(prefix)

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
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin'])
        ]
        return [task]

    def ptask2(self, vlan):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def ptask3(self, vlan1, vlan2):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, self.ips[0], self.principals),
            self._create_target(self.nodes[1], vlan2, self.ips[1], ['extra_user1'])
        ]

        return [task]

    def ptask4(self, vlan1, vlan2):
        task = self._create_template(self.prefix, "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, None, []),
            self._create_target(self.nodes[1], vlan2, None, ['extra_user2'])
        ]

        return [task]


class TaskGenerator:
    def __init__(self, script):
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


# noinspection PyUnusedLocal
class FakeSENSEApiHandler:
    def __init__(self, gen):
        self.url = 'fake_url'
        self.gen = gen.generate()
        self.last_task = []
        self.task_state_map = dict()
        self.counter = 0
        self.stop_processing = False

    # noinspection PyMethodMayBeStatic
    def post_metadata(self, metadata, domain, name):
        return True

    def retrieve_tasks(self, assigned, status):
        try:
            for task in self.gen:
                self.last_task = task
                task[0]['uuid'] = str(self.counter) + '-' + task[0]['uuid']
                self.counter += 1
                return task if not self.stop_processing else list()
        except GeneratorDone as e:
            # assert len(self.task_state_map) == self.counter
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

        if kwargs['state'] in ['REJECTED', 'WAITING']:
            log.warning(f'faking updating task attempts:{json.dumps(data, indent=2)}:{kwargs}')
            self.stop_processing = True

        return True

    def accept_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets,  message=message)
        return self._update_task(data, uuid=uuid, state='ACCEPTED')

    def reject_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='REJECTED')

    def fail_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='FAILED')

    def wait_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='WAITING')

    def finish_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='FINISHED')


def create_sense_meta_manager(database, config_file, sense_api_handler=None):
    db = DBLayer(path=database)
    pm = ProfileManager(db, None)
    sm = ServiceManager(db)
    cfg.setdb(db, pm, sm)
    parser = ConfigParser(allow_no_value=True)
    parser.read(config_file)

    if 'JANUS' in parser:
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
