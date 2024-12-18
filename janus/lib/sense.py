import json
import logging
import time
from configparser import ConfigParser
from threading import Thread
from functools import reduce
from types import SimpleNamespace

from pydantic import ValidationError
from sense.client.discover_api import DiscoverApi
from sense.client.metadata_api import MetadataApi
from sense.client.requestwrapper import RequestWrapper
from sense.client.task_api import TaskApi
from sense.client.workflow_combined_api import WorkflowCombinedApi
from tinydb import Query

from tinydb.table import Table

from janus.api.db import DBLayer
from janus.settings import JanusConfig
from janus.api.kubernetes import KubernetesApi
from janus.api.models import Node, ContainerProfile, ContainerProfileSettings, NetworkProfile, NetworkProfileSettings

SENSE_METADATA_URL = 'sense-metadata-url'
SENSE_METADATA_ASSIGNED = 'sense-metdata-assigned'
JANUS_DEVICE_MANAGER = 'janus.device.manager'
SENSE_DOMAIN_INFO = 'sense-metdata-domain-info'

log = logging.getLogger(__name__)


class Base(object):
    def __init__(self, cfg: JanusConfig):
        self._cfg = cfg

    @property
    def image_table(self) -> Table:
        return self.db.get_table('images')

    @property
    def network_table(self) -> Table:
        return self.db.get_table('network')

    @property
    def host_table(self) -> Table:
        return self.db.get_table('host')

    @property
    def nodes_table(self) -> Table:
        return self.db.get_table('nodes')

    @property
    def sense_session_table(self) -> Table:
        return self.db.get_table('sense_session')

    @property
    def cfg(self) -> JanusConfig:
        return self._cfg

    @property
    def db(self) -> DBLayer:
        return self.cfg.db

    def save_sense_session(self, sense_instance):
        self.db.upsert(self.sense_session_table, sense_instance, 'key', sense_instance['key'])

    def find_sense_session(self, *, user=None, instance_id=None, name=None, status=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if instance_id:
            queries.append(Query().key == instance_id)

        if name:
            queries.append(Query().name == name)

        if status:
            queries.append(Query().status == status)

        return self.db.search(self.sense_session_table, query=reduce(lambda a, b: a & b, queries))

    def save_image(self, image):
        self.db.upsert(self.image_table, image, 'name', image['name'])

    def find_images(self, *, name):
        images = self.db.search(self.image_table, query=(Query().name == name))
        return images

    def save_network_profile(self, network_profile):
        self.db.upsert(self.network_table, network_profile, 'name', network_profile['name'])

    def save_host_profile(self, host_profile):
        self.db.upsert(self.host_table, host_profile, 'name', host_profile['name'])

    def find_network_profiles(self, *, name):
        network_profiles = self.db.search(self.network_table,
                                          query=(Query().name == name))

        return network_profiles

    def find_host_profiles(self, *, user=None, name=None, net_name=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if name:
            queries.append(Query().name == name)

        if net_name:
            queries.append(Query().settings.mgmt_net.name == net_name)

        host_profiles = self.db.search(self.host_table, query=reduce(lambda a, b: a & b, queries))
        return host_profiles

    # noinspection PyMethodMayBeStatic
    def _update_users(self, resource, users, func):
        resource_users = resource['users'] if 'users' in resource else list()
        resource['users'] = list()
        resource['users'].extend(users)
        resource['users'] = sorted(list(set(resource['users'])))

        if sorted(resource_users) != resource['users']:
            func(resource)

        return resource

    def get_or_create_network_profile(self, name, targets, users, groups=None):
        network_profile_name = name
        network_profiles = self.find_network_profiles(name=network_profile_name)
        vlans = [t['vlan'] for t in targets]
        vlans = list(set(vlans))

        if network_profiles:
            assert len(network_profiles) == 1
            network_profile = self._update_users(network_profiles[0], users, self.save_network_profile)
            return network_profile

        network_profile_settings = {
            "driver": "macvlan",
            "mode": "bridge",
            "enable_ipv6": False,
            "ipam": {
                "config": [
                    # "type": "host-local", TODO We can't have this here ....
                    {"subnet": "10.1.1.10", "gateway": "10.1.1.1"}
                ]
            },
            "options": {
                "vlan": str(vlans[0])
            }
        }

        users = users or list()
        groups = groups or list()
        network_profile = dict(name=network_profile_name, settings=network_profile_settings, users=users, groups=groups)
        log.debug(f'network_profile: {json.dumps(network_profile)}')

        try:
            NetworkProfileSettings(**network_profile['settings'])
            NetworkProfile(**network_profile)
        except ValidationError as e:
            raise e

        self.save_network_profile(network_profile=network_profile)
        return network_profile

    def get_or_create_host_profile(self, name, users, groups=None):
        # network_profile = self.get_or_create_network_profile(name=name, users=users, groups=groups)
        # network_profile = self._update_users(network_profile, users, self.save_network_profile)
        network_profiles = self.find_network_profiles(name=name)
        assert len(network_profiles) == 1

        network_profile_name = network_profiles[0]['name']
        host_profiles = self.find_host_profiles(name=name, net_name=network_profile_name)

        if host_profiles:
            assert len(host_profiles) == 1
            host_profile = self._update_users(host_profiles[0], users, self.save_host_profile)
            return host_profile

        host_profile_settings = {
            "mgmt_net": {
                "name": network_profile_name,
                "ipv4_addr": None,
                "ipv6_addr": None
            }
        }

        # noinspection PyProtectedMember
        default_settings = self.cfg._base_profile.copy()
        default_settings.update((k, host_profile_settings[k])
                                for k in default_settings.keys() & host_profile_settings.keys())

        groups = groups or list()
        host_profile = dict(name=name, settings=default_settings, users=users, groups=groups)
        log.debug(f'host_profile:{json.dumps(host_profile)}')

        try:
            ContainerProfileSettings(**default_settings)
            ContainerProfile(**host_profile)
        except ValidationError as e:
            raise e

        self.save_host_profile(host_profile)
        return host_profile


class SENSEApiHandler:
    def __init__(self, req_wrapper):
        self.workflow_client = WorkflowCombinedApi(req_wrapper=req_wrapper)
        self.discover_client = DiscoverApi(req_wrapper=req_wrapper)
        self.task_client = TaskApi(req_wrapper=req_wrapper)
        self.metadata_client = MetadataApi(req_wrapper=req_wrapper)

    def is_instance_valid(self, si_uuid):
        try:
            status = self.workflow_client.instance_get_status(si_uuid=si_uuid)
            return status in ['CREATE - READY', 'REINSTATE - READY'], status
        except ValueError as ve:
            if 'NOT FOUND' in str(ve).upper():
                return False, "NOT FOUND"

            raise ve

    def find_instance_by_id(self, si_uuid):
        response = self.discover_client.discover_service_instances_get()
        instances = response['instances']

        for instance in instances:
            temp = SimpleNamespace(**instance)

            if temp.referenceUUID == si_uuid:
                instance['intents'] = []

                for intent in temp.intents:
                    intent['json'] = json.loads(intent['json'])
                    instance['intents'].append(intent)

                return instance

        return None

    def retrieve_tasks(self, assigned, status):
        records = self.task_client.get_tasks_agent_status(assigned=assigned, status=status)
        return records

    def accept_task(self, uuid):
        data = None
        status = 'ACCEPTED'
        return self.task_client.update_task(json.dumps(data), uuid=uuid, state=status)

    def finish_task(self, uuid, url):
        data = {"callbackURL": url}
        return self.task_client.update_task(json.dumps(data), uuid=uuid, state='FINISHED')

    def delete_task(self, uuid):
        self.task_client.delete_task(uuid=uuid)

    def get_metadata(self, domain, name):
        return self.metadata_client.get_metadata(domain=domain, name=name)

    def post_metadata(self, metadata, domain, name):
        self.metadata_client.post_metadata(data=json.dumps(metadata), domain=domain, name=name)


class SENSEMetaManager(Base):
    def __init__(self, cfg: JanusConfig, properties: dict):
        super().__init__(cfg)
        self.sense_api_handler = SENSEApiHandler(req_wrapper=RequestWrapper())
        self.kube_api = KubernetesApi()
        self.properties = properties
        log.info(f"Initialized {__name__}")

    def load_cluster_node_map(self):
        clusters = self.db.all(self.nodes_table)
        clusters = [cluster for cluster in clusters if 'cluster_nodes' in cluster]
        node_cluster_map = dict()

        for cluster in clusters:
            for node in cluster['cluster_nodes']:
                cluster_info = dict(cluster_id=cluster['id'], namespace=cluster['namespace'])
                node_cluster_map[node['name']] = cluster_info

        return node_cluster_map

    def retrieve_tasks(self):
        assigned = self.properties[SENSE_METADATA_ASSIGNED]
        tasks = self.sense_api_handler.retrieve_tasks(assigned=assigned, status='PENDING')

        if not tasks:
            return tasks

        contexts = [(task['config']['context']['uuid'], task['config']['context']['alias'],) for task in tasks]
        instance_map = dict((context, list(),) for context in contexts)

        for task in tasks:
            context = (task['config']['context']['uuid'], task['config']['context']['alias'],)
            instance_map[context].append((task['uuid'], task['config']['targets'],))

        node_cluster_map = self.load_cluster_node_map()
        node_names = [n for n in node_cluster_map]
        sense_sessions = list()

        for context, task_list in instance_map.items():
            instance_id = context[0]
            valid, status = self.sense_api_handler.is_instance_valid(si_uuid=instance_id)

            if not valid:
                log.warning(f'instance {context} is {status}')
                continue

            task_info = dict(task_list)
            targets = sum(task_info.values(), [])
            endpoints = [target['name'] for target in targets]

            if len(endpoints) != 2:
                log.warning(f'missing endpoint for instance {context}:endpoints={endpoints}')
                continue

            unknown_endpoints = [endpoint for endpoint in endpoints if endpoint not in node_names]

            if unknown_endpoints:
                log.warning(f'unknown endpoint for instance {context}:endpoints={unknown_endpoints}')
                continue

            clusters = []
            principals = list()

            for target in targets:
                target['cluster_info'] = node_cluster_map[target['name']]
                clusters.append(node_cluster_map[target['name']]['cluster_id'])
                principals.extend(target['principals'])

            alias = context[1]

            # if not alias:
            #     temp = self.sense_api_handler.find_instance_by_id(instance_id)
            #
            #     if temp is None:
            #         log.warning(f'instance not found for {context}:endpoints={endpoints}')
            #         continue
            #
            #     alias = temp['alias']

            if not alias:
                alias = f'sense-janus-{"-".join(instance_id.split("-")[0:2])}'
            else:
                alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

            users = list(set(principals))
            sense_session = dict(key=instance_id,
                                 name=alias,
                                 task_info=task_info,
                                 users=users,
                                 status='PENDING',
                                 clusters=list(set(clusters)))
            self.save_sense_session(sense_instance=sense_session)
            log.debug(f'saved sense instance:{json.dumps(sense_session)}')
            sense_sessions.append(sense_session)

        return sense_sessions

    def accept_tasks(self):
        sense_instances = self.find_sense_session(status='PENDING')

        for sense_instance in sense_instances:
            task_info = sense_instance['task_info']
            uuids = [uuid for uuid in task_info]

            for uuid in uuids:
                self.sense_api_handler.accept_task(uuid)

            sense_instance['status'] = 'ACCEPTED'
            self.save_sense_session(sense_instance=sense_instance)

        return sense_instances

    def finish_tasks(self):
        sense_instances = self.find_sense_session(status='ACCEPTED')

        for sense_instance in sense_instances:
            name = sense_instance['name']
            users = sense_instance['users']
            task_info = sense_instance['task_info']
            targets = sum(task_info.values(), [])
            network_profile = self.get_or_create_network_profile(name=name, targets=targets, users=users)
            assert network_profile is not None
            host_profile = self.get_or_create_host_profile(name=name, users=users)
            assert host_profile is not None
            assert 'settings' in host_profile and 'tools' in host_profile['settings']
            sense_instance['host_profile'] = host_profile['name']
            sense_instance['network_profile'] = network_profile['name']

            for image in sum([self.find_images(name=tool) for tool in host_profile['settings']['tools']], list()):
                self._update_users(resource=image, users=users, func=self.save_image)

            CREATE_NETWORK = False

            if CREATE_NETWORK:
                vlan_infos = [(t['vlan'], t['cluster_info']['cluster_id'],) for t in sum(task_info.values(), [])]
                if 'networks' not in sense_instance:
                    sense_instance['networks'] = list()

                for vlan_info in set(vlan_infos):
                    vlan = vlan_info[0]
                    cluster_id = vlan_info[1]
                    network = self.create_network(cluster_id=cluster_id,
                                                  name=name + "-" + str(vlan), vlan=vlan, host_profile_name=name)
                    assert network is not None
                    network_name = network['name'] if 'name' in network else network['metadata']['name']
                    network_info = (network_name, cluster_id,)

                    if network_info not in sense_instance['networks']:
                        sense_instance['networks'].append(network_info)
                        self.save_sense_session(sense_instance=sense_instance)

            for uuid in [uuid for uuid in task_info]:
                self.sense_api_handler.finish_task(uuid, self.properties[SENSE_METADATA_URL])

            sense_instance['status'] = 'FINISHED'
            self.save_sense_session(sense_instance=sense_instance)

        return sense_instances

    def cleanup_tasks(self):
        invalid_sense_instances = list()

        for sense_instance in self.db.all(self.sense_session_table):
            instance_id = sense_instance['key']
            valid, _ = self.sense_api_handler.is_instance_valid(si_uuid=instance_id)

            if not valid:
                alias = sense_instance['name']

                for network_info in sense_instance['networks'].copy():
                    network_name = network_info[0]
                    cluster_id = network_info[1]
                    self.delete_network(cluster_id=cluster_id, name=network_name)
                    sense_instance['networks'].remove(network_info)
                    self.save_sense_session(sense_instance=sense_instance)

                DELETE_TASKS = False

                if DELETE_TASKS:
                    task_info = sense_instance['task_info']
                    uuids = [uuid for uuid in task_info]

                    for uuid in uuids:
                        self.sense_api_handler.delete_task(uuid)

                self.db.remove(self.host_table, name=alias)
                self.db.remove(self.sense_session_table, name=alias)
                sense_instance['status'] = 'DELETED'
                invalid_sense_instances.append(sense_instance)

        return invalid_sense_instances

    def update_clusters_user_infos(self):
        sense_instances = self.find_sense_session(status='FINISHED')
        clusters = self.db.all(self.nodes_table)
        clusters = [cluster for cluster in clusters if 'cluster_nodes' in cluster]
        updated_clusters = list()

        for cluster in clusters:
            temp = cluster['users'] if 'users' in cluster else list()
            cluster['users'] = list()

            for sense_instance in sense_instances:
                if cluster['id'] in sense_instance['clusters']:
                    cluster['users'].extend(sense_instance['users'])

            cluster['users'] = sorted(list(set(cluster['users'])))

            if sorted(temp) != sorted(cluster['users']):
                self.db.upsert(self.nodes_table, cluster, 'name', cluster['name'])
                updated_clusters.append(cluster)

        return updated_clusters

    def update_metadata(self):
        domain_info = self.properties[SENSE_DOMAIN_INFO].split('/')
        # metadata = self.sense_api_handler.get_metadata(domain=domain_info[0], name=domain_info[1])
        clusters = self.db.all(self.nodes_table)
        metadata = dict()
        agents = metadata["agents"] = dict()
        number_of_nodes = 0

        for cluster in clusters:
            if 'cluster_nodes' in cluster:
                for node in cluster['cluster_nodes']:
                    agents[node['name']] = node
                    number_of_nodes += 1
            else:
                agents[cluster['name']] = cluster
                number_of_nodes += 1

        self.sense_api_handler.post_metadata(metadata=metadata, domain=domain_info[0], name=domain_info[1])
        return metadata, number_of_nodes

    def delete_network(self, cluster_id, name):
        node = Node(id=1, name=cluster_id)
        self.kube_api.remove_network(node, name)

    def create_network(self, cluster_id, name, vlan, host_profile_name):
        node = Node(id=1, name=cluster_id)
        networks = self.kube_api.get_networks(node)
        networks = [cnet for cnet in networks if cnet['name'] == name]

        if networks:
            assert len(networks) == 1
            return networks[0]

        host_profiles = self.find_host_profiles(name=host_profile_name)
        assert len(host_profiles) == 1, f'expected a host profile named {host_profile_name}'
        network_profile_name = host_profiles[0]['settings']['mgmt_net']['name']

        network_profiles = self.find_network_profiles(name=host_profiles[0]['settings']['mgmt_net']['name'])
        assert len(network_profiles) == 1, f'expected a network profile named {network_profile_name}'
        network_profile = network_profiles[0]
        ipam_type = network_profile['settings']['ipam']['config']['type']
        mode = network_profile['settings']['mode']
        driver = network_profile['settings']['driver']
        config = {"cniVersion": "0.3.1",
                  "name": name,
                  "plugins": [{"name": name,
                               "type": driver,
                               "master": f"vlan.{vlan}",
                               "mode": mode,
                               "vlan": vlan,
                               "isDefaultGateway": False,
                               "forceAddress": False,
                               "ipMasq": False,
                               "hairpinMode": False,
                               "ipam": {
                                   "type": ipam_type,
                                   "subnet": "10.1.11.0/24", "rangeStart": "10.1.11.10", "rangeEnd": "10.1.11.255"
                               }
                               }
                              ]}

        spec = dict(config=json.dumps(config))
        kind = 'NetworkAttachmentDefinition'
        apiVersion = 'k8s.cni.cncf.io/v1'
        metadata = dict(name=name)
        network = self.kube_api.create_network(node,
                                               name, apiVersion=apiVersion, kind=kind, metadata=metadata, spec=spec)
        return network

    def run(self):
        _, number_of_nodes = self.update_metadata()
        log.debug(f'Metadata: Number of nodes: {number_of_nodes}')

        sense_instances = self.retrieve_tasks()

        if sense_instances:
            log.info(f'Validated tasks: {len(sense_instances)}')

        sense_instances = self.accept_tasks()

        if sense_instances:
            log.info(f'Accepted tasks: {len(sense_instances)}')

        sense_instances = self.finish_tasks()

        if sense_instances:
            log.info(f'Finished tasks: {len(sense_instances)}')
            self.update_clusters_user_infos()

        sense_instances = self.cleanup_tasks()

        if sense_instances:
            log.info(f'Cleaned up tasks: {len(sense_instances)}')
            self.update_clusters_user_infos()


class SENSEMetaRunner:
    def __init__(self, cfg: JanusConfig, properties: dict):
        self._stop = False
        self._interval = 30
        self._th = None
        self._sense_mngr = SENSEMetaManager(cfg, properties)
        log.info(f"Initialized {__name__}")

    def start(self):
        self._th = Thread(target=self._run, args=())
        self._th.start()

    def stop(self):
        log.debug(f"Stopping {__name__}")
        self._stop = True
        self._th.join()

    def _run(self):
        cnt = 0
        while not self._stop:
            time.sleep(1)
            cnt += 1
            if cnt == self._interval:
                try:
                    self._sense_mngr.run()
                    log.info(f'SenseMetaRunner ran ok')
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.error(f'Error in SenseMetaRunner : {e}')

                cnt = 0


def parse_from_config(cfg: JanusConfig, parser: ConfigParser, plugin_section='PLUGINS'):
    sense_properties = dict()

    for plugin in parser[plugin_section]:
        if plugin == 'sense-metadata-plugin':
            sense_meta_plugin = parser.get("PLUGINS", 'sense-metadata-plugin', fallback=None)

            if sense_meta_plugin:
                cfg.sense_metadata = parser.getboolean(sense_meta_plugin, 'sense-metadata-enabled', fallback=False)
                sense_metadata_url = parser.get(sense_meta_plugin, SENSE_METADATA_URL, fallback=None)
                sense_metadata_assigned = parser.get(sense_meta_plugin, SENSE_METADATA_ASSIGNED,
                                                     fallback=JANUS_DEVICE_MANAGER)
                sense_metadata_domain_info = parser.get(sense_meta_plugin, SENSE_DOMAIN_INFO,
                                                        fallback='JANUS/AES_TESTING')
                sense_properties[SENSE_METADATA_URL] = sense_metadata_url
                sense_properties[SENSE_DOMAIN_INFO] = sense_metadata_domain_info
                sense_properties[SENSE_METADATA_ASSIGNED] = sense_metadata_assigned

    return sense_properties
