import json
import logging
import time
from threading import Thread
from functools import reduce

from sense.client.discover_api import DiscoverApi
from sense.client.metadata_api import MetadataApi
from sense.client.requestwrapper import RequestWrapper
from sense.client.task_api import TaskApi
from sense.client.workflow_combined_api import WorkflowCombinedApi
from tinydb import Query
from types import SimpleNamespace

from tinydb.table import Table

from janus.api.db import DBLayer
from janus.settings import JanusConfig
from janus.api.kubernetes import KubernetesApi
from janus.api.models import Node

log = logging.getLogger(__name__)


class Base(object):
    def __init__(self, cfg: JanusConfig):
        self._cfg = cfg

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
    def sense_instance_table(self) -> Table:
        return self.db.get_table('sense_instance')

    @property
    def cfg(self) -> JanusConfig:
        return self._cfg

    @property
    def db(self) -> DBLayer:
        return self.cfg.db

    def save_sense_instance(self, sense_instance):
        self.db.upsert(self.sense_instance_table, sense_instance, 'key', sense_instance['key'])

    def find_sense_instances(self, *, user=None, instance_id=None, name=None, status=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if instance_id:
            queries.append(Query().key == instance_id)

        if name:
            queries.append(Query().name == name)

        if status:
            queries.append(Query().status == status)

        return self.db.search(self.sense_instance_table, query=reduce(lambda a, b: a & b, queries))

    def save_network_profile(self, network_profile):
        # network_profile = dict(name=name, settings=profile_settings)
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

    def get_or_create_network_profile(self):
        network_profile_name = 'vlan-attachement'
        network_profiles = self.find_network_profiles(name=network_profile_name)

        if network_profiles:
            assert len(network_profiles) == 1
            return network_profiles[0]

        network_profile_settings = '''
                             {
                                 "driver": "macvlan",
                                 "mode": "bridge",
                                 "enable_ipv6": false,
                                 "ipam": {
                                   "config": {
                                     "type": "host-local"
                                   }
                                 }
                             }
                         '''
        network_profile_settings = json.loads(network_profile_settings)
        network_profile = dict(name=network_profile_name, settings=network_profile_settings)
        self.save_network_profile(network_profile=network_profile)
        log.debug(f'network_profile: {json.dumps(network_profile)}')
        return network_profile

    def get_or_create_host_profile(self, name, users, groups=None):
        network_profile = self.get_or_create_network_profile()
        network_profile_name = network_profile['name']
        host_profiles = self.find_host_profiles(name=name, net_name=network_profile_name)

        if host_profiles:
            assert len(host_profiles) == 1
            return host_profiles[0]

        host_profile_settings = '''
         {
             "mgmt_net": {
               "name": "vlan-attachement",
               "ipv4_addr": null,
               "ipv6_addr": null
             }
         }
         '''

        host_profile_settings = json.loads(host_profile_settings)
        groups = groups or list()
        host_profile = dict(name=name, settings=host_profile_settings, users=users, groups=groups)
        log.debug(f'host_profile: {json.dumps(host_profile)}')
        self.save_host_profile(host_profile)
        return host_profile


class SENSEApiHandler:
    def __init__(self, req_wrapper):
        self.workflow_client = WorkflowCombinedApi(req_wrapper=req_wrapper)
        self.discover_client = DiscoverApi(req_wrapper=req_wrapper)
        self.task_client = TaskApi(req_wrapper=req_wrapper)
        self.metadata_client = MetadataApi(req_wrapper=req_wrapper)

    def instance_status(self, si_uuid):
        return self.workflow_client.instance_get_status(si_uuid=si_uuid)

    def is_instance_valid(self, si_uuid):
        status = self.instance_status(si_uuid)
        return status in ['CREATE - READY', 'REINSTATE - READY'], status

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

    def find_instance_by_alias(self, alias):
        response = self.discover_client.discover_service_instances_get(search=alias)
        instances = response['instances']

        if not instances:
            return None

        instance = instances[0]
        return instance['referenceUUID']

    def retrieve_tasks(self):
        records = self.task_client.get_tasks(assigned="janus.device.manager")
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
    def __init__(self, cfg: JanusConfig):
        super().__init__(cfg)
        self.sense_api_handler = SENSEApiHandler(req_wrapper=RequestWrapper())
        self.kube_api = KubernetesApi()
        log.info(f"Initialized {__name__}")

    def load_cluster_node_map(self):
        clusters = self.db.all(self.nodes_table)
        clusters = [cluster for cluster in clusters if 'cluster_nodes' in cluster]
        node_cluster_map = dict()

        for cluster in clusters:
            for node in cluster['cluster_nodes']:
                node_cluster_map[node['name']] = (cluster['id'], cluster['namespace'],)

        return node_cluster_map

    def retrieve_tasks(self):
        tasks = self.sense_api_handler.retrieve_tasks()
        instance_map = dict()

        for task in tasks:
            if task['status'].upper() != 'PENDING':
                continue

            instance_id = task['config']['context']

            if instance_id not in instance_map:
                instance_map[instance_id] = list()

            task_list = instance_map[instance_id]
            task_list.append(task)

        if not instance_map:
            return list()

        node_cluster_map = self.load_cluster_node_map()
        node_names = [n for n in node_cluster_map]
        sense_instances = list()

        for instance_id, task_list in instance_map.items():
            valid, status = self.sense_api_handler.is_instance_valid(si_uuid=instance_id)

            if not valid:
                log.warning(f'instance {instance_id} is {status}')
                continue

            task_info = dict()
            principals = list()
            endpoints = list()

            for task in task_list:
                targets = task['config']['targets']
                task_info[task['uuid']] = targets
                endpoints.extend([target['name'] for target in targets])
                principals.extend(task['config']['principals'])

            if len(endpoints) != 2:
                continue

            have_all_targets = True

            for endpoint in endpoints:
                have_all_targets = have_all_targets and endpoint in node_names

            if not have_all_targets:
                continue

            temp = self.sense_api_handler.find_instance_by_id(instance_id)

            if temp is None:
                continue

            for _, targets in task_info.items():
                for target in targets:
                    target['cluster_info'] = node_cluster_map[target['name']]

            alias = temp['alias']
            users = list(set(principals))
            sense_instance = dict(key=instance_id,
                                  name=alias,
                                  task_info=task_info,
                                  users=users,
                                  status='PENDING',
                                  networks=list())
            self.save_sense_instance(sense_instance=sense_instance)
            log.debug(f'saved sense instance:{json.dumps(sense_instance)}')
            sense_instances.append(sense_instance)

        return sense_instances

    def accept_tasks(self):
        sense_instances = self.find_sense_instances(status='PENDING')

        for sense_instance in sense_instances:
            task_info = sense_instance['task_info']
            uuids = [uuid for uuid in task_info]

            for uuid in uuids:
                self.sense_api_handler.accept_task(uuid)

            sense_instance['status'] = 'ACCEPTED'
            self.save_sense_instance(sense_instance=sense_instance)

        return sense_instances

    def finish_tasks(self):
        sense_instances = self.find_sense_instances(status='ACCEPTED')

        for sense_instance in sense_instances:
            name = sense_instance['name']
            users = sense_instance['users']
            host_profile = self.get_or_create_host_profile(name=name, users=users)
            assert host_profile is not None
            task_info = sense_instance['task_info']
            uuids = [uuid for uuid in task_info]
            vlans = set()
            cluster_id = None

            for _, targets in task_info.items():
                for target in targets:
                    vlans.add(target['vlan'])
                    cluster_id = target['cluster_info'][0]

            assert cluster_id is not None

            for vlan in vlans:
                network = self.create_network(cluster_id=cluster_id,
                                              name=name + "-" + str(vlan), vlan=vlan, host_profile_name=name)
                assert network is not None
                network_name = network['name'] if 'name' in network else network['metadata']['name']

                if network_name not in sense_instance['networks']:
                    sense_instance['networks'].append(network_name)
                    self.save_sense_instance(sense_instance=sense_instance)

            for uuid in uuids:
                self.sense_api_handler.finish_task(uuid, "randomUrl")

            sense_instance['status'] = 'FINISHED'
            self.save_sense_instance(sense_instance=sense_instance)

        return sense_instances

    def cleanup_tasks(self):
        invalid_sense_instances = list()

        for sense_instance in self.db.all(self.sense_instance_table):
            instance_id = sense_instance['key']
            valid, _ = self.sense_api_handler.is_instance_valid(si_uuid=instance_id)

            if not valid:
                task_info = sense_instance['task_info']
                uuids = [uuid for uuid in task_info]
                alias = sense_instance['name']
                cluster_id = None

                for _, targets in task_info.items():
                    for target in targets:
                        cluster_id = target['cluster_info'][0]
                        break

                assert cluster_id is not None
                for network in sense_instance['networks'].copy():
                    self.delete_network(cluster_id=cluster_id, name=network)
                    sense_instance['networks'].remove(network)
                    self.save_sense_instance(sense_instance=sense_instance)

                for uuid in uuids:
                    self.sense_api_handler.delete_task(uuid)

                self.db.remove(self.host_table, name=alias)
                self.db.remove(self.sense_instance_table, name=alias)
                sense_instance['status'] = 'DELETED'
                invalid_sense_instances.append(sense_instance)

        return invalid_sense_instances

    def update_metadata(self):
        metadata = self.sense_api_handler.get_metadata(domain="JANUS", name="AES_TESTING")
        clusters = self.db.all(self.nodes_table)
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

        self.sense_api_handler.post_metadata(metadata=metadata, domain="JANUS", name="AES_TESTING")
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

        sense_instances = self.cleanup_tasks()

        if sense_instances:
            log.info(f'Cleaned up tasks: {len(sense_instances)}')


class SENSEMetaRunner:
    def __init__(self, cfg: JanusConfig):
        self._stop = False
        self._interval = 30
        self._th = None
        self._sense_mngr = SENSEMetaManager(cfg)
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
                    log.error(f'Error in SenseMetaRunner : {e}')

                cnt = 0
