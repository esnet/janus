import copy
import json
import logging
import time
from configparser import ConfigParser
from functools import reduce
from threading import Thread

from kubernetes.client import ApiException
from pydantic import ValidationError
from sense.client.metadata_api import MetadataApi
from sense.client.requestwrapper import RequestWrapper
from sense.client.task_api import TaskApi
from tinydb import Query
from tinydb.table import Table

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.models import Node, ContainerProfile, ContainerProfileSettings, NetworkProfile, NetworkProfileSettings
from janus.settings import JanusConfig

SENSE_METADATA_URL = 'sense-metadata-url'
SENSE_METADATA_ASSIGNED = 'sense-metadata-assigned'
JANUS_DEVICE_MANAGER = 'janus.device.manager'
SENSE_DOMAIN_INFO = 'sense-metadata-domain-info'

log = logging.getLogger(__name__)


class Base(object):
    def __init__(self, cfg: JanusConfig):
        self._cfg = cfg

    @property
    def janus_session_table(self) -> Table:
        return self.db.get_table('active')

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

    def save_sense_session(self, sense_session):
        self.db.upsert(self.sense_session_table, sense_session, 'key', sense_session['key'])

    def find_sense_session(self, *, user=None, sense_session_key=None, name=None, status=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if sense_session_key:
            queries.append(Query().key == sense_session_key)

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

    def find_cluster(self, *, cluster_id=None, name=None):
        if id:
            clusters = self.db.search(self.nodes_table, query=(Query().id == cluster_id))
        else:
            clusters = self.db.search(self.nodes_table, query=(Query().name == name))

        return clusters

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

    def find_janus_session(self, *, user=None, name=None, host_profile_name=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if name:
            queries.append(Query().name == name)

        if host_profile_name:
            queries.append(Query().request.any(Query().profile == host_profile_name))

        janus_sessions = self.db.search(self.janus_session_table, query=reduce(lambda a, b: a & b, queries))
        return janus_sessions

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
        # TODO CHECK IF MODIFIED AND MAYBE DESTROY EXISTING SESSIONS
        # network_profiles = self.find_network_profiles(name=network_profile_name)
        #
        # if network_profiles:
        #     assert len(network_profiles) == 1
        #     network_profile = self._update_users(network_profiles[0], users, self.save_network_profile)
        #     return network_profile

        vlans = [t['vlan'] for t in targets]
        vlans = list(set(vlans))
        portNames = [t['portName'] for t in targets if 'portName' in t and t['portName']]
        portNames = list(set(portNames))
        portNames = portNames or [f'vlan.{vlans[0]}']

        subnets = [t['ip'] for t in targets if 'ip' in t and t['ip']]
        subnets = subnets or ['192.168.1.0/24']
        config = list()
        bws = [t['bw'] for t in targets if 'bw' in t and t['bw']]

        if bws:
            options = dict(vlan=str(vlans[0]), bw=str(bws[0]))
        else:
            options = dict(vlan=str(vlans[0]))

        options['parent'] = portNames[0]

        from ipaddress import IPv4Network

        subnet = subnets[0]
        idx = subnet.rindex(".")
        prefixlen = IPv4Network(subnet, strict=False).prefixlen
        config.append(dict(subnet=subnet[0:idx + 1] + '0/' + str(prefixlen), gateway=subnet[0:idx + 1] + '1'))
        network_profile_settings = {
            "driver": "macvlan",
            "mode": "bridge",
            "enable_ipv6": False,
            "ipam": {
                "config": config
            },
            "options": options
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

    def get_or_create_host_profile(self, name, network_profile_name, users, groups=None):
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
        self.task_client = TaskApi(req_wrapper=req_wrapper)
        self.metadata_client = MetadataApi(req_wrapper=req_wrapper)

    def retrieve_tasks(self, assigned, status):
        records = self.task_client.get_tasks_agent_status(assigned=assigned, status=status)
        return records

    def accept_task(self, uuid):
        data = None
        return self.task_client.update_task(json.dumps(data), uuid=uuid, state='ACCEPTED')

    def reject_task(self, uuid, message):
        data = {"message": message}
        return self.task_client.update_task(json.dumps(data), uuid=uuid, state='REJECTED')

    def finish_task(self, uuid, data):
        return self.task_client.update_task(json.dumps(data), uuid=uuid, state='FINISHED')

    def get_metadata(self, domain, name):
        return self.metadata_client.get_metadata(domain=domain, name=name)

    def post_metadata(self, metadata, domain, name):
        return self.metadata_client.post_metadata(data=json.dumps(metadata), domain=domain, name=name)


class SENSEMetaManager(Base):
    def __init__(self, cfg: JanusConfig, properties: dict):
        super().__init__(cfg)
        self.sense_api_handler = SENSEApiHandler(req_wrapper=RequestWrapper())
        self.kube_api = KubernetesApi()
        self.properties = properties
        log.info(f"Initialized {__name__}:{properties}")

    def load_cluster_node_map(self):
        clusters = self.db.all(self.nodes_table)
        clusters = [cluster for cluster in clusters if 'cluster_nodes' in cluster]
        node_cluster_map = dict()

        for cluster in clusters:
            for node in cluster['cluster_nodes']:
                cluster_info = (dict(cluster_id=cluster['id'], namespace=cluster['namespace']))
                node_cluster_map[node['name']] = cluster_info

        return node_cluster_map

    def retrieve_tasks(self):
        assigned = self.properties[SENSE_METADATA_ASSIGNED]
        tasks = self.sense_api_handler.retrieve_tasks(assigned=assigned, status='PENDING')

        if not tasks or not isinstance(tasks, list):
            if not isinstance(tasks, list):
                log.warning(f'warning retrieving tasks: {tasks}')

            return list()

        node_cluster_map = self.load_cluster_node_map()
        node_names = [n for n in node_cluster_map]
        validated_sense_sessions = list()

        for task in tasks:
            config = task['config']
            command = config['command']
            task_id = task['uuid']

            if command not in ['handle-sense-instance', 'instance-termination-notice']:
                self.sense_api_handler.reject_task(task_id, f"unknown command:{command}")
                continue

            instance_id = config['context']['uuid']
            alias = config['context']['alias']
            sense_sessions = self.find_sense_session(sense_session_key=instance_id)
            assert len(sense_sessions) <= 1
            sense_session = sense_sessions[0] if sense_sessions else dict()

            if command == 'instance-termination-notice':
                if not sense_session:
                    message = {'url': self.properties[SENSE_METADATA_URL],
                               'targets': [],
                               'message': f'no session found for {instance_id}'}
                    self.sense_api_handler.finish_task(task_id, message)
                    continue

                sense_session['command'] = command
                sense_session['status'] = 'PENDING'
                sense_session['termination_task'] = task_id
            else:
                targets = config['targets']

                if len(targets) == 0:
                    # TODO should we finish task with existing targets?
                    log.warning(f'no targets for instance {instance_id}')
                    self.sense_api_handler.reject_task(task_id, f"no targets")
                    continue
                elif len(targets) > 2:
                    log.warning(f'unknown endpoint for instance {instance_id}:too many targets:{len(targets)}')
                    self.sense_api_handler.reject_task(task_id, f"too many targets:{len(targets)}")
                    continue

                task_info = dict()
                task_info[task_id] = targets
                endpoints = [target['name'] for target in targets]
                unknown_endpoints = [endpoint for endpoint in endpoints if endpoint not in node_names]

                if unknown_endpoints:
                    log.warning(f'unknown endpoint for instance {instance_id}:endpoints={unknown_endpoints}')
                    self.sense_api_handler.reject_task(task_id, f"unkown targets:{ unknown_endpoints}")
                    continue

                clusters = sense_session['clusters'] if 'clusters' in sense_session else list()
                users = sense_session['users'] if 'users' in sense_session else list()

                for target in targets:
                    target['cluster_info'] = node_cluster_map[target['name']]
                    clusters.append(node_cluster_map[target['name']]['cluster_id'])
                    users.extend(target['principals'])

                target_names = [target['name'] for target in targets]

                if 'task_info' in sense_session:
                    old_task_info = sense_session['task_info']

                    for old_task_id, old_targets in copy.deepcopy(old_task_info).items():
                        old_task_info[old_task_id] = list()

                        for old_target in old_targets.copy():
                            if old_target['name'] not in target_names:
                                old_task_info[old_task_id].append(old_target)

                        if not old_task_info[old_task_id]:
                            del old_task_info[old_task_id]

                    task_info.update(old_task_info)

                if not alias:
                    alias = f'sense-janus-{"-".join(instance_id.split("-")[0:2])}'
                else:
                    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

                users = list(set(users))
                sense_session = dict(key=instance_id,
                                     name=alias,
                                     task_info=task_info,
                                     users=users,
                                     status='PENDING',
                                     command=command,
                                     clusters=list(set(clusters)))

            self.save_sense_session(sense_session=sense_session)
            log.debug(f'saved sense session:{json.dumps(sense_session)}')
            validated_sense_sessions.append(sense_session)

        return validated_sense_sessions

    def accept_tasks(self):
        sense_sessions = self.find_sense_session(status='PENDING')

        for sense_session in sense_sessions:
            task_info = sense_session['task_info']

            if 'termination_task' in sense_session:
                self.sense_api_handler.accept_task(sense_session['termination_task'])
            else:
                uuids = [uuid for uuid in task_info]

                for uuid in uuids:
                    self.sense_api_handler.accept_task(uuid)

            sense_session['status'] = 'ACCEPTED'
            self.save_sense_session(sense_session=sense_session)

        return sense_sessions

    def finish_tasks(self):
        sense_sessions = self.find_sense_session(status='ACCEPTED')

        for sense_session in sense_sessions:
            if 'termination_task' in sense_session:
                assert sense_session['command'] != 'handle-sense-instance'
                self.terminate_sense_session(sense_session=sense_session)
                message = {'url': self.properties[SENSE_METADATA_URL],
                           'targets': [],
                           'message': f'session for {sense_session["key"]} has been terminated'}
                self.sense_api_handler.finish_task(sense_session['termination_task'], message)
                sense_session['status'] = 'DELETED'
                continue

            assert sense_session['command'] == 'handle-sense-instance'
            name = sense_session['name'].lower()
            users = sense_session['users']
            task_info = sense_session['task_info']
            targets = sum(task_info.values(), [])
            network_profile = self.get_or_create_network_profile(name=name + "-net", targets=targets, users=users)
            assert network_profile is not None
            assert network_profile is not None
            network_profile_name = network_profile['name']
            host_profile = self.get_or_create_host_profile(name=name,
                                                           network_profile_name=network_profile_name, users=users)
            assert host_profile is not None
            assert 'settings' in host_profile and 'tools' in host_profile['settings']
            sense_session['host_profile'] = host_profile['name']
            sense_session['network_profile'] = network_profile['name']

            for image in sum([self.find_images(name=tool) for tool in host_profile['settings']['tools']], list()):
                self._update_users(resource=image, users=users, func=self.save_image)

            for uuid in [uuid for uuid in task_info]:
                targets = []

                for target in task_info[uuid]:
                    if 'cluster_info' in target:
                        targets.append(dict(name=target['name'], cluster_info=target['cluster_info']))
                    else:
                        target.append(dict(name=target['name']))

                message = {'url': self.properties[SENSE_METADATA_URL],
                           'targets': targets,
                           'message': f'session for {sense_session["key"]} has been handled'
                           }
                self.sense_api_handler.finish_task(uuid, message)

            sense_session['status'] = 'FINISHED'
            self.save_sense_session(sense_session=sense_session)

        return sense_sessions

    def update_clusters_user_infos(self):
        sense_sessions = self.find_sense_session(status='FINISHED')
        clusters = self.db.all(self.nodes_table)
        clusters = [cluster for cluster in clusters if 'cluster_nodes' in cluster]
        updated_clusters = list()

        for cluster in clusters:
            temp = cluster['users'] if 'users' in cluster else list()
            cluster['users'] = list()

            for sense_session in sense_sessions:
                if cluster['id'] in sense_session['clusters']:
                    cluster['users'].extend(sense_session['users'])

            cluster['users'] = sorted(list(set(cluster['users'])))

            if sorted(temp) != sorted(cluster['users']):
                self.db.upsert(self.nodes_table, cluster, 'name', cluster['name'])
                updated_clusters.append(cluster)

        return updated_clusters

    def update_images(self):
        users = list()
        host_profiles = self.db.all(self.host_table)
        images = self.db.all(self.image_table)

        for host_profile in host_profiles:
            if 'users' in host_profile:
                users.extend(host_profile['users'])

        for image in images:
            image['users'] = users
            self.save_image(image)

    '''
    JANUS API:
    delete pods
    delete janus session
    delete net-attachement .....
    delete host profile
    delete network profile
    clean up users  and clean sense_session
    
    Do we need to stop session first?
    PUT /api/janus/controller/stop/1 HTTP/1.1" 200
    DELETE /api/janus/controller/active/1?force=true HTTP/1.1
    DELETE /api/janus/controller/profiles/network/sense-janus-vlan-attachement HTTP/1.1
    DELETE /api/janus/controller/profiles/host/sense-janus-617c8b1a-d23c HTTP/1.1
    '''
    def terminate_sense_session(self, sense_session: dict):
        host_profile_name = sense_session['host_profile']
        network_profile_name = sense_session['network_profile']
        cluster_id = sense_session['clusters'][0]  # TODO cannot assume same cluster for pods and network...

        for janus_session in self.find_janus_session(host_profile_name=host_profile_name):
            clusters = janus_session['services']
            pods = list()

            for cluster_name, services in clusters.items():
                print("CLUSTER", cluster_name, "Number Of Service:", len(services))
                pods.extend([service['sname'] for service in services])

            for pod in pods:
                self.delete_pod(cluster_id=cluster_id, name=pod)

            # TODO This is controller delete
            # @ns.route('/active/<int:aid>')
            # from janus.api.utils import commit_db
            # commit_db(janus_session, janus_session['id'], delete=True, realized=True)
            # commit_db(janus_session, janus_session['id'], delete=True)
            self.db.remove(self.janus_session_table, ids=janus_session['id'])

        self.delete_network(cluster_id=cluster_id, name=network_profile_name)
        clusters = self.find_cluster(cluster_id=cluster_id)
        cluster = clusters[0]

        if network_profile_name in cluster['networks']:
            del cluster['networks'][network_profile_name]
            self.db.upsert(self.nodes_table, cluster, 'name', cluster['name'])

        self.db.remove(self.host_table, name=host_profile_name)
        self.db.remove(self.network_table, name=network_profile_name)
        self.db.remove(self.sense_session_table, name=sense_session['name'])

    def show_sense_session(self, sense_session: dict):
        print("**************** BEGIN SENSE SESSION  ***************")
        host_profile_name = sense_session['host_profile']
        network_profile_name = sense_session['network_profile']
        print("SENSE_SESSION:", sense_session['name'], host_profile_name, network_profile_name)

        for janus_session in self.find_janus_session(host_profile_name=host_profile_name):
            pods = list()
            clusters = janus_session['services']

            for cluster_name, services in clusters.items():
                print("CLUSTER", cluster_name, "Number Of Service:", len(services))
                pods.extend([service['sname'] for service in services])

            print("FOUND JANUS SESSION:", janus_session['uuid'],
                  janus_session['state'],
                  "OWNER=", janus_session['user'],
                  "USERS=", janus_session['users'],
                  "PODS=", pods)
        print("**************** END SENSE SESSION  ***************")

    def show_summary(self):
        print("**************** BEGIN SUMMARY ***************")
        sense_sessions = self.find_sense_session(status='FINISHED')
        for sense_session in sense_sessions:
            self.show_sense_session(sense_session=sense_session)
        images = self.db.all(self.image_table)
        print("IMAGES:", images)
        print("**************** END SUMMARY ***************")

    def get_agents(self):
        agents = dict()
        clusters = self.db.all(self.nodes_table)

        for cluster in clusters:
            if 'cluster_nodes' in cluster:
                for node in cluster['cluster_nodes']:
                    agents[node['name']] = node
            else:
                agents[cluster['name']] = cluster

        return agents

    def update_metadata(self, agents):
        domain_info = self.properties[SENSE_DOMAIN_INFO].split('/')
        metadata = dict(agents=agents)
        return self.sense_api_handler.post_metadata(metadata=metadata, domain=domain_info[0], name=domain_info[1])

    def delete_network(self, cluster_id, name):
        node = Node(id=1, name=cluster_id)

        try:
            self.kube_api.remove_network(node, name)
        except ApiException as ae:
            if str(ae.status) != "404":
                raise ae

    def delete_pod(self, cluster_id, name):
        node = Node(id=1, name=cluster_id)

        try:
            self.kube_api.stop_container(node, name)
        except ApiException as ae:
            if str(ae.status) != "404":
                raise ae

    def run(self):
        agents = self.get_agents()
        number_of_nodes = len(agents)

        if number_of_nodes == 0:  # Wait for nodes to be populated
            log.warning(f'Waiting on nodes: Number of nodes: {number_of_nodes}')
            return

        ret = self.update_metadata(agents=agents)

        if not isinstance(ret, dict):
            log.warning(f'Expected a dict when updating data: {ret}')
        else:
            log.debug(f'Updated Metadata: Number of nodes: {number_of_nodes}')

        sense_instances = self.retrieve_tasks()

        if sense_instances:
            log.info(f'Retrieved and validated tasks: {len(sense_instances)}')

        sense_instances = self.accept_tasks()

        if sense_instances:
            log.info(f'Accepted tasks: {len(sense_instances)}')

        sense_instances = self.finish_tasks()

        if sense_instances:
            log.info(f'Finished tasks: {len(sense_instances)}')
            self.update_clusters_user_infos()
            self.update_images()

        self.show_summary()


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
        quiet = False
        while not self._stop:
            time.sleep(1)
            cnt += 1
            if cnt == self._interval:
                try:
                    if not quiet:
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
