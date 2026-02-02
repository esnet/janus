import json
import logging
from functools import reduce

from pydantic import ValidationError
from tinydb import Query
from tinydb.table import Table

from janus.api.db import DBLayer
from janus.api.models import NetworkProfileSettings, NetworkProfile, ContainerProfileSettings, ContainerProfile
from janus.settings import JanusConfig

log = logging.getLogger(__name__)


class DBHandler(object):
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
    def networks_table(self) -> Table:
        return self.db.get_table('networks')

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

        assert len(queries) >= 1
        sense_sessions = self.db.search(self.sense_session_table, query=reduce(lambda a, b: a & b, queries))
        return sense_sessions

    def check_modified(self, new_sense_session):
        sense_sessions = self.find_sense_session(sense_session_key=new_sense_session['key'])
        assert len(sense_sessions) <= 1

        if not sense_sessions:
            return True

        old_sense_session = sense_sessions[0]

        if old_sense_session.get('state') == 'MODIFIED':
            return True

        old_targets = sum(old_sense_session['task_info'].values(), [])
        new_targets = sum(new_sense_session['task_info'].values(), [])

        if len(old_targets) != len(new_targets):
            return True

        import copy

        old_targets = [copy.deepcopy(t) for t in old_targets]

        for target in old_targets:
            del target['principals']
            parent = target.get('portName')
            parent = parent if parent and '?' not in parent else f'vlan.{target["vlan"]}'
            target['portName'] = parent

        new_targets = [copy.deepcopy(t) for t in new_targets]

        for target in new_targets:
            del target['principals']
            parent = target.get('portName')
            parent = parent if parent and '?' not in parent else f'vlan.{target["vlan"]}'
            target['portName'] = parent

        old_targets = sorted(old_targets, key=lambda t: t['name'])
        new_targets = sorted(new_targets, key=lambda t: t['name'])
        return old_targets != new_targets

    def save_image(self, image):
        self.db.upsert(self.image_table, image, 'name', image['name'])

    def find_images(self, *, name):
        images = self.db.search(self.image_table, query=(Query().name == name))
        return images

    def find_cluster(self, name):
        return self.db.search(self.nodes_table, query=(Query().name == name))

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
            queries.append(Query().settings.data_net.name == net_name)

        host_profiles = self.db.search(self.host_table, query=reduce(lambda a, b: a & b, queries))
        return host_profiles

    def find_janus_session(self, *, user=None, name=None, host_profile_names=None):
        queries = list()

        if user:
            queries.append(Query().users.any([user]))

        if name:
            queries.append(Query().name == name)

        if host_profile_names:
            queries.append(Query().request.any(Query().profile.one_of(host_profile_names)))

        if not queries:
            return list()

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

    def get_or_create_network_profile(self, name, target, users, subnet, groups=None, host_networking=False):
        network_profiles = list()

        if host_networking:
            network_profile_settings = {
                "driver": "host",
                "mode": None,
                "enable_ipv6": False,
                "ipam": None,
                "options": None
            }

            network_profile = dict(name=name + '-host',
                                   settings=network_profile_settings, users=users, groups=groups)
        else:
            vlan = target['vlan']
            parent = target.get('portName')
            parent = parent if parent and '?' not in parent else f'vlan.{vlan}'
            bw = target.get('bw')
            config = list()

            if bw:
                options = dict(vlan=str(vlan), bw=str(bw))
            else:
                options = dict(vlan=str(vlan))

            options['parent'] = parent
            options['mtu'] = '1500'

            from ipaddress import IPv4Network

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
            network_profile = dict(name=name,  # AES TODO + '-' + str(vlan),
                                   settings=network_profile_settings, users=users, groups=groups)

        log.debug(f'network_profile: {json.dumps(network_profile)}')

        try:
            NetworkProfileSettings(**network_profile['settings'])
            NetworkProfile(**network_profile)
        except ValidationError as e:
            raise e

        self.save_network_profile(network_profile=network_profile)
        network_profiles.append(network_profile)
        return network_profiles

    def get_or_create_host_profile(self, name, network_profile, addr, users, groups=None, host_networking=False):
        if host_networking:
            host_profile_settings = dict(cpu=0.2,
                                         memory=1073741824,
                                         mgmt_net=dict(name=network_profile['name'], ipv4_addr=addr, ipv6_addr=None),
                                         ctrl_port_range=None,
                                         data_net=None)
        else:
            host_profile_settings = dict(cpu=0.2,
                                         memory=1073741824,
                                         mgmt_net=None,
                                         ctrl_port_range=None,
                                         data_net=dict(name=network_profile['name'], ipv4_addr=addr, ipv6_addr=None))

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

    def get_agents(self):
        agents = dict()
        clusters = self.db.all(self.nodes_table)

        for cluster in clusters:
            if 'cluster_nodes' in cluster:
                for node in cluster['cluster_nodes']:
                    cluster_info = (dict(cluster_name=cluster['name']))
                    temp_node = dict(host_addresses=node['host_addresses'])
                    temp_node['cluster_info'] = cluster_info
                    agents[node['name']] = temp_node
            elif 'url' in cluster:
                agent_name = cluster['url']
                cluster_info = (dict(cluster_name=cluster['name']))
                node = dict(host_addresses=[agent_name])
                node['cluster_info'] = cluster_info
                agents[agent_name] = node

        return agents

    def create_profiles(self, sense_session, host_networking=False):
        task_info = sense_session['task_info']
        name = sense_session['name'].lower()
        users = sense_session['users']
        targets = sorted(sum(task_info.values(), []), key=lambda t: t['name'])
        subnets = ['192.168.1.0/24']
        nprofs = self.get_or_create_network_profile(name=name + '-net', target=targets[0],
                                                    subnet=subnets[0], users=users, host_networking=host_networking)
        hprofs = list()
        assert len(nprofs) == 1
        addr = None
        hprof = self.get_or_create_host_profile(name=name, network_profile=nprofs[0], addr=addr,
                                                users=users, host_networking=host_networking)
        hprofs.append(hprof)
        sense_session['network_profile'] = [nprof['name'] for nprof in nprofs]
        sense_session['host_profile'] = [hprof['name'] for hprof in hprofs]

    def remove_profiles(self, sense_session: dict):
        for name in sense_session['network_profile']:
            self.db.remove(self.network_table, name=name)

        for name in sense_session['host_profile']:
            self.db.remove(self.host_table, name=name)

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
            image['users'] = list(set(users))
            self.save_image(image)

    def update_builtin_host_network_profile(self):
        users = list()
        host_profiles = self.db.all(self.host_table)
        network_profiles = self.find_network_profiles(name='host')

        for host_profile in host_profiles:
            if 'users' in host_profile:
                users.extend(host_profile['users'])

        for network_profile in network_profiles:
            network_profile['users'] = list(set(users))
            self.save_network_profile(network_profile)

    def update_janus_sessions(self, sense_session: dict):
        for network_profile_name in sense_session['network_profile']:
            network_profiles = self.find_network_profiles(name=network_profile_name)
            assert len(network_profiles) == 1
            network_profiles[0]['users'] = sense_session['users']
            self.save_network_profile(network_profiles[0])

        for host_profile_name in sense_session['host_profile']:
            host_profiles = self.find_host_profiles(name=host_profile_name)
            assert len(host_profiles) == 1
            host_profiles[0]['users'] = sense_session['users']
            self.save_host_profile(host_profiles[0])

        for janus_session in self.find_janus_session(host_profile_names=sense_session['host_profile']):
            janus_session['users'] = sense_session['users']
            janus_session['user'] = sense_session['users'][0] if sense_session['users'] else 'admin'
            self.db.update(self.janus_session_table, janus_session, ids=janus_session['id'])
