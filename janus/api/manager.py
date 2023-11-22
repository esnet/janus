import re
import logging
from janus.api.portainer import PortainerDockerApi
from janus.api.kubernetes import KubernetesApi
from janus.api.constants import State, EPType
from janus.lib import AgentMonitor
import janus.settings as settings

log = logging.getLogger(__name__)


class ServiceManager():
    def __init__(self, db):
        self._db = db
        self._am = AgentMonitor()
        self.service_map = {
            EPType.PORTAINER: PortainerDockerApi(),
            EPType.KUBERNETES: KubernetesApi()
        }

    def _add_node_cb(self, node, name, url):
        try:
            table = self._db.get_table('images')
            for img in node.get('images'):
                iname = re.split(':|@', img)[0]
                self._db.upsert(table, {'image': img, 'name': iname}, 'name', iname)
        except Exception as e:
            log.error("Could not save images for {}: {}".format(url, e))
        try:
            (n, ret) = self._am.check_agent(name, url)
            node['host'] = ret.json()
            (n, ret) = self._am.tune(name, url)
            node['host']['tuning'] = ret.json()
        except Exception as e:
            log.error("Could not fetch agent info from {}: {}".format(url, e))
            self._am.start_agent(node)

    def get_nodes(self, nname=None, refresh=False):
        nodes = list()
        for k, s in self.service_map.items():
            ns = s.get_nodes(nname, cb=self._add_node_cb, refresh=refresh)
            nodes.extend(ns)
        return nodes

    def add_node(self, ep, **kwargs):
        eptype = ep.get('type')
        n = self.service_map[eptype].create_node(ep, **kwargs)
        # Tune remote endpoints after addition if requested
        if settings.AGENT_AUTO_TUNE:
            self._am.tune(ep, post=True)

    def remove_node(self, node=None, nname=None):
        if nname:
            ntable = self._db.get_table('nodes')
            node = self._db.get(ntable, name=nname)
        eptype = node.get('endpoint_type')
        return self.service_map[eptype].remove_node(node.get('id'))

    def get_handler(self, node=None, nname=None):
        if nname:
            ntable = self._db.get_table('nodes')
            node = self._db.get(ntable, name=nname)
        eptype = node.get('endpoint_type')
        return self.service_map[eptype]

    def get_auth_token(self, node=None, ntype=EPType.PORTAINER):
        return self.service_map[ntype].auth_token
