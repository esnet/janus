import re
import logging
from janus.api.portainer import PortainerDockerApi
from janus.api.kubernetes import KubernetesApi
from janus.api.slurm import JanusSlurmApi
from janus.api.constants import State, EPType
from janus.lib import AgentMonitor
from janus.api.models import Node
from janus.api.utils import error_svc, handle_image
from janus.settings import cfg, AGENT_AUTO_TUNE


log = logging.getLogger(__name__)


class ServiceManager():
    def __init__(self, db):
        self._db = db
        self._am = AgentMonitor()
        self.service_map = {
            EPType.PORTAINER: PortainerDockerApi(),
            EPType.KUBERNETES: KubernetesApi(),
            EPType.SLURM: JanusSlurmApi()
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
            try:
                ns = s.get_nodes(nname, cb=self._add_node_cb, refresh=refresh)
                nodes.extend(ns)
            except Exception as e:
                import traceback
                traceback.print_exc()
                log.error(f"Error retrieving nodes from {k}: {e}")
        return nodes

    def add_node(self, ep, **kwargs):
        eptype = ep.get('type')
        n = self.service_map[eptype].create_node(ep, **kwargs)
        # Tune remote endpoints after addition if requested
        if AGENT_AUTO_TUNE:
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
        if not node:
            log.error(f"Node does not exist (node={node}, nname={nname})")
            return None
        eptype = node.get('endpoint_type')
        return self.service_map[eptype]

    def get_auth_token(self, node=None, ntype=EPType.PORTAINER):
        return self.service_map[ntype].auth_token


    def init_service(self, s, errs=False):
        n = s.get('node')
        sname = s.get('sname')
        nname = n.get('name')
        img = s.get('image')
        handler = self.get_handler(n)

        if handler.type == EPType.PORTAINER:
            # Docker-specific v4 vs v6 image registry nonsense. Need to abstract this away.
            try:
                handle_image(n, img, handler, s.get('pull_image'))
            except Exception as e:
                log.error(f"Could not pull image {img} on node {nname}: {e}")
                errs = error_svc(s, e)
                try:
                    v6img = f"registry.ipv6.docker.com/{img}"
                    handle_image(n, v6img, handler, s.get('pull_image'))
                    s['image'] = v6img
                except Exception as e:
                    log.error(f"Could not pull image {v6img} on node {nname}: {e}")
                    errs = error_svc(s, e)
                    return None, None

        # clear any errors if image resolved
        s['errors'] = list()
        errs = False
        try:
            ret = handler.create_container(Node(**n), img, sname, **s['kwargs'])
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error(f"Could not create container on {nname}: {e}")
            errs = error_svc(s, e)
            return None, None

        if not (cfg.dryrun):
            try:
                # if specified, connect the management network to this created container
                if s['mgmt_net']:
                    handler.connect_network(Node(**n), s['mgmt_net']['id'], ret['Id'],
                                            **s['net_kwargs'])
            except Exception as e:
                log.error("Could not connect network on {nname}: {e}")
                errs = error_svc(s, e)
                return None, None

        s['container_id'] = ret['Id']
        s['container_name'] = sname
        # don't save node object in service record
        if s.get("node"):
            del s['node']
        return ret['Id'], nname
