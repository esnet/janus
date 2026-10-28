import re
import logging
from janus.api.portainer import PortainerDockerApi
from janus.api.kubernetes import KubernetesApi
from janus.api.slurm import JanusSlurmApi
from janus.api.edge import JanusEdgeApi
from janus.api.constants import EPType
from janus.lib import AgentMonitor
from janus.api.models import Node, AddEndpointRequest
from janus.api.pubsub import Publisher
from janus.api.utils import error_svc, handle_image
from janus.settings import cfg, AGENT_AUTO_TUNE


log = logging.getLogger(__name__)


class ServiceManagerException(Exception):
    pass


class ServiceManager:
    def __init__(self, db):
        self._db = db
        self._am = AgentMonitor()
        self.service_map = {
            EPType.PORTAINER: PortainerDockerApi(),
            EPType.KUBERNETES: KubernetesApi(),
            EPType.SLURM: JanusSlurmApi(),
            EPType.EDGE: JanusEdgeApi(),
        }
        self._pubsub = Publisher()

    @property
    def pubsub(self) -> Publisher:
        return self._pubsub

    def _add_node_cb(self, node: dict, name, url):
        try:
            table = self._db.get_table("images")
            for img in node.get("images"):
                iname = re.split(":|@", img)[0]
                self._db.upsert(table, {"image": img, "name": iname}, "name", iname)
        except Exception as e:
            log.error("Could not save images for {}: {}".format(url, e))
        try:
            ret = self._am.check_agent(Node(**node), url)
            node["host"] = ret.json()
            ret = self._am.tune(url)
            node["host"]["tuning"] = ret.json()
        except Exception as e:
            log.error("Could not fetch agent info from {}: {}".format(url, e))
            self._am.start_agent(Node(**node))

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

    def add_node(self, ep: AddEndpointRequest, **kwargs):
        eptype = ep.type
        n = self.service_map[eptype].create_node(ep, **kwargs)
        # Tune remote endpoints after addition if requested
        if AGENT_AUTO_TUNE:
            try:
                self._am.tune(ep.public_url, post=True)
            except Exception as e:
                log.error(f"Could not apply auto-tuning, agent not running?: {e}")
        return n

    def remove_node(self, node: dict = None, nname=None):
        if nname:
            ntable = self._db.get_table("nodes")
            node = self._db.get(ntable, name=nname)
        if not node:
            return None
            
        eptype = node.get("endpoint_type")
        nid = node.get("id")
        nname = node.get("name")
        
        try:
            self.service_map[eptype].remove_node(nid)
        except Exception as e:
            # If the backend says it's not found (404), we still want to remove it locally
            if "(404)" in str(e) or "Not Found" in str(e):
                log.warning(f"Node {nname} (id {nid}) not found on backend, removing locally only.")
            else:
                raise e
        
        # Always remove from local DB if we reached this point
        ntable = self._db.get_table("nodes")
        self._db.remove(ntable, name=nname)
        log.info(f"Removed node {nname} (id {nid}) from Janus database.")
        return True

    def get_handler(self, node: dict = None, nname=None):
        if nname:
            ntable = self._db.get_table("nodes")
            node = self._db.get(ntable, name=nname)
        if not node:
            log.error(f"Node does not exist (node={node}, nname={nname})")
            return None
        eptype = node.get("endpoint_type")
        return self.service_map[eptype]

    def get_auth_token(self, node: dict = None, ntype=EPType.PORTAINER):
        try:
            return self.service_map[ntype].auth_token
        except Exception as e:
            log.error(f"Could not get token for ntype {ntype}: {e}")
            return {"error": str(e)}

    def init_service(self, s, errs=False):
        n = s.get("node")
        sname = s.get("sname")
        nname = n.get("name")
        img = s.get("image")
        handler = self.get_handler(n)

        if handler.type == EPType.PORTAINER:
            # Docker-specific v4 vs v6 image registry nonsense. Need to abstract this away.
            try:
                handle_image(Node(**n), img, handler, s.get("pull_image"))
            except Exception as e:
                log.error(f"Could not pull image {img} on node {nname}: {e}")
                error_svc(s, e)
                try:
                    v6img = f"registry.ipv6.docker.com/{img}"
                    handle_image(Node(**n), v6img, handler, s.get("pull_image"))
                    s["image"] = v6img
                except Exception as e:
                    log.error(f"Could not pull image {v6img} on node {nname}: {e}")
                    error_svc(s, e)
                    return None, None

        # clear any errors if image resolved
        s["errors"] = list()
        try:
            ret = handler.create_container(Node(**n), img, sname, **s["kwargs"])
        except Exception as e:
            import traceback

            traceback.print_exc()
            log.error(f"Could not create container on {nname}: {e}")
            error_svc(s, e)
            return None, None

        if not (cfg.dryrun):
            try:
                # if specified, connect the management network to this created container
                if s["mgmt_net"]:
                    net_kwargs = s["net_kwargs"] if "net_kwargs" in s else dict()
                    handler.connect_network(
                        Node(**n), s["mgmt_net"]["id"], ret["Id"], **net_kwargs
                    )
            except Exception as e:
                log.error("Could not connect network on {nname}: {e}")
                error_svc(s, e)
                return None, None

        s["container_id"] = ret["Id"]
        s["container_name"] = sname
        # don't save node object in service record
        if s.get("node"):
            del s["node"]
        return ret["Id"], nname
