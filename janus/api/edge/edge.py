import json
import logging

from janus.api.constants import EPType
from janus.api.models import (
    Node,
    SessionRequest,
    AddEndpointRequest
)

from janus.api.service import Service
from janus.settings import cfg
from simple_websocket.ws import Server
from threading import Lock

log = logging.getLogger(__name__)


class EdgeServerSocket:
    def __init__(self, name, sock: Server):
        self.name = name
        self.sock: Server = sock
        self.mutex = Lock()

    def send(self, json_text):
        self.sock.send(json_text)

    def receive(self):
        return self.sock.receive()

    def __str__(self):
        return f'edge@{self.name}'


class JanusEdgeApi(Service):
    def __init__(self):
        super().__init__()
        self._exec_map = dict()
        self.cfg = cfg
        self.edges = dict()

    @property
    def type(self):
        return EPType.EDGE

    def add_edge(self, name, sock: Server):
        self.edges[name] = EdgeServerSocket(name, sock)

    def get_edge(self, name) -> EdgeServerSocket:
        if name not in self.edges:
            raise Exception(f'edge {name} is not registered')

        return self.edges[name]

    def _send_message(self, edge: EdgeServerSocket, event, value):
        log.info(f"sending message to {edge}:event={event}")
        message = {"event": event, "value": value}

        with edge.mutex:
            edge.send(json.dumps({"event": event, "value": value}))
            data = edge.receive()

        reply = json.loads(data)

        if 'error' in reply:
            log.error(f"received reply from {edge}:event={message['event']}:error={reply['error']}")
            raise Exception(reply['error'])

        log.info(f"received OK reply from {edge}:event={event}")
        return reply['value']

    def get_nodes(self, nname=None, cb=None, refresh=False):
        value = dict(args=[refresh], kwargs=dict())
        event = 'get_nodes'
        nodes = list()

        for node_name, edge in self.edges.items():
            try:
                ret = self._send_message(edge, event, value)
                nodes.extend(ret)
            except Exception as e:
                log.error(f"__do_get_nodes from {node_name}:got {e}")

        return nodes

    def remove_node(self, nid):
        pass

    def get_images(self, node: Node):
        pass

    def get_networks(self, node: Node):
        pass

    def get_containers(self, node: Node):
        pass

    def get_logs(self, node: Node, container, since=0, stderr=1, stdout=1, tail=100, timestamps=0):
        pass

    def pull_image(self, node: Node, image, tag):
        pass

    def create_node(self, ep: AddEndpointRequest, **kwargs):
        log.info(ep)
        pass

    def create_container(self, node: Node, image: str, cname: str = None, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, image, cname], kwargs=kwargs)
        return self._send_message(edge, 'create_container', value)

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, service], kwargs=kwargs)
        return self._send_message(edge, 'start_container', value)

    def stop_container(self, node: Node, container, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        return self._send_message(edge, 'stop_container', value)

    def create_network(self, node: Node, net_name, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, net_name], kwargs=kwargs)
        return self._send_message(edge, 'create_network', value)

    def inspect_container(self, node: Node, container):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=dict())
        return self._send_message(edge, 'inspect_container', value)

    def remove_container(self, node: Node, container):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=dict())
        return self._send_message(edge, 'remove_container', value)

    def connect_network(self, node: Node, network, container, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, network, container], kwargs=kwargs)
        return self._send_message(edge, 'connect_network', value)

    def remove_network(self, node: Node, network, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, network], kwargs=kwargs)
        return self._send_message(edge, 'remove_network', value)

    # NOTE: prof is of type ContainerProfile
    def resolve_networks(self, node: dict, prof, **kwargs):
        from janus.api.models import Network
        from janus.api.constants import Constants

        node_as_dict = node
        node = Node(**node)
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        nprof = None

        # TODO Here we are assuming either management net or data net.
        for net in [Network(prof.settings.mgmt_net), Network(prof.settings.data_net)]:
            if net.name and net.name not in [Constants.NET_NONE, Constants.NET_HOST, Constants.NET_BRIDGE]\
                    and not net.is_host():
                nprof = self.cfg.pm.get_profile(Constants.NET, net.name)
                break

        prof = json.loads(prof.model_dump_json())

        if nprof is not None:
            nprof = json.loads(nprof.model_dump_json())
            kwargs['nprof'] = nprof

        value = dict(args=[node, prof], kwargs=kwargs)
        ret, networks = self._send_message(edge, 'resolve_networks', value)
        node_as_dict['networks'].update(networks)
        return ret

    def exec_create(self, node: Node, container, **kwargs):
        edge: EdgeServerSocket = self.get_edge(node.name)
        # node_name = node.name  # TODO The kube session is getting created in exec_stream below. Is that a problem?
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        ret = self._send_message(edge, 'exec_create', value)
        return ret

    def exec_start(self, node: Node, ectx, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, ectx], kwargs=kwargs)
        return self._send_message(edge, 'exec_start', value)

    def exec_stream(self, node: Node, container, eid, **kwargs):
        edge = self.get_edge(node.name)
        node_name = node.name
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, eid], kwargs=kwargs)
        self._send_message(edge, 'exec_stream', value)

        from janus.api.utils import ExecWebsocketServerSession

        if not self._exec_map.get(node_name):
            self._exec_map[node_name] = dict()
            self._exec_map[node_name][container] = ExecWebsocketServerSession(edge.sock, edge.mutex)
        else:
            self._exec_map[node_name][container] = ExecWebsocketServerSession(edge.sock, edge.mutex)

        return self._exec_map.get(node_name).get(container)

    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports, **kwargs):
        from janus.api.models import Network
        from janus.api.utils import (
            get_next_ipv4,
            get_next_ipv6,
        )

        edge = self.get_edge(sreq.node['name'])
        prof = sreq.profile
        node = sreq.node
        nname = node.get('name')
        dnet = Network(prof.settings.data_net, nname)

        if dnet.name:
            data_net_overrides = kwargs

            if data_net_overrides and data_net_overrides.get('name'):
                dnet_name = data_net_overrides['name']
                key = f"{nname}-{dnet_name}"
                data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True, key=key, name=dnet_name)
                data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True, key=key, name=dnet_name)
            else:
                data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True)
                data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True)

            kwargs['data_ipv4'] = data_ipv4
            kwargs['data_ipv6'] = data_ipv6

        sreq = json.loads(sreq.model_dump_json())
        value = dict(args=[sname, sreq], kwargs=kwargs)
        return self._send_message(edge, 'create_service_record', value)
