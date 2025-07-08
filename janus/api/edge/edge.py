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

log = logging.getLogger(__name__)


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
        self.edges[name] = sock

    def get_edge(self, name):
        if name not in self.edges:
            raise Exception(f'edge {name} is not registered')

        return self.edges[name]

    def _do_get_nodes(self, event, refresh=False):
        value = dict(args=[refresh], kwargs=dict())
        nodes = list()

        for node_name, edge in self.edges.items():
            message = {"msg": {"handler": EPType.EDGE.name,
                     "event": event,
                     "value": value}}
            try:
                edge.send(json.dumps(message))
                data = edge.receive()
                reply = json.loads(data)
                nodes.extend(reply['value'])
            except Exception as e:
                log.error(f"__do_get_nodes from {node_name}:got {e}")

        return nodes

    def get_nodes(self, nname=None, cb=None, refresh=False):
        return self._do_get_nodes('get_nodes', refresh=refresh)

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
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'create_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, service], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'start_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def stop_container(self, node: Node, container, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'stop_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def create_network(self, node: Node, net_name, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, net_name], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'create_network',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def inspect_container(self, node: Node, container):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=dict())
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'inspect_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def remove_container(self, node: Node, container):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=dict())
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'remove_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def connect_network(self, node: Node, network, container, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, network, container], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'connect_network',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def remove_network(self, node: Node, network, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, network], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'remove_network',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    # prof is of type ContainerProfile
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

        value = dict(args=[node, prof, nprof], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'resolve_networks',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        ret, networks = reply['value']
        node_as_dict['networks'].update(networks)
        return ret

    def exec_create(self, node: Node, container, **kwargs):
        edge = self.get_edge(node.name)
        node_name = node.name
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'exec_create',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)

        import queue

        q = queue.Queue()

        if not self._exec_map.get(node_name):
            self._exec_map[node_name] = dict()
            self._exec_map[node_name][container] = q
        else:
            self._exec_map[node_name][container] = q

        return reply['value']

    def exec_start(self, node: Node, ectx, **kwargs):
        edge = self.get_edge(node.name)
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, ectx], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'exec_start',
                           "value": value}}
        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def exec_stream(self, node: Node, container, eid, **kwargs):
        edge = self.get_edge(node.name)
        node_name = node.name
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, eid], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'exec_stream',
                           "value": value}}

        edge.send(json.dumps(message))

        def _get_stream(aqueue, aedge):
            while True:
                # noinspection PyBroadException
                try:
                    r = aedge.receive()
                    r = json.loads(r)
                    ret = r['value']
                    aqueue.put(ret)

                    if ret.get("eof"):
                        break
                except Exception as e:
                    log.error(f"_get_stream got  {e}")
                    import traceback
                    traceback.print_exc()

        from threading import Thread

        q = self._exec_map.get(node_name).get(container)
        t = Thread(target=_get_stream, args=(q, edge, ))
        t.start()
        return q

    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
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
        kwargs = dict()

        if dnet.name:
            data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True)
            data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True)
            kwargs['data_ipv4'] = data_ipv4
            kwargs['data_ipv6'] = data_ipv6

        print(f"CURRR V4:{addrs_v4}")
        print(f"CURRR V6:{addrs_v6}")

        sreq = json.loads(sreq.model_dump_json())
        value = dict(args=[sname, sreq], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'create_service_record',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']
