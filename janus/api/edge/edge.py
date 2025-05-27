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

log = logging.getLogger(__name__)


class JanusEdgeApi(Service):
    def __init__(self):
        super().__init__()
        self._exec_map = dict()
        self.cfg = cfg

    @property
    def type(self):
        return EPType.EDGE

    def _do_get_nodes(self, event, refresh=False):
        value = dict(args=[refresh], kwargs=dict())
        edge = self.cfg.sm.get_edge()

        message = {"msg": {"handler": EPType.EDGE.name,
                 "event": event,
                 "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def get_nodes(self, nname=None, cb=None, refresh=False):
        return self._do_get_nodes('get_nodes', refresh=refresh)

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
        # node = json.loads(node.model_dump_json())
        # value = dict(args=[node, image, cname], kwargs=kwargs)
        # edge = self.cfg.sm.get_edge()
        # message = {"msg": {"handler": EPType.EDGE.name,
        #                    "event": 'create_container',
        #                    "value": value}}
        #
        # edge.send(json.dumps(message))
        # data = edge.receive()
        # reply = json.loads(data)
        # return reply['value']
        return {"Id": cname}  # TODO

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, service], kwargs=kwargs)
        edge = self.cfg.sm.get_edge()
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'start_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def stop_container(self, node: Node, container, **kwargs):
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        edge = self.cfg.sm.get_edge()
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'stop_container',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def create_network(self, node: Node, net_name, **kwargs):
        pass

    def inspect_container(self, node: Node, container):
        pass

    def remove_container(self, node: Node, container):
        pass

    def connect_network(self, node: Node, network, container, **kwargs):
        pass

    def exec_create(self, node: Node, container, **kwargs):
        node_name = node.name
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container], kwargs=kwargs)
        edge = self.cfg.sm.get_edge()
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
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, ectx], kwargs=kwargs)
        edge = self.cfg.sm.get_edge()
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'exec_start',
                           "value": value}}
        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']

    def exec_stream(self, node: Node, container, eid, **kwargs):
        node_name = node.name
        node = json.loads(node.model_dump_json())
        value = dict(args=[node, container, eid], kwargs=kwargs)
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'exec_stream',
                           "value": value}}

        edge = self.cfg.sm.get_edge()
        edge.send(json.dumps(message))

        def _get_stream(aqueue, aedge):
            while True:
                try:
                    r = aedge.receive()
                    r = json.loads(r)
                    ret = r['value']
                    aqueue.put(ret)

                    if ret.get("eof"):
                        break
                except Exception as e:
                    import traceback

                    traceback.print_exc()

        from threading import Thread

        q = self._exec_map.get(node_name).get(container)
        t = Thread(target=_get_stream, args=(q, edge, ))
        t.start()
        return q

    def remove_network(self, node: Node, network, **kwargs):
        pass

    def resolve_networks(self, node: dict, prof):
        pass

    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
        sreq = json.loads(sreq.model_dump_json())
        value = dict(args=[sname, sreq], kwargs=dict())
        edge = self.cfg.sm.get_edge()
        message = {"msg": {"handler": EPType.EDGE.name,
                           "event": 'create_service_record',
                           "value": value}}

        edge.send(json.dumps(message))
        data = edge.receive()
        reply = json.loads(data)
        return reply['value']
