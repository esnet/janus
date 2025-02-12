import os
import time
import functools
import json
import logging
import queue
import requests
from copy import copy, deepcopy
from threading import Thread

from janus.api.service import Service
from janus.api.constants import EPType, WSEPType
from janus.api.constants import Constants
from janus.settings import cfg
from janus.api.pubsub import TOPIC
from janus.api.models import (
    Node,
    Network,
    ContainerProfile,
    SessionRequest,
    AddEndpointRequest
)
from janus.api.utils import (
    get_next_cport,
    get_next_sport,
    get_next_vf,
    get_next_ipv4,
    get_next_ipv6,
    get_numa,
    get_cpu,
    get_cpuset,
    get_mem,
    is_subset
)


log = logging.getLogger(__name__)

def send_event(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ret = func(*args, **kwargs)
        cfg.sm.pubsub.publish({"msg": {"handler": EPType.SLURM.name,
                                       "event": func.__name__,
                                       "value": ret}},
                              TOPIC.event_stream)
        return ret
    return wrapper

class JanusEdgeApi(Service):
    def __init__(self):
        pass

    @property
    def type(self):
        return EPType.EDGE

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        return ret

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

    @send_event
    def create_container(self, node: Node, image: str, cname: str = None, **kwargs):
        return {"Id": cname}

    @send_event
    def start_container(self, node: Node, container: str, service=None, **kwargs):
        return {"Id": container}

    @send_event
    def stop_container(self, node: Node, container, **kwargs):
        return {"Id": container}

    def create_network(self, node: Node, net_name, **kwargs):
        pass

    def inspect_container(self, node: Node, container):
        pass

    def remove_container(self, node: Node, container):
        pass

    def connect_network(self, node: Node, network, container, **kwargs):
        pass

    def exec_create(self, node: Node, container, **kwargs):
        pass

    def exec_start(self, node: Node, ectx, **kwargs):
        pass

    def exec_stream(self, node: Node, container, eid, **kwargs):
        pass

    def remove_network(self, node: Node, network, **kwargs):
        pass

    def resolve_networks(self, node: dict, prof):
        pass

    @send_event
    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
        srec = dict()
        srec = dict()
        node = sreq.node
        prof = sreq.profile
        constraints = sreq.constraints
        nname = node.get('name')
        cname = sname
        dnet = Network(prof.settings.data_net, nname)
        mnet = Network(prof.settings.mgmt_net, nname)
        args_override = sreq.arguments
        cmd = None
        if args_override:
            cmd = shlex.split(args_override)
        elif prof.settings.arguments:
            cmd = shlex.split(prof.settings.arguments)

        kwargs = {}

        srec['mgmt_net'] = node['networks'].get(mnet.name, None)
        #srec['mgmt_ipv4'] = mgmt_ipv4
        #srec['mgmt_ipv6'] = mgmt_ipv6
        srec['data_net'] = node['networks'].get(dnet.name, None)
        srec['data_net_name'] = dnet.name
        #srec['data_ipv4'] = data_ipv4.split("/")[0] if data_ipv4 else None
        #srec['data_ipv6'] = data_ipv6.split("/")[0] if data_ipv6 else None
        srec['container_user'] = kwargs.get("USER_NAME", None)

        srec['kwargs'] = kwargs
        srec['sname'] = sname
        srec['node'] = node
        srec['node_id'] = node['id']
        #srec['serv_port'] = sport
        #srec['ctrl_port'] = cport
        srec['ctrl_host'] = constraints.nodeName if constraints.nodeName else node['public_url']
        srec['image'] = sreq.image
        srec['profile'] = prof.name
        srec['pull_image'] = prof.settings.pull_image
        srec['errors'] = list()
        return srec
