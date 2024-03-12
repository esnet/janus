import os
import time
import json
import logging
import queue
from threading import Thread

import openapi_client
from openapi_client.api.slurm_api import SlurmApi
from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config
from openapi_client.models.v0038_job_submission import V0038JobSubmission
from openapi_client.models.v0038_job_properties import V0038JobProperties

from janus.api.service import Service
from janus.api.constants import EPType
from janus.api.constants import Constants
from janus.settings import cfg
from janus.api.models import (
    Node,
    Network,
    ContainerProfile,
    SessionRequest
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

class JanusSlurmApi(Service):
    DEF_MEM = "4G"
    DEF_CPU = "2"

    def __init__(self):
        self.api_name = os.getenv('SLURM_NAME')
        self.api_jwt = os.getenv('SLURM_JWT')
        self.api_url = os.getenv('SLURM_URL')
        self.api_user = os.getenv('SLURM_USER')
        if self.api_url:
            c = Config()
            c.host = self.api_url
            self._config = c
            self._headers = {"X-SLURM-USER-NAME": self.api_user,
                             "X-SLURM-USER-TOKEN": self.api_jwt}
        else:
            self._config = None

    def _get_client(self):
        return SlurmApi(Client(self._config))

    @property
    def type(self):
        return EPType.SLURM

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        if not self._config:
            return ret
        api_client = self._get_client()
        res = api_client.slurm_v0038_get_nodes(_headers=self._headers)
        host_info = {
            "cpu": {
                "brand_raw": str(),
                "count": 0,
            },
            "mem": {
                "total": 0
            }
        }
        node_count = 0
        cnodes = list()
        archs = set()
        for n in res.nodes:
            cnode = {
                "name": n.name,
                "addresses": [ n.address ]
            }
            host_info['cpu']['count'] += int(n.cpus)
            archs.add(n.architecture)
            cnodes.append(cnode)
            node_count += 1

        host_info['cpu']['brand_raw'] = " ".join(archs)
        ret.append({
            "name": self.api_name,
            "id": self.api_name,
            "endpoint_type": EPType.SLURM,
            "endpoint_status": 1,
            "cluster_node_count": node_count,
            "cluster_nodes": cnodes,
            "networks": dict(),
            "host": host_info,
            "url": self.api_url,
            "public_url": self.api_url
        })
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

    def create_node(self, nname, eptype, **kwargs):
        pass

    def create_container(self, node: Node, image: str, name: str = None, **kwargs):
        pass

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        pass

    def stop_container(self, node: Node, container, **kwargs):
        pass

    def create_network(self, node: Node, net_name, **kwargs):
        pass

    def inspect_container(self, node: Node, container):
        pass

    def remove_container(self, node: Node, container):
        pass

    def connect_network(self, node: Node, network, container):
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

    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
        srec = dict()
        return srec
