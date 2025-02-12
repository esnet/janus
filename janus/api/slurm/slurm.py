import os
import time
import functools
import json
import logging
import queue
import requests
from copy import copy, deepcopy
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
from janus.api.pubsub import TOPIC
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

class JanusSlurmApi(Service):
    DEF_MEM = "4G"
    DEF_CPU = "2"
    DEF_VER = "v0.0.38"

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
                             "X-SLURM-USER-TOKEN": self.api_jwt,
                             "Content-Type": "application/json"}
        else:
            self._config = None

    def _get_client(self):
        return SlurmApi(Client(self._config))

    def _get_headers(self, service):
        user = service.get('slurm_user')
        jwt = service.get('slurm_jwt')
        if user and jwt:
            return {"X-SLURM-USER-NAME": user,
                    "X-SLURM-USER-TOKEN": jwt,
                    "Content-Type": "application/json"}
        else:
            return self._headers

    @property
    def type(self):
        return EPType.SLURM

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        if not self._config:
            return ret
        api_client = self._get_client()
        try:
            res = api_client.slurm_v0038_get_nodes(_headers=self._headers)
        except Exception as e:
            log.error(f"Could not get nodes from SLURM endpoint: {e}")
            return ret
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
            if n.architecture:
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

    @send_event
    def create_container(self, node: Node, image: str, cname: str = None, **kwargs):
        return {"Id": cname}

    @send_event
    def start_container(self, node: Node, container: str, service=None, **kwargs):
        @send_event
        def nodelist(service, job_id):
            while True:
                try:
                    #job = api_client.slurm_v0038_get_job(job_id=str(job_id), _headers=self._headers)
                    res = requests.get(f"{self.api_url}/slurm/{self.DEF_VER}/job/{job_id}",
                                       headers=self._get_headers(service))
                    job = json.loads(res.text)
                    nodes = job.get("jobs")[0].get("nodes")
                    if nodes:
                        return {"Id": service.get("sname"), "nodes": nodes}
                except Exception as e:
                    log.error(f"Could not query for job {job_id}: {e}")
                    return {"Id": service.get("sname"), "nodes": "N/A"}
                time.sleep(1.0)

        jid = None
        kw = copy(service.get('kwargs'))
        script = kw.pop('script')
        api_client = self._get_client()
        #job = V0038JobSubmission(script=script,
        #                         job=V0038JobProperties(**kw))
        #sub = api_client.slurm_v0038_submit_job(job, _headers=self._headers)
        #jid = sub.job_id
        req = {"script": script,
               "job": kw}
        try:
            res = requests.post(f"{self.api_url}/slurm/{self.DEF_VER}/job/submit",
                                data=json.dumps(req),
                                headers=self._get_headers(service))
            if res.status_code != 200:
                raise Exception(f"Slurm API returned {res.status_code}: {res.text}")
            js = json.loads(res.text)
            jid = js.get("job_id")
            if service:
                service['container_id'] = jid
        except Exception as e:
            log.error(f"Could not submit job: {e}")
            raise e
        t = Thread(target=nodelist, args=(service, jid))
        t.start()
        return {"Id": jid}

    @send_event
    def stop_container(self, node: Node, container, **kwargs):
        service = kwargs.get('service', dict())
        try:
            res = requests.delete(f"{self.api_url}/slurm/{self.DEF_VER}/job/{container}",
                                  headers=self._get_headers(service))
            if res.status_code != 200:
                raise Exception(f"Slurm API returned {res.status_code}: {res.text}")
            js = json.loads(res.text)
        except Exception as e:
            log.error(f"Could not submit job: {e}")
            raise e
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
        sess_kwargs = sreq.kwargs
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

        kwargs = {
            'name': cname,
            'environment': {'PATH': '/bin:/usr/bin/:/usr/local/bin/'},
        }

        if constraints.nodeExec:
            kwargs['script'] = constraints.nodeExec

        if constraints.nodePath:
            kwargs['current_working_directory'] = constraints.nodePath

        if constraints.nodeQueue:
            kwargs['partition'] = constraints.nodeQueue

        if constraints.nodeCount:
            kwargs['nodes'] = constraints.nodeCount

        if constraints.time:
            kwargs['time_limit'] = constraints.time

        account = sess_kwargs.get("SLURM_ACCOUNT", None)
        if account:
            kwargs['account'] = account

        srec['slurm_user'] = sess_kwargs.get("SLURM_USER", None)
        srec['slurm_jwt'] = sess_kwargs.get("SLURM_JWT", None)
        srec['mgmt_net'] = node['networks'].get(mnet.name, None)
        #srec['mgmt_ipv4'] = mgmt_ipv4
        #srec['mgmt_ipv6'] = mgmt_ipv6
        srec['data_net'] = node['networks'].get(dnet.name, None)
        srec['data_net_name'] = dnet.name
        #srec['data_ipv4'] = data_ipv4.split("/")[0] if data_ipv4 else None
        #srec['data_ipv6'] = data_ipv6.split("/")[0] if data_ipv6 else None
        srec['container_user'] = sess_kwargs.get("USER_NAME", None)

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
