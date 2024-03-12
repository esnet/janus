import os
import time
import json
import logging
import queue
from threading import Thread
from kubernetes import client, config
from kubernetes.config import KUBE_CONFIG_DEFAULT_LOCATION
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
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


class KubernetesApi(Service):
    CNI_VERSION = "0.3.1"
    NS = "default"
    DEF_MEM = "4Gi"
    DEF_CPU = "2"

    def __init__(self):
        self.api_key = os.getenv('KUBE_API_KEY')
        self.api_cluster_url = os.getenv('KUBE_CLUSTER_URL')
        self.api_cluster_name = os.getenv('KUBE_CLUSTER_NAME')
        self.api_namespace = os.getenv('KUBE_NAMESPACE')
        self.config = None
        self._exec_map = dict()

    @property
    def type(self):
        return EPType.KUBERNETES

    def _get_namespace(self, ctx_name):
        try:
            contexts,_ = self._get_contexts()
            ctx = [x for x in contexts if x.get('name') == ctx_name][0]
            return ctx.get('context').get('namespace', self.NS)
        except Exception as e:
            log.error(f"Could not get namespace for ctx={ctx_name}: {e}")
            return self.NS

    def _get_client(self, ctx_name):
        if self.config:
            return client.ApiClient(self.config)
        return config.new_client_from_config(context=ctx_name)

    def _get_contexts(self):
        if self.api_key and self.api_cluster_name and self.api_cluster_url:
            self.config = client.Configuration()
            self.config.host = self.api_cluster_url
            self.config.api_key['authorization'] = self.api_key
            self.config.api_key_prefix['authorization'] = 'Bearer'
            self.config.verify_ssl = False
            return [{'name': self.api_cluster_name,
                     'context': {
                         'host': self.api_cluster_url,
                         'namespace': self.api_namespace}
                     }], None
        else:
            return config.list_kube_config_contexts()

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        if not refresh:
            return ret
        contexts, active_context = self._get_contexts()
        if not contexts:
            log.error("Cannot find any contexts in current configuration.")
            return

        node_count = 0
        for ctx in contexts:
            ctx_name = ctx.get('name')
            if nname and nname != ctx_name:
                continue
            host_info = {
                "cpu": {
                    "brand_raw": str(),
                    "count": 0,
                },
                "mem": {
                    "total": 0
                }
            }
            cnodes = list()
            archs = set()
            api_client = self._get_client(ctx_name)
            v1 = client.CoreV1Api(api_client)
            try:
                res = v1.list_node(watch=False)
            except Exception as e:
                log.error(f"Could not list nodes on cluster {ctx_name}: {e}")
                continue
            for i in res.items:
                cnode = {
                    "name": i.metadata.name,
                    "addresses": [ a.address if a.type == "InternalIP" else "" for a in i.status.addresses ]
                }
                host_info['cpu']['count'] += int(i.status.capacity.get('cpu'))
                archs.add(i.status.node_info.architecture)
                cnodes.append(cnode)
                node_count += 1
            namespace = ctx.get('context').get('namespace', self.NS)
            host_info['cpu']['brand_raw'] = " ".join(archs)
            cnets = dict()
            try:
                capi = client.CustomObjectsApi(api_client)
                res = capi.list_namespaced_custom_object("k8s.cni.cncf.io",
                                                         "v1",
                                                         namespace,
                                                         "network-attachment-definitions")
                for i in res.get('items'):
                    meta = i.get('metadata')
                    name = meta.get('name')
                    spec = json.loads(i.get('spec').get('config'))
                    plugins = spec.get('plugins')[0]
                    snets = list()
                    if plugins.get('ipam') and plugins.get('ipam').get('addresses'):
                        for a in plugins.get('ipam').get('addresses'):
                            snets.append({
                                'Subnet': a.get('address'),
                                'Gateway': a.get('gateway')
                            })
                    cnets[name] = {
                        'name': meta.get('name'),
                        'id': meta.get('uid'),
                        'namespace': meta.get('namespace'),
                        'driver': plugins.get('type'),
                        'parent': plugins.get('master'),
                        'mode': plugins.get('mode'),
                        'vlan': plugins.get('vlanId'),
                        'mtu': plugins.get('mtu'),
                        'subnet': snets,
                        '_data': i
                    }
            except ApiException as e:
                log.warn(f"Could not find network attachement definitions on cluster {ctx_name}: {e}")

            ret.append({
                "name": ctx_name,
                "id": ctx_name,
                "namespace": namespace,
                "endpoint_type": EPType.KUBERNETES,
                "endpoint_status": 1,
                "cluster_node_count": node_count,
                "cluster_nodes": cnodes,
                "networks": cnets,
                "host": host_info,
                "url": api_client.configuration.host,
                "public_url": KUBE_CONFIG_DEFAULT_LOCATION
            })
        return ret

    def get_images(self, node: Node):
        pass

    def get_networks(self, node: Node):
        pass

    def get_containers(self, node: Node):
        pass

    def get_logs(self, node: Node, container, since=0, stderr=1, stdout=1, tail=100, timestamps=0):
        api_client = self._get_client(node.name)
        api = client.CoreV1Api(api_client)
        ret = api.read_namespaced_pod_log(
            name=container,
            namespace=self._get_namespace(node.name),
            tail_lines=tail,
            pretty='true',
        )
        return {"response": ret}

    def pull_image(self, node: Node, image, tag):
        pass

    def create_node(self, nname, eptype, **kwargs):
        pass

    def create_container(self, node: Node, image: str, name: str = None, **kwargs):
        return {"Id": name}

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        api_client = self._get_client(node.name)
        v1 = client.CoreV1Api(api_client)
        resp = v1.create_namespaced_pod(body=service['kwargs'],
                                        namespace=self._get_namespace(node.name))
        resp = v1.read_namespaced_pod(name=container,
                                      namespace=self._get_namespace(node.name))
        log.info(f"Pod transitioned to state: {resp.status.phase}")

    def stop_container(self, node: Node, container, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CoreV1Api(api_client)
        res = api.delete_namespaced_pod(str(container), self._get_namespace(node.name))

    def create_network(self, node: Node, net_name, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CustomObjectsApi(api_client)
        created_resource = api.create_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            plural="network-attachment-definitions",
            namespace=self._get_namespace(node.name),
            body=kwargs
        )
        log.info(f"Created network {net_name} on {node}")

    def inspect_container(self, node: Node, container):
        pass

    def remove_container(self, node: Node, container):
        pass

    def connect_network(self, node: Node, network, container):
        pass

    def exec_create(self, node: Node, container, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CoreV1Api(api_client)
        res = stream(api.connect_get_namespaced_pod_exec,
                     name=container,
                     namespace=self._get_namespace(node.name),
                     command=kwargs.get("Cmd"),
                     container=container,
                     stderr=kwargs.get("AttachStderr"),
                     stdin=kwargs.get("AttachStdin"),
                     stdout=kwargs.get("AttachStdout"),
                     tty=kwargs.get("Tty"),
                     _preload_content=False
                     )

        def _get_stream(res, q):
            while res.is_open():
                res.update(1)
                q.put({"msg": res.read_all(), "eof": False})
            q.put({"msg": None, "eof": True})

        q = queue.Queue()
        t = Thread(target=_get_stream, args=(res, q))
        t.start()

        if not self._exec_map.get(node.name):
            self._exec_map[node.name] = dict()
            self._exec_map[node.name][container] = q
        else:
            self._exec_map[node.name][container] = q

        return {"response": "websocket created"}

    def exec_start(self, node: Node, ectx, **kwargs):
        return ectx

    def exec_stream(self, node: Node, container, eid, **kwargs):
        return self._exec_map.get(node.name).get(container)

    def remove_network(self, node: Node, network, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CustomObjectsApi(api_client)
        api.delete_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            name=network,
            plural="network-attachment-definitions",
            namespace=self._get_namespace(node.name)
        )
        log.info(f"Removed network {network} on {node.name}")

    def resolve_networks(self, node: dict, prof):
        def _build_net(p):
            addrs = list()
            if p.settings.ipam:
                for s in p.settings.ipam.get('config', []):
                    addrs.append({'address': s['subnet'],
                                  'gateway': s['gateway']
                                  })
            cfg = {
                "cniVersion": self.CNI_VERSION,
                "plugins": [
                    {
                        "type": p.settings.driver,
                        "vlanId": int(p.settings.options.get('vlan')) if p.settings.options.get('vlan') else 1,
                        "master": p.settings.options.get('parent'),
                        "mtu": int(p.settings.options.get('mtu')) if p.settings.options.get('mtu') else 1500,
                        "ipam": {
                            "type": "static",
                            #"addresses": addrs
                        }
                    }
                ]
            }
            ret = {
                "apiVersion": "k8s.cni.cncf.io/v1",
                "kind": "NetworkAttachmentDefinition",
                "metadata": {
                    "name": p.name,
                },
                "spec": {
                    "config": json.dumps(cfg)
                }
            }
            return ret

        created = False
        for net in [Network(prof.settings.mgmt_net), Network(prof.settings.data_net)]:
            if not net.name or net.name in [Constants.NET_NONE, Constants.NET_HOST, Constants.NET_BRIDGE]:
                continue
            nname = node.get('name')
            nprof = cfg.pm.get_profile(Constants.NET, net.name)
            if not nprof:
                raise Exception(f"Network profile {net.name} not found")
            kwargs = _build_net(nprof)
            ninfo = node.get('networks').get(net.name)
            # We are done if the node already has a network and it matches our profile
            if ninfo and is_subset(kwargs, ninfo.get('_data')):
                continue
            # Otherwise we need to either create or recreate the network
            if not ninfo:
                log.info(f"Network {net.name} not found on {nname}, attempting to create")
            elif ninfo and not is_subset(kwargs, ninfo.get('_data')):
                log.info(f"Network {net.name} found on {nname} but differs from profile, attempting to recreate")
                try:
                    self.remove_network(Node(**node), net.name)
                except Exception as e:
                    log.warn(f"Removing network {net.name} on {nname} failed: {e}")
            self.create_network(Node(**node), net.name, **kwargs)
            created = True
        return created

    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
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

        mgmt_ipv4 = None
        mgmt_ipv6 = None
        data_ipv4 = None
        data_ipv6 = None
        cport = get_next_cport(node, prof, cports)
        sport = get_next_sport(node, prof, sports)

        limits = dict()
        mem = get_mem(node, prof)
        mem = constraints.memory if constraints.memory else mem
        limits["memory"] = mem if mem else self.DEF_MEM
        cpu = get_cpu(node, prof)
        cpu = constraints.cpu if constraints.cpu else cpu
        limits["cpu"] = cpu if cpu else self.DEF_CPU

        kwargs = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": cname,
            },
            "spec": {
                "containers": [
                    {
                        "name": cname,
                        "image": sreq.image,
                        "resources": {"limits": limits},
                        "tty": True
                    }
                ]
            }
        }

        if constraints.nodeName:
            kwargs['spec'].update({"nodeName": constraints.nodeName})

        if (mnet.is_host()):
            kwargs['spec'].update({"hostNetwork": True})
        if (dnet.name):
            ips = []
            data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True)
            data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True)
            if data_ipv4:
                ips.append(data_ipv4)
            if data_ipv6:
                ips.append(data_ipv6)
            dnet_conf = [
                {
                    "name": dnet.name,
                    "ips": ips
                }
            ]
            anno = {
                "annotations": {
                    "k8s.v1.cni.cncf.io/networks": json.dumps(dnet_conf)
                }
            }
            kwargs['metadata'].update(anno)

        srec['mgmt_net'] = node['networks'].get(mnet.name, None)
        srec['mgmt_ipv4'] = mgmt_ipv4
        srec['mgmt_ipv6'] = mgmt_ipv6
        srec['data_net'] = node['networks'].get(dnet.name, None)
        srec['data_net_name'] = dnet.name
        srec['data_ipv4'] = data_ipv4.split("/")[0] if data_ipv4 else None
        srec['data_ipv6'] = data_ipv6.split("/")[0] if data_ipv6 else None
        srec['container_user'] = kwargs.get("USER_NAME", None)

        srec['kwargs'] = kwargs
        srec['sname'] = sname
        srec['node'] = node
        srec['node_id'] = node['id']
        srec['serv_port'] = sport
        srec['ctrl_port'] = cport
        srec['ctrl_host'] = constraints.nodeName if constraints.nodeName else node['public_url']
        srec['image'] = sreq.image
        srec['profile'] = prof.name
        srec['pull_image'] = prof.settings.pull_image
        #srec['qos'] = qos
        srec['errors'] = list()
        return srec
