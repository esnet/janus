import time
import json
import logging
from kubernetes import client, config
from kubernetes.config import KUBE_CONFIG_DEFAULT_LOCATION
from kubernetes.client.rest import ApiException
from janus.api.service import Service
from janus.api.constants import EPType
from janus.api.models import Network, ContainerProfile, Node
from janus.api.constants import Constants
from janus.settings import cfg
from janus.api.utils import (
    get_next_cport,
    get_next_sport,
    get_next_vf,
    get_next_ipv4,
    get_next_ipv6,
    get_numa,
    get_cpuset,
    get_mem,
    cname_from_id,
    is_subset
)


log = logging.getLogger(__name__)


class KubernetesApi(Service):
    CNI_VERSION = "0.3.1"
    NS = "default"

    def __init__(self):
        pass

    @property
    def type(self):
        return EPType.KUBERNETES

    def _get_client(self, ctx_name):
        return config.new_client_from_config(context=ctx_name)

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        if not refresh:
            return ret
        contexts, active_context = config.list_kube_config_contexts()
        if not contexts:
            log.error("Cannot find any context in kube-config file.")
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
            res = v1.list_node(watch=False)
            for i in res.items:
                cnode = {
                    "name": i.metadata.name,
                    "addresses": [ a.to_dict() for a in i.status.addresses ]
                }
                host_info['cpu']['count'] += int(i.status.capacity.get('cpu'))
                archs.add(i.status.node_info.architecture)
                cnodes.append(cnode)
                node_count += 1

            host_info['cpu']['brand_raw'] = " ".join(archs)
            cnets = dict()
            try:
                capi = client.CustomObjectsApi(api_client)
                res = capi.list_cluster_custom_object("k8s.cni.cncf.io", "v1", "network-attachment-definitions")
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

    def get_images(self, node):
        pass

    def get_networks(self, node):
        pass

    def get_containers(self, node):
        pass

    def get_logs(self, node, container, since=0, stderr=1, stdout=1, tail=100, timestamps=0):
        pass

    def pull_image(self, node, image, tag):
        pass

    def create_node(self, nname, eptype, **kwargs):
        pass

    def create_container(self, node: str, image: str, name: str = None, **kwargs):
        return {"Id": name}

    def start_container(self, node: str, container: str, service=None, **kwargs):
        api_client = self._get_client(node)
        v1 = client.CoreV1Api(api_client)
        print (service['kwargs'])
        resp = v1.create_namespaced_pod(body=service['kwargs'],
                                        namespace=self.NS)
        resp = v1.read_namespaced_pod(name=container,
                                      namespace='default')
        log.info(f"Pod transitioned to state: {resp.status.phase}")

    def stop_container(self, node, container):
        api = client.CoreV1Api(node)
        res = api.delete_namespaced_pod(container, self.NS)

    def create_network(self, nname, net_name, **kwargs):
        api_client = self._get_client(nname)
        api = client.CustomObjectsApi(api_client)
        created_resource = api.create_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            plural="network-attachment-definitions",
            namespace=self.NS,
            body=kwargs
        )
        log.info(f"Created network {net_name} on {nname}")

    def inspect_container(self, node, container):
        pass

    def remove_container(self, node, container):
        pass

    def connect_network(self, node, network, container):
        pass

    def exec_create(self, node, container, **kwargs):
        pass

    def exec_start(self, node, eid):
        pass

    def remove_network(self, node, network, **kwargs):
        api_client = self._get_client(node)
        api = client.CustomObjectsApi(api_client)
        api.delete_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            name=network,
            plural="network-attachment-definitions",
            namespace=self.NS
        )
        log.info(f"Removed network {network} on {node}")

    def resolve_networks(self, node, prof):
        def _build_net(p):
            cfg = {
                "cniVersion": self.CNI_VERSION,
                "plugins": [
                    {
                        "type": p.settings.driver,
                        "vlanId": int(p.settings.options.get('vlan')) if p.settings.options.get('vlan') else 1,
                        "master": p.settings.options.get('parent'),
                        "mtu": int(p.settings.options.get('mtu')) if p.settings.options.get('mtu') else 1500,
                        "ipam": {
                            "type": "static"
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
                    self.remove_network(node.get('id'), net.name)
                except Exception as e:
                    log.warn(f"Removing network {net.name} on {nname} failed: {e}")
            self.create_network(node.get('id'), net.name, **kwargs)
            created = True
        return created

    def create_service_record(self, sid, node, img, prof: ContainerProfile,
                              addrs_v4, addrs_v6, cports, sports,
                              arguments, remove_container, **kwargs):
        srec = dict()
        nname = node.get('name')
        cname = cname_from_id(sid)
        dnet = Network(prof.settings.data_net, nname)
        mnet = Network(prof.settings.mgmt_net, nname)
        args_override = arguments
        cmd = None
        if args_override:
            cmd = shlex.split(args_override)
        elif prof.settings.arguments:
            cmd = shlex.split(prof.settings.arguments)

        data_ipv6 = None
        cport = get_next_cport(node, prof, cports)
        sport = get_next_sport(node, prof, sports)

        dnet_conf = [
            {
                "name": dnet.name,
                "ips": [ "10.1.1.101/24" ]
            }
        ]

        kwargs = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": cname,
                "annotations": {
                    "k8s.v1.cni.cncf.io/networks": json.dumps(dnet_conf)
                }
            },
            "spec": {
                "containers": [
                    {
                        "name": cname,
                        "command": ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"],
                        "image": "dtnaas/tools",
                        "ports": [
                            {
                                "containerPort": 80
                            }
                        ]
                    }
                ]
            }
        }

        srec['mgmt_net'] = node['networks'].get(mnet.name, None)
        #srec['mgmt_ipv4'] = mgmt_ipv4
        #srec['mgmt_ipv6'] = mgmt_ipv6
        srec['data_net'] = node['networks'].get(dnet.name, None)
        srec['data_net_name'] = dnet.name
        #srec['data_ipv4'] = data_ipv4
        #srec['data_ipv6'] = data_ipv6
        srec['container_user'] = kwargs.get("USER_NAME", None)

        srec['kwargs'] = kwargs
        srec['node'] = node
        srec['node_id'] = node['id']
        srec['serv_port'] = sport
        srec['ctrl_port'] = cport
        srec['ctrl_host'] = node['public_url']
        srec['image'] = img
        srec['profile'] = prof.name
        srec['pull_image'] = prof.settings.pull_image
        #srec['qos'] = qos
        srec['errors'] = list()
        return srec
