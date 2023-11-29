import json
import logging
from kubernetes import client, config
from kubernetes.config import KUBE_CONFIG_DEFAULT_LOCATION
from kubernetes.client.rest import ApiException
from janus.api.service import Service
from janus.api.constants import EPType


log = logging.getLogger(__name__)


class KubernetesApi(Service):
    def __init__(self):
        pass

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
            ctx_name = ctx.get('name')
            archs = set()
            api_client = config.new_client_from_config(context=ctx_name)
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
                "public_url": KUBE_CONFIG_DEFAULT_LOCATION,
                "url": KUBE_CONFIG_DEFAULT_LOCATION
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

    def create_container(self, node, image, name=None, **kwargs):
        pass

    def create_network(self, node, name, **kwargs):
        pass

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

    def create_service_record(self, node, img, prof, addrs_v4, addrs_v6, cports, sports,
                              arguments, remove_container, **kwargs):
        
        pass
