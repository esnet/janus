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
        contexts, active_context = config.list_kube_config_contexts()
        if not contexts:
            log.error("Cannot find any context in kube-config file.")
            return

        node_count = 0
        cnodes = dict()
        for ctx in contexts:
            ctx_name = ctx.get('name')
            api_client = config.new_client_from_config(context=ctx_name)
            v1 = client.CoreV1Api(api_client)
            res = v1.list_node(watch=False)
            cnodes[ctx_name] = list()
            for i in res.items:
                cnode = {
                    "name": i.metadata.name,
                    "addresses": [ a.to_dict() for a in i.status.addresses ]
                }
                cnodes[ctx_name].append(cnode)
                node_count += 1

            cnets = dict()
            try:
                capi = client.CustomObjectsApi(api_client)
                res = capi.list_cluster_custom_object("k8s.cni.cncf.io", "v1", "network-attachment-definitions")
                for i in res.get('items'):
                    meta = i.get('metadata')
                    name = meta.get('name')
                    spec = json.loads(i.get('spec').get('config'))
                    plugins = spec.get('plugins')[0]
                    cnets[name] = {
                        'name': meta.get('name'),
                        'id': meta.get('uid'),
                        'namespace': meta.get('namespace'),
                        'driver': plugins.get('type'),
                        'subnet': dict(),
                        '_data': i
                    }
            except ApiException as e:
                log.warn("Exception when calling ApiextensionsV1Api->read_custom_resource_definition: {e}")

            ret.append({
                "name": ctx_name,
                "id": ctx_name,
                "endpoint_type": EPType.KUBERNETES,
                "endpoint_status": 1,
                "cluster_node_count": node_count,
                "cluster_nodes": cnodes,
                "networks": cnets,
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
