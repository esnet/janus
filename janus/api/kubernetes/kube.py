import json
import logging
import os
import time


from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.config import KUBE_CONFIG_DEFAULT_LOCATION
from kubernetes.stream import stream

from janus.api.constants import Constants
from janus.api.constants import EPType
from janus.api.models import (
    Node,
    Network,
    SessionRequest
)
from janus.api.service import Service
from janus.api.utils import (
    get_next_cport,
    get_next_sport,
    get_next_ipv4,
    get_next_ipv6,
    get_cpu,
    get_mem,
    is_subset
)
from janus.settings import cfg

log = logging.getLogger(__name__)


class KubernetesApi(Service):
    CNI_VERSION = "0.3.1"
    NS = "janus"
    DEF_MEM = "4Gi"
    DEF_CPU = "2"
    RETRIES = 12
    SLEEP_TIME = 5

    def __init__(self):
        super().__init__()
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
            contexts, _ = self._get_contexts()
            ctx = [x for x in contexts if x.get('name') == ctx_name][0]
            return ctx.get('context').get('namespace', self.NS)
        except Exception as e:
            log.error(f"Could not get namespace for ctx={ctx_name}: {e}")
            # return self.NS  AES RETURNING THE DEFAULT MASKS THE ERROR
            raise e

    def _get_client(self, ctx_name):
        try:
            if self.config:
                return client.ApiClient(self.config)
            return config.new_client_from_config(context=ctx_name)
        except Exception as e:
            log.error(f"Could not get Kubernetes client for {ctx_name}: {e}")
            return None

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
            # noinspection PyBroadException
            try:
                return config.list_kube_config_contexts()
            except Exception:
                return [], None

    def get_nodes(self, nname=None, cb=None, refresh=False):
        ret = list()
        if not refresh:
            return ret
        contexts, active_context = self._get_contexts()
        if not contexts:
            log.error("Cannot find any contexts in current configuration.")
            return ret

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
            if not api_client:
                continue
            v1 = client.CoreV1Api(api_client)
            try:
                res = v1.list_node(watch=False)
            except Exception as e:
                log.error(f"Could not list nodes on cluster {ctx_name}: {e}")
                continue
            for i in res.items:
                addresses = i.status.addresses

                cnode = {
                    "name": i.metadata.name,
                    "addresses": [a.address if a.type == "InternalIP" else "" for a in i.status.addresses],
                    "internal_addresses": [a.address for a in addresses if a.type == 'InternalIP'],
                    "external_addresses": [a.address for a in addresses if a.type == 'ExternalIP'],
                    "host_addresses": [a.address for a in addresses if a.type == "Hostname"],
                    "phase": i.status.phase
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
                    cnets[name] = self.to_cnet(i)

            except ApiException as e:
                log.warning(f"Could not find network attachement definitions on cluster {ctx_name}: {e}")

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
        cnets = list()
        api_client = self._get_client(node.name)
        namespace = self._get_namespace(node.name)

        api = client.CustomObjectsApi(api_client)
        res = api.list_namespaced_custom_object("k8s.cni.cncf.io",
                                                "v1",
                                                namespace,
                                                "network-attachment-definitions")

        for i in res.get('items'):
            cnets.append(self.to_cnet(i))

        return cnets

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

    def get_pods(self, node: Node):
        from kubernetes.client.models.v1_pod_list import V1PodList
        from kubernetes.client.models.v1_pod import V1Pod
        from kubernetes.client.models.v1_object_meta import V1ObjectMeta
        from kubernetes.client.models.v1_pod_status import V1PodStatus
        from kubernetes.client.models.v1_pod_spec import V1PodSpec

        api_client = self._get_client(node.name)
        v1 = client.CoreV1Api(api_client)
        namespace = self._get_namespace(node.name)
        res: V1PodList = v1.list_namespaced_pod(namespace)
        cpods = list()

        for i in res.items:
            pod: V1Pod = i
            metadata: V1ObjectMeta = pod.metadata
            status: V1PodStatus = pod.status
            spec: V1PodSpec = pod.spec
            container_statuses = status.container_statuses or list()

            cpod = {
                "name": metadata.name,
                "labels": metadata.labels,
                "container_states": [s.state for s in container_statuses],
                "host_ip": status.host_ip,
                "container_images": [s.image for s in container_statuses],
                "phase": status.phase,
                "node_name": spec.node_name,
                "annotations": metadata.annotations
            }

            cpods.append(cpod)

        return cpods

    def start_container(self, node: Node, container: str, service=None, **kwargs):
        api_client = self._get_client(node.name)
        v1 = client.CoreV1Api(api_client)
        v1.create_namespaced_pod(body=service['kwargs'],
                                 namespace=self._get_namespace(node.name))
        pod = v1.read_namespaced_pod(name=container,
                                     namespace=self._get_namespace(node.name))
        attempt = 0
        retries = self.RETRIES

        while pod.status.phase == 'Pending' and attempt < retries:
            attempt += 1
            log.debug(f"Starting container={pod.metadata.name}:phase={pod.status.phase}:attempt={attempt}/{retries}")
            pod = v1.read_namespaced_pod(name=container, namespace=self._get_namespace(node.name))

            if pod.status.phase == 'Running':
                break

            time.sleep(self.SLEEP_TIME)

        if pod.status.phase != 'Running':
            wait_time = attempt * self.SLEEP_TIME
            log.error(f'Starting container={pod.metadata.name}:phase={pod.status.phase}:wait_time={wait_time}s')
            raise Exception(f'Starting container={pod.metadata.name}:phase={pod.status.phase}:wait_time={wait_time}s')

        cpod = {
            "name": pod.metadata.name,
            "phase": pod.status.phase
        }

        log.info(f"Started container: {cpod}:wait_time={attempt * self.SLEEP_TIME}s")
        return cpod

    def stop_container(self, node: Node, container, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CoreV1Api(api_client)
        pod = api.delete_namespaced_pod(str(container), self._get_namespace(node.name))
        deleted = False
        attempt = 0
        retries = self.RETRIES

        while attempt < retries:
            try:
                pod = api.read_namespaced_pod(name=container, namespace=self._get_namespace(node.name))
            except ApiException as ae:
                deleted = str(ae.status) == "404"

                if deleted:
                    break

                log.warning(f'Stopping container={pod.metadata.name}:{ae}:attempt={attempt}/{retries}')

            attempt += 1
            time.sleep(self.SLEEP_TIME)

        if not deleted:
            log.error(f'Stopping container={pod.metadata.name}:wait_time={attempt * self.SLEEP_TIME}s')
            raise Exception(f'Stopping container={pod.metadata.name}:wait_time={attempt * self.SLEEP_TIME}s')

        cpod = {
            "name": pod.metadata.name,
            "phase": pod.status.phase
        }

        log.info(f"Stopped container: {cpod}:wait_time={attempt * self.SLEEP_TIME}s")
        return cpod

    def inspect_container(self, node: Node, container, **kwargs):
        pass

    def remove_container(self, node: Node, container, **kwargs):
        pass

    def connect_network(self, node: Node, network, container, **kwargs):
        pass

    def exec_create(self, node: Node, container, **kwargs):
        from kubernetes.stream.ws_client import WSClient
        from janus.api.utils import ExecKubeSession

        api_client = self._get_client(node.name)
        api = client.CoreV1Api(api_client)
        ws_client: WSClient = stream(api.connect_get_namespaced_pod_exec,
                                     name=container,
                                     namespace=self._get_namespace(node.name),
                                     command=kwargs.get("Cmd"),
                                     container=container,
                                     stderr=kwargs.get("AttachStderr", True),
                                     stdin=kwargs.get("AttachStdin", True),
                                     stdout=kwargs.get("AttachStdout", True),
                                     tty=kwargs.get("Tty", False),
                                     _preload_content=False
                                     )

        if not self._exec_map.get(node.name):
            self._exec_map[node.name] = dict()
            self._exec_map[node.name][container] = ExecKubeSession(ws_client)
        else:
            self._exec_map[node.name][container] = ExecKubeSession(ws_client)

        return {"response": "websocket created"}

    def exec_start(self, node: Node, ectx, **kwargs):
        return ectx

    def exec_stream(self, node: Node, container, eid, **kwargs):
        return self._exec_map.get(node.name).get(container)

    @staticmethod
    def to_cnet(net):
        meta = net.get('metadata')
        name = meta.get('name')
        spec = json.loads(net.get('spec').get('config'))
        plugins = spec.get('plugins')[0]
        snets = list()
        if plugins.get('ipam') and plugins.get('ipam').get('addresses'):
            for a in plugins.get('ipam').get('addresses'):
                snets.append({
                    'Subnet': a.get('address'),
                    'Gateway': a.get('gateway')
                })

        cnet = {
            'name': name,
            'id': meta.get('uid'),
            'namespace': meta.get('namespace'),
            'driver': plugins.get('type'),
            'parent': plugins.get('master'),
            'mode': plugins.get('mode'),
            'vlan': plugins.get('vlanId'),
            'mtu': plugins.get('mtu'),
            'subnet': snets,
            '_data': net
        }

        return cnet

    def create_network(self, node: Node, net_name, **kwargs):
        api_client = self._get_client(node.name)
        api = client.CustomObjectsApi(api_client)
        net = api.create_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            plural="network-attachment-definitions",
            namespace=self._get_namespace(node.name),
            body=kwargs
        )
        log.info(f"Created network {net_name} on {node}:{net}")
        return self.to_cnet(net)

    def get_network(self, node: Node, network):
        api_client = self._get_client(node.name)
        api = client.CustomObjectsApi(api_client)
        net = api.get_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            name=network,
            plural="network-attachment-definitions",
            namespace=self._get_namespace(node.name)
        )

        return self.to_cnet(net)

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

    # AES TODO
    # noinspection PyMethodOverriding
    def resolve_networks(self, node: dict, prof, **kwargs):
        from janus.api.models import NetworkProfile

        def _build_net(p, data_net_overrides):
            data_net_overrides = data_net_overrides or dict()
            p.settings.options.update(data_net_overrides)

            net_cfg = {
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
                    "name": p.settings.options.get('name', p.name)
                },
                "spec": {
                    "config": json.dumps(net_cfg)
                }
            }
            return ret

        created = False

        for net in [Network(prof.settings.mgmt_net), Network(prof.settings.data_net)]:
            if not net.name or net.name in [Constants.NET_NONE, Constants.NET_HOST, Constants.NET_BRIDGE]\
                    or net.is_host():
                continue

            nname = node.get('name')

            if kwargs.get('nprof'):
                nprof = NetworkProfile(**kwargs['nprof'])
            else:
                nprof = cfg.pm.get_profile(Constants.NET, net.name)

            if not nprof:
                raise Exception(f"Network profile {net.name} not found")

            net_attach_def = _build_net(nprof, kwargs)
            net_name = kwargs.get('name', net.name)
            ninfo = None

            try:
                ninfo = self.get_network(Node(**node), net_name)

                if is_subset(net_attach_def, ninfo.get('_data')):
                    log.info(f"Found matching network {net_name} found on {nname}")
                    node['networks'][net_name] = ninfo
                    continue
            except ApiException as ae:
                if str(ae.status) != "404":
                    raise ae

            if ninfo:
                log.warning(f"Removing non matching network {net_name} on {nname}")
                self.remove_network(Node(**node), net_name)

                if net_name in node['networks']:
                    del node['networks'][net_name]

            log.info(f"Creating: No matching network {net_name} found on {nname}")
            ninfo = self.create_network(Node(**node), net_name, **net_attach_def)
            node['networks'][net_name] = ninfo
            created = True

        return created

    # noinspection PyTypeChecker
    def create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports, **kwargs):
        data_net_overrides = kwargs
        srec = dict()
        node = sreq.node
        prof = sreq.profile
        constraints = sreq.constraints
        nname = node.get('name')
        cname = sname
        dnet = Network(prof.settings.data_net, nname)
        mnet = Network(prof.settings.mgmt_net, nname)

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
            # kwargs['spec'].update({"nodeName": constraints.nodeName}) # Does not work anymore with nautilus.
            node_affinity = client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                    node_selector_terms=[
                        client.V1NodeSelectorTerm(
                            match_expressions=[
                                client.V1NodeSelectorRequirement(
                                    key='kubernetes.io/hostname',  # Standard node name label
                                    operator='In',
                                    values=[constraints.nodeName]  # The node you want the pod to land on
                                )
                            ]
                        )
                    ]
                )
            )

            affinity = client.V1Affinity(node_affinity=node_affinity)
            sanitized_affinity = client.ApiClient().sanitize_for_serialization(affinity)
            kwargs['spec'].update({"affinity": sanitized_affinity})
            toleration = client.V1Toleration(
               key="nautilus.io/reservation",
               operator="Equal", 
               value="sense",
               effect="NoSchedule"
            )
            sanitized_toleration = client.ApiClient().sanitize_for_serialization(toleration)
            kwargs['spec'].update({"tolerations": [sanitized_toleration]})

        if mnet.is_host():
            kwargs['spec'].update({"hostNetwork": True})

        srec['data_net'] = None
        srec['data_net_name'] = None

        if dnet.name:
            ips = []

            if data_net_overrides:
                if 'data_ipv4' in data_net_overrides:  # Used by edge
                    data_ipv4 = data_net_overrides['data_ipv4']
                else:
                    dnet_name = data_net_overrides['name']
                    key = f"{nname}-{dnet_name}"
                    data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True, key=key, name=dnet_name)

                if 'data_ipv6' in data_net_overrides:
                    data_ipv6 = data_net_overrides['data_ipv6']
                else:
                    dnet_name = data_net_overrides['name']
                    key = f"{nname}-{dnet_name}"
                    data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True, key=key, name=dnet_name)
            else:
                data_ipv4 = get_next_ipv4(dnet, addrs_v4, cidr=True)
                data_ipv6 = get_next_ipv6(dnet, addrs_v6, cidr=True)

            if data_ipv4:
                ips.append(data_ipv4)
            if data_ipv6:
                ips.append(data_ipv6)

            dnet_conf = [
                {
                    "name": data_net_overrides.get('name', dnet.name),
                    "ips": ips
                }
            ]
            anno = {
                "annotations": {
                    "k8s.v1.cni.cncf.io/networks": json.dumps(dnet_conf)
                }
            }
            kwargs['metadata'].update(anno)

            srec['data_net'] = node['networks'][data_net_overrides.get('name', dnet.name)]
            srec['data_net_name'] = data_net_overrides.get('name', dnet.name)

        srec['mgmt_net'] = node['networks'].get(mnet.name, None)
        srec['mgmt_ipv4'] = mgmt_ipv4
        srec['mgmt_ipv6'] = mgmt_ipv6
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
        # srec['qos'] = qos
        srec['errors'] = list()
        return srec
