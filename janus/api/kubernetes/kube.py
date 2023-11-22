from kubernetes import client, config
from kubernetes.client import configuration
from janus.api.service import Service


class KubernetesApi(Service):
    def __init__(self):
        pass

    def get_nodes(self, nname=None, cb=None, refresh=False):
        return list()

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
