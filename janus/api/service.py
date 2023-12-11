from abc import ABC, abstractmethod


class Service(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def resolve_networks(self, node, prof):
        pass

    @abstractmethod
    def create_service_record(self, node, img, prof, addrs_v4, addrs_v6, cports, sports,
                              arguments, remove_container, **kwargs):
        pass

    @abstractmethod
    def get_nodes(self, nname=None, cb=None, refresh=False):
        pass

    @abstractmethod
    def get_images(self, node):
        pass

    @abstractmethod
    def get_networks(self, node):
        pass

    @abstractmethod
    def get_containers(self, node):
        pass

    @abstractmethod
    def get_logs(self, node, container, since=0, stderr=1, stdout=1, tail=100, timestamps=0):
        pass

    @abstractmethod
    def pull_image(self, node, image, tag):
        pass

    @abstractmethod
    def create_node(self, nname, eptype, **kwargs):
        pass

    @abstractmethod
    def create_container(self, node, image, name=None, **kwargs):
        pass

    @abstractmethod
    def create_network(self, node, name, **kwargs):
        pass

    @abstractmethod
    def inspect_container(self, node, container):
        pass

    @abstractmethod
    def remove_container(self, node, container):
        pass

    @abstractmethod
    def connect_network(self, node, network, container):
        pass

    @abstractmethod
    def exec_create(self, node, container, **kwargs):
        pass

    @abstractmethod
    def exec_start(self, node, eid):
        pass
