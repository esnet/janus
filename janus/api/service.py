from abc import ABC, abstractmethod
from janus.api.models import Node, SessionRequest


class Service(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def resolve_networks(self, node: dict, prof):
        pass

    @abstractmethod
    def create_service_record(self, sid, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports):
        pass

    @abstractmethod
    def get_nodes(self, nname=None, cb=None, refresh=False):
        pass

    @abstractmethod
    def get_images(self, node: Node):
        pass

    @abstractmethod
    def get_networks(self, node: Node):
        pass

    @abstractmethod
    def get_containers(self, node: Node):
        pass

    @abstractmethod
    def get_logs(self, node: Node, container, since=0, stderr=1, stdout=1, tail=100, timestamps=0):
        pass

    @abstractmethod
    def pull_image(self, node: Node, image, tag):
        pass

    @abstractmethod
    def create_node(self, nname, eptype, **kwargs):
        pass

    @abstractmethod
    def create_container(self, node: Node, image, name=None, **kwargs):
        pass

    @abstractmethod
    def create_network(self, node: Node, name, **kwargs):
        pass

    @abstractmethod
    def inspect_container(self, node: Node, container):
        pass

    @abstractmethod
    def remove_container(self, node: Node, container):
        pass

    @abstractmethod
    def connect_network(self, node: Node, network, container):
        pass

    @abstractmethod
    def remove_network(self, node: Node, network, **kwargs):
        pass

    @abstractmethod
    def start_container(self, node: Node, container, service=None, **kwargs):
        pass

    @abstractmethod
    def stop_container(self, node: Node, container, **kwargs):
        pass

    @abstractmethod
    def exec_create(self, node: Node, container, **kwargs):
        pass

    @abstractmethod
    def exec_start(self, node: Node, eid):
        pass

    @abstractmethod
    def exec_stream(self, node: Node, container, eid, **kwargs):
        pass
