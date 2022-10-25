class Network(object):
    def __init__(self, net, node=None):
        self.name = None
        self.ipv4 = None
        self.ipv6 = None
        self.node = node
        if isinstance(net, list):
            raise Exception("List of networks not supported")
        if isinstance(net, dict):
            self.name = net.get("name", None)
            self.ipv4 = net.get("ipv4_addr", None)
            self.ipv6 = net.get("ipv6_addr", None)
        elif isinstance(net, str):
            self.name = net
    @property
    def key(self):
        return f"{self.node}-{self.name}" if self.node else self.name

    def is_host(self):
        if self.name and self.name == "host":
            return True
        return False
