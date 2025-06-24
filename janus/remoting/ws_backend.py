import logging
from configparser import ConfigParser

from janus.settings import cfg

log = logging.getLogger(__name__)


class WebsocketBackend:
    def __init__(self, properties, database):
        super().__init__()
        self.cfg = cfg
        self.ws = None
        self.properties = properties

        from janus.api.constants import EPType
        from janus.api.db import DBLayer
        from janus.api.manager import ServiceManager
        from janus.api.profile import ProfileManager

        self.eptype = EPType(self.properties['eptype'])

        if self.eptype not in [EPType.KUBERNETES, EPType.PORTAINER, EPType.SLURM]:
            raise Exception(f"Invalid endpoint type {self.eptype}")

        db = DBLayer(path=database)
        pm = ProfileManager(db, None)
        sm = ServiceManager(db)

        sm.service_map = {
            self.eptype: sm.service_map[self.eptype]
        }

        self.cfg.setdb(db, pm, sm)
        self.handler = sm.service_map[self.eptype]
        self.node_name = None

    # noinspection PyUnusedLocal
    def get_nodes(self,  value: dict):
        from janus.api.constants import EPType
        from janus.api.db import init_db

        args = value['args']
        refresh = args[0]

        if refresh:
            init_db(refresh=True)
        else:
            init_db(refresh=False)

        dbase = self.cfg.db
        table = dbase.get_table('nodes')
        nodes = dbase.all(table)

        if nodes:
            node = nodes[0]
            self.node_name = node['name']
            node['endpoint_type'] = EPType.EDGE
            node['name'] = self.properties['name']
            nodes = [node]

        return nodes

    def create_service(self, value: dict):
        from janus.api.models import SessionRequest
        from janus.api.models import SessionConstraints

        args = value['args']
        sname = args[0]
        sr_args = args[1]
        node = sr_args['node']
        image = sr_args['image']
        profile = sr_args['profile']
        constraints = sr_args['constraints']
        arguments = sr_args['arguments']
        remove_container = sr_args['remove_container']
        kwargs = sr_args['kwargs']
        overrides = sr_args['overrides']
        constraints = SessionConstraints(**constraints)
        node['name'] = self.node_name
        sreq = SessionRequest(node=node,
                              profile=profile,
                              image=image,
                              arguments=arguments,
                              remove_container=remove_container,
                              constraints=constraints,
                              kwargs=kwargs,
                              overrides=overrides
                              )
        addrs_v4 = set()  # keep a running set of addresses and ports allocated for this request
        addrs_v6 = set()
        cports = set()
        sports = set()
        service = self.handler.create_service_record(sname, sreq, addrs_v4, addrs_v6, cports, sports)
        service['node']['name'] = self.properties['name']
        log.info(f"{__name__}:exec create service record OK:{node['name']}:{sname}")
        return service

    def create_container(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        image = args[1]
        cname = args[2]
        kwargs = value['kwargs']
        ret = self.handler.create_container(node, image, cname, **kwargs)
        log.info(f"{__name__}:create container OK:{node.name}:{cname}:{image}:{ret}")
        return ret

    def start_container(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        container = args[1]
        service = args[2]
        kwargs = value['kwargs']
        ret = self.handler.start_container(node, container, service, **kwargs)
        log.info(f"{__name__}:start container OK:{node.name}:{container}:{ret}")
        return ret

    def stop_container(self, value: dict):
        from kubernetes.client import ApiException as KubeApiException
        from portainer_api.rest import ApiException as PortainerApiException
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        container = args[1]
        kwargs = value['kwargs']

        try:
            ret = self.handler.stop_container(node, container, **kwargs)
            log.info(f"{__name__}:exec stopped container OK:{node.name}:{container}:{ret}")
            return ret
        except (KubeApiException, PortainerApiException) as ae:
            if str(ae.status) != "404":
                import traceback
                traceback.print_exc()
                log.error(f"{__name__}:error stopping container:{node.name}:{container}:{ae}")
        except Exception as e:
            log.error(f"{__name__}:exec error stopping container:{node.name}:{container}:{e}")

        return None

    def exec_create(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        container = args[1]
        kwargs = value['kwargs']

        ret = self.handler.exec_create(node, container, **kwargs)
        log.info(f"{__name__}:exec start stream OK:{node.name}:{container}:{ret}")
        return ret

    def exec_start(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        ectx = args[1]
        kwargs = value['kwargs']

        print(f"edge exec start: {node.name}:{ectx}")
        ret = self.handler.exec_start(node, ectx, **kwargs)
        log.info(f"{__name__}:exec start stream OK:{node.name}:{ret}")
        return ret

    def exec_stream(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        container = args[1]
        eid = args[2]
        kwargs = value['kwargs']

        ret = self.handler.exec_stream(node, container, eid, **kwargs)
        log.info(f"{__name__}:exec stream OK:{node.name}:{container}:{eid}:{ret}")
        return ret

    def create_network(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        net_name = args[1]
        kwargs = value['kwargs']
        ret = self.handler.create_network(node, net_name, **kwargs)
        log.info(f"{__name__}:create network OK:{node.name}:{net_name}:{ret}")
        return ret

    def connect_network(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        nid = args[1]
        cid = args[2]
        kwargs = value['kwargs']
        ret = self.handler.connect_network(node, nid, cid, **kwargs)
        log.info(f"{__name__}:connect network OK:{node.name}:{nid}:{cid}:{ret}")
        return ret

    def remove_network(self,  value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        nid = args[1]
        kwargs = value['kwargs']
        ret = self.handler.remove_network(node, nid, **kwargs)
        log.info(f"{__name__}:remove network OK:{node.name}:{nid}:{ret}")
        return ret

    def resolve_networks(self, value: dict):
        print("HHHHHHHELLLO AES")
        from janus.api.models import Node
        from janus.api.models import ContainerProfile

        import json

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        prof = args[1]
        kwargs = value['kwargs']
        # resolve_networks expects the node as a dict.
        ret = self.handler.resolve_networks(json.loads(node.model_dump_json()),
                                            ContainerProfile(**prof),
                                            **kwargs)
        log.info(f"{__name__}:resolve network OK:{node.name}:{prof}:{ret}")
        return ret

    def inspect_container(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        cid = args[1]
        kwargs = value['kwargs']
        ret = self.handler.inspect_container(node, cid, **kwargs)
        log.info(f"{__name__}:inspect container OK:{node.name}:{cid}:{ret}")
        return ret

    def remove_container(self, value: dict):
        from janus.api.models import Node

        args = value['args']
        node = Node(**args[0])
        node.name = self.node_name
        cid = args[1]
        kwargs = value['kwargs']
        ret = self.handler.remove_container(node, cid, **kwargs)
        log.info(f"{__name__}:remove container OK:{node.name}:{cid}:{ret}")
        return ret

    def run(self, runner):
        from janus.api.models_ws import EdgeAgentRegister
        from janus.api.constants import WSType, WSEPType
        import json
        import websocket
        import ssl

        msg = {"type": WSType.AGENT_REGISTER,
               "jwt": self.properties.get('jwt'),
               "name": self.properties.get('name'),
               "edge_type": WSEPType(1000 + self.eptype),
               "public_url": self.properties.get('public_url')}

        EdgeAgentRegister(**msg)

        log.info(f"{__name__}:registration={msg}")
        # CTRL_HOST = os.getenv("JANUS_WEB_CTRL_HOST", "localhost")
        # CTRL_PORT = os.getenv("JANUS_WEB_CTRL_PORT", "5000")
        # CTRL_WS_PROTOCOL = os.getenv("CTRL_WS_PROTOCOL", "wss")
        # JANUS_CONTROLLER_WS_URL = "{}://{}:{}".format(CTRL_WS_PROTOCOL, CTRL_HOST, CTRL_PORT)

        ws_url = self.properties.get('ws_url')  # f"{JANUS_CONTROLLER_WS_URL}/ws"
        log.info(f"{__name__}:controller is at ={ws_url}")

        ws = self.ws = websocket.create_connection(ws_url, sslopt={"cert_reqs": ssl.CERT_NONE})
        ws.send(json.dumps(msg))
        log.info(f"{__name__}:registered ok ....")

        while runner.keep_running():
            try:
                log.debug(f"{__name__}:waiting on messages ...")
                data = ws.recv()
                message = json.loads(data)

                if not isinstance(message, dict) or message.get("error"):
                    raise Exception(f"Got invalid or error message: {message}")

                js = message.get('msg')

                if js['event'] != 'echo':
                    log.info(f"{__name__}:received {js['handler']}:{js['event']}...")

                if js['event'] == 'get_nodes':
                    js['value'] = self.get_nodes(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'create_service_record':
                    js['value'] = self.create_service(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'create_container':
                    js['value'] = self.create_container(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'start_container':
                    js['value'] = self.start_container(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'stop_container':
                    js['value'] = self.stop_container(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'create_network':
                    js['value'] = self.create_network(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'connect_network':
                    js['value'] = self.connect_network(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'remove_network':
                    js['value'] = self.remove_network(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'resolve_networks':
                    js['value'] = self.resolve_networks(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'inspect_container':
                    js['value'] = self.inspect_container(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'remove_container':
                    js['value'] = self.remove_container(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'exec_create':
                    js['value'] = self.exec_create(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'exec_start':
                    js['value'] = self.exec_start(js['value'])
                    ws.send(json.dumps(js))
                elif js['event'] == 'exec_stream':
                    q = self.exec_stream(js['value'])

                    while True:
                        r = q.get()
                        js['value'] = r
                        ws.send(json.dumps(js))

                        if r.get("eof"):
                            break
                elif js['event'] == 'echo':
                    ws.send(json.dumps(js))
                else:
                    js['value'] = f"Event {js['event']} is not supported."
                    ws.send(json.dumps(js))
            except Exception as e:
                log.error(f'Error in receiving while loop {__name__} : {e}')
                ws.close()
                raise e


class WebsocketBackendRunner:
    def __init__(self, janus_config_path, database):
        self._stop = False
        self._interval = 10
        self._th = None
        parser = ConfigParser(allow_no_value=True)
        parser.read(janus_config_path)
        properties = WebsocketBackendRunner._parse_from_config(parser)
        self.ws_backend = WebsocketBackend(properties, database)
        log.info(f"Initialized {__name__}:{ properties}")

    @staticmethod
    def _parse_from_config(parser: ConfigParser, agent_registration='AGENT_REGISTRATION'):
        if agent_registration not in parser:
            raise Exception("Missing agent registration configuration")

        name = parser.get(agent_registration, 'AGENT_NAME', fallback=None)
        public_url = parser.get(agent_registration, 'JWT', fallback=None)
        jwt = parser.get(agent_registration, 'PUBLIC_URL', fallback=None)
        eptype = parser.get(agent_registration, 'ENDPOINT_TYPE', fallback=2)
        ws_url = parser.get(agent_registration, 'CONTROLLER_WS_URL',
                            fallback='wss://localhost:5000/ws')

        return dict(name=name, public_url=public_url, jwt=jwt, eptype=int(eptype), ws_url=ws_url)

    def start(self):
        from threading import Thread

        log.debug(f"Started {__name__}")
        self._th = Thread(target=self._run, args=())
        self._th.start()

    def stop(self):
        log.debug(f"Stopping {__name__}")
        self._stop = True
        self.ws_backend.ws.close()
        self._th.join()

    def keep_running(self):
        return not self._stop

    def _run(self):
        import time

        cnt = 0
        log.debug(f"Running {__name__}:stop={self._stop}")

        while not self._stop:
            time.sleep(1)
            cnt += 1
            if cnt == self._interval:
                try:
                    self.ws_backend.run(self)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.error(f'Error running {__name__} : {e}')

                cnt = 0
