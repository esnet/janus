from tests.sense_test_utils import get_logger, get_db_file_path

from janus.api.db import DBLayer
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.settings import cfg

log = get_logger()


def init_managers(database):
    db = DBLayer(path=database)
    pm = ProfileManager(db, None)
    sm = ServiceManager(db)

    from janus.api.constants import EPType
    from janus.api.kubernetes import KubernetesApi

    sm.service_map = {
        EPType.KUBERNETES: KubernetesApi()
    }

    cfg.setdb(db, pm, sm)


# noinspection PyUnusedLocal
def get_nodes(nname=None, cb=None, refresh=False):
    from janus.api.constants import EPType

    database = get_db_file_path(db_file_name='db-test-edge.json')
    init_managers(database)

    from janus.api.db import init_db

    if refresh:
        init_db(refresh=True)
    else:
        init_db(refresh=False)

    dbase = cfg.db
    table = dbase.get_table('nodes')
    nodes = dbase.all(table)

    for node in nodes:
        node['endpoint_type'] = EPType.EDGE

    return nodes


def create_service(value: dict):
    from janus.api.kubernetes import KubernetesApi
    from janus.api.models import SessionRequest
    from janus.api.models import SessionConstraints
    kube_api = KubernetesApi()

    # create_service_record(self, sname, sreq: SessionRequest, addrs_v4, addrs_v6, cports, sports, ** kwargs):
    args = value['args']
    sname = args[0]

    print("SNAME:", sname)
    print("SessionRequest:", args[1].keys())

    # (['node', 'image', 'profile', 'constraints', 'arguments', 'remove_container', 'kwargs', 'overrides'])
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
    ret = kube_api.create_service_record(sname, sreq, addrs_v4, addrs_v6, cports, sports)
    print("edge created service OK ....")
    return ret


def start_container(value: dict):
    from janus.api.kubernetes import KubernetesApi

    # def start_container(self, node: Node, container: str, service=None, **kwargs):
    args = value['args']
    from janus.api.models import Node
    node = Node(**args[0])
    container = args[1]
    service = args[2]
    kwargs = value['kwargs']

    kube_api = KubernetesApi()
    ret = kube_api.start_container(node, container, service, **kwargs)
    print("edge started container OK ....")
    return ret


def stop_container(value: dict):
    from janus.api.kubernetes import KubernetesApi
    from kubernetes.client import ApiException as KubeApiException
    from portainer_api.rest import ApiException as PortainerApiException

    # stop_container(self, node: Node, container, **kwargs)
    args = value['args']
    from janus.api.models import Node
    node = Node(**args[0])
    container = args[1]
    kwargs = value['kwargs']

    kube_api = KubernetesApi()

    try:
        ret = kube_api.stop_container(node, container, **kwargs)
        print("edge stopped container OK ....")
        return ret
    except (KubeApiException, PortainerApiException) as ae:
        if str(ae.status) == "404":
            print("edge container not found ....")
    except Exception as e:
        print(f"edge error stopping container .... {e}")
        import traceback

        traceback.print_exc()
        return None


def exec_create(value: dict):
    # exec_create(self, node: Node, container, ** kwargs)
    args = value['args']
    from janus.api.models import Node
    node = Node(**args[0])
    container = args[1]
    kwargs = value['kwargs']

    handler = cfg.sm.get_handler(nname=node.name)
    print(f"edge exec create: {node.name}:{container}")
    ret = handler.exec_create(node, container, **kwargs)
    print(f"edge exec create OK: {ret}")
    return ret


def exec_start(value: dict):
    # exec_start(self, node: Node, ectx, **kwargs)
    args = value['args']
    from janus.api.models import Node
    node = Node(**args[0])
    ectx = args[1]
    kwargs = value['kwargs']

    handler = cfg.sm.get_handler(nname=node.name)
    print(f"edge exec start: {node.name}:{ectx}")
    ret = handler.exec_start(node, ectx, **kwargs)
    print(f"edge exec start OK: {ret}")
    return ret


def exec_stream(value: dict):
    # exec_stream(self, node: Node, container, eid, ** kwargs)
    args = value['args']
    from janus.api.models import Node
    node = Node(**args[0])
    container = args[1]
    eid = args[2]
    kwargs = value['kwargs']

    handler = cfg.sm.get_handler(nname=node.name)
    print(f"edge exec stream: {node.name}:{container}:{eid}")
    ret = handler.exec_stream(node, container, eid, **kwargs)
    print(f"edge exec stream OK ....{ret}")
    return ret


def run_edge():
    from janus.api.models_ws import EdgeAgentRegister
    from janus.api.constants import WSType
    import json
    from janus.api.constants import WSEPType

    msg = {"type": WSType.AGENT_REGISTER,
           "jwt": "",
           "name": "",
           "edge_type": WSEPType.KUBERNETES,
           "public_url": ""}

    req = EdgeAgentRegister(**msg)
    print(req)

    req = EdgeAgentRegister(type=WSType.AGENT_REGISTER,
                            jwt="",
                            name="",
                            edge_type=WSEPType.KUBERNETES,
                            public_url="")

    print(req)
    print(req.model_dump_json())

    import websocket
    import ssl
    import os

    nodes = get_nodes()
    CTRL_HOST = os.getenv("JANUS_WEB_CTRL_HOST", "localhost")
    CTRL_PORT = os.getenv("JANUS_WEB_CTRL_PORT", "5000")
    CTRL_WS_PROTOCOL = os.getenv("CTRL_WS_PROTOCOL", "wss")
    JANUS_CONTROLLER_WS_URL = "{}://{}:{}".format(CTRL_WS_PROTOCOL, CTRL_HOST, CTRL_PORT)

    ws_url = f"{JANUS_CONTROLLER_WS_URL}/ws"
    print(ws_url)

    ws = websocket.create_connection(ws_url, sslopt={"cert_reqs": ssl.CERT_NONE})
    ws.send(json.dumps(msg))
    print("DONE SENDING SUBSCRIBE MESSAGE")

    while True:
        try:
            print(f"EDGE YOYO WAITING ON RECEIVE ....")
            data = ws.recv()
            message = json.loads(data)
            js = message.get('msg')
            print("Received ", js['handler'], js['event'])

            if js['event'] == 'get_nodes':
                js['value'] = nodes
                ws.send(json.dumps(js))
            elif js['event'] == 'create_service_record':
                js['value'] = create_service(js['value'])
                ws.send(json.dumps(js))
            elif js['event'] == 'start_container':
                js['value'] = start_container(js['value'])
                ws.send(json.dumps(js))
            elif js['event'] == 'stop_container':
                js['value'] = stop_container(js['value'])
                ws.send(json.dumps(js))
            elif js['event'] == 'exec_create':
                js['value'] = exec_create(js['value'])
                ws.send(json.dumps(js))
            elif js['event'] == 'exec_start':
                js['value'] = exec_start(js['value'])
                ws.send(json.dumps(js))
            elif js['event'] == 'exec_stream':
                q = exec_stream(js['value'])

                while True:
                    r = q.get()
                    js['value'] = r
                    print(f"EDGE YOYO SENDING: {r}")
                    ws.send(json.dumps(js))

                    if r.get("eof"):
                        break

        except Exception as e:
            print(e)
            import traceback

            traceback.print_exc()
            ws.close()
            break


if __name__ == '__main__':
    try:
        run_edge()
    except Exception as bad:
        print("******")
        print(bad)
        print("******")
        import traceback

        traceback.print_exc()
        pass
