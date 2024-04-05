import json
import logging

from janus.settings import cfg
from janus.api.constants import WSType
from janus.api.models_ws import WSExecStream
from janus.api.models import Node
from janus.api.pubsub import Subscriber, TOPIC


log = logging.getLogger(__name__)

def handle_websocket(sock):
    data = sock.receive()
    try:
        js = json.loads(data)
    except Exception as e:
        log.error(f"Invalid websocket request: {e}")
        sock.send(json.dumps({"error": "Invalid request"}))
        return
    if js.get("type") == WSType.EVENTS:
        peer = sock.sock.getpeername()
        log.debug(f"Got event stream request from {peer}")
        sub = Subscriber(peer)
        res = cfg.sm.pubsub.subscribe(sub, TOPIC.event_stream)
        while True:
            r = sub.read()
            if r.get("eof"):
                break
            sock.send(json.dumps(r.get("msg")))
    if js.get("type") == WSType.EXEC_STREAM:
        log.debug(f"Got exec stream request from {sock.sock.getpeername()}")
        req = WSExecStream(**js)
        handler = cfg.sm.get_handler(nname=req.node)
        try:
            res = handler.exec_stream(Node(id=req.node_id, name=req.node), req.container, req.exec_id)
        except Exception as e:
            log.error(f"No exec stream found for node {req.node} and container {req.container}: {e}")
            sock.send(json.dumps({"error": "Exec stream not found"}))
            return
        while True:
            r = res.get()
            if r.get("eof"):
                break
            sock.send(r.get("msg"))
