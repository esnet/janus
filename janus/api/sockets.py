import json
import logging

from janus.settings import cfg
from janus.api.constants import WSType
from janus.api.models_ws import WSExecStream
from janus.api.models import Node


log = logging.getLogger(__name__)

def handle_websocket(sock):
    data = sock.receive()
    try:
        js = json.loads(data)
    except Exception as e:
        log.error(f"Invalid websocket request: {e}")
        sock.send(json.dumps({"error": "Invalid request"}))
        return
    if js.get("type") == WSType.EXEC_STREAM:
        req = WSExecStream(**js)
        handler = cfg.sm.get_handler(nname=req.node)
        try:
            res = handler.exec_stream(Node(id=req.node, name=req.node), req.container)
        except Exception as e:
            log.error(f"No exec stream found for node {req.node} and container {req.container}")
            sock.send(json.dumps({"error": "Exec stream not found"}))
            return
        while res.is_open():
            res.update(timeout=1)
            if res.peek_stdout():
                sock.send(res.read_stdout())
                print (res.read_stdout())
            if res.peek_stderr():
                sock.send(res.read_stderr())
                print (res.read_stderr())





