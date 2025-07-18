import json
import logging
import queue
import websocket
from threading import Thread

from janus.settings import cfg
from janus.api.constants import WSType
from janus.api.models_ws import WSExecStream, EdgeAgentRegister
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

    typ = js.get("type")
    if typ is None or typ not in [*WSType]:
        sock.send(json.dumps({"error": f"Invalid websocket request type: {typ}"}))
        return

    if typ == WSType.AGENT_COMM:
        while True:
            msg = sock.receive()
            if msg.strip() == "q" or msg.strip() == "quit":
                return
            sock.send(msg)

    if typ == WSType.AGENT_REGISTER:
        peer = sock.sock.getpeername()
        try:
            req = EdgeAgentRegister(**js)
            cfg.sm.add_node(req)
        except Exception as e:
            log.error(f"Invalid request: {e}")
            sock.send(json.dumps({"error": f"Invalid request: {e}"}))
            return

    if typ == WSType.EVENTS:
        peer = sock.sock.getpeername()
        log.debug(f"Got event stream request from {peer}")
        sub = Subscriber(peer)
        res = cfg.sm.pubsub.subscribe(sub, TOPIC.event_stream)
        while True:
            r = sub.read()
            if r.get("eof"):
                break
            sock.send(json.dumps(r.get("msg")))

    if typ == WSType.EXEC_STREAM:
        log.debug(f"Got exec stream request from {sock.sock.getpeername()}")
        req = WSExecStream(**js)
        handler = cfg.sm.get_handler(nname=req.node)
        session = handler.exec_stream(Node(id=req.node_id, name=req.node), req.container, req.exec_id)
        receive_queue = session.receive_queue
        send_queue = session.send_queue

        def forward_output():
            try:
                for chunk in iter(receive_queue.get, None):
                    sock.send(chunk)
            finally:
                receive_queue.task_done()

        output_thread = Thread(target=forward_output, daemon=True)
        output_thread.start()

        try:
            while True:
                data = sock.receive()
                if data in ("exit", "quit", "\x03"):
                    break
                send_queue.put(data)
        finally:
            sock.send(json.dumps({"status": "session_ended"}))
            session.close()
            output_thread.join(2)