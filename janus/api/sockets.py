import json
import logging
from threading import Thread

from janus.settings import cfg
from janus.api.constants import WSType, EPType
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
        import time

        # sock is of type simple_websocket.ws.Server
        peer = sock.sock.getpeername()
        try:
            req = EdgeAgentRegister(**js)
        except Exception as e:
            log.error(f"Invalid request from {peer}: {e}: {js}")
            sock.send(json.dumps({"error": f"Invalid request: {e}"}))
            return

        try:
            from janus.api.jwt_utils import JwtUtils

            JwtUtils.verify_token(req.jwt)
        except Exception as e:
            log.error(f"Invalid token from {peer}: {e}: {req.jwt}")
            sock.send(json.dumps({"error": f"Invalid token: {e}"}))
            return

        try:
            edge_handle = cfg.sm.add_node(req, sock=sock)
            log.info(f"Added edge {peer}: {req.name}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error(f"Severe error add edge from {peer}: {e}")
            sock.send(json.dumps({"error": f"Controller in trouble: {e}"}))
            return

        # noinspection PyProtectedMember
        while edge_handle.sock.connected and edge_handle.active:
            time.sleep(1)

        log.warning(f"AGENT_REGISTER:inactive edge handle{peer}: {edge_handle.sock.connected}:{edge_handle.active}")
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
            while output_thread.is_alive():
                data = sock.receive(1)

                if not data:
                    continue

                if data in ("exit", "quit", "\x03"):
                    break
                send_queue.put(data)
        finally:
            sock.send(json.dumps({"status": "session_ended"}))
            session.close()
            output_thread.join(2)
