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

        try:
            res = handler.exec_stream(Node(id=req.node_id, name=req.node), req.container, req.exec_id)
        except Exception as e:
            error = f"Exec stream failed for node {req.node} and container {req.container}: {e}"
            log.error(error)
            sock.send(json.dumps({"error": error}))
            return

        if not res:
            error = f"No Exec stream found for node {req.node} and container {req.container}"
            log.error(error)
            sock.send(json.dumps({"error": error}))
            return

        if not isinstance(res, tuple):
            while True:
                r = res.get()

                if r.get("eof"):
                    break
                sock.send(r.get("msg"))

            return

        receive_queue, send_queue, ws, sender_thread, receiver_thread = res

        def forward_output():
            try:
                while True:
                    response = receive_queue.get()
                    if response is None:
                        log.debug("Output forwarding completed")
                        break
                    try:
                        sock.send(response)
                    except websocket.WebSocketConnectionClosedException:
                        log.debug("Client disconnected during output")
                        break
            except Exception as e:
                log.error(f"Output error: {str(e)}")
            finally:
                receive_queue.task_done()

        output_thread = Thread(target=forward_output)
        output_thread.start()

        try:
            while True:
                try:
                    user_input = sock.receive()
                    if user_input.strip().lower() in ["exit", "quit"]:
                        break
                    if user_input == '\x03':  # Ctrl+C
                        log.debug("Received SIGINT, terminating session")
                        break
                    send_queue.put(user_input)
                except websocket.WebSocketConnectionClosedException:
                    log.debug("Client closed connection")
                    break
        except Exception as e:
            log.error(f"Input handling error: {e}")
        finally:
            try:
                sock.send(json.dumps({"status": "session_ended"}))
            except:
                pass
            handler.close_stream(ws, send_queue, receive_queue, sender_thread, receiver_thread)
            output_thread.join(timeout=2)
            log.debug("Stream cleanup completed")