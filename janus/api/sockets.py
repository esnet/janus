import json
import logging
import asyncio

from janus.settings import cfg
from janus.api.constants import WSType
from janus.api.models_ws import WSExecStream, EdgeAgentRegister
from janus.api.models import Node
from janus.api.pubsub import Subscriber, TOPIC


log = logging.getLogger(__name__)

async def handle_websocket(sock):
    data = await sock.receive()
    try:
        js = json.loads(data)
    except Exception as e:
        log.error(f"Invalid websocket request: {e}")
        await sock.send(json.dumps({"error": "Invalid request"}))
        return

    typ = js.get("type")
    if typ is None or typ not in [*WSType]:
        await sock.send(json.dumps({"error": f"Invalid websocket request type: {typ}"}))
        return

    if typ == WSType.AGENT_COMM:
        while True:
            msg = await sock.receive()
            if msg.strip() == "q" or msg.strip() == "quit":
                return
            await sock.send(msg)

    if typ == WSType.AGENT_REGISTER:
        peer = sock.sock.getpeername()
        try:
            req = EdgeAgentRegister(**js)
            cfg.sm.add_node(req)
        except Exception as e:
            log.error(f"Invalid request: {e}")
            await sock.send(json.dumps({"error": f"Invalid request: {e}"}))
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
            await sock.send(json.dumps(r.get("msg")))

    # if typ == WSType.EXEC_STREAM:
    #     log.debug(f"Got exec stream request from {sock.sock.getpeername()}")
    #     req = WSExecStream(**js)
    #     handler = cfg.sm.get_handler(nname=req.node)
    #     try:
    #         res = handler.exec_stream(Node(id=req.node_id, name=req.node), req.container, req.exec_id)
    #     except Exception as e:
    #         log.error(f"No exec stream found for node {req.node} and container {req.container}: {e}")
    #         sock.send(json.dumps({"error": "Exec stream not found"}))
    #         return
    #     while True:
    #         r = res.get()
    #         if r.get("eof"):
    #             break
    #         sock.send(r.get("msg"))

    if typ == WSType.EXEC_STREAM:
        log.debug(f"Got exec stream request from {sock.sock.getpeername()}")
        req = WSExecStream(**js)
        handler = cfg.sm.get_handler(nname=req.node)
        try:
            exec_id = handler.exec_create(Node(id=req.node_id, name=req.node), req.container, req.cmd)
            exec_response = handler.exec_start(Node(id=req.node_id, name=req.node), exec_id)
            # ws, q = handler.exec_stream(Node(id=req.node_id, name=req.node), exec_id)
            ws, q = handler.exec_stream(Node(id=req.node_id, name=req.node), req.container, req.exec_id)
        except Exception as e:
            log.error(f"No exec stream found for node {req.node} and container {req.container}: {e}")
            await sock.send(json.dumps({"error": "Exec stream not found"}))
            return

        async def send_input():
            while True:
                user_input = await sock.receive()
                if user_input.strip().lower() in ["q", "quit"]:
                    break
                # sock.send(user_input)
                ws.send(user_input)

        async def receive_output():
            while True:
                r = q.get()
                if r.get("eof"):
                    break
                await sock.send(r.get("msg"))

        await asyncio.gather(send_input(), receive_output())
