import logging
from janus import settings
from janus.settings import cfg

from flask import jsonify
from flask_openapi3 import APIBlueprint, Tag
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash
from janus.api.models import QoS_Agent
from .sys.cpu import build_cpu
from .sys.mem import build_mem
from .sys.net import build_sriov
from .sys.numa import build_numa
from .sys.disk import build_block
from .sys.sysctl import get_tune, set_tune
from .sys.tc import get_eth_iface_rules, Netem, Delay, Latency, Filter, Pacing
from .models_api import InterfaceQuery, TuneRequest


# Basic auth
httpauth = HTTPBasicAuth()

log = logging.getLogger(__name__)

tag = Tag(name="janus/agent", description="Operations for node tuning")
api_prefix = getattr(settings, "API_PREFIX", "") or ""
api = APIBlueprint(
    "agent", __name__, url_prefix=api_prefix + "/janus/agent", abp_tags=[tag]
)


@httpauth.error_handler
def auth_error(status):
    return jsonify(error="Unauthorized"), status


@httpauth.verify_password
def verify_password(username, password):
    users = cfg.get_users()
    if username in users and check_password_hash(users.get(username), password):
        return username


@api.get("/node", summary="Returns static node resources")
def get_node():
    """
    Returns static node resources
    """
    ret = dict()
    try:
        ret["cpu"] = build_cpu()
    except Exception as e:
        log.warning(f"Could not build CPU info: {e}")
        ret["cpu"] = {}

    try:
        ret["mem"] = build_mem()
    except Exception as e:
        log.warning(f"Could not build MEM info: {e}")
        ret["mem"] = {}

    try:
        ret["numa"] = build_numa()
    except Exception as e:
        log.warning(f"Could not build NUMA info: {e}")
        ret["numa"] = {}

    try:
        ret["sriov"] = build_sriov()
    except Exception as e:
        log.warning(f"Could not build SRIOV info: {e}")
        ret["sriov"] = {}

    try:
        ret["block"] = build_block()
    except Exception as e:
        log.warning(f"Could not build BLOCK info: {e}")
        ret["block"] = {}

    return jsonify(ret), 200


@api.get("/tune", summary="Get node tuning settings")
def get_tune_endpoint():
    return jsonify(get_tune())


@api.post("/tune", summary="Set node tuning settings")
@httpauth.login_required
def post_tune_endpoint(body: TuneRequest):
    try:
        ret = set_tune(body.config)
    except Exception as e:
        return str(e), 500
    return jsonify(ret), 200


@api.get("/tc/netem", summary="Get netem rules")
def get_tc_netem(query: InterfaceQuery):
    iface = query.interface
    container = query.container

    if iface is None and container is None:
        return "No interface or container id specified", 400

    response = get_eth_iface_rules(iface, docker=container)

    if "error" in response:
        return jsonify(response), 400

    return jsonify(response), 200


@api.post("/tc/netem", summary="Set netem rules")
@httpauth.login_required
def post_tc_netem(body: QoS_Agent):
    default = {
        "interface": None,
        "delay": None,
        "loss": None,
        "rate": None,
        "corrupt": None,
        "reordering": None,
        "limit": None,
        "dport": None,
        "ip": None,
        "container": None,
    }

    try:
        req = body.model_dump()
        log.info(req)

        iface = req.get("interface", None)
        container = req.get("container", None)

        if iface is None and container is None:
            return "No interface or container id specified", 400

        default.update(req)
        req = default

    except Exception as e:
        return str(e), 500

    try:
        ret = Netem(req, verbose=True)
    except Exception as e:
        return str(e), 500
    return jsonify(ret), 200


@api.delete("/tc/netem", summary="Delete netem rules")
@httpauth.login_required
def delete_tc_netem(body: TuneRequest):
    try:
        req = body.config
        log.info(req)

        iface = req.get("interface", None)
        container = req.get("container", None)

        if iface is None and container is None:
            return "No interface or container id specified", 400

    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        ret = Netem(req, verbose=True, delete=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(ret), 200


@api.get("/tc/delay", summary="Get delay rules")
def get_tc_delay(query: InterfaceQuery):
    iface = query.interface
    if iface is None:
        return "No interface specified", 400
    return jsonify(get_eth_iface_rules(iface))


@api.post("/tc/delay", summary="Set delay rules")
@httpauth.login_required
def post_tc_delay(body: TuneRequest):
    default = {
        "interface": None,
        "latency": None,
        "loss": None,
        "dport": None,
        "dmask": None,
        "id": None,
        "maxrate": None,
        "ip": None,
        "type": None,
    }
    try:
        req = body.config
        log.info(req)
        default.update(req)
        req = default
        log.info(req)
    except Exception as e:
        return str(e), 500

    try:
        ret = Delay(req)
    except Exception as e:
        return str(e), 500

    return jsonify(ret), 200


@api.get("/tc/latency", summary="Get latency rules")
def get_tc_latency(query: InterfaceQuery):
    iface = query.interface
    if iface is None:
        return "No interface specified", 400
    return jsonify(get_eth_iface_rules(iface))


@api.post("/tc/latency", summary="Set latency rules")
@httpauth.login_required
def post_tc_latency(body: TuneRequest):
    default = {
        "interface": None,
        "latency": None,
        "loss": None,
        "dport": None,
        "dmask": None,
        "id": None,
        "maxrate": None,
        "ip": None,
        "type": None,
    }
    try:
        req = body.config
        log.info(req)
        default.update(req)
        req = default
        log.info(req)
    except Exception as e:
        return str(e), 500

    try:
        ret = Latency(req)
    except Exception as e:
        return str(e), 500

    return jsonify(ret), 200


@api.get("/tc/filter", summary="Get filter rules")
def get_tc_filter(query: InterfaceQuery):
    iface = query.interface
    if iface is None:
        return "No interface specified", 400
    return jsonify(get_eth_iface_rules(iface))


@api.post("/tc/filter", summary="Set filter rules")
@httpauth.login_required
def post_tc_filter(body: TuneRequest):
    try:
        req = body.config
        log.debug(req)
    except Exception as e:
        return str(e), 500

    try:
        ret = Filter(req)
    except Exception as e:
        return str(e), 500
    return jsonify(ret), 200


@api.get("/tc/pacing", summary="Get pacing rules")
def get_tc_pacing(query: InterfaceQuery):
    iface = query.interface
    if iface is None:
        return "No interface specified", 400
    return jsonify(get_eth_iface_rules(iface))


@api.post("/tc/pacing", summary="Set pacing rules")
@httpauth.login_required
def post_tc_pacing(body: TuneRequest):
    try:
        req = body.config
        log.debug(req)
        Pacing(req)
    except Exception as e:
        return str(e), 500
    else:
        return "OK", 200


@api.delete("/tc/pacing", summary="Delete pacing rules")
@httpauth.login_required
def delete_tc_pacing(body: TuneRequest):
    try:
        req = body.config
        log.debug(req)
        Pacing(req, delete=True)
    except Exception as e:
        return str(e), 500
    return "OK", 200
