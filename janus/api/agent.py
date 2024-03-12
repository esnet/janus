import logging
from urllib import response
from pydantic import ValidationError
from janus.settings import cfg

from flask import request, jsonify
from flask_restx import Namespace, Resource
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash
from janus.api.models import QoS_Agent
from .sys.cpu import build_cpu
from .sys.mem import build_mem
from .sys.net import build_sriov
from .sys.numa import build_numa
from .sys.disk import build_block
from .sys.sysctl import DEF_SYSCTL, get_tune, set_tune
from .sys.tc import get_eth_iface_rules, Delay, Latency, Filter, Pacing, Netem


# Basic auth
httpauth = HTTPBasicAuth()

log = logging.getLogger(__name__)

ns = Namespace('janus/agent', description='Operations for node tuning')

@httpauth.error_handler
def auth_error(status):
    return jsonify(error="Unauthorized"), status


@httpauth.verify_password
def verify_password(username, password):
    users = cfg.get_users()
    if username in users and \
            check_password_hash(users.get(username), password):
        return username


@ns.route('/node')
class NodeCollection(Resource):

    def get(self):
        """
        Returns static node resources
        """
        ret = dict()
        ret['cpu'] = build_cpu()
        ret['mem'] = build_mem()
        ret['numa'] = build_numa()
        ret['sriov'] = build_sriov()
        ret['block'] = build_block()

        return ret, 200


@ns.route('/tune')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TuneCollection(Resource):

    def get(self):
        return get_tune()

    @httpauth.login_required
    def post(self):
        req = None
        try:
            req = request.get_json()
            if req and type(req) is not dict:
                res = jsonify(error="Body is not a json dictionary")
                res.status_code = 400
                return res
            log.debug(req)
        except:
            pass

        try:
            ret = set_tune(req)
        except Exception as e:
            return str(e), 500
        return ret, 200

@ns.route('/tc/netem')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TrafficControlNetem(Resource):
    def get(self):
        iface = request.args.get('interface', None)
        container = request.args.get('container', None)

        if iface is None and container is None:
            return "No interface or container id specified", 400

        response = get_eth_iface_rules(iface, docker=container)

        if "error" in response:
            return response, 400

        return response, 200

    @httpauth.login_required
    def post(self):
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
            "container": None
        }

        try:
            req = request.get_json()
            QoS_Agent(**req)
            log.info(req)

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            iface = req.get('interface', None)
            container = req.get('container', None)

            if iface is None and container is None:
                return "No interface or container id specified", 400

            default.update(req)
            req = default

        except ValidationError as e:
            return str(e), 400

        except Exception as e:
            return str(e), 500

        try:
            ret = Netem(req, verbose=True)
        except Exception as e:
            return str(e), 500
        return ret, 200

    @httpauth.login_required
    def delete(self):
        try:
            req = request.get_json()
            log.info(req)

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            iface = req.get('interface', None)
            container = req.get('container', None)

            if iface is None and container is None:
                return "No interface or container id specified", 400

        except Exception as e:
            return {"error": str(e)}, 400

        try:
            ret = Netem(req, verbose=True, delete=True)
        except Exception as e:
            return {"error": str(e)}, 400
        return ret, 200


@ns.route('/tc/delay')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TrafficControlDelay(Resource):
    def get(self):
        iface = request.args.get('interface', None)

        if iface is None:
            return "No interface specified", 400

        return get_eth_iface_rules(iface)
        # return {"response":"get_eth_iface_rules() --> Backend not implemented!"}

    @httpauth.login_required
    def post(self):
	    # Making every value as None, if user missed entering any key than
	    # a default value is assigned by tc.py
        default = { "interface"  : None,
                "latency"    : None,
                "loss"       : None,
                "dport"      : None,
                "dmask"      : None,
                "id"         : None,
                "maxrate"    : None,
                "ip"         : None,
                "type"       : None,
                }
        try:
            req = request.get_json()
            log.info(req)

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            default.update(req)
            req = default
            # if req is None:
            #     return "Interface and latency must be specified", 400
                # req = {"interface"  : "eth100",
                #        "latency"    : "20ms",
                #          }
            log.info(req)
        except Exception as e:
            return str(e), 500

        try:
            ret = Delay(req)
        except Exception as e:
            return str(e), 500

        return ret, 200


@ns.route('/tc/latency')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TrafficControlLatency(Resource):
    def get(self):
        iface = request.args.get('interface', None)

        if iface is None:
            return "No interface specified", 400

        return get_eth_iface_rules(iface)
        # return {"response":"get_eth_iface_rules() --> Backend not implemented!"}

    @httpauth.login_required
    def post(self):
        default = { "interface"  : None,
                "latency"    : None,
                "loss"       : None,
                "dport"      : None,
                "dmask"      : None,
                "id"         : None,
                "maxrate"    : None,
                "ip"         : None,
                "type"       : None,
                }
        try:
            req = request.get_json()
            log.info(req)

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            default.update(req)
            req = default
            # if req is None:
            #     return "Interface and latency must be specified", 400
                # req = {"interface"  : "eth100",
                #        "latency"    : "20ms",
                #        "loss"       : "0.2%"
                #          }
            log.info(req)
        except Exception as e:
            return str(e), 500

        try:
            ret = Latency(req)
        except Exception as e:
            return str(e), 500

        return ret, 200


@ns.route('/tc/filter')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TrafficControlFilter(Resource):
    def get(self):
        iface = request.args.get('interface', None)

        if iface is None:
            return "No interface specified", 400

        return get_eth_iface_rules(iface)
        # return {"response":"get_eth_iface_rules() --> Backend not implemented!"}

    @httpauth.login_required
    def post(self):
        req = { "interface"  : None,
                "latency"    : None,
                "loss"       : None,
                "dport"      : None,
                "dmask"      : None,
                "id"         : None,
                "maxrate"    : None,
                "ip"         : None,
                "type"       : None,
                }
        try:
            req = request.get_json()
            if req and type(req) is not dict:
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res
            else:
                req = { "interface" : "eth100",
                        "latency"   : "20ms",
                        "loss"      : "0.1%",
                        "dport"     : "2048",
                        "dmask"     : "0xff00",
                        "id"        : "2",
                        }
            log.debug(req)
        except:
            pass

        try:
            ret = Filter(req)
        except Exception as e:
            return str(e), 500
        return ret, 200

@ns.route('/tc/pacing')
@ns.response(400, 'Bad Request')
@ns.response(500, 'Internal Server Error')
class TrafficControlPacing(Resource):
    def get(self):
        iface = request.args.get('interface', None)

        if iface is None:
            return "No interface specified", 400

        return get_eth_iface_rules(iface)

    @httpauth.login_required
    def post(self):
        try:
            req = request.get_json()
            if req and type(req) is not dict:
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res
            log.debug(req)
            ret = Pacing(req)
        except Exception as e:
            return str(e), 500
        else:
            return "OK", 200

    @httpauth.login_required
    def delete(self):
        try:
            req = request.get_json()
            if req and type(req) is not dict:
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res
            log.debug(req)
            ret = Pacing(req, delete=True)
        except Exception as e:
            return str(e), 500
        return "OK", 200
