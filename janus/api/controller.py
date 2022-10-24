import logging
import uuid
import time
import json
from enum import Enum
from tinydb import TinyDB, Query
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor

from flask import request, jsonify
from flask_restplus import Namespace, Resource
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash
from werkzeug.exceptions import BadRequest

from pydantic import ValidationError
from urllib.parse import urlsplit
from janus import settings
from janus.settings import cfg
from .utils import create_service, commit_db, precommit_db, error_svc, handle_image, set_qos
from .db import init_db
from .validator import Profile as ProfileSchema

# XXX: Portainer will eventually go behind an ABC interface
# so we can support other provisioning backends
from portainer_api.configuration import Configuration as Config
from portainer_api.api_client import ApiClient
from portainer_api.api import AuthApi
from portainer_api.models import AuthenticateUserRequest
from portainer_api.rest import ApiException
from .portainer_docker import PortainerDockerApi
from .endpoints_api import EndpointsApi


class State(Enum):
    UNKNOWN = 0
    INITIALIZED = 1
    STARTED = 2
    STOPPED = 3
    MIXED = 4

class EPType(Enum):
    UNKNOWN = 0,
    PORTAINER = 1
    KUBERNETES = 2
    DOCKER = 3

# Basic auth
httpauth = HTTPBasicAuth()

log = logging.getLogger(__name__)

ns = Namespace('janus/controller', description='Operations for Janus on-demand container provisioning')

pclient = None
auth_expire = None
db_init = False

@httpauth.error_handler
def auth_error(status):
    return jsonify(error="Unauthorized"), status

@httpauth.verify_password
def verify_password(username, password):
    users = cfg.get_users()
    if username in users and \
            check_password_hash(users.get(username), password):
        return username

class auth(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        global db_init
        try:
            client = self.do_auth()

            # also setup testing DB
            # could also update DB on some interval...
            if not db_init:
                init_db(client, refresh=True)
                db_init = True
        except Exception as e:
            return json.loads(e.body), 500
        return self.func(*args, **kwargs)

    def do_auth(self):
        global pclient
        global auth_expire
        if auth_expire and pclient and (time.time() < auth_expire):
            return pclient

        pcfg = Config()
        pcfg.host = cfg.PORTAINER_URI
        pcfg.username = cfg.PORTAINER_USER
        pcfg.password = cfg.PORTAINER_PASSWORD
        pcfg.verify_ssl = cfg.PORTAINER_VERIFY_SSL

        if not pcfg.username or not pcfg.password:
            raise Exception("No Portainer username or password defined")

        pclient = ApiClient(pcfg)
        aa_api = AuthApi(pclient)
        res = aa_api.authenticate_user(AuthenticateUserRequest(pcfg.username,
                                                               pcfg.password))

        pcfg.api_key = {'Authorization': res.jwt}
        pcfg.api_key_prefix = {'Authorization': 'Bearer'}

        log.debug("Authenticating with token: {}".format(res.jwt))
        auth_expire = time.time() + 14400

        return pclient


@ns.route('/active')
@ns.route('/active/<int:id>')
@ns.route('/active/<user>')
class ActiveCollection(Resource):

    @httpauth.login_required
    def get(self, id=None, user=None):
        """
        Returns dictionary of active requests
        """
        DB = TinyDB(cfg.get_dbpath())
        Doc = Query()
        table = DB.table('active')
        docs = list()
        if id:
            doc = table.get(doc_id=id)
            if doc:
                docs = [doc]
        elif user:
            docs = table.search(Doc.user == user)
        else:
            docs = table.all()
        ret = []
        for d in docs:
            ret.append({d.doc_id: d})
        return ret

    @ns.response(204, 'Allocation successfully deleted.')
    @ns.response(404, 'Not found.')
    @ns.response(500, 'Internal server error')
    @httpauth.login_required
    @auth
    def delete(self, id):
        """
        Deletes an active allocation (e.g. stops containers)
        """
        DB = TinyDB(cfg.get_dbpath())
        Node = Query()
        nodes = DB.table('nodes')
        table = DB.table('active')
        doc = table.get(doc_id=id)
        if doc == None:
            return {"error": "Not found", "id": id}, 404

        force = request.args.get('force', None)
        dapi = PortainerDockerApi(pclient)
        futures = list()

        allocations = doc.get("allocations", dict())
        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in allocations.items():
                try:
                    n = nodes.search(Node.name == k)[0]
                    if not (cfg.dryrun):
                        for alloc in v:
                            futures.append(executor.submit(dapi.stop_container, n['id'], alloc))
                except Exception as e:
                    log.error("Could not find node/container to stop, or already stopped: {}".format(k))
        if not (cfg.dryrun):
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    if "container_id" in res:
                        log.debug(f"Removing container {res['container_id']}")
                        dapi.remove_container(res['node_id'], res['container_id'])
                except Exception as e:
                    log.error("Could not remove container on remote node: {}".format(e))
                    if not force:
                        return {"error": "{}".format(e)}, 503
        # delete always removes realized state info
        commit_db(doc, id, delete=True, realized=True)
        commit_db(doc, id, delete=True)
        return None, 204

@ns.response(400, 'Bad Request')
@ns.route('/nodes')
@ns.route('/nodes/<node>')
@ns.route('/nodes/<int:id>')
class NodeCollection(Resource):

    @httpauth.login_required
    @auth
    def get(self, node: str = None, id: int = None):
        """
        Returns list of existing nodes
        """
        DB = TinyDB(cfg.get_dbpath())
        DB.clear_cache()
        refresh = request.args.get('refresh', None)
        if refresh and refresh.lower() == 'true':
            log.info("Refreshing endpoint DB...")
            global pclient
            init_db(pclient, refresh=True)
        else:
            init_db(pclient, refresh=False)
        table = DB.table('nodes')
        if node or id:
            Node = Query()
            nodes = table.search(Node.id == id) if id else table.search(Node.name == node)
            return nodes if nodes else list()
        return table.all()

    @ns.response(204, 'Node successfully deleted.')
    @ns.response(404, 'Not found.')
    @httpauth.login_required
    @auth
    def delete(self, node: str = None, id: int = None):
        """
        Deletes a node (endpoint)
        """
        if not node and not id:
            return {"error": "Must specify node name or id"}, 400
        DB = TinyDB(cfg.get_dbpath())
        Node = Query()
        nodes = DB.table('nodes')
        doc = nodes.get(Node.id == id) if id else nodes.get(Node.name == node)
        if doc == None:
            return {"error": "Not found"}, 404
        eapi = EndpointsApi(pclient)
        try:
            eapi.endpoint_delete(doc.get('id'))
        except Exception as e:
            log.info("Could not remove portainer endpoint, ignoring...")
        nodes.remove(doc_ids=[doc.doc_id])
        return None, 204

    @httpauth.login_required
    @auth
    def post(self):
        """
        Handle the creation of a new endpoint (Node)
        """
        req = request.get_json()
        if not req:
            raise BadRequest("Body is empty")
        if type(req) is dict:
            req = [req]
        log.debug(req)

        eps = list()
        try:
            for r in req:
                url_split = urlsplit(r['url'])
                ep = {"name": r['name'],
                      "url": r['url'],
                      "public_url": url_split.hostname if not "public_url" in r else r['public_url'],
                      "type": EPType(r['type'])}
                eps.append(ep)
        except Exception as e:
            br = BadRequest()
            br.data = f"error decoding request: {e}"
            raise br

        eapi = EndpointsApi(pclient)
        try:
            for ep in eps:
                if ep['type'] == EPType.PORTAINER:
                    eptype = 2 # We use Portainer Agent registration method
                else:
                    raise BadRequest("Unsupported endpoint type")
                kwargs = {"url": ep['url'],
                          "public_url": ep['public_url'],
                          "tls": "true",
                          "tls_skip_verify": "true",
                          "tls_skip_client_verify": "true"}
                ret = eapi.endpoint_create(name=ep['name'], endpoint_type=eptype, **kwargs)
        except Exception as e:
            return {"error": "{}".format(e)}, 500

        try:
            log.info("New Node added, refreshing endpoint DB...")
            init_db(pclient, refresh=True)
        except Exception as e:
            return {"error": "Refresh DB failed"}, 500
        return None, 204

@ns.response(200, 'OK')
@ns.response(400, 'Bad Request')
@ns.response(503, 'Service unavailable')
@ns.route('/create')
class Create(Resource):

    @httpauth.login_required
    @auth
    def post(self):
        """
        Handle the creation of a container service
        """
        svcs = dict()
        req = request.get_json()
        if not req:
            raise BadRequest("Body is empty")
        if type(req) is dict:
            req = [req]
        log.debug(req)

        try:
            # keep a running set of addresses and ports allocated for this request
            addrs_v4 = set()
            addrs_v6 = set()
            cports = set()
            sports = set()
            for r in req:
                for s in r['instances']:
                    kwargs = r.get("kwargs", dict())
                    if s not in svcs:
                        svcs[s] = list()
                    svcs[s].append(create_service(s, r['image'], r['profile'], addrs_v4, addrs_v6,
                                                  cports, sports, **kwargs))
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error("Could not allocate request: {}".format(e))
            return {"error": "{}".format(e)}, 503

        # setup simple accounting
        record = {'uuid': str(uuid.uuid4()),
                  'user': httpauth.current_user(),
                  'state': State.INITIALIZED.name,
                  'allocations': dict()}

        dapi = PortainerDockerApi(pclient)
        # get an ID from the DB
        Id = precommit_db()
        for k, v in svcs.items():
            for s in svcs[k]:
                # the portainer node this service will start on
                n = s['node']
                img = s['image']
                if (cfg.dryrun):
                    ret = {'Id': str(uuid.uuid4())}
                else:
                    try:
                        handle_image(n, img, dapi)
                        name = f"janus_{Id}" if Id else None
                        ret = dapi.create_container(n['id'], img, name, **s['docker_kwargs'])
                    except ApiException as e:
                        log.error("Could not create container on {}: {}: {}".format(n['name'],
                                                                                    e.reason,
                                                                                    e.body))
                        error_svc(s, e)
                        continue

                if not (cfg.dryrun):
                    try:
                        # if specified, connect the management network to this created container
                        if s['mgmt_net']:
                            dapi.connect_network(n['id'], s['mgmt_net']['id'], ret['Id'],
                                                 **s['net_kwargs'])
                    except ApiException as e:
                        log.error("Could not connect network on {}: {}: {}".format(n['name'],
                                                                                   e.reason,
                                                                                   e.body))
                        error_svc(s, e)
                        continue

                s['container_id'] = ret['Id']
                if n['name'] not in record['allocations']:
                    record['allocations'].update({n['name']: list()})
                record['allocations'][n['name']].append(ret['Id'])

                del s['node']

        # complete accounting
        record['services'] = svcs
        record['request'] = req
        return commit_db(record, Id)

@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/start/<int:id>')
class Start(Resource):

    @httpauth.login_required
    @auth
    def put(self, id=None):
        """
        Handle the starting of container services
        """
        DB = TinyDB(cfg.get_dbpath())
        Srv = Query()
        table = DB.table('active')
        ntable = DB.table('nodes')
        if id:
            svc = table.get(doc_id=id)
        if not svc:
            return {"error": "id not found"}, 404

        if svc['state'] == State.STARTED.name:
            return {"error": "Service {} already started".format(svc['uuid'])}, 503

        # start the services
        error = False
        dapi = PortainerDockerApi(pclient)
        services = svc.get("services", dict())
        for k,v in services.items():
            for s in v:
                if not s['container_id']:
                    log.debug("Skipping service with no container_id: {}".format(k))
                    continue
                c = s['container_id']
                Node = Query()
                node = ntable.get(Node.name == k)
                log.debug("Starting container {} on {}".format(c, k))

                if not (cfg.dryrun):
                    try:
                        dapi.start_container(node['id'], c)

                        log.info("s: {}".format(s["qos"]))
                        if s['qos']: # is not None and s['qos'].isinstance(dict)
                            qos = s["qos"]
                            qos["container"] = c
                            set_qos(node["public_url"], qos)

                    except ApiException as e:
                        log.error("Could not start container on {}: {}: {}".format(k,
                                                                                   e.reason,
                                                                                   e.body))
                        error_svc(s, e)
                        error = True
                        continue
        svc['state'] = State.MIXED.name if error else State.STARTED.name
        return commit_db(svc, id, realized=True)

@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/stop/<int:id>')
class Stop(Resource):

    @httpauth.login_required
    @auth
    def put(self, id=None):
        """
        Handle the stopping of container services
        """
        DB = TinyDB(cfg.get_dbpath())
        Srv = Query()
        table = DB.table('active')
        ntable = DB.table('nodes')
        if id:
            svc = table.get(doc_id=id)
        if not svc:
            return {"error": "id not found"}, 404

        if svc['state'] == State.STOPPED.name:
            return {"error": "Service {} already stopped".format(svc['uuid'])}, 503
        if svc['state'] == State.INITIALIZED.name:
            return {"error": "Service {} is in initialized state".format(svc['uuid'])}, 503

        # stop the services
        error = False
        dapi = PortainerDockerApi(pclient)
        for k,v in svc['services'].items():
            for s in v:
                if not s['container_id']:
                    log.debug("Skipping service with no container_id: {}".format(k))
                    continue
                c = s['container_id']
                Node = Query()
                node = ntable.get(Node.name == k)
                log.debug("Stopping container {} on {}".format(c, k))
                if not (cfg.dryrun):
                    try:
                        dapi.stop_container(node['id'], c)
                    except ApiException as e:
                        log.error("Could not stop container on {}: {}: {}".format(k,
                                                                                  e.reason,
                                                                                  e.body))
                        error_svc(s, e)
                        error = True
                        continue
        svc['state'] = State.MIXED.name if error else State.STOPPED.name
        return commit_db(svc, id, delete=True, realized=True)

@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/exec')
class Exec(Resource):

    @httpauth.login_required
    @auth
    def post(self):
        """
        Handle the execution of a container command inside Service
        """
        svcs = dict()
        req = request.get_json()
        if type(req) is not dict or "Cmd" not in req:
            return {"error": "invalid request format"}, 400
        if "node" not in req:
            return {"error": "node not specified"}, 400
        if "container" not in req:
            return {"error": "container not specified"}, 400
        if type(req["Cmd"]) is not list:
            return {"error": "Cmd is not a list"}, 400
        log.debug(req)

        nname = req["node"]

        DB = TinyDB(cfg.get_dbpath())
        Node = Query()
        table = DB.table('nodes')
        node = table.get(Node.name == nname)
        if not node:
            return {"error": "Node not found: {}".format(nname)}

        container = req["container"]
        cmd = req["Cmd"]

        dapi = PortainerDockerApi(pclient)
        kwargs = {'AttachStdin': False,
                  'AttachStdout': True,
                  'AttachStderr': True,
                  'Tty': True,
                  'Cmd': cmd
                  }
        try:
            ret = dapi.exec_create(node["id"], container, **kwargs)
            ret = dapi.exec_start(node["id"], ret["Id"])
        except ApiException as e:
            log.error("Could not exec in container on {}: {}: {}".format(nname,
                                                                         e.reason,
                                                                         e.body))
            return {"error": e.reason}, 503
        return ret


@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/qos')
class QoS(Resource):
    @httpauth.login_required
    def get(self):
        name = request.args.get('name', None)
        if name:
            qos = cfg.get_qos(name)
            return {name: qos}

        return cfg.get_qos_list()


@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/profiles')
class Profile(Resource):
    @httpauth.login_required
    def get(self):
        refresh = request.args.get('refresh', None)
        reset = request.args.get('reset', None)
        pname = request.args.get('pname', None)
        if refresh and refresh.lower() == 'true':
            try:
                cfg.read_profiles()
            except Exception as e:
                return {"error": str(e)}, 500

        if reset and reset.lower() == 'true':
            try:
                cfg.read_profiles(reset=True)
            except Exception as e:
                return {"error": str(e)}, 500

        if pname:
            res = cfg.get_profile(pname, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(pname)}, 404

            return res
        else:
            log.info("Returning all profiles")
            return cfg.get_profiles(inline=True)

    @httpauth.login_required
    def post(self):
        try:
            req = request.get_json()

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            if "name" not in req or "settings" not in req:
                res = jsonify(error="please follow this format: {\"name\": \"myprofile\", \"settings\": {\"key\": \"value\"}}")
                res.status_code = 400
                return res

            configs = req["settings"]
            pname = req["name"]
            res = cfg.get_profile(pname, inline=True)
            if res:
                return {"error": "Profile {} already exists!".format(pname)}, 400

            default = cfg._base_profile.copy()
            default.update((k, configs[k]) for k in default.keys() & configs.keys())
            ProfileSchema(**default)

        except ValidationError as e:
            return str(e), 400

        except Exception as e:
            return str(e), 500

        try:
            DB = cfg._DB #TinyDB(cfg.get_dbpath())
            profile_tbl = DB.table('profiles')
            log.info("Creating profile {}".format(
                profile_tbl.insert({
                'name': pname,
                "settings": default
                })
            ))
        except Exception as e:
            return str(e), 500

        return cfg.get_profile(pname), 200

    @httpauth.login_required
    def put(self):
        try:
            req = request.get_json()

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            if "name" not in req or "settings" not in req:
                res = jsonify(error="please follow this format: {\"name\": \"myprofile\", \"settings\": {\"key\": \"value\"}}")
                res.status_code = 400
                return res

            configs = req["settings"]
            pname = req["name"]
            pname = req["name"]
            if pname == "default":
                return {"error": "Cannot update default profile!"}, 400

            res = cfg.get_profile(pname, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(pname)}, 404

            default = res.copy()
            log.info(default)
            default.update((k, configs[k]) for k in default.keys() & configs.keys())
            ProfileSchema(**default)

        except ValidationError as e:
            return {"error" : str(e)}, 400

        except Exception as e:
            return {"error" : str(e)}, 500

        try:
            DB = cfg._DB #TinyDB(cfg.get_dbpath())
            query = Query()
            profile_tbl = DB.table('profiles')
            profile_tbl.upsert({
                "settings": default
            }, query.name == pname)
        except Exception as e:
            return str(e), 500

        return cfg.get_profile(pname), 200

    @httpauth.login_required
    def delete(self):
        try:
            req = request.get_json()

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            if "name" not in req:
                res = jsonify(error="please follow this format: {\"name\": \"myprofile\"}")
                res.status_code = 400
                return res

            pname = req["name"]
            if pname == "default":
                return {"error": "Cannot delete default profile"}, 400

            res = cfg.get_profile(pname, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(pname)}, 404

        except Exception as e:
            return str(e), 500

        try:
            DB = cfg._DB #TinyDB(cfg.get_dbpath())
            query = Query()
            profile_tbl = DB.table('profiles')
            profile_tbl.remove(query.name == pname)
        except Exception as e:
            return str(e), 500

        return {}, 204
