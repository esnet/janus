import logging
from functools import wraps
from urllib.parse import urlsplit

from flask import request, jsonify, abort
from flask_httpauth import HTTPBasicAuth
from flask_restx import Namespace, Resource
from pydantic import ValidationError
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError
from werkzeug.security import check_password_hash

from janus.api.db import init_db, QueryUser
from janus.api.models import (
    Node,
    ContainerProfile,
    NetworkProfile,
    VolumeProfile
)
from janus.api.models_api import (
    AddEndpointRequest,
    SessionRequest,
    ProfileRequest,
    ExecRequest,
    AuthRequest
)
from janus.api.utils import (
    Constants
)
from janus.settings import cfg
from janus.api.constants import WSType

# Basic auth
httpauth = HTTPBasicAuth()
log = logging.getLogger(__name__)
ns = Namespace('janus/controller', description='Operations for Janus on-demand container provisioning')

addEndpointRequestModel = ns.schema_model('AddEndpointModel', AddEndpointRequest.model_json_schema())
sessionRequestModel = ns.schema_model('SessionRequestModel', SessionRequest.model_json_schema())
profileRequestModel = ns.schema_model('ProfileRequestModel', ProfileRequest.model_json_schema())
execRequestModel = ns.schema_model('ExecRequestModel', ExecRequest.model_json_schema())
authRequestModel = ns.schema_model('AuthRequestModel', AuthRequest.model_json_schema())


@httpauth.error_handler
def auth_error(status):
    return jsonify(error="Unauthorized"), status


@httpauth.verify_password
def verify_password(username, password):
    users = cfg.get_users()
    if username in users and \
       check_password_hash(users.get(username), password):
        return username


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not httpauth.current_user() == 'admin':
            abort(403)
        return f(*args, **kwargs)
    return wrapper


def get_authinfo(request):
    api_user = httpauth.current_user()
    if api_user == 'admin':
        user = request.args.get('user', None)
        group = request.args.get('group', None)
    else:
        user = api_user
        group = None
    log.debug(f"User: {user}, Group: {group}")
    return (user, group)


@ns.route('/active/<int:aid>/logs/<path:nname>', methods=['GET'])
class LogCollection(Resource, QueryUser):

    @httpauth.login_required
    @ns.doc(params={"timestamps": {"description": "Include timestamps in logs", "type": "integer"},
                    "stderr": {"description": "Include stderr in logs", "type": "integer"},
                    "stdout": {"description": "Include stdout in logs", "type": "integer"},
                    "since": {"description": "Return logs since this timestamp", "type": "integer"},
                    "tail": {"description": "Number of lines to return from the end of the log", "type": "integer"}})
    def get(self, aid=None, nname=None):
        """
        Display logs for a specific active session and node.
        """
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": aid})
        dbase = cfg.db
        table = dbase.get_table('active')
        if query and aid:
            res = dbase.get(table, query=query)
            if not res:
                return {"error": "Not found"}, 404
            if nname:
                try:
                    ts = request.args.get('timestamps', 0)
                    stderr = request.args.get('stderr', 1)
                    stdout = request.args.get('stdout', 1)
                    since = request.args.get('since', 0)
                    tail = request.args.get('tail', 100)
                    svc = res['services'][nname]
                    cid = svc[0]['container_id']
                    n = Node(id=svc[0]['node_id'], name=nname)
                    handler = cfg.sm.get_handler(nname=nname)
                    return handler.get_logs(n, cid, since, stderr, stdout, tail, ts)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    return {"error": f"Could not retrieve container logs: {e}"}, 500


@ns.route('/active', methods=['GET'])
@ns.route('/active/<int:aid>')
class ActiveCollection(Resource, QueryUser):

    @httpauth.login_required
    @ns.doc(params={"fields": "Comma separated list of fields to return"})
    def get(self, aid=None, nname=None):
        """
        Get active sessions
        """
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": aid})
        fields = request.args.get('fields')
        dbase = cfg.db
        table = dbase.get_table('active')
        if query and aid:
            res = dbase.get(table, query=query)
            if not res:
                return {"error": "Not found"}, 404
            if (fields):
                return {k: v for k, v in res.items() if k in fields.split(',')}
            else:
                return res
        elif query:
            return dbase.search(table, query=query)
        else:
            res = dbase.all(table)
            if (fields):
                ret = list()
                for r in res:
                    if not r:
                        continue
                    ret.append({k: v for k, v in r.items() if k in fields.split(',')})
                return ret
            else:
                return res

    @ns.response(204, 'Allocation successfully deleted.')
    @ns.response(404, 'Not found.')
    @ns.response(500, 'Internal server error')
    @httpauth.login_required
    def delete(self, aid):
        """
        Delete a session by id.
        """
        (user, group) = get_authinfo(request)

        from janus.api.session_manager import SessionManager, ResourceNotFoundException, SessionManagerException

        try:
            force = request.args.get('force', None)
            session_manager = SessionManager()
            session_manager.delete(aid, force, user, group)
            return None, 204
        except ResourceNotFoundException as e:
            raise NotFound(f'Creating session failed:{e}')
        except SessionManagerException as e:
            raise InternalServerError(f'Stopping session failed:{e}')
        except Exception as e:
            raise InternalServerError(f'Stopping session failed:FATAL:{type(e)}:{e}')


@ns.response(400, 'Bad Request')
@ns.route('/nodes', methods=['GET', 'POST'])
@ns.route('/nodes/<node>', methods=['GET', 'DELETE'])
@ns.route('/nodes/<int:id>', methods=['GET', 'DELETE'])
class NodeCollection(Resource, QueryUser):

    @httpauth.login_required
    @ns.doc(params={"refresh": "Set to 'true' to refresh the endpoint DB"})
    def get(self, node: str = None, id: int = None):
        """
        Get a list of existing nodes (endpoints).
        """
        (user, group) = get_authinfo(request)
        refresh = request.args.get('refresh', None)
        if refresh and refresh.lower() == 'true':
            log.info("Refreshing endpoint DB...")
            init_db(refresh=True)
        else:
            init_db(refresh=False)
        dbase = cfg.db
        table = dbase.get_table('nodes')
        query = self.query_builder(user, group, {"id": id, "name": node})
        if query and (id or node):
            res = dbase.get(table, query=query)
            if not res:
                return {"error": "Not found"}, 404
            return res
        elif query:
            return dbase.search(table, query=query)
        else:
            return dbase.all(table)

    @ns.response(204, 'Node successfully deleted.')
    @ns.response(404, 'Not found.')
    @httpauth.login_required
    def delete(self, node: str = None, id: int = None):
        """
        Deletes a node (endpoint).
        """
        if not node and not id:
            return {"error": "Must specify node name or id"}, 400
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id, "name": node})
        dbase = cfg.db
        nodes = dbase.get_table('nodes')
        doc = dbase.get(nodes, query=query)
        if doc == None:
            return {"error": "Not found"}, 404
        try:
            cfg.sm.remove_node(doc)
        except Exception as e:
            log.info(f"Could not remove node, ignoring: {e}")
        dbase.remove(nodes, ids=doc.doc_id)
        return None, 204

    @httpauth.login_required
    @ns.expect([addEndpointRequestModel], validate=True)
    def post(self):
        """
        Add a new node (endpoint).
        """
        req = ns.payload
        if type(req) is dict:
            req = [req]
        log.debug(req)
        eps = list()
        try:
            for r in req:
                ep = AddEndpointRequest(**r)
                if not ep.public_url:
                    url_split = urlsplit(r['url'])
                    ep.public_url = url_split.hostname
                eps.append(ep)
        except Exception as e:
            br = BadRequest()
            br.data = f"error decoding request: {e}"
            raise br
        try:
            for ep in eps:
                ret = cfg.sm.add_node(ep)
        except Exception as e:
            return {"error": str(e)}, 500

        try:
            log.info("New Node added, refreshing endpoint DB...")
            init_db(refresh=True)
        except Exception as e:
            return {"error": "Refresh DB failed"}, 500
        return None, 204


@ns.response(200, 'OK')
@ns.response(400, 'Bad Request')
@ns.response(503, 'Service unavailable')
@ns.route('/create')
class Create(Resource, QueryUser):

    @httpauth.login_required
    @ns.expect([sessionRequestModel], validate=True)
    def post(self):
        """
        Create one or more new sessions.
        """
        (user, group) = get_authinfo(request)
        req = ns.payload
        if type(req) is dict:
            req = [req]
        log.debug(req)
        from janus.api.session_manager import (SessionManager,
                                               InvalidSessionRequestException,
                                               ResourceNotFoundException,
                                               SessionManagerException)

        try:
            session_manager = SessionManager()
            current_user = user if user else httpauth.current_user()
            users = user.split(",") if user else []
            session_manager.validate_request(req)
            session_requests = session_manager.parse_requests(user, group, req)
            session_manager.create_networks(session_requests)
            janus_sessionid = session_manager.create_session(user, group, session_requests, req, current_user, users)
            return {janus_sessionid: dict(id=janus_sessionid)}
        except InvalidSessionRequestException as e:
            raise BadRequest(f'Creating session failed: {e}')
        except ResourceNotFoundException as e:
            raise NotFound(f'Creating session failed: {e}')
        except SessionManagerException as e:
            raise InternalServerError(f'Creating session failed: {e}')
        except Exception as e:
            raise InternalServerError(f'Creating session failed. Unexpected: {type(e)}:{e}')


@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/start/<int:id>')
class Start(Resource, QueryUser):
    @httpauth.login_required
    def put(self, id):
        """
        Start a container service by id.
        """
        from janus.api.session_manager import SessionManager, ResourceNotFoundException, SessionManagerException

        (user, group) = get_authinfo(request)

        try:
            session_manager = SessionManager()
            return session_manager.start_session(id, user, group)
        except ResourceNotFoundException as e:
            raise NotFound(f'Creating session failed:{e}')
        except SessionManagerException as e:
            raise InternalServerError(f'Starting session failed:{e}')
        except Exception as e:
            raise InternalServerError(f'Starting ession failed:FATAL:{type(e)}:{e}')


@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/stop/<int:id>')
class Stop(Resource, QueryUser):

    @httpauth.login_required
    def put(self, id):
        """
        Stop a container service by id.
        """
        from janus.api.session_manager import SessionManager, ResourceNotFoundException, SessionManagerException

        try:
            session_manager = SessionManager()
            return session_manager.stop_session(id)
        except ResourceNotFoundException as e:
            raise NotFound(f'Creating session failed:{e}')
        except SessionManagerException as e:
            raise InternalServerError(f'Stopping session failed:{e}')
        except Exception as e:
            raise InternalServerError(f'Stopping session failed:FATAL:{type(e)}:{e}')


@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/exec')
class Exec(Resource):

    @httpauth.login_required
    @ns.expect(execRequestModel, validate=True)
    def post(self):
        """
        Execute a container command inside an active session.
        """
        svcs = dict()
        start = False
        attach = True
        tty = False
        req = ns.payload
        log.debug(req)

        nname = req["node"]
        if str(req.get("start", "")).lower() == "true":
            start = True
        if str(req.get("attach", "")).lower() == "false":
            attach = False
        if str(req.get("tty", "")).lower() == "true":
            tty = True

        dbase = cfg.db
        table = dbase.get_table('nodes')
        node = dbase.get(table, name=nname)
        if not node:
            return {"error": f"Node not found: {nname}"}

        container = req["container"]
        cmd = req["Cmd"]

        kwargs = {'AttachStdin': attach,
                  'AttachStdout': attach,
                  'AttachStderr': attach,
                  'Tty': tty,
                  'Cmd': cmd
                  }
        try:
            handler = cfg.sm.get_handler(node)
            ret = handler.exec_create(Node(**node), container, **kwargs)
            if start:
                handler.exec_start(Node(**node), ret)
        except Exception as e:
            log.error(f"Could not exec in container on {nname}: {e.reason}: {e.body}")
            return {"error": e.reason}, 503
        return ret


@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/images')
@ns.route('/images/<path:name>')
class Images(Resource, QueryUser):

    @httpauth.login_required
    def get(self, name=None):
        """
        Get a list of images or a specific image by name.
        """
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, {"name": name})
        dbase = cfg.db
        table = dbase.get_table('images')
        if name:
            res = dbase.get(table, query=query)
            if not res:
                return {"error": "Not found"}, 404
            return res
        elif query:
            return dbase.search(table, query=query)
        else:
            return dbase.all(table)


@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/profiles', methods=['GET'], defaults={"resource": "host"})
@ns.route('/profiles/<path:resource>', methods=['GET'])
@ns.route('/profiles/<path:resource>/<path:rname>')
class Profile(Resource):
    resources = [
        Constants.HOST,
        Constants.NET,
        Constants.VOL,
        Constants.QOS
    ]

    @httpauth.login_required
    @ns.doc(params={"resource": {"example": "|".join(Constants.PROFILE_RESOURCES),
                                 "description": "Resource type of the profile to get (defaults to host)"},
                    "refresh": {"description": "Set to 'true' to refresh the profile DB from disk"},
                    "reset": {"description": "Set to 'true' to reset the profile DB and read from disk"}})
    def get(self, resource, rname=None):
        """
        Get a list of profiles.
        """
        if resource and resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        refresh = request.args.get('refresh', None)
        reset = request.args.get('reset', None)
        (user, group) = get_authinfo(request)
        if refresh and refresh.lower() == 'true':
            try:
                cfg.pm.read_profiles(refresh=True)
            except Exception as e:
                return {"error": str(e)}, 500

        if reset and reset.lower() == 'true':
            try:
                cfg.pm.read_profiles(reset=True)
            except Exception as e:
                return {"error": str(e)}, 500

        if rname:
            res = cfg.pm.get_profile(resource, rname, user, group, inline=True)
            if not res:
                return {"error": f"Profile not found: {rname}"}, 404
            return res.dict()
        else:
            log.debug("Returning all profiles")
            ret = [p.dict() for p in cfg.pm.get_profiles(resource, user, group, inline=True)]
            return ret if ret else list()

    @httpauth.login_required
    @ns.expect(profileRequestModel, validate=True)
    @ns.doc(params={"resource": {"example": "|".join(Constants.PROFILE_RESOURCES),
                                 "description": "Resource type to create the profile for"}})
    def post(self, resource=None, rname=None):
        """
        Create a new profile.
        """
        try:
            if not resource or resource not in self.resources:
                return {"error": f"Invalid resource path: {resource}"}, 404

            req = ns.payload
            configs = req["settings"]
            res = cfg.pm.get_profile(resource, rname, inline=True)
            if res:
                return {"error": f"Profile {rname} already exists!"}, 400

            if resource == Constants.HOST:
                default = cfg._base_profile.copy()

            elif resource == Constants.VOL:
                default = cfg._base_volumes.copy()

            elif resource == Constants.NET:
                default = cfg._base_networks.copy()

            default.update((k, configs[k]) for k in default.keys() & configs.keys())
            prof = {"name": rname, "settings": default}
            if resource == Constants.HOST:
                ContainerProfile(**prof)
            elif resource == Constants.VOL:
                VolumeProfile(**prof)
            elif resource == Constants.NET:
                NetworkProfile(**prof)

        except ValidationError as e:
            return str(e), 400

        except Exception as e:
            return str(e), 500

        try:
            tbl = cfg.db.get_table(resource)
            record = {'name': rname, "settings": default}
            res = cfg.db.insert(tbl, record)
            log.info(f"Created {res}")
        except Exception as e:
            return str(e), 500

        return cfg.pm.get_profile(resource, rname).dict(), 200

    @httpauth.login_required
    @ns.expect(profileRequestModel, validate=True)
    @ns.doc(params={"resource": {"example": "|".join(Constants.PROFILE_RESOURCES),
                                 "description": "Resource type to update the profile for"}})
    def put(self, resource=None, rname=None):
        """
        Update an existing profile.
        """
        if not resource or resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        try:
            (user, group) = get_authinfo(request)
            req = ns.payload
            configs = req["settings"]
            if rname == "default":
                return {"error": "Cannot update default profile!"}, 400

            res = cfg.pm.get_profile(resource, rname, user, group, inline=True)
            if not res:
                return {"error": f"Profile not found: {rname}"}, 404

            res = res.dict()
            default = res.copy()
            default['settings'].update(configs)

            if resource == Constants.HOST:
                ContainerProfile(**default)
            elif resource == Constants.NET:
                NetworkProfile(**default)
            elif resource == Constants.VOL:
                VolumeProfile(**default)

        except ValidationError as e:
            return {"error": str(e)}, 400

        except Exception as e:
            return {"error": str(e)}, 500

        try:
            profile_tbl = cfg.db.get_table(resource)
            cfg.db.update(profile_tbl, default, name=rname)
        except Exception as e:
            return str(e), 500

        return cfg.pm.get_profile(resource, rname).dict(), 200

    @httpauth.login_required
    def delete(self, resource=None, rname=None):
        """
        Remove a profile.
        """
        if not resource or resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        try:
            (user, group) = get_authinfo(request)

            if not rname:
                raise BadRequest("Must specify profile name")

            if rname == "default":
                raise BadRequest("Cannot delete default profile")

            res = cfg.pm.get_profile(resource, rname, user, group, inline=True)
            if not res:
                return {"error": f"Profile not found: {rname}"}, 404

        except Exception as e:
            return str(e), 500

        try:
            profile_tbl = cfg.db.get_table(resource)
            cfg.db.remove(profile_tbl, name=rname)
        except Exception as e:
            return str(e), 500

        return {}, 204


@ns.response(200, 'OK')
@ns.response(204, 'Not modified')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/auth/<path:resource>', methods=['GET'])
@ns.route('/auth/<path:resource>/<int:rid>')
@ns.route('/auth/<path:resource>/<path:rname>')
class JanusAuth(Resource, QueryUser):
    resources = Constants.AUTH_RESOURCES
    get_resources = ["jwt"]
    resource_db_map = {
        "nodes": "nodes",
        "images": "images",
        "profiles": "host",
        "active": "active"
    }

    def _marshall_req(self):
        req = request.get_json()
        if not req:
            raise BadRequest("Body is empty")
        if type(req) is not dict:
            raise BadRequest("Malformed data, expecting dict")
        users = req.get("users", None)
        groups = req.get("groups", None)
        if users == None or groups == None:
            raise BadRequest("users and groups not present in POST data")
        log.debug(req)
        return (users, groups)

    # def query_builder(self, id=None, name=None):
    #     qs = list()
    #     if id:
    #         qs.append(eq(where('id'), id))
    #     elif name:
    #         qs.append(eq(where('name'), name))
    #     if len(qs):
    #         return reduce(lambda a, b: a & b, qs)
    #     return None

    @httpauth.login_required
    @ns.doc(params={"resource": {"example": "|".join(Constants.AUTH_RESOURCES+["jwt"]),
                                 "description": "Resource type to get auth for"}})
    def get(self, resource, rid=None, rname=None):
        """
        Get user and group attributes for a named resource.
        """
        if resource not in self.resources and resource not in self.get_resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        # Returns active token for backend client (e.g. Portainer)
        if resource in self.get_resources:
            return {"jwt": cfg.sm.get_auth_token()}, 200
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, qargs={"id": rid, "name": rname})
        if not query:
            return {"error": "Must specify resource id or name"}
        table = cfg.db.get_table(self.resource_db_map.get(resource))
        res = cfg.db.get(table, query=query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        users = res.get("users", list())
        groups = res.get("groups", list())
        return {"users": users, "groups": groups}, 200

    @httpauth.login_required
    @ns.doc(params={"resource": {"example": "|".join(Constants.AUTH_RESOURCES),
                                 "description": "Resource type to update auth for"}})
    @ns.expect(authRequestModel, validate=True)
    def post(self, resource, rid=None, rname=None):
        """
        Set user and group attributes for a named resource.
        """
        if resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        (users, groups) = self._marshall_req()
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, qargs={"id": rid, "name": rname})
        dbase = cfg.db
        table = cfg.db.get_table(self.resource_db_map.get(resource))
        res = dbase.get(table, query=query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        new_users = list(set(users).union(set(res.get("users", list()))))
        new_groups = list(set(groups).union(set(res.get("groups", list()))))
        res['users'] = new_users
        res['groups'] = new_groups
        dbase.update(table, res, query=query)
        return res, 200

    @httpauth.login_required
    @ns.doc(params={"resource": {"example": "|".join(Constants.AUTH_RESOURCES),
                                 "description": "Resource type to delete auth for"}})
    def delete(self, resource, rid=None, rname=None):
        """
        Remove user and group attributes for a named resource.
        """
        (users, groups) = self._marshall_req()
        (user, group) = get_authinfo(request)
        query = self.query_builder(user, group, qargs={"id": rid, "name": rname})
        dbase = cfg.db
        table = cfg.db.get_table(self.resource_db_map.get(resource))
        res = dbase.get(table, query=query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        for u in users:
            try:
                res['users'].remove(u)
            except:
                pass
        for g in groups:
            try:
                res['groups'].remove(g)
            except:
                pass
        dbase.update(table, res, query=query)
        return res, 200
