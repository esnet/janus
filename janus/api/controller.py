import logging
import uuid
import time
import json
from enum import Enum
from tinydb import Query, where
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from operator import eq
from functools import reduce

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
from .ansible_job import AnsibleJob

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

class QueryUser:
    def query_builder(self, user=None, group=None, qargs=dict()):
        qs = list()
        if user:
            qs.append(where('users').any(user) | eq(where('user'), user))
        if group:
            qs.append(where('groups').any(group))
        for k,v in qargs.items():
            if v:
                qs.append(eq(where(k), v))
        if len(qs):
            return reduce(lambda a, b: a & b, qs)
        return None

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

def get_authinfo(request):
    user = request.args.get('user', None)
    group = request.args.get('group', None)
    log.debug(f"User: {user}, Group: {group}")
    return (user,group)

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
        pclient.jwt = res.jwt
        auth_expire = time.time() + 14400

        return pclient


@ns.route('/active')
@ns.route('/active/<int:id>')
class ActiveCollection(Resource, QueryUser):

    @httpauth.login_required
    def get(self, id=None):
        """
        Returns active sessions
        """
        table = cfg.db.table('active')
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id})
        if query and id:
            res = table.get(query)
            if not res:
                return {"error": "Not found"}, 404
            return res
        elif query:
            return table.search(query)
        else:
            return table.all()

    @ns.response(204, 'Allocation successfully deleted.')
    @ns.response(404, 'Not found.')
    @ns.response(500, 'Internal server error')
    @httpauth.login_required
    @auth
    def delete(self, id):
        """
        Deletes an active allocation (e.g. stops containers)
        """
        nodes = cfg.db.table('nodes')
        table = cfg.db.table('active')
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id})
        doc = table.get(query)
        if doc == None:
            return {"error": "Not found", "id": id}, 404

        force = request.args.get('force', None)
        dapi = PortainerDockerApi(pclient)
        futures = list()

        allocations = doc.get("allocations", dict())
        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in allocations.items():
                try:
                    Node = Query()
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
class NodeCollection(Resource, QueryUser):

    @httpauth.login_required
    @auth
    def get(self, node: str = None, id: int = None):
        """
        Returns list of existing nodes
        """
        cfg.db.clear_cache()
        (user,group) = get_authinfo(request)
        refresh = request.args.get('refresh', None)
        if refresh and refresh.lower() == 'true':
            log.info("Refreshing endpoint DB...")
            global pclient
            init_db(pclient, refresh=True)
        else:
            init_db(pclient, refresh=False)
        table = cfg.db.table('nodes')
        query = self.query_builder(user, group, {"id": id, "name": node})
        if query and (id or node):
            res = table.get(query)
            if not res:
                return {"error": "Not found"}, 404
            return res
        elif query:
            return table.search(query)
        else:
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
        Node = Query()
        nodes = cfg.db.table('nodes')
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id, "name": node})
        doc = nodes.get(query)
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
class Create(Resource, QueryUser):

    @httpauth.login_required
    @auth
    def post(self):
        """
        Handle the creation of a container service
        """
        (user,group) = get_authinfo(request)
        req = request.get_json()
        if not req:
            raise BadRequest("Body is empty")
        if type(req) is dict:
            req = [req]
        log.debug(req)

        cfg.db.clear_cache()
        ntable = cfg.db.table('nodes')
        ptable = cfg.db.table('profiles')
        itable = cfg.db.table('images')

        # Do auth and resource availability checks first
        create = list()
        for r in req:
            instances = r.get("instances", None)
            profile = r.get("profile", None)
            image = r.get("image", None)
            if instances == None or profile == None or image == None:
                raise BadRequest("Missing fields in POST data")
            # Profile
            if not profile or profile == "default":
                profile = settings.DEFAULT_PROFILE
            query = self.query_builder(user, group, {"name": profile})
            prof = cfg.get_profile(profile, user, group)
            if not prof:
                return {"error": f"Profile {profile} not found"}, 404
            # Image
            # By default try to pull the specified image name even if
            # we don't know about it
            if not user and not group:
                img = image
            else:
                query = self.query_builder(user, group, {"name": image})
                img = itable.get(query)
                if not img:
                    return {"error": f"Image {image} not found"}, 404
                img = image
            # Nodes
            for ep in instances:
                query = self.query_builder(user, group, {"name": ep})
                node = ntable.get(query)
                if not node:
                    return {"error": f"Node {ep} not found"}, 404
                create.append(
                    {"node": node,
                     "profile": prof,
                     "image": img,
                     "kwargs": r.get("kwargs", dict())
                     }
                )

        svcs = dict()
        try:
            # keep a running set of addresses and ports allocated for this request
            addrs_v4 = set()
            addrs_v6 = set()
            cports = set()
            sports = set()
            for r in create:
                s = r['node']['name']
                if s not in svcs:
                    svcs[s] = list()
                svcs[s].append(create_service(r['node'], r['image'], r['profile'], addrs_v4, addrs_v6,
                                              cports, sports, **r['kwargs']))
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error("Could not allocate request: {}".format(e))
            return {"error": "{}".format(e)}, 503

        # setup simple accounting
        record = {'uuid': str(uuid.uuid4()),
                  'user': user if user else httpauth.current_user(),
                  'state': State.INITIALIZED.name,
                  'allocations': dict()}

        dapi = PortainerDockerApi(pclient)
        # get an ID from the DB
        Id = precommit_db()
        errs = False
        for k, v in svcs.items():
            for s in svcs[k]:
                # the portainer node this service will start on
                n = s['node']
                img = s['image']
                if (cfg.dryrun):
                    ret = {'Id': str(uuid.uuid4())}
                else:
                    # Docker-specific v4 vs v6 image registry nonsense. Need to abstract this away.
                    try:
                        handle_image(n, img, dapi, s['pull_image'])
                    except Exception as e:
                        log.error("Could not pull image {} on node {}, {}: {}".format(img,
                                                                                      n['name'],
                                                                                      e.reason,
                                                                                      e.body))
                        errs = error_svc(s, e)
                        try:
                            v6img = f"registry.ipv6.docker.com/{img}"
                            handle_image(n, v6img, dapi, s['pull_image'])
                            s['image'] = v6img
                        except Exception as e:
                            log.error("Could not pull image {} on node {}, {}: {}".format(v6img,
                                                                                          n['name'],
                                                                                          e.reason,
                                                                                          e.body))
                            errs = error_svc(s, e)
                            continue
                    # clear any errors if image resolved
                    s['errors'] = list()
                    try:
                        name = f"janus_{Id}" if Id else None
                        ret = dapi.create_container(n['id'], img, name, **s['docker_kwargs'])
                    except ApiException as e:
                        log.error("Could not create container on {}: {}: {}".format(n['name'],
                                                                                    e.reason,
                                                                                    e.body))
                        errs = error_svc(s, e)
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
                        errs = error_svc(s, e)
                        continue

                s['container_id'] = ret['Id']
                s['container_name'] = name
                if n['name'] not in record['allocations']:
                    record['allocations'].update({n['name']: list()})
                record['allocations'][n['name']].append(ret['Id'])
                del s['node']

        # complete accounting
        record['id'] = Id
        record['services'] = svcs
        record['request'] = req
        if errs:
            precommit_db(Id, delete=True)
            return {Id: record}
        else:
            return commit_db(record, Id)


@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/start/<int:id>')
class Start(Resource, QueryUser):

    @httpauth.login_required
    @auth
    def put(self, id=None):
        """
        Handle the starting of container services
        """
        table = cfg.db.table('active')
        ntable = cfg.db.table('nodes')
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id})
        if id:
            svc = table.get(query)
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

                #
                # Ansible job is requested if configured
                # - Enviroment variabls must be set to access Ansible Tower server:
                #   TOWER_HOST, TOWER_USERNAME, TOWER_PASSWORD, TOWER_SSL_VERIFY
                # - It may take some time for the ansible job to finish or timeout (300 seconds)
                #
                prof = cfg.get_profile(s['profile'])
                for psname in prof['settings']['post_starts']:
                    ps = cfg.get_poststart(psname)
                    if ps['type'] == 'ansible':
                        jt_name = ps['jobtemplate']
                        gateway = ps['gateway']
                        ipprot = ps['ipprot']
                        inf = ps['interface']
                        limit = ps['limit']
                        default_name= ps['container_name']
                        container_name= s.get('container_name', default_name)
                        ex_vars = f'{{"ipprot": "{ipprot}", "interface": "{inf}", "gateway": "{gateway}", "container": "{container_name}"}}'
                        job = AnsibleJob()
                        try:
                            result = job.launch(job_template=jt_name, monitor=True, wait=True, timeout=600, extra_vars=ex_vars, limits=limit)
                        except Exception as e:
                            error_svc(s, e)
                            continue
                # End of Ansible job
        svc['state'] = State.MIXED.name if error else State.STARTED.name
        return commit_db(svc, id, realized=True)

@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/stop/<int:id>')
class Stop(Resource, QueryUser):

    @httpauth.login_required
    @auth
    def put(self, id=None):
        """
        Handle the stopping of container services
        """
        Srv = Query()
        table = cfg.db.table('active')
        ntable = cfg.db.table('nodes')
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
        start = False
        attach = True
        tty = False
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
        if "start" in req:
            start = req["start"]
        if "attach" in req:
            attach = req["attach"]
        if "tty" in req:
            tty = req["tty"]

        Node = Query()
        table = cfg.db.table('nodes')
        node = table.get(Node.name == nname)
        if not node:
            return {"error": "Node not found: {}".format(nname)}

        container = req["container"]
        cmd = req["Cmd"]

        dapi = PortainerDockerApi(pclient)
        kwargs = {'AttachStdin': False,
                  'AttachStdout': attach,
                  'AttachStderr': attach,
                  'Tty': tty,
                  'Cmd': cmd
                  }
        try:
            ret = dapi.exec_create(node["id"], container, **kwargs)
            if start:
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
@ns.route('/images')
@ns.route('/images/<path:name>')
class Images(Resource, QueryUser):

    @httpauth.login_required
    def get(self, name=None):
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"name": name})
        table = cfg.db.table('images')
        if name:
            res = table.get(query)
            if not res:
                return {"error": "Not found"}, 404
            return res
        elif query:
            return table.search(query)
        else:
            return table.all()

@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/profiles')
@ns.route('/profiles/<name>')
class Profile(Resource):

    @httpauth.login_required
    def get(self, name=None):
        refresh = request.args.get('refresh', None)
        reset = request.args.get('reset', None)
        (user,group) = get_authinfo(request)
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

        if name:
            res = cfg.get_profile(name, user, group, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(name)}, 404
            return res
        else:
            log.debug("Returning all profiles")
            ret = cfg.get_profiles(user, group, inline=True)
            return ret if ret else list()

    @httpauth.login_required
    def post(self, name=None):
        try:
            req = request.get_json()

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                res.status_code = 400
                return res

            if "settings" not in req:
                res = jsonify(error="please follow this format: {\"settings\": {\"key\": \"value\"}}")
                res.status_code = 400
                return res

            configs = req["settings"]
            res = cfg.get_profile(name, inline=True)
            if res:
                return {"error": "Profile {} already exists!".format(name)}, 400

            default = cfg._base_profile.copy()
            default.update((k, configs[k]) for k in default.keys() & configs.keys())
            ProfileSchema(**default)

        except ValidationError as e:
            return str(e), 400

        except Exception as e:
            return str(e), 500

        try:
            profile_tbl = cfg.db.table('profiles')
            log.info("Creating profile {}".format(
                profile_tbl.insert({
                'name': name,
                "settings": default
                })
            ))
        except Exception as e:
            return str(e), 500

        return cfg.get_profile(name), 200

    @httpauth.login_required
    def put(self, name=None):
        try:
            (user,group) = get_authinfo(request)
            req = request.get_json()

            if (req is None) or (req and type(req) is not dict):
                res = jsonify(error="Body is not json dictionary")
                raise BadRequest(res)

            if "settings" not in req:
                res = jsonify(error="please follow this format: {\"settings\": {\"key\": \"value\"}}")
                raise BadRequest(res)

            configs = req["settings"]
            if name == "default":
                return {"error": "Cannot update default profile!"}, 400

            res = cfg.get_profile(name, user, group, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(name)}, 404

            default = res.copy()
            default['settings'].update(configs)
            ProfileSchema(**default)

        except ValidationError as e:
            return {"error" : str(e)}, 400

        except Exception as e:
            return {"error" : str(e)}, 500

        try:
            query = Query()
            profile_tbl = cfg.db.table('profiles')
            profile_tbl.update(default, query.name == name)
        except Exception as e:
            return str(e), 500

        return cfg.get_profile(name), 200

    @httpauth.login_required
    def delete(self, name=None):
        try:
            (user,group) = get_authinfo(request)

            if not name:
                raise BadRequest("Must specify profile name")

            if name == "default":
                raise BadRequest("Cannot delete default profile")

            res = cfg.get_profile(name, user, group, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(name)}, 404

        except Exception as e:
            return str(e), 500

        try:
            query = Query()
            profile_tbl = cfg.db.table('profiles')
            profile_tbl.remove(query.name == name)
        except Exception as e:
            return str(e), 500

        return {}, 204

@ns.response(200, 'OK')
@ns.response(204, 'Not modified')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/auth/<path:resource>')
@ns.route('/auth/<path:resource>/<int:rid>')
@ns.route('/auth/<path:resource>/<path:rname>')
class JanusAuth(Resource):
    resources = ["nodes",
                 "images",
                 "profiles",
                 "active"]
    get_resources = ["jwt"]

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

    def query_builder(self, id=None, name=None):
        qs = list()
        if id:
            qs.append(eq(where('id'), id))
        elif name:
            qs.append(eq(where('name'), name))
        return reduce(lambda a, b: a & b, qs)

    @httpauth.login_required
    @auth
    def get(self, resource, rid=None, rname=None):
        if resource not in self.resources and resource not in self.get_resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        # Returns active token for backend client (e.g. Portainer)
        if resource in self.get_resources:
            global plient
            return {"jwt": pclient.jwt}, 200
        query = self.query_builder(rid, rname)
        table = cfg.db.table(resource)
        res = table.get(query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        users = res.get("users", list())
        groups = res.get("groups", list())
        return {"users": users, "groups": groups}, 200

    @httpauth.login_required
    def post(self, resource, rid=None, rname=None):
        (users, groups) = self._marshall_req()
        query = self.query_builder(rid, rname)
        table = cfg.db.table(resource)
        res = table.get(query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        new_users = list(set(users).union(set(res.get("users", list()))))
        new_groups = list(set(groups).union(set(res.get("groups", list()))))
        res['users'] = new_users
        res['groups'] = new_groups
        table.update(res, query)
        return res, 200

    @httpauth.login_required
    def delete(self, resource, rid=None, rname=None):
        (users, groups) = self._marshall_req()
        query = self.query_builder(rid, rname)
        table = cfg.db.table(resource)
        res = table.get(query)
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
        table.update(res, query)
        return res, 200
