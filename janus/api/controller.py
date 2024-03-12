import logging
import uuid
from tinydb import where
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from operator import eq
from functools import reduce

from flask import request, jsonify
from flask_restx import Namespace, Resource
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash
from werkzeug.exceptions import BadRequest

from pydantic import ValidationError
from urllib.parse import urlsplit
from janus import settings
from janus.lib import AgentMonitor
from janus.settings import cfg
from janus.api.constants import State, EPType
from janus.api.db import init_db, QueryUser
from janus.api.ansible_job import AnsibleJob
from janus.api.models import (
    Node,
    Network,
    ContainerProfile,
    NetworkProfile,
    VolumeProfile,
    SessionRequest,
    SessionConstraints
)
from janus.api.utils import (
    commit_db,
    cname_from_id,
    precommit_db,
    set_qos,
    error_svc,
    Constants
)


# Basic auth
httpauth = HTTPBasicAuth()
log = logging.getLogger(__name__)
ns = Namespace('janus/controller', description='Operations for Janus on-demand container provisioning')


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
    api_user = httpauth.current_user()
    if api_user == 'admin':
        user = request.args.get('user', None)
        group = request.args.get('group', None)
    else:
        user = api_user
        group = None
    log.debug(f"User: {user}, Group: {group}")
    return (user, group)


@ns.route('/active')
@ns.route('/active/<int:aid>')
@ns.route('/active/<int:aid>/logs/<path:nname>')
class ActiveCollection(Resource, QueryUser):

    @httpauth.login_required
    def get(self, aid=None, nname=None):
        """
        Returns active sessions
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
            else:
                if (fields):
                    return {k: v for k,v in res.items() if k in fields.split(',')}
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
                    ret.append({k: v for k,v in r.items() if k in fields.split(',')})
                return ret
            else:
                return res

    @ns.response(204, 'Allocation successfully deleted.')
    @ns.response(404, 'Not found.')
    @ns.response(500, 'Internal server error')
    @httpauth.login_required
    def delete(self, aid):
        """
        Deletes an active allocation (e.g. stops containers)
        """
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": aid})
        dbase = cfg.db
        nodes = dbase.get_table('nodes')
        table = dbase.get_table('active')
        doc = dbase.get(table, query=query)
        if doc == None:
            return {"error": "Not found", "id": aid}, 404

        force = request.args.get('force', None)
        futures = list()

        allocations = doc.get("allocations", dict())
        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in allocations.items():
                try:
                    n = dbase.search(nodes, name=k)[0]
                    handler = cfg.sm.get_handler(n)
                    if not (cfg.dryrun):
                        for alloc in v:
                            futures.append(executor.submit(handler.stop_container, Node(**n), alloc, **n))
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.error("Could not find node/container to stop, or already stopped: {}".format(k))
        if not (cfg.dryrun):
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    if "container_id" in res:
                        log.debug(f"Removing container {res['container_id']}")
                        handler = cfg.sm.get_handler(nname=res['node_name'])
                        handler.remove_container(Node(name=res['node_name'], id=res['node_id']), res['container_id'])
                except Exception as e:
                    log.error("Could not remove container on remote node: {}".format(e))
                    if not force:
                        return {"error": "{}".format(e)}, 503
        # delete always removes realized state info
        commit_db(doc, aid, delete=True, realized=True)
        commit_db(doc, aid, delete=True)
        return None, 204

@ns.response(400, 'Bad Request')
@ns.route('/nodes')
@ns.route('/nodes/<node>')
@ns.route('/nodes/<int:id>')
class NodeCollection(Resource, QueryUser):

    @httpauth.login_required
    def get(self, node: str = None, id: int = None):
        """
        Returns list of existing nodes
        """
        (user,group) = get_authinfo(request)
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
        Deletes a node (endpoint)
        """
        if not node and not id:
            return {"error": "Must specify node name or id"}, 400
        (user,group) = get_authinfo(request)
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

        try:
            for ep in eps:
                kwargs = {}
                ret = cfg.sm.add_node(ep, **kwargs)
        except Exception as e:
            return {"error": "{}".format(e)}, 500

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

        dbase = cfg.db
        ntable = dbase.get_table('nodes')
        ptable = dbase.get_table('profiles')
        itable = dbase.get_table('images')

        # Do auth and resource availability checks first
        create = list()
        for r in req:
            instances = r.get("instances", None)
            profile = r.get("profile", None)
            image = r.get("image", None)
            arguments = r.get("arguments", None)
            remove_container = r.get("remove_container", None)
            if instances == None or profile == None or image == None:
                raise BadRequest("Missing fields in POST data")
            # Profile
            if not profile or profile == "default":
                profile = settings.DEFAULT_PROFILE
            query = self.query_builder(user, group, {"name": profile})
            prof = cfg.pm.get_profile(Constants.HOST, profile, user, group)
            if not prof:
                return {"error": f"Profile {profile} not found"}, 404
            # Image
            # By default try to pull the specified image name even if
            # we don't know about it
            if not user and not group:
                img = image
            else:
                parts = image.split(":")
                query = self.query_builder(user, group, {"name": parts[0]})
                img = dbase.get(itable, query=query)
                if not img:
                    return {"error": f"Image {image} not found"}, 404
                img = image
            # Endpoints and Networks
            for ep in instances:
                c = dict()
                if isinstance(ep, dict):
                    c = SessionConstraints(**ep)
                    ename = ep.get('name', None)
                elif isinstance(ep, str):
                    ename = ep
                else:
                    return {"error": f"Invalid endpoint type: {ep}"}, 400
                if not ename:
                    return {"error": f"Invalid endpoint name: {ename}"}, 400
                query = self.query_builder(user, group, {"name": ename})
                node = dbase.get(ntable, query=query)
                if not node:
                    return {"error": f"Endpoint {ename} not found"}, 404
                try:
                    handler = cfg.sm.get_handler(node)
                    init_db(node.get('name'), refresh=True)
                    ret = handler.resolve_networks(node, prof)
                    if ret:
                        # Networks were updated by the handler, refresh Node DB
                        init_db(node.get('name'), refresh=True)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    return {"error": f"Endpoint {ename} has invalid network: {e}"}, 400
                # refresh node handle from DB since node state may have been updated above
                node = dbase.get(ntable, query=query)
                create.append(SessionRequest(node=node,
                                             profile=prof,
                                             image=img,
                                             arguments=arguments,
                                             remove_container=remove_container,
                                             constraints=c,
                                             kwargs=r.get("kwargs", dict())))

        # get an ID from the DB
        Id = precommit_db()
        svcs = dict()
        try:
            # keep a running set of addresses and ports allocated for this request
            addrs_v4 = set()
            addrs_v6 = set()
            cports = set()
            sports = set()
            i = 1
            for s in create:
                nname = s.node.get('name')
                if nname not in svcs:
                    svcs[nname] = list()
                sname = cname_from_id(Id, i)
                handler = cfg.sm.get_handler(s.node)
                rec = handler.create_service_record(sname, s, addrs_v4, addrs_v6, cports, sports)
                if not rec:
                    raise Exception(f"No service record created for node {nname}")
                svcs[nname].append(rec)
                i+=1
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

        errs = False
        for k, services in svcs.items():
            for s in services:
                if (cfg.dryrun):
                    ret = {'Id': str(uuid.uuid4())}
                    name = "janus_dryrun"
                else:
                    try:
                        aid, nname = cfg.sm.init_service(s, errs)
                        if nname not in record['allocations']:
                            record['allocations'].update({nname: list()})
                        record['allocations'][nname].append(aid)
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        log.error(f"Could not initialize service: {e}")
                        continue

        # complete accounting
        record['id'] = Id
        record['services'] = svcs
        record['request'] = req
        record['users'] = user.split(",") if user else []
        record['groups'] = group.split(",") if group else []
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

    def _do_poststart(self, s):
        #
        # Ansible job is requested if configured
        # - Enviroment variabls must be set to access Ansible Tower server:
        #   TOWER_HOST, TOWER_USERNAME, TOWER_PASSWORD, TOWER_SSL_VERIFY
        # - It may take some time for the ansible job to finish or timeout (300 seconds)
        #
        prof = cfg.pm.get_profile(Constants.HOST, s['profile'])
        for psname in prof.settings.post_starts:
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

    @httpauth.login_required
    def put(self, id=None):
        """
        Handle the starting of container services
        """
        (user,group) = get_authinfo(request)
        query = self.query_builder(user, group, {"id": id})
        dbase = cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        if id:
            svc = dbase.get(table, query=query)
        if not svc:
            return {"error": "id not found"}, 404

        if svc['state'] == State.STARTED.name:
            return {"error": "Service {} already started".format(svc['uuid'])}, 503

        # start the services
        error = False
        services = svc.get("services", dict())
        for k,v in services.items():
            for s in v:
                if not s['container_id']:
                    log.warn("Skipping service with no container_id: {}".format(k))
                    error = True
                    continue
                c = s['container_id']
                node = dbase.get(ntable, name=k)
                handler = cfg.sm.get_handler(node)
                if not node:
                    log.error("Container node {} not found for container_id: {}".format(k, c))
                    return {"error": "Node not found: {}".format(k)}, 404
                log.debug("Starting container {} on {}".format(c, k))

                if not (cfg.dryrun):
                    # Error acconting
                    orig_errcnt = len(s.get('errors'))

                    try:
                        handler.start_container(Node(**node), c, s)
                        if s.get('qos'):
                            qos = s["qos"]
                            qos["container"] = c
                            set_qos(node["public_url"], qos)
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        log.error("Could not start container on {}: {}".format(k,e))
                        error_svc(s, e)
                        error = True
                        continue

                    # Handle post_start tasks
                    self._do_poststart(s)

                    # Trim logged errors
                    errcnt = len(s.get('errors'))
                    if not errcnt-orig_errcnt:
                        s['errors'] = list()
                    else:
                        s['errors'] = s['errors'][orig_errcnt-errcnt:]
                # End of Ansible job
        svc['state'] = State.MIXED.name if error else State.STARTED.name
        return commit_db(svc, id, realized=True)

@ns.response(200, 'OK')
@ns.response(404, 'Not found')
@ns.response(503, 'Service unavailable')
@ns.route('/stop/<int:id>')
class Stop(Resource, QueryUser):

    @httpauth.login_required
    def put(self, id=None):
        """
        Handle the stopping of container services
        """
        dbase = cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        if id:
            svc = dbase.get(table, ids=id)
        if not svc:
            return {"error": "id not found"}, 404

        if svc['state'] == State.STOPPED.name:
            return {"error": "Service {} already stopped".format(svc['uuid'])}, 503
        if svc['state'] == State.INITIALIZED.name:
            return {"error": "Service {} is in initialized state".format(svc['uuid'])}, 503

        # stop the services
        error = False
        for k,v in svc['services'].items():
            for s in v:
                if not s.get('container_id'):
                    log.warn("Skipping service with no container_id: {}".format(k))
                    continue
                c = s['container_id']
                node = dbase.get(ntable, name=k)
                handler = cfg.sm.get_handler(node)
                if not node:
                    log.error("Container node {} not found for container_id: {}".format(k, c))
                    return {"error": "Node not found: {}".format(k)}, 404
                log.debug("Stopping container {} on {}".format(c, k))
                if not (cfg.dryrun):
                    try:
                        handler.stop_container(Node(**node), c)
                    except Exception as e:
                        log.error("Could not stop container on {}: {}".format(k,e))
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

        dbase = cfg.db
        table = dbase.get_table('nodes')
        node = dbase.get(table, name=nname)
        if not node:
            return {"error": "Node not found: {}".format(nname)}

        container = req["container"]
        cmd = req["Cmd"]

        kwargs = {'AttachStdin': False,
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
            log.error("Could not exec in container on {}: {}: {}".format(nname,
                                                                         e.reason,
                                                                         e.body))
            return {"error": e.reason}, 503
        return ret

@ns.response(200, 'OK')
@ns.response(503, 'Service unavailable')
@ns.route('/images')
@ns.route('/images/<path:name>')
class Images(Resource, QueryUser):

    @httpauth.login_required
    def get(self, name=None):
        (user,group) = get_authinfo(request)
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
@ns.route('/profiles')
@ns.route('/profiles/<path:resource>')
@ns.route('/profiles/<path:resource>/<path:rname>')
class Profile(Resource):
    resources = [
        Constants.HOST,
        Constants.NET,
        Constants.VOL,
        Constants.QOS
    ]

    @httpauth.login_required
    def get(self, resource="host", rname=None):
        if resource and resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        refresh = request.args.get('refresh', None)
        reset = request.args.get('reset', None)
        (user,group) = get_authinfo(request)
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
                return {"error": "Profile not found: {}".format(rname)}, 404
            return res.dict()
        else:
            log.debug("Returning all profiles")
            ret = [ p.dict() for p in cfg.pm.get_profiles(resource, user, group, inline=True) ]
            return ret if ret else list()

    @httpauth.login_required
    def post(self, resource=None, rname=None):
        try:
            if not resource or resource not in self.resources:
                return {"error": f"Invalid resource path: {resource}"}, 404

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
            res = cfg.pm.get_profile(resource, rname, inline=True)
            if res:
                return {"error": "'{}' already exists!".format(rname)}, 400

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
            # default = req["settings"]
            record = {'name': rname, "settings": default}
            log.info("Creating {}".format(cfg.db.insert(tbl, record)))
        except Exception as e:
            return str(e), 500

        return cfg.pm.get_profile(resource, rname).dict(), 200

    @httpauth.login_required
    def put(self, resource=None, rname=None):
        if not resource or resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
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
            if rname == "default":
                return {"error": "Cannot update default profile!"}, 400

            res = cfg.pm.get_profile(resource, rname, user, group, inline=True).dict()
            if not res:
                return {"error": "Profile not found: {}".format(rname)}, 404

            default = res.copy()
            default['settings'].update(configs)

            if resource == Constants.HOST:
                ContainerProfile(**default)
            elif resource == Constants.NET:
                NetworkProfile(**default)
            elif resource == Constants.VOL:
                VolumeProfile(**default)

        except ValidationError as e:
            return {"error" : str(e)}, 400

        except Exception as e:
            return {"error" : str(e)}, 500

        try:
            profile_tbl = cfg.db.get_table(resource)
            cfg.db.update(profile_tbl, default, name=rname)
        except Exception as e:
            return str(e), 500

        return cfg.pm.get_profile(resource, rname).dict(), 200

    @httpauth.login_required
    def delete(self, resource=None, rname=None):
        if not resource or resource not in self.resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        try:
            (user,group) = get_authinfo(request)

            if not rname:
                raise BadRequest("Must specify profile name")

            if rname == "default":
                raise BadRequest("Cannot delete default profile")

            res = cfg.pm.get_profile(resource, rname, user, group, inline=True)
            if not res:
                return {"error": "Profile not found: {}".format(rname)}, 404

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
    def get(self, resource, rid=None, rname=None):
        if resource not in self.resources and resource not in self.get_resources:
            return {"error": f"Invalid resource path: {resource}"}, 404
        # Returns active token for backend client (e.g. Portainer)
        if resource in self.get_resources:
            return {"jwt": cfg.sm.get_auth_token()}, 200
        query = self.query_builder(rid, rname)
        table = cfg.db.get_table(resource)
        res = cfg.db.get(table, query=query)
        if not res:
            return {"error": f"{resource} resource not found with id {rid if rid else rname}"}, 404
        users = res.get("users", list())
        groups = res.get("groups", list())
        return {"users": users, "groups": groups}, 200

    @httpauth.login_required
    def post(self, resource, rid=None, rname=None):
        (users, groups) = self._marshall_req()
        query = self.query_builder(rid, rname)
        dbase = cfg.db
        table = dbase.get_table(resource)
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
    def delete(self, resource, rid=None, rname=None):
        (users, groups) = self._marshall_req()
        query = self.query_builder(rid, rname)
        dbase = cfg.db
        table = dbase.get_table(resource)
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
