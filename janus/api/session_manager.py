import concurrent
import logging.config
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

from kubernetes.client import ApiException as KubeApiException
from portainer_api.rest import ApiException as PortainerApiException

from janus.api.ansible_job import AnsibleJob
from janus.api.constants import State
from janus.api.db import QueryUser
from janus.api.models import SessionConstraints, SessionRequest, Node, Network
from janus.api.utils import (
    commit_db,
    cname_from_id,
    precommit_db,
    error_svc,
    Constants,
    keys_lower,
    # set_qos
)
from janus.settings import cfg

log = logging.getLogger(__name__)


class InvalidSessionRequestException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ResourceNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)


class SessionManagerException(Exception):
    def __init__(self, message):
        super().__init__(message)


# noinspection PyMethodMayBeStatic
class SessionManager(QueryUser):

    def __init__(self):
        pass

    def update_network(self, node, pnet, net_name):
        k = node.get('name')
        n = net_name
        w = node['networks'][net_name]
        key = f"{k}-{n}"
        pnet = cfg.pm.get_profile(Constants.NET, pnet.name)
        subnet = w.get('subnet', [])

        # try to get subnet information from profile if not tracked in endpoint
        if not subnet:
            subnet = pnet.settings.ipam.get('config') if pnet else []

        subnet = [keys_lower(x) for x in subnet]
        dbase = cfg.db
        net_table = dbase.get_table('networks')
        curr = dbase.get(net_table, key=key)

        if not curr:
            net = {'name': n,
                   'key': key,
                   'subnet': list(subnet),
                   'allocated_v4': [],
                   'allocated_v6': []}
        else:
            net = curr
            net['subnet'] = list(subnet)

        dbase.upsert(net_table, net, 'key', key)

    def validate_request(self, req: List[Dict]):
        for r in req:
            instances = r.get("instances", list())
            profile = r.get("profile", None)
            image = r.get("image", None)

            if not instances or not profile or not image:
                raise InvalidSessionRequestException("Missing fields in POST data")

            overrides = r.get("overrides", list())

            if overrides and len(overrides) != len(instances):
                raise InvalidSessionRequestException("Number of overrides must match the number of instances")

            for override in overrides:
                if 'endpoint' not in override:
                    raise InvalidSessionRequestException("Override must have endpoint attribute")

            for ep in instances:
                ename = None

                if isinstance(ep, dict):
                    ename = ep.get('name', None)
                elif isinstance(ep, str):
                    ename = ep

                if not ename:
                    raise InvalidSessionRequestException(f"Invalid endpoint type: {ep}")

    def parse_requests(self, user, group, requests: List[Dict]) -> List[SessionRequest]:
        dbase = cfg.db
        ntable = dbase.get_table('nodes')
        itable = dbase.get_table('images')
        session_requests = list()

        for r in requests:
            instances = r["instances"]
            prof = cfg.pm.get_profile(Constants.HOST, r["profile"], user, group)
            entrypoint = r.get("entrypoint", None)
            dns = r.get("dns", list())

            if not prof:
                raise ResourceNotFoundException(f"Profile {r['profile']} not found")

            query = self.query_builder(user, group, {"name": r['image'].split(":")[0]})

            if not dbase.get(itable, query=query):
                raise ResourceNotFoundException(f"Image {r['image']} not found")

            overrides = r.get("overrides", list())

            # Endpoints and Networks
            for idx, ep in enumerate(instances):
                c = dict()
                ename = None
                if isinstance(ep, dict):
                    c = SessionConstraints(**ep)
                    ename = ep.get('name', None)
                elif isinstance(ep, str):
                    ename = ep

                assert ename is not None, f"invalid endpoint {ep}"
                query = self.query_builder(user, group, {"name": ename})
                node = dbase.get(ntable, query=query)

                if not node:
                    raise ResourceNotFoundException(f"Ednpoint {ename} not found")

                session_requests.append(
                    SessionRequest(node=node,
                                   profile=prof,
                                   image=r['image'],
                                   arguments=r.get("arguments", None),
                                   entrypoint=entrypoint,
                                   dns=dns,
                                   remove_container=r.get("remove_container", None),
                                   constraints=c,
                                   kwargs=r.get("kwargs", dict()),
                                   overrides=overrides[idx] if overrides else dict()
                                   )
                )

        return session_requests

    def create_networks(self, session_requests: List[SessionRequest]):
        dbase = cfg.db
        ntable = dbase.get_table('nodes')
        net_names = dict()

        for sesssion_request in session_requests:
            prof = sesssion_request.profile
            sesssion_request.node = dbase.get(ntable, name=sesssion_request.node['name'])
            overrides = sesssion_request.overrides

            if overrides:
                cfg.sm.get_handler(sesssion_request.node).resolve_networks(sesssion_request.node, prof, **overrides)
            else:
                cfg.sm.get_handler(sesssion_request.node).resolve_networks(sesssion_request.node, prof)

            dbase.upsert(ntable, sesssion_request.node, 'name', sesssion_request.node['name'])

            for net in [Network(prof.settings.mgmt_net), Network(prof.settings.data_net)]:
                if not net.name or net.name in [Constants.NET_NONE, Constants.NET_HOST, Constants.NET_BRIDGE]\
                        or net.is_host():
                    continue

                net_name = overrides['name'] if overrides else net.name
                self.update_network(sesssion_request.node, net, net_name)
                net_names[f"{net_name}@{sesssion_request.node['name']}"] = dict(
                    network=net_name,
                    cluster=sesssion_request.node['name']
                )

        return net_names

    def create_session(self, user, group, session_requests: List[SessionRequest], req, current_user, users=None):
        users = users or list()
        db_id = precommit_db()
        svcs = dict()  # get an ID from the DB
        addrs_v4 = set()  # keep a running set of addresses and ports allocated for this request
        addrs_v6 = set()
        cports = set()
        sports = set()
        all_overrides = dict()

        try:
            for i, s in enumerate(session_requests):
                nname = s.node.get('name')
                if nname not in svcs:
                    svcs[nname] = list()

                if s.profile.name.startswith('sense-janus'):
                    sname = cname_from_id(db_id, i + 1, s.profile.name)
                else:
                    sname = cname_from_id(db_id, i + 1, 'janus' + '-' + s.profile.name)

                overrides = s.overrides

                if overrides:
                    all_overrides[overrides['endpoint']] = overrides
                    rec = cfg.sm.get_handler(s.node).create_service_record(
                        sname, s, addrs_v4, addrs_v6, cports, sports, **overrides
                    )
                else:
                    rec = cfg.sm.get_handler(s.node).create_service_record(
                        sname, s, addrs_v4, addrs_v6, cports, sports
                    )

                assert rec is not None
                svcs[nname].append(rec)
        except Exception as e:
            import traceback
            traceback.print_exc()
            precommit_db(Id=db_id, delete=True)
            raise SessionManagerException(f'Was not able to create all services:{e}')

        if not cfg.dryrun:
            try:
                for k, services in svcs.items():
                    for s in services:
                        cfg.sm.init_service(s)
            except Exception as e:
                import traceback
                traceback.print_exc()
                precommit_db(Id=db_id, delete=True)
                raise SessionManagerException(f'Was not able to able to initialize all services:{e}')

        record = dict(uuid=str(uuid.uuid4()), user=user if user else current_user, state=State.INITIALIZED.name)
        record['id'] = db_id
        record['services'] = svcs
        record['request'] = req
        record['users'] = user.split(",") if user else users
        record['groups'] = group.split(",") if group else []
        record['overrides'] = all_overrides
        commit_db(record, db_id)
        return db_id

    @staticmethod
    def _do_poststart(s):
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
                default_name = ps['container_name']
                container_name = s.get('container_name', default_name)
                ex_vars = (f'{{"ipprot": "{ipprot}", "interface": "{inf}", "gateway": "{gateway}", '
                           f'"container": "{container_name}"}}')
                job = AnsibleJob()

                try:
                    # noinspection PyTypeChecker
                    job.launch(job_template=jt_name, monitor=True, wait=True, timeout=600,
                               extra_vars=ex_vars, limits=limit)
                except Exception as e:
                    error_svc(s, e)
                    continue

    def start_session(self, session_id, user=None, group=None):
        """
        Handle the starting of container services
        """
        dbase = cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        assert session_id
        query = self.query_builder(user, group, {"id": session_id})
        svc = dbase.get(table, query=query)

        if not svc:
            raise ResourceNotFoundException(f'no janus session found for {session_id}')

        if svc['state'] == State.STARTED.name:
            return svc

        # start the services
        error = False
        services = svc.get("services", dict())
        for k, v in services.items():
            for s in v:
                node = dbase.get(ntable, name=k)

                if not node:
                    raise ResourceNotFoundException(f"Node not found: {k}")

                handler = cfg.sm.get_handler(node)
                cid = s["container_id"]
                log.debug(f"Starting container {cid} on {k}")

                if not cfg.dryrun:
                    # Error accounting
                    orig_errcnt = len(s.get('errors'))

                    try:
                        handler.start_container(Node(**node), cid, s)  # TODO Handle qos
                    except PortainerApiException as e:
                        log.error(f"Portainer error for {k}: {e.body}")
                        error_svc(s, e.body)
                        error = True
                        continue
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        log.error(f"Could not start container on {k}: {e}")
                        error_svc(s, e)
                        error = True
                        continue

                    # Handle post_start tasks
                    self._do_poststart(s)

                    # Trim logged errors
                    errcnt = len(s.get('errors'))
                    if not errcnt - orig_errcnt:
                        s['errors'] = list()
                    else:
                        s['errors'] = s['errors'][orig_errcnt - errcnt:]
                # End of Ansible job

        if not error and svc.get('peer'):
            peer = svc.get('peer')

            if isinstance(svc.get('peer'), list):
                peer = peer[0]

            try:
                self.start_session(peer['id'])
            except Exception as e:
                log.error(f"Could not start peer session using {peer['id']}: {e}")

        svc['state'] = State.MIXED.name if error else State.STARTED.name
        return commit_db(svc, session_id, realized=True)

    def stop_session(self, session_id):
        dbase = cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        assert session_id is not None
        svc = dbase.get(table, ids=session_id)

        if not svc:
            raise ResourceNotFoundException(f'no janus session found for {session_id}')

        if svc['state'] == State.STOPPED.name:
            return svc

        if svc['state'] == State.INITIALIZED.name:
            return svc

        # stop the services
        error = False
        for k, v in svc['services'].items():
            for s in v:
                cid = s['container_id']
                node = dbase.get(ntable, name=k)

                if not node:
                    raise ResourceNotFoundException(f"Container node {k} not found for container_id: {cid}")

                log.debug(f"Stopping container {cid} on {k}")

                if not cfg.dryrun:
                    try:
                        cfg.sm.get_handler(node).stop_container(Node(**node), cid, **{'service': s})
                    except Exception as e:
                        log.error(f"Could not stop container on {k}: {e}")
                        error_svc(s, e)
                        error = True
                        continue

        svc['state'] = State.MIXED.name if error else State.STOPPED.name
        svc = commit_db(svc, session_id, delete=True, realized=True)

        if peers := svc.get('peer'):
            assert isinstance(peers, list)
            for peer in peers:
                try:
                    self.stop_session(peer['id'])
                except Exception as e:
                    log.error(f"Could not stop peer session using {peer['id']}: {e}")

        return svc

    def delete(self, aid, force=False, user=None, group=None):
        """
        Deletes an active allocation (e.g. stops containers)
        """
        query = self.query_builder(user, group, {"id": aid})
        dbase = cfg.db
        nodes = dbase.get_table('nodes')
        table = dbase.get_table('active')
        doc = dbase.get(table, query=query)

        if doc is None:
            return

        futures = list()

        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in doc['services'].items():
                ex = None
                try:
                    n = dbase.get(nodes, name=k)
                    if not n:
                        raise ResourceNotFoundException(f"Node {k} not found")
                    handler = cfg.sm.get_handler(n)
                    if not cfg.dryrun:
                        for s in v:
                            futures.append(executor.submit(handler.stop_container,
                                                           Node(**n), s.get('container_id'),
                                                           **{'service': s,
                                                              'name': k}))
                except (KubeApiException, PortainerApiException) as ae:
                    if str(ae.status) == "404":
                        continue

                    ex = ae
                except Exception as e:
                    ex = e

                if ex:
                    import traceback
                    traceback.print_exc()
                    log.error(f"Could not stop container: {k}:{ex}")

        if not cfg.dryrun:
            for future in concurrent.futures.as_completed(futures):
                ex = None

                try:
                    res = future.result()
                    if "container_id" in res:
                        log.debug(f"Removing container {res['container_id']}")
                        handler = cfg.sm.get_handler(nname=res['node_name'])
                        handler.remove_container(Node(name=res['node_name'], id=res['node_id']), res['container_id'])
                except (KubeApiException, PortainerApiException) as ae:
                    if str(ae.status) == "404":
                        continue

                    ex = ae
                except Exception as e:
                    ex = e

                if ex and not force:
                    log.error(f"Could not remove container on remote node: {type(ex)}:{ex}")
                    raise ex

        # delete always removes realized state info
        commit_db(doc, aid, delete=True, realized=True)
        commit_db(doc, aid, delete=True)

    def exec(self, req):
        """
        Handle the execution of a container command inside Service
        """
        start = False
        attach = True
        tty = False
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
            return {"error": f"Node not found: {nname}"}

        container = req["container"]
        cmd = req["Cmd"]

        kwargs = {'AttachStdin': False,
                  'AttachStdout': attach,
                  'AttachStderr': attach,
                  'Tty': tty,
                  'Cmd': cmd
                  }

        handler = cfg.sm.get_handler(node)
        ret = handler.exec_create(Node(**node), container, **kwargs)
        if start:
            handler.exec_start(Node(**node), ret)

        return ret
