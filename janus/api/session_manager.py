import concurrent
import logging.config
import uuid
from concurrent.futures import ThreadPoolExecutor

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


class SessionManager(QueryUser):

    def __init__(self, db=None):
        self.db = db

    def update_networks(self, node):
        data_nets = list()
        profs = cfg.pm.get_profiles(Constants.HOST)
        dbase = self.db or cfg.db

        for p in profs:
            for net in [Network(p.settings.mgmt_net), Network(p.settings.data_net)]:
                if net.name not in data_nets:
                    data_nets.append(net.name)

        # simple IPAM for data networks
        net_table = dbase.get_table('networks')
        node_table = dbase.get_table('nodes')
        k = node.get('name')

        res = dbase.get(node_table, name=k)
        nets = res.get('networks', dict())

        for n, w in nets.items():
            subnet = w.get('subnet', [])
            # try to get subnet information from profile if not tracked in endpoint
            if not subnet:
                pnet = cfg.pm.get_profile(Constants.NET, n)

                # noinspection PyBroadException
                try:
                    subnet = pnet.settings.ipam.get('config') if pnet else []
                except Exception:
                    subnet = []

            subnet = [keys_lower(x) for x in subnet]
            key = f"{k}-{n}"
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

    def create_session(self, user, group, req, current_user, users=None):
        users = users or list()

        if type(req) is dict:
            req = [req]

        log.debug(req)
        dbase = self.db or cfg.db
        ntable = dbase.get_table('nodes')

        # Do auth and resource availability checks first
        create = list()
        for r in req:
            instances = r["instances"]
            profile = r["profile"]
            img = r['image']
            arguments = r.get("arguments", None)
            remove_container = r.get("remove_container", None)
            prof = cfg.pm.get_profile(Constants.HOST, profile, user, group)
            assert prof is not None

            # Endpoints and Networks
            for ep in instances:
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
                assert node is not None, f"did not find endpoint {ename}"

                # TODO should we just update regardless???
                # The db.json has beeen delered

                # if cfg.sm.get_handler(node).resolve_networks(node, prof):
                cfg.sm.get_handler(node).resolve_networks(node, prof)
                dbase.upsert(ntable, node, 'name', node['name'])
                self.update_networks(node)

                create.append(SessionRequest(node=node,
                                             profile=prof,
                                             image=img,
                                             arguments=arguments,
                                             remove_container=remove_container,
                                             constraints=c,
                                             kwargs=r.get("kwargs", dict())))

        assert 0 < len(create) <= 2, 'expected one or two requests'

        # get an ID from the DB
        db_id = precommit_db()
        svcs = dict()

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

            if s.profile.name.startswith('sense-janus'):
                sname = cname_from_id(db_id, i, s.profile.name)
            else:
                sname = cname_from_id(db_id, i)

            # noinspection PyBroadException
            try:
                rec = cfg.sm.get_handler(s.node).create_service_record(sname, s, addrs_v4, addrs_v6, cports, sports)

                if rec:
                    svcs[nname].append(rec)
            except Exception:
                import traceback
                traceback.print_exc()
                break

            i += 1

        if (i - 1) != len(create):
            precommit_db(Id=db_id, delete=True)
            raise Exception("Was not able to create all services")

        # setup simple accounting
        record = {'uuid': str(uuid.uuid4()),
                  'user': user if user else current_user,
                  'state': State.INITIALIZED.name}

        for k, services in svcs.items():
            for s in services:
                cfg.sm.init_service(s)

        # complete accounting
        record['id'] = db_id
        record['services'] = svcs
        record['request'] = req
        record['users'] = user.split(",") if user else users
        record['groups'] = group.split(",") if group else []
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
        dbase = self.db or cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        assert session_id
        query = self.query_builder(user, group, {"id": session_id})
        svc = dbase.get(table, query=query)

        if not svc:
            raise Exception(f'no janus session found for {session_id}')

        if svc['state'] == State.STARTED.name:
            return svc

        # start the services
        error = False
        services = svc.get("services", dict())
        for k, v in services.items():
            for s in v:
                cid = s.get("container_id")
                if not cid:
                    log.warning(f"Skipping service with no container_id: {k}")
                    error = True
                    continue
                node = dbase.get(ntable, name=k)
                if not node:
                    log.error(f"Container node {k} not found for container_id: {cid}")
                    return {"error": f"Node not found: {k}"}, 404
                handler = cfg.sm.get_handler(node)
                log.debug(f"Starting container {cid} on {k}")

                if not cfg.dryrun:
                    # Error acconting
                    orig_errcnt = len(s.get('errors'))

                    try:
                        handler.start_container(Node(**node), cid, s)  # TODO Handle qos
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
                    if not errcnt-orig_errcnt:
                        s['errors'] = list()
                    else:
                        s['errors'] = s['errors'][orig_errcnt-errcnt:]
                # End of Ansible job
        svc['state'] = State.MIXED.name if error else State.STARTED.name
        return commit_db(svc, session_id, realized=True)

    def stop_session(self, session_id):
        dbase = self.db or cfg.db
        table = dbase.get_table('active')
        ntable = dbase.get_table('nodes')
        assert session_id is not None
        svc = dbase.get(table, ids=session_id)

        if not svc:
            raise Exception(f"janus session {session_id} not found")

        if svc['state'] == State.STOPPED.name:
            # log.warning(f"Service {svc['uuid']} already stopped")
            return svc

        if svc['state'] == State.INITIALIZED.name:
            # log.warning(f"Service {svc['uuid']} already in initialized state")
            return svc

        # stop the services
        error = False
        for k, v in svc['services'].items():
            for s in v:
                cid = s.get('container_id')

                if not cid:
                    log.warning(f"Skipping service with no container_id: {k}")
                    continue

                node = dbase.get(ntable, name=k)

                if not node:
                    raise Exception(f"Container node {k} not found for container_id: {cid}")

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

        if error:
            raise Exception(str(svc['errors']))

        if svc.get('peer'):
            peer = svc.get('peer')

            if isinstance(svc.get('peer'), list):
                peer = peer[0]

            try:
                self.stop_session(peer['id'])
            except Exception as e:
                log.error(f"Could not stop peer container using {peer['id']}: {e}")

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
            raise Exception(f"Session Not found {id}")

        futures = list()

        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in doc['services'].items():
                try:
                    n = dbase.get(nodes, name=k)
                    if not n:
                        log.error(f"Node {k} not found")
                        return {"error": f"Node not found: {k}"}, 404
                    handler = cfg.sm.get_handler(n)
                    if not cfg.dryrun:
                        for s in v:
                            futures.append(executor.submit(handler.stop_container,
                                                           Node(**n), s.get('container_id'),
                                                           **{'service': s,
                                                              'name': k}))
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.error(f"Could not find node/container to stop, or already stopped: {k}:{e}")
        if not cfg.dryrun:
            for future in concurrent.futures.as_completed(futures):
                from kubernetes.client import ApiException as KubeApiException
                from portainer_api.rest import ApiException as PortainerApiException

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

                if ex:
                    log.error(f"Could not remove container on remote node: {type(ex)}:{ex}")
                    if not force:
                        return {"error": str(e)}, 503

        # delete always removes realized state info
        commit_db(doc, aid, delete=True, realized=True)
        commit_db(doc, aid, delete=True)
        return None, 204
