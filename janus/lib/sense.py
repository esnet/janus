import json
import logging
import time
from threading import Thread

from sense.client.requestwrapper import RequestWrapper
from kubernetes.client import ApiException as KubeApiException
from portainer_api.rest import ApiException as PortainerApiException

from janus.api.models import Node
from janus.api.session_manager import SessionManager
from janus.lib.sense_api_handler import SENSEApiHandler
from janus.lib.sense_db_handler import DBHandler
from janus.lib.sense_utils import SenseConstants, SenseUtils
from janus.settings import JanusConfig

log = logging.getLogger(__name__)


class SENSEMetaManager(DBHandler):
    def __init__(self, cfg: JanusConfig, properties: dict, sense_api_handler=None):
        super().__init__(cfg)
        url = properties[SenseConstants.SENSE_METADATA_URL]
        self.sense_api_handler = sense_api_handler or SENSEApiHandler(url=url, req_wrapper=RequestWrapper())
        self.properties = properties
        self.session_manager = SessionManager()
        self.retries = SenseConstants.SENSE_PLUGIN_RETRIES
        self.counter = 0
        janus_session_creation = properties[SenseConstants.SENSE_JANUS_SESSION_CREATION].lower()
        self.create_janus_sessions = SenseConstants.SENSE_JANUS_SESSION_CREATE in janus_session_creation
        self.start_janus_sessions = SenseConstants.SENSE_JANUS_SESSION_START in janus_session_creation
        log.info(f"Initialized {__name__}:properties={json.dumps(properties, indent=2)}")

    def update_metadata(self, agents):
        from datetime import datetime, timezone

        domain_info = self.properties[SenseConstants.SENSE_DOMAIN_INFO].split('/')
        name = domain_info[1].upper()
        metadata = dict(agents=agents)
        ret = self.sense_api_handler.post_metadata(metadata=metadata, domain=domain_info[0], name=name)

        if ret:
            utc_iso_str = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%dT%H:%M:%S.%f")[:-3]
            name += '_SERVER_INFO'
            images = self.db.all(self.image_table)
            images = [image['name'] for image in images]
            metadata = dict(agents=dict(),
                            timestamp=utc_iso_str,
                            sense_plugin_version=SenseConstants.SENSE_PLUGIN_VERSION,
                            counter=self.counter,
                            options=dict(network=['macvlan', 'host'], images=images))

            ret = self.sense_api_handler.post_metadata(metadata=metadata, domain=domain_info[0], name=name)

        self.counter += 1
        return ret

    def delete_network(self, cluster_name, name):
        ntable = self.db.get_table('nodes')
        node = self.db.get(ntable, name=cluster_name)
        handler = self.cfg.sm.get_handler(node)

        try:
            handler.remove_network(Node(**node), name)
        except (KubeApiException, PortainerApiException) as ae:
            if str(ae.status) != "404":
                raise ae

    @staticmethod
    def _instances(targets):
        instances = list()

        for t in targets:
            if 'cluster_info' in t:
                instances.append(dict(name=t['cluster_info']['cluster_name'], nodeName=t['name']))
            else:
                instances.append(dict(name=t['name']))

        return instances

    def create_janus_session(self, sense_session, host_networking=False):
        session_manager = SessionManager()
        targets = sum(sense_session['task_info'].values(), [])
        targets = sorted(targets, key=lambda t: t['name'])
        host_profiles = sense_session['host_profile']
        network_profiles = sense_session['network_profile']
        owner = sense_session['users'][0] if sense_session['users'] else 'admin'
        overrides = list()
        requests = list()

        assert len(host_profiles) == 1

        for target in targets:
            option = dict(endpoint=target['name'])

            if not host_networking:
                vlan = target['vlan']
                option.update(dict(name=f"{network_profiles[0]}-{vlan}",
                              vlan=str(vlan),
                              parent=f"vlan.{vlan}",
                              mtu=1500))
            elif host_networking and target.get('ip'):
                addr = target['ip']
                option.update(dict(ip_addr=addr[:addr.index('/')] if '/' in addr else addr))

            overrides.append(option)

        for idx, instance in enumerate(self._instances(targets)):
            request = dict(
                instances=[instance],
                profile=host_profiles[0],
                errors=[],
                image='dtnaas/tools:latest',
                arguments=str(),
                kwargs=dict(USER_NAME=str(), PUBLIC_KEY=str()),
                remove_container=False,
                overrides=[overrides[idx]]
            )
            requests.append(request)

        log.info(f'creating janus sample session using sense_session {sense_session["name"]}:{requests}')
        session_manager.validate_request(requests)
        session_requests = session_manager.parse_requests(None, None, requests)
        # SenseUtils.dump_sessions_requests(session_requests)
        net_names = session_manager.create_networks(session_requests)
        sense_session['networks'] = net_names
        janus_session_id = session_manager.create_session(
            None, None, session_requests, requests, owner, users=sense_session["users"]
        )

        return [janus_session_id]

    def start_janus_session(self, janus_session_ids):
        for janus_session_id in janus_session_ids:
            try:
                self.session_manager.start_session(janus_session_id)
            except Exception as e:
                log.error(f'not able to start janus session {janus_session_id}:{e}')

    def terminate_janus_sessions(self, sense_session: dict):
        for janus_session in self.find_janus_session(host_profile_names=sense_session['host_profile']):
            self.session_manager.stop_session(janus_session['id'])
            self.session_manager.delete(janus_session['id'], force=True)

        for v in sense_session.get("networks", dict()).values():
            cluster_name = v['cluster']
            clusters = self.find_cluster(name=cluster_name)
            cluster = clusters[0] if clusters else None

            if not cluster:
                log.warning(f'did not find cluster {cluster_name} while terminating sessions {sense_session}')
                continue

            net_name = v['network']
            self.delete_network(cluster_name=cluster_name, name=net_name)
            self.db.remove(self.networks_table, name=net_name)

            if net_name in cluster['networks']:
                del cluster['networks'][net_name]
                self.db.upsert(self.nodes_table, cluster, 'name', cluster_name)

    def _retrieve_instance_termination_notice_command(self, task):
        config = task['config']
        command = config['command']
        task_id = task['uuid']
        instance_id = config['context']['uuid']
        alias = config['context']['alias']
        sense_sessions = self.find_sense_session(sense_session_key=instance_id)
        sense_session = sense_sessions[0] if sense_sessions else dict()

        if not sense_session:
            self.sense_api_handler.finish_task(task_id, list(), f'no session found for {instance_id}:{alias}')
            return None

        sense_session['command'] = command
        sense_session['status'] = 'PENDING'
        sense_session['termination_task'] = task_id
        self.save_sense_session(sense_session=sense_session)
        return sense_session

    def _retrieve_handle_sense_instance_command(self, task, agents=None, node_names=None):
        node_cluster_map = agents or self.get_agents()
        node_names = node_names or [n for n in node_cluster_map]
        config = task['config']
        command = config['command']
        task_id = task['uuid']
        instance_id = config['context']['uuid']
        alias = config['context']['alias']
        targets = config['targets']
        task_info = dict()
        task_info[task_id] = targets
        endpoints = [target['name'] for target in targets]

        unknown_endpoints = [endpoint for endpoint in endpoints if endpoint not in node_names]
        if unknown_endpoints:
            log.warning(f'unknown endpoint for instance {instance_id}:endpoints={unknown_endpoints}')
            self.sense_api_handler.reject_task(task_id, targets, f"unkown targets:{unknown_endpoints}")
            return None

        clusters = list()
        for target in targets:
            agent = node_cluster_map[target['name']]

            if 'cluster_info' in agent:
                cluster_info = agent['cluster_info']
                target['cluster_info'] = cluster_info
                clusters.append(cluster_info['cluster_name'])
            else:
                clusters.append(target['name'])

        if not alias:
            alias = f'sense-janus-{"-".join(instance_id.split("-")[0:2])}'
        else:
            alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

        users = list()
        for target in targets:
            users.extend(target['principals'])

        users = sorted(list(set(users)))

        if 'admin' in users:
            users.remove('admin')

        sense_session = dict(key=instance_id,
                             name=alias,
                             task_info=task_info,
                             users=users,
                             status='PENDING',
                             command=command,
                             clusters=list(set(clusters)))

        sense_session['state'] = 'MODIFIED' if self.check_modified(sense_session) else 'OK'
        self.save_sense_session(sense_session=sense_session)

        if sense_session['state'] == 'MODIFIED':
            log.debug(f'saved pending sense session:{json.dumps(sense_session)}')

        return sense_session

    def retrieve_tasks(self, agents=None):
        assigned = self.properties[SenseConstants.SENSE_METADATA_ASSIGNED]
        tasks = self.sense_api_handler.retrieve_tasks(assigned=assigned, status='PENDING')

        if not tasks:
            return None, None, None

        node_cluster_map = agents or self.get_agents()
        node_names = [n for n in node_cluster_map]
        validated_sense_sessions = list()
        rejected_tasks = list()
        failed_tasks = list()

        for task in tasks:
            task_id = None
            targets = list()

            try:
                config = task['config']
                targets = config['targets']
                command = config['command']
                task_id = task['uuid']
                log.debug(f'RETRIEVED_TASK:command={command}:task_id={task_id}:targets={targets}')

                if command not in ['handle-sense-instance', 'instance-termination-notice']:
                    self.sense_api_handler.reject_task(task_id, targets, f"unknown command:{command}")
                    rejected_tasks.append(task)
                    continue

                instance_id = config['context']['uuid']
                sense_sessions = self.find_sense_session(sense_session_key=instance_id)
                assert len(sense_sessions) <= 1
                sense_session = sense_sessions[0] if sense_sessions else dict()

                if sense_session and 'status' in sense_session and sense_session['status'] == 'PENDING':
                    log.debug(f'DELAYING_HANDLING_RETRIEVED_TASK:command={command}:task_id={task_id}:targets={targets}')
                    continue

                log.debug(f'HANDLING_RETRIEVED_TASK:command={command}:task_id={task_id}:targets={targets}')
                users = list()

                for target in targets:
                    users.extend(target['principals'])

                if not targets or not users or command == 'instance-termination-notice':
                    config['command'] = 'instance-termination-notice'
                    ret = self._retrieve_instance_termination_notice_command(task)
                else:
                    ret = self._retrieve_handle_sense_instance_command(task, agents, node_names)

                if ret:
                    validated_sense_sessions.append(ret)
                else:
                    rejected_tasks.append(task)
            except Exception as e:
                import traceback
                traceback.print_exc()

                log.error(f'Error in RetrieveTask: {e}')
                if task_id:
                    self.sense_api_handler.fail_task(task_id, targets, f'retrieve_task:{type(e)}:{e}')

                failed_tasks.append(task)

        return validated_sense_sessions, rejected_tasks, failed_tasks

    def accept_tasks(self):
        sense_sessions = self.find_sense_session(status='PENDING')

        for sense_session in sense_sessions:
            task_info = sense_session['task_info']
            targets = sum(task_info.values(), [])

            if 'termination_task' in sense_session:
                uuid = sense_session['termination_task']
                self.sense_api_handler.accept_task(uuid, targets, '')
            else:
                uuids = [uuid for uuid in task_info]

                for uuid in uuids:
                    self.sense_api_handler.accept_task(uuid, targets, '')

            sense_session['status'] = 'ACCEPTED'
            self.save_sense_session(sense_session=sense_session)

        return sense_sessions

    def _finish_handle_sense_instance_command(self, task_id, sense_session):
        modified = sense_session['state'] == 'MODIFIED'

        if modified:
            error = False

            if 'network_profile' not in sense_session:
                sense_session['network_profile'] = list()

            if 'host_profile' not in sense_session:
                sense_session['host_profile'] = list()

            try:
                self.terminate_janus_sessions(sense_session=sense_session)
                sense_session['janus_session_id'] = list()
                sense_session['networks'] = dict()
                self.remove_profiles(sense_session=sense_session)
                sense_session['network_profile'] = list()
                sense_session['host_profile'] = list()
                sense_session['terminate_error_message'] = None
            except Exception as e:
                import traceback
                traceback.print_exc()
                log.warning(f'error terminating janus session: {e}')
                sense_session['errors'] += 1
                sense_session['terminate_error_message'] = f'error terminating janus session: {e}'
                error = True

            if not error:
                try:
                    if sum(sense_session['task_info'].values(), []):
                        self.create_profiles(sense_session=sense_session)

                        if self.create_janus_sessions:
                            janus_session_ids = self.create_janus_session(sense_session=sense_session)
                            sense_session['janus_session_id'] = janus_session_ids

                            if self.start_janus_sessions:
                                self.session_manager.start_session(session_id=janus_session_ids[0])
                    else:
                        sense_session['users'] = list()
                        sense_session['clusters'] = list()

                    sense_session['errors'] = 0
                    sense_session['error_message'] = None
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.warning(f'error creating profiles or janus session: {e}')
                    sense_session['errors'] += 1
                    sense_session['error_message'] = f'error creating profiles or janus session: {e}'

        if 0 < sense_session['errors'] < self.retries:
            targets = SenseUtils.to_targets(sense_session)
            mesg = sense_session['terminate_error_message'] \
                if sense_session['terminate_error_message'] else sense_session['error_message']
            self.sense_api_handler.wait_task(task_id, targets, mesg)
            self.save_sense_session(sense_session=sense_session)
            return

        if sense_session['errors'] == 0:
            sense_session['status'] = 'FINISHED'
            sense_session['state'] = 'MODIFIED_OK' if modified else 'OK'
            mesg = f'session for {sense_session["key"]} has been handled'
        else:
            sense_session['status'] = 'FINISHED'
            sense_session['state'] = 'PARTIALLY_HANDLED'
            sense_session['errors'] = 0
            mesg = f'session for {sense_session["key"]} has been partially handled:errors={sense_session["errors"]}'
            log.warning(f'giving up on creating janus session: {mesg}')

        self.save_sense_session(sense_session=sense_session)
        self.update_janus_sessions(sense_session=sense_session)
        targets = SenseUtils.to_targets(sense_session=sense_session)
        self.sense_api_handler.finish_task(task_id, targets, mesg)

    def _finish_instance_termination_notice_command(self, task_id, sense_session):
        try:
            self.terminate_janus_sessions(sense_session=sense_session)
            self.remove_profiles(sense_session=sense_session)
            sense_session['errors'] = 0
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.warning(f'error terminating sense session: {e}')
            sense_session['errors'] += 1

        if 0 < sense_session['errors'] < self.retries:
            self.save_sense_session(sense_session=sense_session)
            return

        if sense_session['errors'] == 0:
            mesg = f'session for {sense_session["key"]} has been terminated'
        else:
            mesg = f'giving up on terminating session for {sense_session["key"]}'

        targets = SenseUtils.to_targets(sense_session)
        self.sense_api_handler.finish_task(task_id, targets, mesg)
        sense_session['status'] = 'DELETED'
        log.warning(f'removing sense session from db: {mesg}')
        self.db.remove(self.sense_session_table, name=sense_session['name'])

    def finish_tasks(self):
        sense_sessions = self.find_sense_session(status='ACCEPTED')

        for sense_session in sense_sessions:
            if 'errors' not in sense_session:
                sense_session['errors'] = 0

            task_id = None

            try:
                if sense_session['command'] != 'handle-sense-instance':
                    assert 'termination_task' in sense_session
                    task_id = sense_session['termination_task']
                    self._finish_instance_termination_notice_command(task_id, sense_session)
                else:
                    uuids = [uuid for uuid in sense_session['task_info']]
                    assert len(uuids) == 1
                    task_id = uuids[0]
                    self._finish_handle_sense_instance_command(task_id, sense_session)
            except Exception as e:
                import traceback
                traceback.print_exc()
                log.error(f'Error in FinishTask : {e}')
                targets = SenseUtils.to_targets(sense_session)
                self.sense_api_handler.fail_task(task_id, targets, f'finish_task:{type(e)}:{e}')
                sense_session['status'] = 'FAILED'
                self.db.remove(self.sense_session_table, name=sense_session['name'])

        return sense_sessions

    def run(self):
        agents = self.get_agents()
        number_of_nodes = len(agents)

        if number_of_nodes == 0:  # Wait for nodes to be populated
            log.warning(f'Waiting on nodes: Number of nodes: {number_of_nodes}')
            return

        if not self.update_metadata(agents=agents):
            return

        sense_sessions, rejected_tasks, failed_tasks = self.retrieve_tasks(agents=agents)
        counters = dict(retrieved=0, rejected=0, accepted=0, terminated=0, finished=0, failed=0)
        print_summary = False

        if sense_sessions:
            counters['retrieved'] = len(sense_sessions)
            print_summary = True

        if rejected_tasks:
            counters['rejected'] = len(rejected_tasks)
            print_summary = True

        if failed_tasks:
            counters['failed'] = len(failed_tasks)
            print_summary = True

        sense_sessions = self.accept_tasks()

        if sense_sessions:
            counters['accepted'] = len(sense_sessions)
            print_summary = True

        sense_sessions = self.finish_tasks()

        if sense_sessions:
            self.update_clusters_user_infos()
            self.update_images()
            self.update_builtin_host_network_profile()
            counters['finished'] = len([s for s in sense_sessions if s['status'] == 'FINISHED'])
            counters['terminated'] = len([s for s in sense_sessions if s['status'] == 'DELETED'])
            counters['failed'] = len([s for s in sense_sessions if s['status'] == 'FAILED'])
            print_summary = True

        if print_summary:
            to_session_summary = SenseUtils.to_sense_session_summary

            sense_session_summaries = [
                to_session_summary(s,
                                   self.find_janus_session(host_profile_names=s['host_profile'])
                                   ) for s in sense_sessions if 'FAILED' != s['status']
            ]
            existing_sessions = self.db.all(self.sense_session_table)
            existing_sessions = [
                dict(name=s['name'],
                     status=s.get('status'),
                     state=s.get('state'),
                     errors=s.get('errors'),
                     number_janus_sessions=len(self.find_janus_session(host_profile_names=s['host_profile'])),
                     users=s['users']) for s in existing_sessions]

            server_info = dict(counters=counters, last_updates=sense_session_summaries, all_sessions=existing_sessions)
            log.info(f'SERVER_INFO:{json.dumps(server_info)}')


class SENSEMetaRunner:
    def __init__(self, cfg: JanusConfig, properties: dict):
        self._stop = False
        self._interval = 10
        self._th = None
        self._sense_mngr = SENSEMetaManager(cfg, properties)
        log.info(f"Initialized {__name__}")

    def start(self):
        log.debug(f"Started {__name__}")
        self._th = Thread(target=self._run, args=())
        self._th.start()

    def stop(self):
        log.debug(f"Stopping {__name__}")
        self._stop = True
        self._th.join()

    def _run(self):
        cnt = 0
        log.debug(f"Running {__name__}:stop={self._stop}")
        while not self._stop:
            time.sleep(1)
            cnt += 1
            if cnt == self._interval:
                try:
                    self._sense_mngr.run()
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    log.error(f'Error in SenseMetaRunner : {e}')

                cnt = 0
