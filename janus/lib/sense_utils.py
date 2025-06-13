from configparser import ConfigParser

from janus.settings import JanusConfig


class SenseConstants:
    SENSE_METADATA_URL = 'sense-metadata-url'
    SENSE_METADATA_ASSIGNED = 'sense-metadata-assigned'
    JANUS_DEVICE_MANAGER = 'janus.device.manager'
    SENSE_DOMAIN_INFO = 'sense-metadata-domain-info'
    SENSE_PLUGIN_VERSION = '0.3'
    SENSE_PLUGIN_RETRIES = 3
    SENSE_JANUS_SESSION_CREATION = 'sense-janus-session-creation'
    SENSE_JANUS_SESSION_CREATE = 'create'
    SENSE_JANUS_SESSION_START = 'start'
    SENSE_JANUS_SESSION_CREATION_DEFAULTS = f'{SENSE_JANUS_SESSION_CREATE},{SENSE_JANUS_SESSION_START}'


class SenseUtils:
    @staticmethod
    def parse_from_config(cfg: JanusConfig, parser: ConfigParser, plugin_section='PLUGINS'):
        if plugin_section in parser:
            for plugin in parser[plugin_section]:
                if plugin == 'sense-metadata-plugin':
                    sense_meta_plugin = parser.get("PLUGINS", 'sense-metadata-plugin', fallback=None)

                    if sense_meta_plugin:
                        sense_properties = dict()
                        cfg.sense_metadata = parser.getboolean(sense_meta_plugin, 'sense-metadata-enabled',
                                                               fallback=False)
                        sense_metadata_url = parser.get(sense_meta_plugin, SenseConstants.SENSE_METADATA_URL,
                                                        fallback=None)
                        sense_metadata_assigned = parser.get(sense_meta_plugin, SenseConstants.SENSE_METADATA_ASSIGNED,
                                                             fallback=SenseConstants.JANUS_DEVICE_MANAGER)
                        sense_metadata_domain_info = parser.get(sense_meta_plugin, SenseConstants.SENSE_DOMAIN_INFO,
                                                                fallback='JANUS/AES_TESTING')

                        sense_properties[SenseConstants.SENSE_METADATA_URL] = sense_metadata_url
                        sense_properties[SenseConstants.SENSE_DOMAIN_INFO] = sense_metadata_domain_info
                        sense_properties[SenseConstants.SENSE_METADATA_ASSIGNED] = sense_metadata_assigned
                        sense_janus_creation = parser.get(sense_meta_plugin,
                                                          SenseConstants.SENSE_JANUS_SESSION_CREATION,
                                                          fallback=SenseConstants.SENSE_JANUS_SESSION_CREATION_DEFAULTS)
                        sense_properties[SenseConstants.SENSE_JANUS_SESSION_CREATION] = sense_janus_creation
                        return sense_properties

        return None

    @staticmethod
    def to_target_summary(targets: list):
        ret = list()

        for target in targets:
            if 'cluster_info' in target:
                ret.append(dict(name=target['name'], cluster_info=target['cluster_info']))
            else:
                ret.append(dict(name=target['name']))

        return ret

    @staticmethod
    def to_targets(sense_session):
        task_info = sense_session.get('task_info', dict())
        targets = sum(task_info.values(), [])
        targets = sorted(targets, key=lambda t: t['name'])
        targets = [t.copy() for t in targets]

        for target in targets:
            target['principals'] = sense_session.get('users', list)
            target['janus_network_profile'] = None
            target['janus_host_profile'] = None
            target['janus_session'] = None

        if network_profiles := sense_session.get('network_profile', list()):
            for idx, target in enumerate(targets):
                network_profile = network_profiles[idx] if idx < len(network_profiles) else network_profiles[0]
                target['janus_network_profile'] = network_profile

        if host_profiles := sense_session.get('host_profile', list()):
            for idx, target in enumerate(targets):
                host_profile = host_profiles[idx] if idx < len(host_profiles) else host_profiles[0]
                target['janus_host_profile'] = host_profile

        if janus_sessions := sense_session.get('janus_session_id', list()):
            for idx, target in enumerate(targets):
                janus_session = janus_sessions[idx] if idx < len(janus_sessions) else janus_sessions[0]
                target['janus_session'] = janus_session

        return targets

    @staticmethod
    def get_service_info(janus_session):
        pods = list()
        clusters = janus_session['services']

        for node_name, services in clusters.items():
            pods.extend([
                dict(container_id=service['container_id'],
                     node=node_name,
                     sname=service['sname'],
                     node_id=service['node_id'],
                     data_ipv4=service['data_ipv4'],
                     data_ipv6=service['data_ipv6'],
                     data_net_name=service['data_net_name'],
                     ctrl_host=service['ctrl_host'],
                     profile=service['profile']
                     ) for service in services
            ])

        return pods

    @staticmethod
    def to_sense_session_summary(sense_session: dict, janus_sessions):
        janus_sessions_summaries = list()

        for janus_session in janus_sessions:
            janus_session_summary = dict(
                uuid=janus_session['uuid'],
                state=janus_session['state'],
                users=janus_session['users'],
                pods=SenseUtils.get_service_info(janus_session),
                peer=janus_session.get('peer')
            )

            janus_sessions_summaries.append(janus_session_summary)

        return dict(sense_session=sense_session['name'],
                    network_profiles=sense_session['network_profile'],
                    host_profiles=sense_session['host_profile'],
                    status=sense_session['status'],
                    state=sense_session.get('state'),
                    errors=sense_session.get('errors'),
                    janus_sessions=janus_sessions_summaries)

    @staticmethod
    def peer_sessions(janus_session1, janus_session2):
        pods1 = SenseUtils.get_service_info(janus_session1)
        pods2 = SenseUtils.get_service_info(janus_session2)
        janus_session1['peer'] = [dict(id=janus_session2['id'], services=pods2)]
        janus_session2['peer'] = [dict(id=janus_session1['id'], services=pods1)]

    @staticmethod
    def dump_sessions_requests(session_requests):
        import json

        dumpable_session_requests = list()

        for sr in session_requests:
            dumpable_sr = sr.model_dump(mode="json")
            dumpable_sr['node'] = dict(name=dumpable_sr['node']['name'])
            # dumpable_sr['profile'] = dict(name=dumpable_sr['profile']['name'],
            #                               data_net=dumpable_sr['profile']['data_net'])
            profile = dict(name=dumpable_sr['profile']['name'],
                           settings=dict(
                               data_net=dumpable_sr['profile']['settings']['data_net']
                           ))
            dumpable_sr['profile'] = profile
            dumpable_sr['constraints'] = dict()
            print("*********")

            print(dumpable_sr)
            print("*********")
            dumpable_session_requests.append(dumpable_sr)

        print(json.dumps(dumpable_session_requests, indent=2))
