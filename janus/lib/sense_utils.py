from configparser import ConfigParser

from janus.settings import JanusConfig


class SenseConstants:
    SENSE_METADATA_URL = 'sense-metadata-url'
    SENSE_METADATA_ASSIGNED = 'sense-metadata-assigned'
    JANUS_DEVICE_MANAGER = 'janus.device.manager'
    SENSE_DOMAIN_INFO = 'sense-metadata-domain-info'
    SENSE_PLUGIN_VERSION = '0.1'
    SENSE_PLUGIN_RETRIES = 3


class SenseUtils:
    @staticmethod
    def parse_from_config(cfg: JanusConfig, parser: ConfigParser, plugin_section='PLUGINS'):
        sense_properties = dict()

        for plugin in parser[plugin_section]:
            if plugin == 'sense-metadata-plugin':
                sense_meta_plugin = parser.get("PLUGINS", 'sense-metadata-plugin', fallback=None)

                if sense_meta_plugin:
                    cfg.sense_metadata = parser.getboolean(sense_meta_plugin, 'sense-metadata-enabled', fallback=False)
                    sense_metadata_url = parser.get(sense_meta_plugin, SenseConstants.SENSE_METADATA_URL, fallback=None)
                    sense_metadata_assigned = parser.get(sense_meta_plugin, SenseConstants.SENSE_METADATA_ASSIGNED,
                                                         fallback=SenseConstants.JANUS_DEVICE_MANAGER)
                    sense_metadata_domain_info = parser.get(sense_meta_plugin, SenseConstants.SENSE_DOMAIN_INFO,
                                                            fallback='JANUS/AES_TESTING')
                    sense_properties[SenseConstants.SENSE_METADATA_URL] = sense_metadata_url
                    sense_properties[SenseConstants.SENSE_DOMAIN_INFO] = sense_metadata_domain_info
                    sense_properties[SenseConstants.SENSE_METADATA_ASSIGNED] = sense_metadata_assigned

        return sense_properties

    @staticmethod
    def to_target_summary(targets: list):
        ret = list()

        for target in targets:
            if 'cluster_info' in target:
                ret.append(dict(name=target['name'], cluster_info=target['cluster_info']))
            else:
                ret.append(dict(name=target['name']))

        return ret
