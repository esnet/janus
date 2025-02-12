from configparser import ConfigParser

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.lib.sense import SENSEMetaManager
from janus.lib.sense_utils import SenseUtils
from janus.settings import cfg, SUPPORTED_IMAGES

import logging

log = logging.getLogger(__name__)


class SenseRunnable:
    def __init__(self, database, config_file, sense_api_handler=None, node_name_filter=None):
        db = DBLayer(path=database)

        pm = ProfileManager(db, None)
        sm = ServiceManager(db)
        cfg.setdb(db, pm, sm)
        self.node_name_filter = node_name_filter or list()
        parser = ConfigParser(allow_no_value=True)
        parser.read(config_file)

        config = parser['JANUS']
        cfg.PORTAINER_URI = str(config.get('PORTAINER_URI', None))
        cfg.PORTAINER_WS = str(config.get('PORTAINER_WS', None))
        cfg.PORTAINER_USER = str(config.get('PORTAINER_USER', None))
        cfg.PORTAINER_PASSWORD = str(config.get('PORTAINER_PASSWORD', None))
        vssl = str(config.get('PORTAINER_VERIFY_SSL', 'True'))

        if vssl == 'False':
            cfg.PORTAINER_VERIFY_SSL = False
            import urllib3
            urllib3.disable_warnings()
        else:
            cfg.PORTAINER_VERIFY_SSL = True

        sense_properties = SenseUtils.parse_from_config(cfg=cfg, parser=parser)
        self.mngr = SENSEMetaManager(cfg, sense_properties, sense_api_handler=sense_api_handler)

        if cfg.sense_metadata:
            cfg.plugins.append(self.mngr)

    def init(self):
        image_table = self.mngr.image_table

        if not self.mngr.db.all(image_table):
            for img in SUPPORTED_IMAGES:
                self.mngr.save_image({"name": img})

        node_table = self.mngr.nodes_table

        if self.mngr.db.all(node_table):
            log.info(f"Nodes already in db .... returning")
            return

        kube_api = KubernetesApi()
        clusters = kube_api.get_nodes(refresh=True)

        for cluster in clusters:
            if self.node_name_filter:
                filtered_nodes = list()

                for node in cluster['cluster_nodes']:
                    if node['name'] in self.node_name_filter:
                        filtered_nodes.append(node)

                cluster['cluster_nodes'] = filtered_nodes
                cluster['users'] = list()

            cluster['allocated_ports'] = list()
            self.mngr.db.upsert(node_table, cluster, 'name', cluster['name'])

        cluster_names = [cluster['name'] for cluster in clusters]
        log.info(f"saved nodes to db from cluster={cluster_names}")

    def run(self):
        for plugin in self.mngr.cfg.plugins:
            plugin.run()
