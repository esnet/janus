from configparser import ConfigParser

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.lib.sense import SENSEMetaManager
from janus.lib.sense_utils import SenseUtils
from janus.settings import cfg, SUPPORTED_IMAGES
from tests.sense_test_utils import get_logger

log = get_logger()


class SenseRunnable:
    def __init__(self, database, config_file, sense_api_handler=None, node_name_filter=None):
        db = DBLayer(path=database)
        pm = ProfileManager(db, None)
        sm = ServiceManager(db)
        cfg.setdb(db, pm, sm)
        self.node_name_filter = node_name_filter or list()
        parser = ConfigParser(allow_no_value=True)
        parser.read(config_file)
        sense_properties = SenseUtils.parse_from_config(cfg=cfg, parser=parser)
        assert sense_properties, f"no sense properties ..... (Missing config file {config_file}?)"
        self.mngr = SENSEMetaManager(cfg, sense_properties, sense_api_handler=sense_api_handler)
        assert cfg.sense_metadata

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
        self.mngr.run()
