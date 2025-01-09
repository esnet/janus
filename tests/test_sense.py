import logging.config
import os
from configparser import ConfigParser

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.lib.sense import SENSEMetaManager, parse_from_config
from janus.settings import cfg, SUPPORTED_IMAGES

logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)


class TestSenseWorkflow:
    def __init__(self, database, config_file, node_name_filter=None):
        db = DBLayer(path=database)

        cfg.setdb(db, None, None)
        self.node_name_filter = node_name_filter or list()

        parser = ConfigParser(allow_no_value=True)
        parser.read(config_file)
        sense_properties = parse_from_config(cfg=cfg, parser=parser)
        self.mngr = SENSEMetaManager(cfg, sense_properties)

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
        assert len(clusters) == 1
        cluster = clusters[0]

        if self.node_name_filter:
            filtered_nodes = list()

            for node in cluster['cluster_nodes']:
                if node['name'] in self.node_name_filter:
                    filtered_nodes.append(node)

            cluster['cluster_nodes'] = filtered_nodes
            cluster['users'] = []

        cluster['networks'] = list()
        self.mngr.db.upsert(node_table, cluster, 'name', cluster['name'])
        log.info(f"saved nodes to db from cluster={cluster['name']}")

    def run(self):
        for plugin in self.mngr.cfg.plugins:
            plugin.run()


'''
  # CREATE DB with nodes
  > rm db-test-sense.json
  > python test_sense.py 
  > cat db-test-sense.json | jq .nodes
  
  # create sense instance, create tasks, and run test again
  # this should handle the tasks and create a host profile ...
  > cd tests
  > python test_sense.py 
  > cat db-test-sense.json | jq .host
  > cat db-test-sense.json | jq .sense_instance
  
  # For now cancel sense instance and run test again 
  # this should delete the tasks and delete the host profile and the sense instance in db ....
  > python test_sense.py 
  > cat db-test-sense.json | jq .host
  > cat db-test-sense.json | jq .sense_instance
'''
if __name__ == '__main__':
    db_file_name = 'db-test-sense.json'
    janus_conf_path = 'janus-sense-test.conf'
    endpoints = ['k8s-gen5-01.sdsc.optiputer.net',
                 'k8s-gen5-02.sdsc.optiputer.net',
                 'losa4-nrp-01.cenic.net',
                 'k8s-3090-01.clemson.edu']

    tsw = TestSenseWorkflow(
        database=os.path.join(os.getcwd(), db_file_name),
        config_file=os.path.join(os.getcwd(), janus_conf_path),
        node_name_filter=endpoints
    )

    tsw.init()
    tsw.run()
