import os

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.lib.sense import SENSEMetaManager
from janus.settings import cfg

import logging.config

logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)


class TestSenseWorkflow:
    def __init__(self, database, node_name_filter=None):
        db = DBLayer(path=database)
        cfg.setdb(db, None, None)
        self.mngr = SENSEMetaManager(cfg)
        self.node_name_filter = node_name_filter or list()

    def init(self):
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

        cluster['networks'] = list()
        self.mngr.db.upsert(node_table, cluster, 'name', cluster['name'])
        log.info(f"saved nodes to db from cluster={cluster['name']}")

    def run(self):
        self.mngr.run()


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
  > cat db-test-sense.json | jq .network
  > cat db-test-sense.json | jq .sense_instance
  > kubectl get net-attach-def 
  
  # cancel sense instance and run test again 
  # this should delete the tasks and delete the host profile and the sense instance in db ....
  > python test_sense.py 
  > cat db-test-sense.json | jq .host
  > cat db-test-sense.json | jq .sense_instance
  > kubectl get net-attach-def 
'''
if __name__ == '__main__':
    db_file_name = 'db-test-sense.json'
    endpoints = ['k8s-gen5-01.sdsc.optiputer.net', 'k8s-gen5-02.sdsc.optiputer.net']

    tsw = TestSenseWorkflow(
        database=os.path.join(os.getcwd(), db_file_name),
        node_name_filter=endpoints
    )

    tsw.init()
    tsw.run()
