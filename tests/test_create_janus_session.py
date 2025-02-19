import logging.config
import os
import json

from janus.api.db import DBLayer
from janus.api.kubernetes import KubernetesApi
from janus.api.manager import ServiceManager
from janus.api.profile import ProfileManager
from janus.api.session_manager import SessionManager
from janus.lib.sense import DBHandler
from janus.settings import cfg, SUPPORTED_IMAGES

logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)

DB_FILE_NAME = 'db-test-sense.json'
JANUS_CONF_TEST_FILE = 'janus-sense-test.conf'
DEBUG = True

FILTER_NODES = False

if FILTER_NODES:
    ENDPOINTS = ['k8s-gen5-01.sdsc.optiputer.net',
                 'k8s-gen5-02.sdsc.optiputer.net',
                 'losa4-nrp-01.cenic.net',
                 'k8s-3090-01.clemson.edu',
                 'node-2-8.sdsc.optiputer.net']
else:
    ENDPOINTS = None


def create_db_handler(database) -> DBHandler:
    db = DBLayer(path=database)

    pm = ProfileManager(db, None)
    sm = ServiceManager(db)
    cfg.setdb(db, pm, sm)
    return DBHandler(cfg)


def _instances(targets):
    instances = list()

    for t in targets:
        if 'cluster_info' in t:
            instances.append(dict(name=t['cluster_info']['cluster_name'], nodeName=t['name']))
        else:
            instances.append(dict(name=t['name']))

    return instances


def create_janus_session(sense_session):
    session_manager = SessionManager()
    targets = sum(sense_session['task_info'].values(), [])
    targets = sorted(targets, key=lambda t: t['name'])
    host_profiles = sense_session['host_profile']
    owner = sense_session["users"][0] if sense_session['users'] else 'admin'

    if len(host_profiles) == 1:
        requests = [dict(
            instances=_instances(targets),
            profile=host_profiles[0],
            errors=[],
            image='dtnaas/tools:latest',
            arguments=str(),
            kwargs=dict(USER_NAME=str(), PUBLIC_KEY=str()),
            remove_container=False
        )]

    else:
        requests = list()

        for idx, host_profile in enumerate(host_profiles):
            request = dict(
                instances=_instances([targets[idx]]),
                profile=host_profile,
                errors=[],
                image='dtnaas/tools:latest',
                arguments=str(),
                kwargs=dict(USER_NAME=str(), PUBLIC_KEY=str()),
                remove_container=False
            )
            requests.append(request)

    log.info(f'creating janus sample session using sense_session {sense_session["name"]}:{requests}')
    session_manager.validate_request(requests)

    if DEBUG:
        print(json.dumps(requests, indent=2))

    session_requests = session_manager.parse_requests(None, None, requests)

    if DEBUG:
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

    session_requests = session_manager.parse_requests(None, None, requests)
    session_manager.create_networks(session_requests)
    janus_session_id = session_manager.create_session(
        None, None, session_requests, requests, owner, sense_session["users"]
    )

    return [janus_session_id]


def test_create_janus_sessions():
    database = os.path.join(os.getcwd(), DB_FILE_NAME)
    db_handler = create_db_handler(database)

    image_table = db_handler.image_table

    if not db_handler.db.all(image_table):
        for img in SUPPORTED_IMAGES:
            db_handler.save_image({"name": img})

    node_table = db_handler.nodes_table

    if db_handler.db.all(node_table):
        log.info(f"Nodes already in db .... returning")
    else:
        kube_api = KubernetesApi()
        clusters = kube_api.get_nodes(refresh=True)
        node_name_filter = list()

        for cluster in clusters:
            if node_name_filter:
                filtered_nodes = list()

                for node in cluster['cluster_nodes']:
                    if node['name'] in node_name_filter:
                        filtered_nodes.append(node)

                cluster['cluster_nodes'] = filtered_nodes
                cluster['users'] = list()

            cluster['allocated_ports'] = list()
            db_handler.db.upsert(node_table, cluster, 'name', cluster['name'])

        cluster_names = [cluster['name'] for cluster in clusters]
        log.info(f"saved nodes to db from cluster={cluster_names}")

    # noinspection PyUnusedLocal
    nrp_caltech_sense_session = {
        "name": "aes-pycharm-caltest-nrp",
        "task_info": {
          "0-ptask1": [
            {
              "name": "k8s-gen4-02.sdsc.optiputer.net",
              "vlan": 3910,
              "ip": None,
              "cluster_info": {
                "cluster_name": "nautilus"
              }
            },
            {
              "name": "sdn-dtn-1-7.ultralight.org",
              "vlan": 3911,
              "ip": None,
              "cluster_info": {
                "cluster_name": "kubernetes-admin@kubernetes"
              }
            }
          ]
        },
        "users": [],
    }

    alias = 'aes-ooo-nrp-ampath-try3'
    instance_id = '07b2f3ae-02f6-489e-b910-4055479103b9'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

    sense_session = {
        "name": alias,
        "task_info": {
            "0-ptask2": [
                {
                    "name": "k8s-gen4-01.ampath.net",
                    "vlan": 1798,
                    "ip": "10.251.89.226/28",
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": 3134,
                    "ip": '10.251.89.225/28',
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        },
        "users": [],
    }

    db_handler.create_profiles(sense_session)
    janus_session_ids = create_janus_session(sense_session)
    print("Done ...:", janus_session_ids)


if __name__ == '__main__':
    test_create_janus_sessions()
