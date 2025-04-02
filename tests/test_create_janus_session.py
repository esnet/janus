import logging.config
import os

from tests.sense_test_utils import NoopSENSEApiHandler, create_sense_meta_manager, load_nodes_if_needed, \
    load_images_if_needed, dump_janus_sessions

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


# nrp_caltech_sense_session = {
#     "name": "aes-pycharm-caltest-nrp",
#     "task_info": {
#         "0-ptask1": [
#             {
#                 "name": "k8s-gen4-02.sdsc.optiputer.net",
#                 "vlan": 3910,
#                 "ip": None,
#                 "cluster_info": {
#                     "cluster_name": "nautilus"
#                 }
#             },
#             {
#                 "name": "sdn-dtn-1-7.ultralight.org",
#                 "vlan": 3911,
#                 "ip": None,
#                 "cluster_info": {
#                     "cluster_name": "kubernetes-admin@kubernetes"
#                 }
#             }
#         ]
#     },
#     "users": [],
# }


def test_create_janus_sessions():
    config_file = os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE)
    sense_api_handler = NoopSENSEApiHandler()
    database = os.path.join(os.getcwd(), DB_FILE_NAME)
    smm = create_sense_meta_manager(database, config_file, sense_api_handler)
    load_images_if_needed(smm.db, smm.image_table)
    load_nodes_if_needed(smm.db, smm.nodes_table, ENDPOINTS)
    alias = 'aes-nrp-ampath'
    instance_id = '3cfd53bb-1788-4a4c-a1a9-1f481156afa3'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

    sense_session = {
        "key": "3cfd53bb-1788-4a4c-a1a9-1f481156afa3",
        "name": alias,
        "task_info": {
            "test-multiple-vlan": [
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": 1779,
                    "ip": "10.251.89.241/28",
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen4-01.ampath.net",
                    "vlan": 1786,
                    "ip": '10.251.89.242/28',
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        },
        "users": ["aessiari@lbl.gov"],
        "clusters": ["nautilus"]
    }

    smm.create_profiles(sense_session)
    smm.save_sense_session(sense_session=sense_session)
    janus_session_ids = smm.create_janus_session(sense_session)
    sense_session['janus_session_id'] = janus_session_ids
    smm.save_sense_session(sense_session=sense_session)

    sense_sessions = smm.find_sense_session(sense_session_key=sense_session['key'])
    assert len(sense_sessions) == 1
    sense_session = sense_sessions[0]

    print("******************* STARTING JANUS SESSIONS ...............")
    smm.session_manager.start_session(session_id=sense_session['janus_session_id'][0])
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)

    smm.terminate_janus_sessions(sense_session)
    print("******************* TERMINATING JANUS SESSIONS ...............")
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)


def test_create_janus_sessions_single_vlan():
    config_file = os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE)
    sense_api_handler = NoopSENSEApiHandler()
    database = os.path.join(os.getcwd(), DB_FILE_NAME)
    smm = create_sense_meta_manager(database, config_file, sense_api_handler)
    load_images_if_needed(smm.db, smm.image_table)
    load_nodes_if_needed(smm.db, smm.nodes_table, ENDPOINTS)
    alias = 'sunami-multipoint-4'
    instance_id = 'a59e040c-ddce-4e30-87c6-755279ca6f02'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

    sense_session = {
        "key": instance_id,
        "name": alias,
        "task_info": {
            "test-single-vlan": [
                {
                    "name": "k8s-gen5-02.sdsc.optiputer.net",
                    "vlan": 3605,
                    "ip": "fc00:0:200:800:0:0:0:2/64",
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": 3605,
                    "ip": 'fc00:0:200:800:0:0:0:1/64',
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        },
        "users": ["aessiari@lbl.gov"],
        "clusters": ["nautilus"]
    }

    smm.create_profiles(sense_session)
    smm.save_sense_session(sense_session=sense_session)
    janus_session_ids = smm.create_janus_session(sense_session)
    sense_session['janus_session_id'] = janus_session_ids
    smm.save_sense_session(sense_session=sense_session)

    sense_sessions = smm.find_sense_session(sense_session_key=sense_session['key'])
    assert len(sense_sessions) == 1
    sense_session = sense_sessions[0]
    print("******************* STARTING JANUS SESSIONS ...............")
    smm.session_manager.start_session(session_id=sense_session['janus_session_id'][0])
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)

    print("******************* TERMINATING JANUS SESSIONS ...............")
    smm.terminate_janus_sessions(sense_session)
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)


# python test_create_janus_session.py > test_create_janus_session.logs  2>&1

if __name__ == '__main__':
    test_create_janus_sessions()
    test_create_janus_sessions_single_vlan()
