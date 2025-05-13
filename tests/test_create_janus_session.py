import os

from tests.sense_test_utils import get_logger, DB_FILE_NAME, JANUS_CONF_TEST_FILE

from tests.sense_test_utils import NoopSENSEApiHandler, create_sense_meta_manager, load_nodes_if_needed, \
    load_images_if_needed, dump_janus_sessions

log = get_logger()


def get_nrp_single_vlan():
    alias = 'sunami-multipoint-4'
    instance_id = 'a59e040c-ddce-4e30-87c6-755279ca6f02'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'
    return {
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


def get_nrp_single_target():
    alias = 'NRP-Gen5-1-2-6'
    instance_id = '7e5c3303-affe-4cbf-bcee-15197e431405'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

    return {
        "key": instance_id,
        "name": alias,
        "task_info": {
            "test-single-vlan-target": [
                {
                    "name": "k8s-gen5-02.sdsc.optiputer.net",
                    "vlan": 1783,
                    "ip": "10.251.87.129/28",
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        },
        "users": ["aessiari@lbl.gov"],
        "clusters": ["nautilus"]
    }


def get_nrp_multiple_vlan():
    alias = 'aes-nrp-ampath-sat'
    instance_id = 'a88cced6-aedb-498a-a1da-085bf75bab71'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'
    return {
        "key": instance_id,
        "name": alias,
        "task_info": {
            "test-multiple-vlan": [
                {
                    "name": "k8s-gen4-01.ampath.net",
                    "vlan": 1781,
                    "ip": "10.251.88.2/28",
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": 3603,
                    "ip": '10.251.88.1/28',
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        },
        "users": ["aessiari@lbl.gov"],
        "clusters": ["nautilus"]
    }


def test_create_janus_sessions(sense_session, host_networking=False):
    config_file = os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE)
    sense_api_handler = NoopSENSEApiHandler()
    database = os.path.join(os.getcwd(), DB_FILE_NAME)
    smm = create_sense_meta_manager(database, config_file, sense_api_handler)
    load_images_if_needed(smm.db, smm.image_table)
    load_nodes_if_needed(smm.db, smm.nodes_table, None)

    smm.create_profiles(sense_session, host_networking=host_networking)
    smm.save_sense_session(sense_session=sense_session)
    janus_session_ids = smm.create_janus_session(sense_session, host_networking=host_networking)
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


# python test_create_janus_session.py > test_create_janus_session.logs  2>&1
if __name__ == '__main__':
    ss = get_nrp_multiple_vlan()
    hn = False
    test_create_janus_sessions(sense_session=ss, host_networking=hn)
