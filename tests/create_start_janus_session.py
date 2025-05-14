import os

from tests.sense_test_utils import NoopSENSEApiHandler, create_sense_meta_manager, load_nodes_if_needed, \
    load_images_if_needed, dump_janus_sessions, get_logger, get_db_file_path, get_janus_conf_file_path

log = get_logger()


def get_session():
    alias = 'aes-nautilus-dev-try1'
    instance_id = 'fe3743a4-012b-43a8-8bd7-a9a4c6f5cd3e'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'
    return {
        "key": instance_id,
        "name": alias,
        "task_info": {
            "test-multiple-vlan": [
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": 3138,
                    "bw": 0,
                    "ip": None,
                    "portName": "vlan.3138",
                    "principals": [
                        "aessiari@lbl.gov"
                    ],
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen5-02.sdsc.optiputer.net",
                    "vlan": 3138,
                    "bw": 0,
                    "ip": None,
                    "portName": "vlan.3138",
                    "principals": [
                        "aessiari@lbl.gov"
                    ],
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }

            ]
        },
        "users": ["aessiari@lbl.gov"],
        "clusters": ["nautilus"]
    }


def create_janus_sessions(sense_session, host_networking=False, terminate=False):
    config_file = get_janus_conf_file_path()
    sense_api_handler = NoopSENSEApiHandler()
    database = get_db_file_path()
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

    if terminate:
        smm.terminate_janus_sessions(sense_session)
        print("******************* TERMINATING JANUS SESSIONS ...............")
        janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
        dump_janus_sessions(janus_sessions)


# python reate_janus_session.py > create_janus_session.logs  2>&1
if __name__ == '__main__':
    hn = False
    create_janus_sessions(sense_session=get_session(), host_networking=False, terminate=False)
