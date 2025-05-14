from tests.sense_test_utils import get_logger, get_db_file_path, get_janus_conf_file_path
from tests.sense_test_utils import NoopSENSEApiHandler, create_sense_meta_manager, load_nodes_if_needed, \
    load_images_if_needed, dump_janus_sessions

log = get_logger()


def get_caltech_session():
    alias = 'host-networking-testing'
    instance_id = 'fakeeed6-aedb-498a-a1da-085bf75bab71'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'
    return {
        "name": alias,
        "key": instance_id,
        "task_info": {
            "test-caltech-host-networking": [
                {
                    "name": "sandie-7.ultralight.org",
                    "vlan": None,
                    "ip": None,
                    "cluster_info": {
                        "cluster_name": "kubernetes-admin@kubernetes"
                    }
                },
                {
                    "name": "sdn-dtn-1-7.ultralight.org",
                    "vlan": None,
                    "ip": None,
                    "cluster_info": {
                        "cluster_name": "kubernetes-admin@kubernetes"
                    }
                }
            ]
        },
        "users": [],
    }


def get_nrp_multiple_vlan():
    alias = 'host-networking-testing'
    instance_id = 'a88cced6-aedb-498a-a1da-085bf75bab71'
    alias = f'sense-janus-{alias.replace(" ", "-")}-{"-".join(instance_id.split("-")[0:2])}'

    return {
        "key": instance_id,
        "name": alias,
        "task_info": {
            "test-multiple-vlan": [
                {
                    "name": "k8s-gen4-01.ampath.net",
                    "vlan": None,
                    "ip": None,
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                },
                {
                    "name": "k8s-gen5-01.sdsc.optiputer.net",
                    "vlan": None,
                    "ip": None,
                    "cluster_info": {
                        "cluster_name": "nautilus"
                    }
                }
            ]
        }
    }

########
# export KUBECONFIG=/home/janus/.kube/config:/home/janus/.kube/config-caltech
# kubectl get pods --kubeconfig ~/.kube/config-caltech  -n sense | grep janus
########


def run_host_networking_task_workflow(sense_session):
    config_file = get_janus_conf_file_path()
    sense_api_handler = NoopSENSEApiHandler()
    database = get_db_file_path()
    smm = create_sense_meta_manager(database, config_file, sense_api_handler)
    load_images_if_needed(smm.db, smm.image_table)
    load_nodes_if_needed(smm.db, smm.nodes_table, None)
    smm.create_profiles(sense_session, host_networking=True)
    smm.save_sense_session(sense_session=sense_session)
    janus_session_ids = smm.create_janus_session(sense_session, host_networking=True)
    sense_session['janus_session_id'] = janus_session_ids
    smm.save_sense_session(sense_session=sense_session)
    sense_sessions = smm.find_sense_session(sense_session_key=sense_session['key'])
    assert len(sense_sessions) == 1
    sense_session = sense_sessions[0]

    print("******************* STARTING JANUS SESSIONS ...............")
    smm.session_manager.start_session(session_id=sense_session['janus_session_id'][0])
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)

    from janus.lib.sense_utils import SenseUtils

    service_info = SenseUtils.get_service_info(janus_sessions[0])

    import json

    print(f"SrcServiceInfo:", json.dumps(service_info[0], indent=2))

    import time

    time.sleep(10)

    idx = 1 if len(service_info) > 1 else 0

    # ['stdbuf', '-o0', '-e0', 'iperf3', '-c', '192.168.1.3', '-i', '2'], 'attach': True, 'tty': True, 'start': False}
    req = {'node': service_info[0]['node'],
           'container': service_info[0]['sname'],
           'Cmd': ['stdbuf', '-o0', '-e0', 'ping', '-c', '3', service_info[idx]['ctrl_host']],
           'attach': True,
           'tty': True,
           'start': False}

    print(f"ExecRequest:", json.dumps(req, indent=2))

    ret = smm.session_manager.exec(req)
    print(ret)

    from janus.api.models import Node
    from janus.api.db import QueryUser

    query = QueryUser().query_builder(None, None, {"name": service_info[0]['node']})
    dbase = smm.cfg.db
    ntable = dbase.get_table('nodes')
    node = dbase.get(ntable, query=query)
    handler = smm.cfg.sm.get_handler(node=node)
    res = handler.exec_stream(node=Node(**node), container=service_info[0]['sname'], eid=None)

    print("BEGIN_PING_RESULT:")
    import sys

    while True:
        r = res.get()

        if r.get("eof"):
            break
        sys.stdout.write(r['msg'])

    print("END_PING_RESULT:")

    smm.terminate_janus_sessions(sense_session)
    print("******************* TERMINATING JANUS SESSIONS ...............")
    janus_sessions = smm.find_janus_session(host_profile_names=sense_session['host_profile'])
    dump_janus_sessions(janus_sessions)


def test_caltech_host_networking():
    run_host_networking_task_workflow(get_caltech_session())


if __name__ == '__main__':
    run_host_networking_task_workflow(get_caltech_session())
