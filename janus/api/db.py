import logging
import requests
import queue
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

from janus import settings
from janus.settings import cfg
from janus.lib import AgentMonitor
from tinydb import Query

from .portainer_docker import PortainerDockerApi
from .endpoints_api import EndpointsApi


log = logging.getLogger(__name__)
mutex = Lock()

def init_db(client, refresh=False):
    def parse_portainer_endpoints(res):
        db = dict()
        for e in res:
            db[e['Name']] = {
                'name': e['Name'],
                'endpoint_status': e['Status'],
                'id': e['Id'],
                'gid': e['GroupId'],
                'url': e['URL'],
                'public_url': e['PublicURL'],
            }
        return db

    def parse_portainer_networks(res):
        db = dict()
        for e in res:
            key = e['Name']
            db[key] = {
                'id': e['Id'],
                'driver': e['Driver'],
                'subnet': e['IPAM']['Config']
            }
            if e["Options"]:
                mutex.acquire()
                db[key].update(e['Options'])
                mutex.release()
        return db

    def parse_portainer_images(res):
        ret = list()
        Q = Query()
        table = cfg.db.table('images')
        for e in res:
            if e['RepoTags']:
                ret.extend(e['RepoTags'])
                e['name'] = e['RepoTags'][0].split(":")[0]
                mutex.acquire()
                table.upsert(e, Q.name == e['name'])
                mutex.release()
        return ret

    def _get_endpoint_info(Id, url, nname, nodes):
        try:
            nets = dapi.get_networks(Id)
            imgs = dapi.get_images(Id)
        except Exception as e:
            log.error("No response from {}".format(url))
            return nodes[nname]
            return

        nodes[nname]['networks'] = parse_portainer_networks(nets)
        nodes[nname]['images'] = parse_portainer_images(imgs)

        try:
            ret = requests.get("{}://{}:{}/api/janus/agent/node".format(settings.AGENT_PROTO,
                                                                        url,
                                                                        settings.AGENT_PORT),
                               verify=settings.AGENT_SSL_VERIFY,
                               timeout=2)
            nodes[nname]['host'] = ret.json()
        except Exception as e:
            log.error("Could not fetch agent info from {}".format(url))
            am.start_agent(nodes[nname])
        return nodes[nname]

    Node = Query()
    node_table = cfg.db.table('nodes')
    eapi = EndpointsApi(client)
    res = None
    nodes = None
    try:
        res = eapi.endpoint_list()
        # ignore some endpoints based on settings
        for r in res:
            if r['Name'] in settings.IGNORE_EPS:
                res.remove(r)
        nodes = parse_portainer_endpoints(res)
        for k,v in nodes.items():
            mutex.acquire()
            node_table.upsert(v, Node.name == k)
            mutex.release()
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error("Backend error: {}".format(e))
        return

    # Endpoint state updated, unless full refresh we can return
    if not refresh:
        return
    else:
        assert(nodes is not None)

    try:
        dapi = PortainerDockerApi(client)
        am   = AgentMonitor(client)
        futures = list()
        with ThreadPoolExecutor(max_workers=8) as executor:
            for k, v in nodes.items():
                futures.append(executor.submit(_get_endpoint_info, v['id'], v['public_url'], k, nodes))
        for future in concurrent.futures.as_completed(futures):
            item = future.result()
            mutex.acquire()
            node_table.upsert(item, Node.name == item['name'])
            mutex.release()
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error("Backend error: {}".format(e))
        return

    # setup some profile accounting
    # these are the data plane networks we care about
    data_nets = list()
    profs = cfg.get_profiles()
    for p in profs:
        for nname in ["data_net", "mgmt_net"]:
            net = p["settings"][nname]
            if isinstance(net, str):
                if net not in data_nets:
                    data_nets.append(net)
            elif isinstance(net, dict):
                if net['name'] not in data_nets:
                    data_nets.append(net['name'])

    # simple IPAM for data networks
    Net = Query()
    net_table = cfg.db.table('networks')
    for k, v in nodes.items():
        # simple accounting for allocated ports (in node table)
        res = node_table.search((Node.name == k) & (Node.allocated_ports.exists()))
        if not len(res):
            node_table.upsert({'allocated_ports': []}, Node.name == k)

        # simple accounting for allocated vfs (in node table)
        res = node_table.search((Node.name == k) & (Node.allocated_vfs.exists()))
        if not len(res):
            node_table.upsert({'allocated_vfs': []}, Node.name == k)

        # now do networks in separate table
        res = node_table.get(Node.name == k)
        nets = res.get('networks', dict())
        for n, w in nets.items():
            subnet = w['subnet']
            if len(subnet) and n in data_nets:
                # otherwise create default record for net
                key = f"{k}-{n}"
                net = {'name': n,
                       'key': key,
                       'subnet': list(subnet),
                       'allocated_v4': [],
                       'allocated_v6': []}
                net_table.upsert(net, Net.key == key)
