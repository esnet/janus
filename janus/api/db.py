import logging
import requests
import queue
import concurrent
import os
import yaml
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

from janus import settings
from janus.settings import cfg
from janus.lib import AgentMonitor
from tinydb import Query
from janus.api.query import QueryUser

from .portainer_docker import PortainerDockerApi
from .endpoints_api import EndpointsApi


log = logging.getLogger(__name__)
mutex = Lock()

SUPPORTED_IMAGES = ['dtnaas/tools',
                    'dtnaas/ofed']

class DBLayer():

    def __init__(self, client):
        self._client = client

        self._profile_path = None
        self._volumes = dict()
        self._qos = dict()
        self._profiles = dict()
        self._post_starts = dict()

        self._base_profile = {
            "privileged": False,
            "systemd": False,
            "pull_image": False,
            "auto_tune": False,
            "cpu": 4,
            "mem": 8589934592,
            "affinity": "network",
            "cpu_set": None,
            "mgmt_net": "bridge",
            "data_net": None,
            "internal_port": None,
            "ctrl_port_range": [30000, 30100],
            "data_port_range": None,
            "serv_port_range": None,
            "features": list(),
            "post_starts": list(),
            "volumes": list(),
            "environment": list(),
            "qos": None,
            "tools": {
                "dtnaas/tools": ["iperf3", "iperf3_server", "escp", "xfer_test"],
                "dtnaas/ofed": ["iperf3", "iperf3_server", "ib_write_bw", "xfer_test"]
            }
        }

        self._features = {
            'rdma': {
                'devices': [
                    {
                        'devprefix': '/dev/infiniband',
                        'names': ['rdma_cm', 'uverbs']
                    }
                ],
                'caps': ['IPC_LOCK'],
                'limits': [{"Name": "memlock", "Soft": -1, "Hard": -1}]
            }
        }

    def mutex_lock(operation):
        def lock_unlock(*args, **kwargs):
            mutex.acquire()
            operation(*args, **kwargs)
            mutex.release()
        return lock_unlock

    def get_table(self, tbl):
        mutex.acquire()
        dbtbl = self._client.db.table(tbl)
        mutex.release()
        return dbtbl

    @mutex_lock
    def remove(self, tbl, **kwargs):
        if "name" in kwargs:
            Q = Query()
            tbl.remove(Q.name == kwargs.get("name"))
        elif "ids" in kwargs:
            tbl.remove(doc_ids=[kwargs.get("ids")])

    @mutex_lock
    def update(self, tbl, default, **kwargs):
        Q = Query()
        if "name" in kwargs:
            tbl.update(default, Q.name == kwargs.get("name"))
        elif "key" in kwargs:
            tbl.update(default, Q.key == kwargs.get("key"))
        elif "ids" in kwargs:
            tbl.update(default, doc_ids=[kwargs.get("ids")])
        else:
            tbl.update(default, kwargs.get("query"))


    @mutex_lock
    def upsert(self, tbl, docs, field, f_value):
        Q = Query()
        if field == 'name':
            tbl.upsert(docs, Q.name == f_value)
        else:
            tbl.upsert(docs, Q.key == f_value)


    def insert(self, tbl, dict):
        mutex.acquire()
        ret = tbl.insert(dict)
        mutex.release()
        return ret


    def get(self, tbl, **kwargs):
        mutex.acquire()
        Q = Query()
        if "name" in kwargs:
            ret = tbl.get(Q.name == kwargs.get("name"))
        elif "key" in kwargs:
            ret = tbl.get(Q.key == kwargs.get("key"))
        elif "ids" in kwargs:
            ret = tbl.get(doc_id=kwargs.get("ids"))
        else:
            ret = tbl.get(kwargs.get("query"))
        mutex.release()
        return ret

    def search(self, tbl, **kwargs):
        mutex.acquire()
        if "name" in kwargs:
            Q = Query()
            ret = tbl.search(Q.name == kwargs.get("name"))
        else:
            ret = tbl.search(kwargs.get("query"))
        mutex.release()
        return ret

    def all(self, tbl):
        mutex.acquire()
        ret = tbl.all()
        mutex.release()
        return ret
    def get_profile_from_db(self, p=None, user=None, group=None):
        profile_tbl = self.get_table('profiles')
        q = QueryUser()
        query = q.query_builder(user, group, {"name": p})
        if query and p:
            ret = self.get(profile_tbl, query=query)
        elif query:
            ret = self.search(profile_tbl, query=query)
        else:
            ret = self.all(profile_tbl)
        return ret

    def get_profile(self, p, user=None, group=None, inline=False):
        return self.get_profile_from_db(p, user, group)

    def get_profiles(self, user=None, group=None, inline=False):
        ret = dict()
        profiles = self.get_profile_from_db(user=user, group=group)
        nprofs = len(profiles) if profiles else 0
        log.info(f"total profiles: {nprofs}")
        return profiles

    def read_profiles(self, path=None, reset=False):
        Q = Query()
        dbase = DBLayer(cfg)
        image_tbl = self.get_table('images')
        for img in SUPPORTED_IMAGES:
            ni = {"name": img}
            dbase.upsert(image_tbl, ni, 'name', img)
        profile_tbl = self.get_table('profiles')
        if reset:
            profile_tbl.truncate()
        if not path:
            path = self._profile_path
        if not path:
            raise Exception("Profile path is not set")
        for f in os.listdir(path):
            entry = os.path.join(path, f)
            if os.path.isfile(entry) and (f.endswith(".yml") or f.endswith(".yaml")):
                with open(entry, "r") as yfile:
                    try:
                        data = yaml.safe_load(yfile)
                        # log.info("read profile directory: {}".format(data))
                        for k, v in data.items():
                            if isinstance(v, dict):
                                if (k == "volumes"):
                                    self._volumes.update(v)

                                if (k == "qos"):
                                    for key, value in v.items():
                                        try:
                                            # temp = self._qos.copy()
                                            QoS_Controller(**value)
                                            self._qos[key] = value
                                        except Exception as e:
                                            log.error("Error reading qos: {}".format(e))
                                    # self._qos.update(v)

                                if (k == "profiles"):
                                    for key, value in v.items():
                                        try:
                                            temp = self._base_profile.copy()
                                            temp.update(value)
                                            Profile(**temp)
                                            self._profiles[key] = temp
                                            dbase.upsert(profile_tbl, {"name": key, "settings": temp}, 'name', key)
                                            # log.info(cfg.get_profile(key))
                                        except Exception as e:
                                            log.error("Error reading profiles: {}".format(e))
                                    # self._profiles.update(v)

                                if (k == "features"):
                                    self._features.update(v)

                                if (k == "post_starts"):
                                    self._post_starts.update(v)

                    except Exception as e:
                        raise Exception(f"Could not load configuration file: {entry}: {e}")
                    yfile.close()

        log.info("qos: {}".format(self._qos.keys()))
        log.info("volumes: {}".format(self._volumes.keys()))
        log.info("volumes: {}".format(self._volumes.keys()))
        log.info("features: {}".format(self._features.keys()))
        log.info("profiles: {}".format(self._profiles.keys()))


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
        dbase = DBLayer(cfg)
        table = dbase.get_table('images')
        for e in res:
            if not e['RepoDigests'] and not e['RepoTags']:
                continue
            if e['RepoTags']:
                e['name'] = e['RepoTags'][0].split(":")[0]
                ret.extend(e['RepoTags'])
            elif e['RepoDigests']:
                e['name'] = e['RepoDigests'][0].split("@")[0]
            if e['name'] == '<none>':
                continue
            mutex.acquire()
            dbase.upsert(table, e, 'name', e['name'])
            mutex.release()
        return ret

    def _get_endpoint_info(Id, url, nname, nodes):
        try:
            nets = dapi.get_networks(Id)
            imgs = dapi.get_images(Id)
        except Exception as e:
            log.error("No response from {}".format(url))
            return nodes[nname]

        nodes[nname]['networks'] = parse_portainer_networks(nets)
        nodes[nname]['images'] = parse_portainer_images(imgs)

        try:
            (n, ret) = am.check_agent(nname, url)
            nodes[nname]['host'] = ret.json()
            (n, ret) = am.tune(nname, url)
            nodes[nname]['host']['tuning'] = ret.json()
        except Exception as e:
            log.error("Could not fetch agent info from {}: {}".format(url, e))
            am.start_agent(nodes[nname])
        return nodes[nname]

    Node = Query()
    dbase = DBLayer(cfg)
    node_table = dbase.get_table('nodes')
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
            dbase.upsert(node_table, v, 'name', k)
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
        tp = ThreadPoolExecutor(max_workers=8)
        for k, v in nodes.items():
            futures.append(tp.submit(_get_endpoint_info, v['id'], v['public_url'], k, nodes))
        for future in futures:
            try:
                item = future.result(timeout=5)
            except Exception as e:
                log.error(f"Timeout waiting on endpoint query, continuing")
                continue
            mutex.acquire()
            dbase.upsert(node_table, item, 'name', item['name'])
            mutex.release()
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error("Backend error: {}".format(e))
        return

    # setup some profile accounting
    # these are the data plane networks we care about
    data_nets = list()
    profs = dbase.get_profiles()
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
    net_table = dbase.get_table('networks')
    for k, v in nodes.items():
        # simple accounting for allocated ports (in node table)
        res = dbase.search(node_table, query=((Node.name == k) & (Node.allocated_ports.exists())))
        if not len(res):
            dbase.upsert(node_table, {'allocated_ports': []}, 'name', k)

        # simple accounting for allocated vfs (in node table)
        res = dbase.search(node_table, query=((Node.name == k) & (Node.allocated_vfs.exists())))
        if not len(res):
            dbase.upsert(node_table, {'allocated_vfs': []}, 'name', k)

        # now do networks in separate table
        res = dbase.get(node_table, name=k)
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
                dbase.upsert(net_table, net, 'key', key)
