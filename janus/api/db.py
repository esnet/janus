import logging
import requests
import queue
import concurrent
import os
import yaml
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

from functools import reduce
from operator import eq
from tinydb import TinyDB, Query, where

from janus import settings
from janus.settings import cfg
from janus.lib import AgentMonitor
from .utils import Constants

from .portainer_docker import PortainerDockerApi
from .endpoints_api import EndpointsApi


log = logging.getLogger(__name__)
mutex = Lock()

class QueryUser:
    def query_builder(self, user=None, group=None, qargs=dict()):
        qs = list()
        user = user.split(',') if user else None
        group = group.split(',') if group else None
        if user and group:
            qs.append(where('users').any(user) | where('groups').any(group))
        elif user:
            qs.append(where('users').any(user))
        elif group:
            qs.append(where('groups').any(group))
        for k,v in qargs.items():
            if v:
                qs.append(eq(where(k), v))
        if len(qs):
            return reduce(lambda a, b: a & b, qs)
        return None

# This layer targets a single backend right now, TinyDB
class DBLayer():

    def __init__(self, db: str = None, **kwargs):
        self._client = TinyDB(kwargs.get("path"))

    def mutex_lock(operation):
        def lock_unlock(*args, **kwargs):
            mutex.acquire()
            ret = operation(*args, **kwargs)
            mutex.release()
            return ret
        return lock_unlock

    @mutex_lock
    def get_table(self, tbl):
        return self._client.table(tbl)

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

    @mutex_lock
    def insert(self, tbl, dict):
        ret = tbl.insert(dict)
        return ret

    @mutex_lock
    def get(self, tbl, **kwargs):
        Q = Query()
        if "name" in kwargs:
            ret = tbl.get(Q.name == kwargs.get("name"))
        elif "key" in kwargs:
            ret = tbl.get(Q.key == kwargs.get("key"))
        elif "ids" in kwargs:
            ret = tbl.get(doc_id=kwargs.get("ids"))
        else:
            ret = tbl.get(kwargs.get("query"))
        return ret

    @mutex_lock
    def search(self, tbl, **kwargs):
        if "name" in kwargs:
            Q = Query()
            ret = tbl.search(Q.name == kwargs.get("name"))
        else:
            ret = tbl.search(kwargs.get("query"))
        return ret

    @mutex_lock
    def all(self, tbl):
        ret = tbl.all()
        return ret


def init_db(client, nname=None, refresh=False):
    def parse_portainer_endpoints(res):
        ret = dict()
        for e in res:
            ret[e['Name']] = {
                'name': e['Name'],
                'endpoint_status': e['Status'],
                'id': e['Id'],
                'gid': e['GroupId'],
                'url': e['URL'],
                'public_url': e['PublicURL']
            }
        return ret

    def parse_portainer_networks(res):
        ret = dict()
        for e in res:
            key = e['Name']
            ret[key] = {
                'id': e['Id'],
                'driver': e['Driver'],
                'subnet': e['IPAM']['Config'],
                '_data': e
            }
            if e["Options"]:
                ret[key].update(e['Options'])
        return ret

    def parse_portainer_images(res):
        ret = list()
        Q = Query()
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
            dbase.upsert(table, e, 'name', e['name'])
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

    if not client:
        log.warning("init_db called with no active client, returning")
        return
    Node = Query()
    dbase = cfg.db
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
            dbase.upsert(node_table, v, 'name', k)
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
            if nname and k != nname:
                continue
            futures.append(tp.submit(_get_endpoint_info, v['id'], v['public_url'], k, nodes))
        for future in futures:
            try:
                item = future.result(timeout=5)
            except Exception as e:
                log.error(f"Timeout waiting on endpoint query, continuing")
                continue
            dbase.upsert(node_table, item, 'name', item['name'])
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error("Backend error: {}".format(e))
        return

    # setup some profile accounting
    # these are the data plane networks we care about
    data_nets = list()
    profs = cfg.pm.get_profiles(Constants.HOST)
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
