import logging
import requests
import queue
import concurrent
import os
import yaml
from threading import Lock

from functools import reduce
from operator import eq
from tinydb import TinyDB, Query, where

from janus import settings
from janus.settings import cfg
from janus.api.models import Network
from .utils import Constants, keys_lower


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


def init_db(nname=None, refresh=False):
    Node = Query()
    dbase = cfg.db
    node_table = dbase.get_table('nodes')
    res = None
    nodes = None
    log.debug(f"Initializing database, refresh={refresh}")
    try:
        nodes = cfg.sm.get_nodes(nname, refresh)
        for n in nodes:
            dbase.upsert(node_table, n, 'name', n['name'])
    except Exception as e:
        log.error("Backend error: {}".format(e))
        return

    # Endpoint state updated, unless full refresh we can return
    if not refresh:
        return
    else:
        assert(nodes is not None)

    # setup some profile accounting
    # these are the data plane networks we care about
    data_nets = list()
    profs = cfg.pm.get_profiles(Constants.HOST)
    for p in profs:
        for net in [Network(p.settings.mgmt_net), Network(p.settings.data_net)]:
            if net.name not in data_nets:
                data_nets.append(net.name)

    # simple IPAM for data networks
    net_table = dbase.get_table('networks')
    for node in nodes:
        k = node.get('name')
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
        if not nets:
            continue
        for n, w in nets.items():
            subnet = w.get('subnet', [])
            # try to get subnet information from profile if not tracked in endpoint
            if not subnet:
                pnet = cfg.pm.get_profile(Constants.NET, n)
                try:
                    subnet = pnet.settings.ipam.get('config') if pnet else []
                except Exception as e:
                    pass
            subnet = [keys_lower(x) for x in subnet]
            key = f"{k}-{n}"
            curr = dbase.get(net_table, key=key)
            if not curr:
                net = {'name': n,
                       'key': key,
                       'subnet': list(subnet),
                       'allocated_v4': [],
                       'allocated_v6': []}
            else:
                net = curr
                net['subnet'] = list(subnet)
            dbase.upsert(net_table, net, 'key', key)
