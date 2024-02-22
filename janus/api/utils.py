import os
import json
import logging
from ipaddress import IPv4Network, IPv4Address
from ipaddress import IPv6Network, IPv6Address

from janus import settings
from janus.api.constants import Constants
from janus.api.models import Network
from janus.settings import cfg
import requests
import shlex

log = logging.getLogger(__name__)

def keys_lower(in_dict):
    return {k.lower(): keys_lower(v) if isinstance(v, dict) else v for k, v in in_dict.items()}

def cname_from_id(sid, idx=1):
    return f"janus-{sid}-{idx}"

def is_subset(subset, superset):
    if isinstance(subset, dict):
        return all(key in superset and is_subset(val, superset[key]) for key, val in subset.items())
    if isinstance(subset, list) or isinstance(subset, set):
        return all(any(is_subset(subitem, superitem) for superitem in superset) for subitem in subset)
    return subset == superset

def precommit_db(Id=None, delete=False):
    dbase = cfg.db
    table = dbase.get_table('active')
    if Id and delete:
        dbase.remove(table, ids=Id)
    else:
        Id = dbase.insert(table, dict())
    return Id

def commit_db_realized(record, node_table, net_table, delete=False):
    dbase = cfg.db
    services = record.get("services", dict())
    for k,v in services.items():
        for s in v:

            node = dbase.get(node_table, name=k)
            if delete:
                try:
                    if s.get('ctrl_port'):
                        node['allocated_ports'].remove(int(s['ctrl_port']))
                    if s.get('data_vfid'):
                        node['allocated_vfs'].remove(s['data_vfid'])
                except Exception as e:
                    pass
            else:
                if s.get('ctrl_port'):
                    node['allocated_ports'].append(int(s['ctrl_port']))
                if s.get('data_vfid'):
                    node['allocated_vfs'].append(s['data_vfid'])
            dbase.update(node_table, node, name=k)

            if s.get('data_net'):
                nobj = Network(s['data_net_name'], k)
                net = dbase.get(net_table, key=nobj.key)
                if delete:
                    try:
                        net['allocated_v4'].remove(s['data_ipv4'])
                    except Exception as e:
                        pass
                    try:
                        net['allocated_v6'].remove(s['data_ipv6'])
                    except Exception as e:
                        pass
                else:
                    if s.get('data_ipv4'):
                        net['allocated_v4'].append(s['data_ipv4'])
                    if s.get('data_ipv6'):
                        net['allocated_v6'].append(s['data_ipv6'])
                dbase.update(net_table, net, key=nobj.key)

def commit_db(record, rid=None, delete=False, realized=False):
    dbase = cfg.db
    node_table = dbase.get_table('nodes')
    net_table = dbase.get_table('networks')
    table = dbase.get_table('active')

    if realized:
        commit_db_realized(record, node_table, net_table, delete)
        dbase.update(table, record, ids=rid)
        return {rid: record}

    if delete:
        dbase.remove(table, ids=rid)
        return {rid: record}
    elif rid:
        dbase.update(table, record, ids=rid)
        return {rid: record}
    else:
        Id = dbase.insert(table, record)
        return {Id: record}

def get_next_vf(node, dnet):
    try:
        docknet = node["networks"][dnet]
        sriov = node["host"]["sriov"]
        nsr = sriov.get(docknet["netdevice"], None)
        avail = set([ (vf["id"], vf['mac']) for vf in nsr["vfs"] ])
        alloced = set(node["allocated_vfs"])
        avail = avail - alloced
    except:
        raise Exception("Could not determine SRIOV VF for data net {}".format(dnet))
    try:
        vf = next(iter(avail))
    except:
        raise Exception("No more SRIOV VFs available for data net {}".format(dnet))
    return vf

def get_next_cport(node, prof, curr=set()):
    if not prof.settings.ctrl_port_range:
        return None
    # make a set out of the port range
    avail = set(range(prof.settings.ctrl_port_range[0],
                      prof.settings.ctrl_port_range[1]+1))
    alloced = node['allocated_ports']
    avail = avail - set(alloced) - curr
    try:
        port = next(iter(avail))
    except:
        raise Exception("No more ctrl ports available")
    curr.add(port)
    return str(port)

def get_next_sport(node, prof, curr=set()):
    if not prof.settings.serv_port_range:
        return None
    # make a set out of the port range
    avail = set(range(prof.settings.serv_port_range[0],
                      prof.settings.serv_port_range[1]+1))
    alloced = node['allocated_ports']
    avail = avail - set(alloced) - curr
    try:
        port = next(iter(avail))
    except:
        raise Exception("No more service ports available")
    curr.add(port)
    return str(port)

def get_next_ipv4(net, curr=set(), cidr=False):
    dbase = cfg.db
    nets = dbase.get_table('networks')
    network = dbase.get(nets, key=net.key)
    if not network:
        raise Exception(f"Network not found: {net.name}")
    # consider all similarly named networks using the same address space
    named_nets = dbase.search(nets, name=net.name)
    alloced = list()
    for n in named_nets:
        alloced.extend(n['allocated_v4'])
    ipnet = None
    for sub in network['subnet']:
        try:
            ipnet = IPv4Network(sub['subnet'])
            gw = IPv4Address(sub.get('gateway'))
            if gw:
                alloced.append(str(gw))
            break
        except:
            pass

    if net.ipv4 and not ipnet:
        raise Exception(f"No IPv4 subnet found for network {net.name}")
    # IPv4 is not configured
    if not net.ipv4 and not ipnet:
        return None

    set_alloced = set([IPv4Address(i) for i in alloced])
    unavail = set.union(set_alloced, curr)
    if net.ipv4:
        if isinstance(net.ipv4, str):
            avail = [IPv4Address(net.ipv4)]
        elif isinstance(net.ipv4, list):
            avail = [IPv4Address(addr) for addr in net.ipv4]
    else:
        avail = ipnet.hosts()
    aiter = iter(avail)
    ipv4 = None
    while not ipv4:
        try:
            test = next(aiter)
            if test not in unavail:
                ipv4 = test
        except:
            raise Exception("No more ipv4 addresses available for network {}".format(net.name))
    curr.add(ipv4)
    if cidr:
        return f"{ipv4}/{ipnet.prefixlen}"
    else:
        return str(ipv4)

def get_next_ipv6(net, curr=set(), cidr=False):
    dbase = cfg.db
    nets = dbase.get_table('networks')
    network = dbase.get(nets, key=net.key)
    if not network:
        raise Exception(f"Network not found: {net.name}")
    # consider all similarly named networks using the same address space
    named_nets = dbase.search(nets, name=net.name)
    alloced = list()
    for n in named_nets:
        alloced.extend(n['allocated_v6'])
    ipnet = None
    for sub in network['subnet']:
        try:
            ipnet = IPv6Network(sub['Subnet'])
            gw = IPv6Address(sub.get('Gateway'))
            if gw:
                alloced.append(str(gw))
            break
        except:
            pass

    if net.ipv6 and not ipnet:
        raise Exception(f"No IPv6 subnet found for network {net.name}")
    # IPv6 is not configured
    if not net.ipv6 and not ipnet:
        return None

    set_alloced = set([IPv6Address(i) for i in alloced])
    unavail = set.union(set_alloced, curr)
    if net.ipv6:
        if isinstance(net.ipv6, str):
            avail = [IPv6Address(net.ipv6)]
        elif isinstance(net.ipv6, list):
            avail = [IPv6Address(addr) for addr in net.ipv6]
    else:
        avail = ipnet.hosts()
    aiter = iter(avail)
    ipv6 = None
    while not ipv6:
        try:
            test = next(aiter)
            if test not in unavail:
                ipv6 = test
        except:
            raise Exception("No more ipv6 addresses available for network {}".format(net.name))
    curr.add(ipv6)
    if cidr:
        return f"{ipv6}/{ipnet.prefixlen}"
    return str(ipv6)

def get_cpuset(node, net, prof):
    if net in node['networks']:
        netdev = node['networks'][net].get('netdevice', None)
        if netdev:
            cpuset = node['host']['sriov'][netdev]['local_cpulist']
            return cpuset
    return None

def get_cpu(node, prof):
    return prof.settings.cpu

def get_numa(node, net, prof):
    return None

def get_mem(node, prof):
    return prof.settings.memory

def error_svc(s, e):
    try:
        restxt = json.loads(e.body)
    except:
        restxt = ''
    try:
        reason = e.reason
    except:
        reason = str(e)
    s['errors'].append({'reason': reason,
                        'response': restxt})
    return True

def handle_image(n, img, handler, pull=False):
    if img not in n['images'] or pull:
        parts = img.split(':')
        if len(parts) == 1:
            if f"{img}:latest" not in n['images'] or pull:
                log.info(f"Pulling image {img} for node {n['name']}")
                handler.pull_image(n['id'], parts[0], 'latest')
        elif len(parts) > 1:
            log.info(f"Pulling image {img} for node {n['name']}")
            handler.pull_image(n['id'], parts[0], parts[1])

def set_qos(url, qos):
        try:
            api_url = "{}://{}:{}/api/janus/agent/tc/netem".format(
                settings.AGENT_PROTO,
                url,
                settings.AGENT_PORT
            )

            # basic authentication for now
            res = requests.post(
                url=api_url,
                json=qos,
                auth=("admin", "admin"),
                verify=settings.AGENT_SSL_VERIFY,
                timeout=2
            )

            log.info(res.json())
        except Exception as e:
            log.error(e)
            # return node, None

