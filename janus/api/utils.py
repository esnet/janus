import os
import json
import logging
from ipaddress import IPv4Network, IPv4Address
from ipaddress import IPv6Network, IPv6Address

from janus import settings
from janus.api.models import Network
from janus.settings import cfg
from tinydb import Query
import requests

log = logging.getLogger(__name__)

def precommit_db(Id=None, delete=False):
    table = cfg.db.table('active')
    if Id and delete:
        table.remove(doc_ids=[Id])
    else:
        Id = table.insert(dict())
    return Id

def commit_db_realized(record, node_table, net_table, delete=False):
    Q = Query()
    services = record.get("services", dict())
    for k,v in services.items():
        for s in v:
            node = node_table.get(Q.name == k)
            if delete:
                try:
                    if s['ctrl_port']:
                        node['allocated_ports'].remove(int(s['ctrl_port']))
                    if s['data_vfid']:
                        node['allocated_vfs'].remove(s['data_vfid'])
                except Exception as e:
                    pass
            else:
                if s['ctrl_port']:
                    node['allocated_ports'].append(int(s['ctrl_port']))
                if s['data_vfid']:
                    node['allocated_vfs'].append(s['data_vfid'])
            node_table.update(node, Q.name == k)

            if (s['data_net']):
                nobj = Network(s['data_net_name'], k)
                net = net_table.get(Q.key == nobj.key)
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
                    if s['data_ipv4']:
                        net['allocated_v4'].append(s['data_ipv4'])
                    if s['data_ipv6']:
                        net['allocated_v6'].append(s['data_ipv6'])
                net_table.update(net, Q.key == nobj.key)

def commit_db(record, rid=None, delete=False, realized=False):
    node_table = cfg.db.table('nodes')
    net_table = cfg.db.table('networks')
    table = cfg.db.table('active')

    if realized:
        commit_db_realized(record, node_table, net_table, delete)
        table.update(record, doc_ids=[rid])
        return {rid: record}

    if delete:
        table.remove(doc_ids=[rid])
        return {rid: record}
    elif rid:
        table.update(record, doc_ids=[rid])
        return {rid: record}
    else:
        Id = table.insert(record)
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
    if not prof['ctrl_port_range']:
        return None
    # make a set out of the port range
    avail = set(range(prof['ctrl_port_range'][0],
                      prof['ctrl_port_range'][1]+1))
    alloced = node['allocated_ports']
    avail = avail - set(alloced) - curr
    try:
        port = next(iter(avail))
    except:
        raise Exception("No more ctrl ports available")
    curr.add(port)
    return str(port)

def get_next_sport(node, prof, curr=set()):
    if not prof['serv_port_range']:
        return None
    # make a set out of the port range
    avail = set(range(prof['serv_port_range'][0],
                      prof['serv_port_range'][1]+1))
    alloced = node['allocated_ports']
    avail = avail - set(alloced) - curr
    try:
        port = next(iter(avail))
    except:
        raise Exception("No more service ports available")
    curr.add(port)
    return str(port)

def get_next_ipv4(net, curr=set()):
    Net = Query()
    nets = cfg.db.table('networks')
    network = nets.get(Net.key == net.key)
    if not network:
        raise Exception(f"Network not found: {net.name}")
    # consider all similarly named networks using the same address space
    named_nets = nets.search(Net.name == net.name)
    alloced = list()
    for n in named_nets:
        alloced.extend(n['allocated_v4'])
    set_alloced = set([IPv4Address(i) for i in alloced])
    if net.ipv4:
        if isinstance(net.ipv4, str):
            avail = set([IPv4Address(net.ipv4)])
        elif isinstance(net.ipv4, list):
            avail = set([IPv4Address(addr) for addr in net.ipv4])
    else:
        ipnet = IPv4Network(network['subnet'][0]['Subnet'])
        avail = set(ipnet.hosts())
    # prune final avail set
    avail = avail - set_alloced - curr
    try:
        ipv4 = next(iter(avail))
    except:
        raise Exception("No more ipv4 addresses available for network {}".format(net.name))
    curr.add(ipv4)
    return str(ipv4)

def get_next_ipv6(net, curr=set()):
    Net = Query()
    nets = cfg.db.table('networks')
    network = nets.get(Net.key == net.key)
    if not network:
        raise Exception(f"Network not found: {net.name}")
    # consider all similarly named networks using the same address space
    named_nets = nets.search(Net.name == net.name)
    alloced = list()
    for n in named_nets:
        alloced.extend(n['allocated_v6'])
    set_alloced = set([IPv6Address(i) for i in alloced])
    ipnet = None
    for sub in network['subnet']:
        try:
            ipnet = IPv6Network(sub['Subnet'])
            break
        except:
            pass

    if net.ipv6 and not ipnet:
        raise Exception(f"No IPv6 subnet found for network {net.name}")
    # IPv6 is not configured
    if not net.ipv6 and not ipnet:
        return None

    ipv6 = None
    unavail = set.union(set_alloced, curr)
    if net.ipv6:
        if isinstance(net.ipv6, str):
            avail = set([IPv6Address(net.ipv6)])
        elif isinstance(net.ipv6, list):
            avail = set([IPv6Address(addr) for addr in net.ipv6])
    else:
        avail = set(ipnet.hosts())
    aiter = iter(avail)
    while not ipv6:
        try:
            test = next(aiter)
            if test not in unavail:
                ipv6 = test
        except:
            raise Exception("No more ipv6 addresses available for network {}".format(net.name))
    curr.add(ipv6)
    return str(ipv6)

def get_cpuset(node, net, prof):
    if net in node['networks']:
        netdev = node['networks'][net].get('netdevice', None)
        if netdev:
            cpuset = node['host']['sriov'][netdev]['local_cpulist']
            return cpuset
    return None

def get_numa(node, net, prof):
    return None

def get_mem(node, prof):
    return prof['mem']

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
    s['container_id'] = None
    return True

def handle_image(n, img, dapi, pull=False):
    if img not in n['images'] or pull:
        parts = img.split(':')
        if len(parts) == 1:
            if f"{img}:latest" not in n['images'] or pull:
                log.info(f"Pulling image {img} for node {n['name']}")
                dapi.pull_image(n['id'], parts[0], 'latest')
        elif len(parts) > 1:
            log.info(f"Pulling image {img} for node {n['name']}")
            dapi.pull_image(n['id'], parts[0], parts[1])

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

def create_service(node, img, prof, addrs_v4, addrs_v6, cports, sports, **kwargs):
    srec = dict()
    nname = node.get('name')
    pname = prof.get('name')
    prof = prof.get('settings')
    qos = cfg.get_qos(prof["qos"]) if "qos" in prof else dict()
    dpr = prof['data_port_range']
    dnet = Network(prof['data_net'], nname)
    mnet = Network(prof['mgmt_net'], nname)
    priv = prof.get('privileged')
    sysd = prof.get('systemd')
    pull = prof.get('pull_image')

    vfid = None
    vfmac = None
    mgmt_ipv4 = None
    mgmt_ipv6 = None
    data_ipv4 = None
    data_ipv6 = None
    cport = get_next_cport(node, prof, cports)
    sport = get_next_sport(node, prof, sports)
    internal_port = prof['internal_port'] or cport

    if dpr:
        dports = "{},{}".format(dpr[0],dpr[1])
    else:
        dports = ""

    mnet_kwargs = {}
    docker_kwargs = {
        "HostName": nname,
        "HostConfig": {
            "PortBindings": dict(),
            "NetworkMode": mnet.name,
            "Mounts": list(),
            "Devices": list(),
            "CapAdd": list(),
            "Ulimits": list(),
            "Privileged": priv
        },
        "ExposedPorts": dict(),
        "Env": [
            "HOSTNAME={}".format(node['public_url']),
            "CTRL_PORT={}".format(cport),
            "SERV_PORT={}".format(sport),
            "DATA_PORTS={}".format(dports),
            "USER_NAME={}".format(kwargs.get("USER_NAME", "")),
            "PUBLIC_KEY={}".format(kwargs.get("PUBLIC_KEY", ""))
        ],
        "Tty": True,
        "StopSignal": "SIGRTMIN+3" if sysd else "SIGTERM"
    }

    if cport:
        docker_kwargs["HostConfig"]["PortBindings"].update({
            "{}/tcp".format(internal_port): [
                {"HostPort": "{}".format(cport)}]
        })
        docker_kwargs["ExposedPorts"].update({
            "{}/tcp".format(internal_port): {}
        })

    if sport:
        docker_kwargs["HostConfig"]["PortBindings"].update({
            "{}/tcp".format(sport): [
                {"HostPort": "{}".format(sport)}]
        })
        docker_kwargs["ExposedPorts"].update({
            "{}/tcp".format(sport): {}
        })

    if mnet.name and not mnet.is_host():
        try:
            minfo = node['networks'][mnet.name]
        except:
            raise Exception("Network not found: {}".format(mnet.name))
        mnet_type = minfo['driver']
        # Remove port mappings if control network requested is not bridge
        if mnet_type != "bridge":
            del docker_kwargs["HostConfig"]["PortBindings"]
            del docker_kwargs["ExposedPorts"]

        if not mnet.is_host() and mnet_type != "bridge":
            # Set mgmt net layer 3
            mgmt_ipv4 = get_next_ipv4(mnet, addrs_v4)
            mgmt_ipv6 = get_next_ipv6(mnet, addrs_v6)
            mnet_kwargs.update({"EndpointConfig": {
                "IPAMConfig": {
                    "IPv4Address": mgmt_ipv4,
                    "IPv6Address": mgmt_ipv6
                }
            }
            })

    # Constrain container memory if requested
    mem = get_mem(node, prof)
    if mem:
        docker_kwargs["HostConfig"].update({"Memory": mem})

    for e in prof['environment']:
        # XXX: do some sanity checking here
        docker_kwargs['Env'].append(e)

    for v in prof['volumes']:
        vol = cfg.get_volume(v)
        if vol:
            readonly = True if "ReadOnly" in vol and vol['ReadOnly'] else False
            mnt = {'Type': vol['type'],
                   'Source': vol.get('source', None),
                   'Target': vol.get('target', None),
                   'ReadOnly': readonly
                   }
            docker_kwargs['HostConfig']['Mounts'].append(mnt)
            if "driver" in vol:
                docker_kwargs['HostConfig']['VolumeDriver'] = vol['driver']

    if dnet.name and not mnet.is_host():
        try:
            dinfo = node['networks'][dnet.name]
        except:
            raise Exception("Network not found: {}".format(dnet.name))

        # Pin CPUs based on data net
        cpus = get_cpuset(node, dnet.name, prof)
        if cpus:
            docker_kwargs["HostConfig"].update({"CpusetCpus": cpus})

        # Set data net layer 3
        data_ipv4 = get_next_ipv4(dnet, addrs_v4)
        data_ipv6 = get_next_ipv6(dnet, addrs_v6)
        print (data_ipv4, data_ipv6)
        docker_kwargs["HostConfig"].update({"NetworkMode": dnet.name})
        docker_kwargs.update({"NetworkingConfig": {
            "EndpointsConfig": {
                dnet.name: {
                    "IPAMConfig": {
                        "IPv4Address": data_ipv4,
                        "IPv6Address": data_ipv6
                    }
                }
            }
        }
        })
        docker_kwargs["Env"].append("DATA_IFACE={}".format(data_ipv4))

        # Need to specify and track sriov vfs explicitly
        ndrv = dinfo.get("driver", None)
        if ndrv == "sriov":
            (vfid, vfmac) = get_next_vf(node, dnet.name)
            #docker_kwargs['NetworkingConfig']['EndpointsConfig'][dnet.name]['IPAMConfig']['MacAddress'] = vfmac
    else:
        docker_kwargs["Env"].append("DATA_IFACE={}".format(node['public_url']))
        if not mnet.is_host() and dpr:
            for p in range(dpr[0], dpr[1]+1):
                docker_kwargs["HostConfig"]["PortBindings"].update(
                    {"{}/tcp".format(p):
                     [{"HostPort": "{}".format(p)}]}
                )
                docker_kwargs["ExposedPorts"].update({"{}/tcp".format(p): {}})

    # handle features enabled for this service
    for f in prof['features']:
        feat = cfg.get_feature(f)
        if feat:
            caps = feat.get('caps', list())
            docker_kwargs['HostConfig']['CapAdd'].extend(caps)
            limits = feat.get('limits', list())
            docker_kwargs['HostConfig']['Ulimits'].extend(limits)

        if feat:
            devices = feat.get('devices', list())
            for d in devices:
                if dnet.name:
                    if "rdma_cm" in d['names']:
                        dev = {'PathOnHost': os.path.join(d['devprefix'], "rdma_cm"),
                               'PathInContainer': os.path.join(d['devprefix'], "rdma_cm"),
                               'CGroupPermissions': "rwm"}
                        docker_kwargs['HostConfig']['Devices'].append(dev)
                    if "uverbs" in d['names']:
                        dev = node["networks"][dnet.name]["netdevice"]
                        vfs = node["host"]["sriov"][dev]["vfs"]
                        for iface in vfs:
                            n = iface["ib_verbs_devs"][0]
                            dev = {'PathOnHost': os.path.join(d['devprefix'], n),
                                   'PathInContainer': os.path.join(d['devprefix'], n),
                                   'CGroupPermissions': "rwm"}
                            docker_kwargs['HostConfig']['Devices'].append(dev)
                else:
                    dev = {'PathOnHost': d['devprefix'],
                           'PathInContainer': d['devprefix'],
                           'CGroupPermissions': "rwm"}
                    docker_kwargs['HostConfig']['Devices'].append(dev)

    srec['mgmt_net'] = node['networks'].get(mnet.name, None)
    srec['mgmt_ipv4'] = mgmt_ipv4
    srec['mgmt_ipv6'] = mgmt_ipv6
    srec['data_net'] = node['networks'].get(dnet.name, None)
    srec['data_net_name'] = dnet.name
    srec['data_ipv4'] = data_ipv4
    srec['data_ipv6'] = data_ipv6
    srec['data_vfmac'] = vfmac
    srec['data_vfid'] = vfid
    srec['container_user'] = kwargs.get("USER_NAME", None)

    srec['node'] = node
    srec['node_id'] = node['id']
    srec['serv_port'] = sport
    srec['ctrl_port'] = cport
    srec['ctrl_host'] = node['public_url']
    srec['docker_kwargs'] = docker_kwargs
    srec['net_kwargs'] = mnet_kwargs
    srec['image'] = img
    srec['profile'] = pname
    srec['pull_image'] = pull
    srec['qos'] = qos
    srec['errors'] = list()
    return srec
