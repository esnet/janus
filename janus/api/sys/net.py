import os
import sys
import virtfs
import psutil
import janus.agent_settings as settings


def build_sriov():
    ret = dict()

    ndevs = psutil.net_if_stats()
    for n,v in ndevs.items():
        if n in settings.IGNORE_NETDEVS:
            continue

        # cannot use 'class' keyword directly
        cls = getattr(virtfs.sysfs, "class")
        dev = getattr(cls.net, n)

        ret[n] = dict()

        try:
            # start with initial sriov info for device
            total_vfs = dev.device.sriov_totalvfs.contents.strip()
            num_vfs = dev.device.sriov_numvfs.contents.strip()
            ret[n]['total_vfs'] = total_vfs
            ret[n]['num_vfs'] = num_vfs
            nodes = list()
            for v in range(0, int(num_vfs)):
                svf = getattr(dev.device.sriov, str(v))
                vf = getattr(dev.device, "virtfn{}".format(v))
                nodes.append({'id': v,
                              'ifnames': vf.net.contents,
                              'mac': svf.node.contents.strip(),
                              'ib_verbs_devs': vf.infiniband_verbs.contents})
            ret[n]['vfs'] = nodes
        except:
            pass

        try:
            ret[n]['local_cpus'] = dev.device.local_cpus.contents.strip()
            ret[n]['local_cpulist'] = dev.device.local_cpulist.contents.strip()
            ret[n]['numa_node'] = dev.device.numa_node.contents.strip()
        except:
            pass

        try:
            ret[n]['ib_verbs_devs'] = dev.device.infiniband_verbs.contents
        except virtfs.exc.NotFound as e:
            pass

        if not ret[n]:
            del ret[n]

    return ret
