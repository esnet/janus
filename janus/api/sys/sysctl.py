import subprocess

MAX_TCPMEM = 536870912
DEF_TCPMEM = 16777216

DEF_SYSCTL = {
    "kernel.pid_max": 4194303,
    "net.ipv4.tcp_max_syn_backlog": 4096,
    "net.ipv4.tcp_fin_timeout": 15,
    "net.ipv4.tcp_wmem": [4096, 87380, MAX_TCPMEM],
    "net.ipv4.tcp_rmem": [4096, 87380, MAX_TCPMEM],
    "net.ipv4.udp_rmem_min": 16384,
    "net.ipv4.tcp_window_scaling": 1,
    "net.ipv4.tcp_slow_start_after_idle": 0,
    "net.ipv4.tcp_timestamps": 1,
    "net.ipv4.tcp_low_latency": 1,
    "net.ipv4.tcp_keepalive_intvl": 15,
    "net.ipv4.tcp_rfc1337": 1,
    "net.ipv4.tcp_keepalive_time": 300,
    "net.ipv4.tcp_keepalive_probes": 5,
    "net.ipv4.tcp_sack": 1,
    "net.ipv4.neigh.default.unres_qlen": 6,
    "net.ipv4.neigh.default.proxy_qlen": 96,
    "net.ipv4.ipfrag_low_thresh": 446464,
    "net.ipv4.ipfrag_high_thresh": 512000,
    "net.core.netdev_max_backlog": 250000,
    "net.core.rmem_default": DEF_TCPMEM,
    "net.core.wmem_default": DEF_TCPMEM,
    "net.core.rmem_max": MAX_TCPMEM,
    "net.core.wmem_max": MAX_TCPMEM,
    "net.core.optmem_max": 40960,
    "net.core.dev_weight": 128,
    "net.core.somaxconn": 1024,
    "net.ipv4.udp_wmem_min": 16384,
    "net.core.default_qdisc": "fq",
    # "net.ipv4.tcp_congestion_control": "htcp",
}


def get_tune():
    sysargs = ["/sbin/sysctl"]
    sysargs.extend(list(DEF_SYSCTL.keys()))
    ret = subprocess.run(sysargs, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    rstr = ret.stdout.decode("utf-8").strip()
    res = dict()
    for item in rstr.split("\n"):
        parts = item.split("=")
        rhs = parts[-1].strip().split("\t")
        res.update({parts[0].strip(): rhs if len(rhs) > 1 else rhs[0]})
    return res


def set_tune(args):
    if args:
        params = args
        for k,v in params.items():
            if k not in DEF_SYSCTL:
                raise Exception({"error": "Unsupported sysctl key {}".format(k)})
    else:
        params = DEF_SYSCTL

    sysargs = ["sudo", "sysctl"]
    for k,v in params.items():
        if isinstance(v, list):
            val = ' '.join(str(p) for p in v)
        else:
            val = v
        sysargs.append(f"{k}={val}")
    ret = subprocess.run(sysargs, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    if ret.returncode:
        raise Exception({"error": ret.stdout})
    if args:
        return args
    return DEF_SYSCTL