import subprocess
import json

# Need to get hashed password of docker container from ~/settings.py
SUDO_PWD = ""

class bcolors:
    HEADER    = '\033[35m'
    OKBLUE    = '\033[94m'
    OKGREEN   = '\033[36m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'


def get_eth_iface_rules(iface):
    # sysargs = ["/sbin/sysctl"]
    sysargs = ["tcshow", iface]

    ret = subprocess.run(sysargs,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    rstr = ret.stdout.decode("utf-8").strip()
    return json.loads(rstr)
    # res = dict()
    # for item in rstr.split("\n"):
    #     parts = item.split("=")
    #     rhs = parts[-1].strip().split("\t")
    #     res.update({parts[0].strip(): rhs if len(rhs)>1 else rhs[0]})
    # return res


def Netem(args, verbose=False, delete=False):
    if "interface" not in args:
        raise Exception("Interface not given")

    iface = args['interface']
    if delete:
        run_cmd = True
        cmd = f"tcdel {iface} --all"
    else:
        run_cmd = False
        cmd = f"tcset {iface}"

        if "latency" in args:
            run_cmd = True
            cmd += f" --delay {args['latency']}"

        if "loss" in args:
            run_cmd = True
            cmd += f" --loss {args['loss']}"

        if "rate" in args:
            run_cmd = True
            cmd += f" --rate {args['rate']}"

        if "limit" in args:
            run_cmd = True
            cmd += f" --limit {args['limit']}"

        cmd += " --change"

    if run_cmd:
        cmd = cmd.split()
        ret = subprocess.run(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)

    return get_eth_iface_rules(iface)


def Delay(args, verbose=False):
    '''
    DELAY
    {Type, IFace0, Latency}
    '''
    if verbose:
        print("Total `tc` args passed: ", len(args))

    iface = args['interface']
    latency = args['latency']

    if iface is not None and latency is not None:
        if verbose:
            print(f"Type: {bcolors.OKBLUE}Delay{bcolors.OKBLUE}")
            print(f"\n---ARGUMENTS---\niface: {iface}\nLatency: {latency}")

        print(f"\n{bcolors.HEADER}Traffic Control Delay!{bcolors.ENDC}")
        print(f"{bcolors.HEADER}Adding {args['latency']} latency to iface {args['interface']}{bcolors.ENDC}")
        cmd = f"tcset {iface} --delay {latency} --change"
        cmd = cmd.split()

        ret = subprocess.run(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

        return get_eth_iface_rules(iface)

        # sysargs = "tc qdisc add dev {0} root netem delay {1} limit 10000".format(args['interface'],
        #                                                                          args['latency'])
        # sysargs = sysargs.split()
        # pwd = subprocess.Popen(['echo', SUDO_PWD],
        #                         stdout=subprocess.PIPE)
        # ret = subprocess.Popen(["sudo", "-S"]+sysargs,
		# 	   stdin=pwd.stdout,
		# 	   stdout=subprocess.PIPE)
        # rstr = ret.stdout.read().decode()
        # return rstr

    else:
        return "Interface and latency must be specified", 400


def Latency(args, verbose=False):
    '''
    LATENCY
    {Type, IFace0, Latency, Loss}
    '''
    if verbose:
        print("Total `tc` args passed: ", len(args))


    iface = args['interface']
    latency = args['latency']
    loss = args['loss']

    if iface is not None and latency is not None and loss is not None:
        if verbose:
            print(f"Type: {bcolors.OKBLUE}Latency{bcolors.OKBLUE}")
            print(f"\n---ARGUMENTS---\niface: {iface}\nLatency: {latency}\nLoss: {loss}")

        print(f"\n{bcolors.HEADER}Traffic Control Latency!{bcolors.ENDC}")
        print(f"{bcolors.HEADER}Adding latency:{latency} and loss:{loss} to iface:{iface}{bcolors.ENDC}")

        cmd = f"tcset {iface} --delay {latency} --loss {loss} --change"
        cmd = cmd.split()

        ret = subprocess.run(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

        return get_eth_iface_rules(iface)

        # sysargs = "tc qdisc add dev {0} root netem delay {1} loss {2} limit 100000".format(args['interface'],
        #                                                                                    args['latency'],
        #                                                                                    args['loss'])
        # sysargs = sysargs.split()
        # pwd = subprocess.Popen(["echo", SUDO_PWD],
        #                         stdout=subprocess.PIPE)
        # ret = subprocess.Popen(["sudo", "-S"]+sysargs,
        #                         stdin=pwd.stdout,
        #                         stdout=subprocess.PIPE)
        # rstr = ret.stdout.read().decode()
    # return rstr
    else:
        return "Interface, latency and loss must be specified", 400


def Filter(args, verbose=False):
    '''
    FILTER
    {Type, IFace0, Latency, Loss, Dport, Dmask, Id}
    '''

    if verbose:
        print("Total `tc` args passed: ", len(args))

    if len(args)==6:
        if verbose:
            print(f"Type: {bcolors.OKBLUE}Filter{bcolors.OKBLUE}")
            print(f"\n---ARGUMENTS---\niface:\t{args['interface']}\nLatency:{args['latency']}\nLoss:\t{args['loss']}\
\nDport:\t{args['dport']}\nDmask:\t{args['dmask']}\nid:\t{args['id']}")

        print(f"\n{bcolors.HEADER}Traffic Control Filter!{bcolors.ENDC}")
        print(f"{bcolors.HEADER}Adding latency:{args['latency']} and loss:{args['loss']} to iface:{args['interface']} (dport={args['dport']}/{args['dmask']}){bcolors.ENDC}")

        sysargs_ = [
        "tc qdisc add dev {0} root handle 1: htb".format(args['interface']),
        "tc class add dev {0} parent 1: classid 1:1 htb rate 100000Mbps".format(args['interface']),
        "tc class add dev {0} parent 1:1 classid 1:{1} htb rate 10000Mbps".format(args['interface'],
                                                                                  args['id']),
        "tc qdisc add dev {0} parent 1:{1} handle {1}0: netem delay {2} loss {3} limit 100000".format(args['interface'],
                                                                                                      args['id'],
                                                                                                      args['latency'],
                                                                                                      args['loss']),
        "tc filter add dev {0} parent 1:0 protocol ip u32 match ip dport {1} {2} flowid 1:{3}".format(args['interface'],
                                                                                                      args['dport'],
                                                                                                      args['dmask'],
                                                                                                      args['id'])
        ]
        # Selecting sysargs based on ID argument.
        print("\n---COMMANDS---")
        if args['id']=='2':
            sysargs = sysargs_[:]
            if verbose:
                for i,s in enumerate(sysargs):
                    print(f"Command {i}: {s}")
        else:
            sysargs = sysargs_[2:]
            if verbose:
                for i,s in enumerate(sysargs):
                    print(f"Command {i}: {s}")
        print()
        for cmd in sysargs:
            print("Executing", cmd)
            cmd = cmd.split()
            pwd = subprocess.Popen(["echo", SUDO_PWD],
                                   stdout=subprocess.PIPE)
            ret = subprocess.Popen(["sudo", "-S"]+cmd,
                                    stdin=pwd.stdout,
                                    stdout=subprocess.PIPE)
            rstr = ret.stdout.read().decode()
    return rstr


def Pacing(args, verbose=False, update=False, delete=False):
    if "interface" not in args:
        raise Exception("Interface not given")

    if delete:
        sysargs_ = [f"tc qdisc del dev {args['interface']} root"]
    elif update:
        if "maxrate" not in args:
            raise Exception("maxrate not given")
        sysargs_ = [f"tc qdisc replace dev {args['interface']} parent 1:3 handle 30: fq maxrate {args['maxrate']}"]
    else:
        if "maxrate" not in args:
            raise Exception("maxrate not given")
        if "ip" not in args:
            raise Exception("ip not given")
        if "tagged" not in args:
            raise Exception("tagged not given")

        proto = "802.1q" if args['tagged'] else "ip"
        sysargs_ = [
            f"tc qdisc add dev {args['interface']} root handle 1: prio",
            f"tc qdisc add dev {args['interface']} parent 1:3 handle 30: fq maxrate {args['maxrate']}",
            f"tc filter add dev {args['interface']} protocol {proto} parent 1:0 u32 match ip dst {args['ip']}/32 flowid 1:3"
        ]

    retstr = ""
    for cmd in sysargs_:
        args = cmd.split()
        p = subprocess.Popen(["sudo"]+args,
	                     stdin=None,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        if p.returncode:
            raise Exception(errs.decode())
        else:
            retstr += f"{outs.decode()}\n"
    return retstr
