import os
import sys
import re
import numa

def build_numa():
    ret = dict()
    if not numa.available():
        return ret
    
    nodes = numa.get_max_node() + 1
    for n in range(0, nodes):
        try:
            ret[n] = {'cpus': list(numa.node_to_cpus(n)),
                      'mem': numa.get_node_size(n)}
        except:
            continue
    return ret
