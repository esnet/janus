import json
import cpuinfo


def build_cpu():
    cpu = cpuinfo.get_cpu_info_json()
    cpu_json = json.loads(cpu)
    del cpu_json['python_version']
    del cpu_json['cpuinfo_version']
    return cpu_json
