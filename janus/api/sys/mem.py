import psutil

def build_mem():
    ret = dict()
    try:
        mem = psutil.virtual_memory()
        ret['total'] = mem.total
        ret['available'] = mem.available
        ret['used'] = mem.used
        ret['percent'] = mem.percent
        ret['active'] = mem.active
    except:
        pass
    return ret
