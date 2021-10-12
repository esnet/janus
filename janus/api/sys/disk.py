import virtfs

IGNORE_BLOCK_STRS = ('loop', 'ram', 'zram')

def build_block():
    ret = dict()
    blk = virtfs.sysfs.block
    for d in blk.contents:
        if d.lower().startswith(IGNORE_BLOCK_STRS):
            continue
        dev = getattr(blk, d)
        try:
            ret[d] = dict()
            ret[d]['size'] = dev.size.contents.strip()
            ret[d]['model'] = dev.device.model.contents.strip()
            numa_node = dev.device.numa_node.contents.strip()
            ret[d]['numa_node'] = numa_node
        except virtfs.exc.NotFound as e:
            continue

    return ret
