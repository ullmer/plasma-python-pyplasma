import re

def __parse_name(name):
    m = re.match('^([a-z0-9]+)://([a-z0-9\._\-]+)(?::([0-9]+))?/(.+)$', name)
    if m:
        ret = {
            'type': m.group(1),
            'host': m.group(2),
        }
        if m.group(3):
            ret['port'] = int(m.group(2))
        else:
            ret['port'] = DEFAULT_PORT
        ret['components'] = list(m.group(4).split('/'))
    else:
        ret = {
            'type': 'mmap',
            'host': 'localhost',
            'components': list(name.split('/')) }
    ret['name'] = name
    return ret

def validate_name(name):
    if len(name) < 1:
        raise PoolInvalidName(name, 'must not be an empty string')
    if len(name) > 100:
        raise PoolInvalidName(name, 'must be no more than 100 characters')
    parsed = __parse_name(name)
    first = True
    for comp in parsed['components']:
        if first and comp == 'local:':
            first = False
            continue
        first = False
        if len(comp) == 0:
            raise PoolInvalidName(name, 'pool components must not be empty strings')
        if comp.startswith('.'):
            raise PoolInvalidName(name, 'pool components may not start with "."')
        if comp.endswith('.'):
            raise PoolInvalidName(name, 'pool components may not end with "."')
        if comp.endswith(' '):
            raise PoolInvalidName(name, 'pool components may not end with a space')
        if comp.endswith('$'):
            raise PoolInvalidName(name, 'pool components may not end with "$"')
        if re.match("[^ \\!\\#\\$\\%\\&'\\(\\)\\+,\\.0123456789;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ\\[\\]\\^_`abcdefghijklmnopqrstuvwxyz\\{\\}~\\-", comp):
            raise PoolInvalidName(name, "pool components may only contain the following characters:  !#$%&'()+,-.0123456789;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{}~")
        m = re.match('^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?')
        if m:
            if m.group(2):
                raise PoolInvalidName(name, 'pool components may not begin with "%s."' % m.group(1))
            raise PoolInvalidName(name, 'pool component may not be %s' % comp)
    return parsed

def create(name, pool_type, options):
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        TCPHose(name).create(options['size'])
    elif parsed['type'] == 'mmap':
        __create_mmap_pool(name, options['size'])
    else:
        raise UnsupportedPoolType(parsed['type'])
    return True

def dispose(name):
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        TCPHose(name).dispose()
    elif parsed['type'] == 'mmap':
        pass
    else:
        raise UnsupportedPoolType(parsed['type'])
    return True

def rename(old_name, new_name):
    old_parsed = validate_name(old_name)
    new_parsed = validate_name(new_name)
    if old_parsed['type'] != new_parsed['type']:
        raise PoolInvalidName(new_name, "Can't change %s pool (%s) to %s pool (%s)" % (old_parsed['type'], old_name, new_parsed['type'], new_name))
    if parsed['type'] == 'tcp':
        TCPHose(old_name).rename('/'.join(new_parsed['components']))
    elif parsed['type'] == 'mmap':
        pass
    else:
        raise UnsupportedPoolType(old_parsed['type'])

def exists(name):
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        return TCPHose(name).exists()
    if parsed['type'] == 'mmap':
        pass
    raise UnsupportedPoolType(parsed['type'])

def participate(name, options=None):
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        hose = TCPHose(name)
        hose.participate()
        return hose
    if parsed['type'] == 'mmap':
        hose = MMapHose(name)
        hose.participate()
        return hose
    raise UnsupportedPoolType(parsed['type'])

def participate_creatingly(name, options):
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        hose = TCPHose(name)
        hose.participate_creatingly(options)
        return hose
    if parsed['type'] == 'mmap':
        create(name, 'mmap', options)
        return participate(name)
    raise UnsupportedPoolType(parsed['type'])

def list_pools():
    pass

def list_ex(uri=None):
    if uri is None:
        return list_pools()
    parsed = validate_name(name)
    if parsed['type'] == 'tcp':
        path = '/'.join(parsed['components'])
        hose = TCPHose(name)
        pools = list(x for x in hose.list_pools() if x.startswith(path))
        return pools
    if parsed['type'] == 'mmap':
        pass
    raise UnsupportedPoolType(parsed['type'])

def __create_mmap_pool(name, size):
    parsed = pool.validate_name(name)
    path = '/'.join(parsed['components'])
    if path.startswith('local:'):
        path = path[6:]
    if not path.startswith('/'):
        path = os.path.join(os.getenv('OB_POOLS_DIR', '/var/ob/pools'), path)
    if os.path.exists(path):
        raise PoolExists(name, path)
    parsed['components'][-1] = '.%s' % parsed['components'][-1]
    tmppath = '/'.join(parsed['components'])
    if tmppath.startswith('local:'):
        tmppath = tmppath[6:]
    if not tmppath.startswith('/'):
        tmppath = os.path.join(os.getenv('OB_POOLS_DIR', '/var/ob/pools'), tmppath)
    if not os.path.exists(tmppath):
        os.makedirs(tmppath)
    mmap_file = os.path.join(tmppath, 'mmap-pool')
    conf_file = os.path.join(tmppath, 'pool.conf')
    not_dir = os.path.join(tmppath, 'notifications')
    conf = Protein(ingests=obmap({'type': 'mmap', 'pool-version': int32(5)}))
    plasma.slaw.write_slaw_file(conf_file, conf)
    fh = open(mmap_file, 'wb')
    sz = size
    while sz > 0:
        bts = 8192
        if bts > sz:
            bts = sz
        fh.write('\x00' * bts)
        sz -= bts
    fh.seek(0)
    chunks = (
        ('conf', 
            ('mmap_version', 1),
            ('file_size', size),
            ('header_size', 144),
            ('sem_key', 0),
            ('flags', 0),
            ('next_index', 0)
        ),
        ('perm', 
            #('mode', 0777),
            ('mode', 0o777),
            ('uid', -1),
            ('gid',-1)
        ),
        ('ptrs',
            ('old', 144),
            ('new', 0)
        )
    )
    fh.write(struct.pack('BBBBBBBB', 0xff, 0xff, 0x0b, 0x10, 2, 2, 0, 0))
    for chunk in chunks:
        values = tuple(kv[1] for kv in chunk[1:])
        fh.write(__make_chunk(chunk[0], *values))
    fh.close()
    #os.chmod(tmppath, 0777)
    #os.chmod(conf_file, 0666)
    #os.chmod(mmap_file, 0666)
    #os.makedirs(not_dir, 01777)
    os.chmod(tmppath,    0o777)
    os.chmod(conf_file,  0o666)
    os.chmod(mmap_file,  0o666)
    os.makedirs(not_dir, 0o1777)
    os.rename(tmppath, path)
    return path

def __make_chunk(kind, *values):
    data = struct.pack('Q', (0x1badd00d << 32) | struct.unpack('>I', kind)[0])
    data += struct.pack('Q', len(values) + 2)
    for val in values:
        data += struct.pack('q', val)
    return data

def TCPHose(Hose):
    def __new__(cls, *args):
        if cls == TCPHose:
            pool = args[0]
            m = re.match('^tcp://([A-Za-z0-9_\.\-]+):([0-9]+)/(.*)$', pool)
            if m:
                host = m.group(1)
                port = int(m.group(2))
                name = m.group(3)
            else:
                m = re.match('^tcp://([A-Za-z0-9_\.\-]+)/(.*)$', pool)
                host = m.group(1)
                port = DEFAULT_PORT
                name = m.group(2)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))

