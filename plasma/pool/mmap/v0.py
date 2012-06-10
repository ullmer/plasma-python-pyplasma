from loam import *
from plasma.const import *
from plasma.exceptions import *
from plasma.pool.mmap import MMapPool
from plasma.pool.util import pools_dir, makedirs, with_umask, with_config_lock
#static const char CFG_KEY_FILE_SIZE [] = "file-size";
#static const char CFG_KEY_HEADER_SIZE [] = "header-size";
#static const char CFG_KEY_INDEX_CAPACITY [] = "index-capacity";

POOL_MMAP_V0_HEADER_SIZE = 24
POOL_MMAP_MAGICV0 = 0x00065b0000af4c81
POOL_MMAP_SLAW_VERSION_SHIFTY = 24

class V0MMapPool(MMapPool):
    _mmap_version = 0

    def __init__(self, *args, **kwargs):
        raise AbstractClassError("%s.__init__ must be defined in a subclass" % type(self).__name__)

    @with_umask
    def create(self, options=None):
        self._slaw_version = 2
        self._slaw_type = 2
        self._pool_flags = 0
        self._size = self._get_size(options)
        self._perm = self._get_permissions(options)
        self._index_capacity = self._get_index_capacity(options)
        header_size = POOL_MMAP_V0_HEADER_SIZE
        if self._index_capacity > 0:
            self._indx = PoolIndex()
            self._indx.capacity = self._index_capacity
            header_size += self._indx.size()
        else:
            self._indx = None
        magic = POOL_MMAP_MAGICV0 | (self._slaw_version << POOL_MMAP_SLAW_VERSION_SHIFTY)
        if size < header_size or size > POOL_SIZE_MAX:
            raise PoolInvalidSizeException("Invalid size %d; must be at least %d and no more than %d" % (size, header_size, POOL_SIZE_MAX))
        makedirs(self._directory, self._perm['mode'], self._perm['uid'], self._perm['gid'])
        makedirs(self._notification_directory, self._perm['mode'], self._perm['uid'], self._perm['gid'])
        self._sem = SemaphoreSet()
        fh = os.fdopen(os.open(self._mmap_fil, os.O_RDWR | os.O_CREAT | os.O_EXCL, self._perm['mode'] & 0666), 'rw')
        self._fh = fh
        os.chown(self._mmap_file, self._perm['uid'], self._perm['gid'])
        os.ftruncate(fh.fileno(), self._size)
        fh.seek(0)
        fh.write(struct.pack('QQQ', header_size, 0, magic))
        if self._indx is not None:
            self._indx.set_pool(fh, POOL_MMAP_V0_HEADER_SIZE)
        self._save_default_config()
        self._save_mmap_config()
        self._fh.close()
        return self

    def _pointers(self):
        pos = self._mmap.tell()
        self._mmap.seek(0)
        (old, new) = struct.unpack('qq', self._mmap.read(16))
        self._mmap.seek(pos)
        return (old, new)

    def _set_pointers(self, old, new):
        pos = self._mmap.tell()
        self._mmap.seek(0)
        self._mmap.write(struct.pack('qq', old, new))
        self._mmap.seek(pos)

    def _open(self):
        pool_conf = plasma.slaw.read_slaw_file(self._pool_conf_file).ingests()
        mmap_conf = plasma.slaw.read_slaw_file(self._mmap_conf_file).ingests()
        perms = pool_conf.ingests().get('perms')
        self._perms = {
            'mode': pool_conf['perms'].x,
            'uid':  pool_conf['perms'].y,
            'gid':  pool_conf['perms'].z
        }
        self._sem = SemaphoreSet(pool_conf['sem-key'])
        self._size = mmap_conf['file-size']
        self._header_size = mmap_conf['header-size']
        self._index_capacity = mmap_conf.get('index-capacity', 0)
        self._mmap_open()
        self._mmap.seek(16)
        (magic,) = struct.unpack('Q', self._mmap.read(8))
        if magic & 0xffffffff00ffffff != POOL_MMAP_MAGICV0:
            raise PoolCorruptException("invalid magic number")
        self._slaw_version = (magic >> POOL_MMAP_SLAW_VERSION_SHIFTY) & 0xff
        if self._index_capacity > 0:
            self._indx = PoolIndex(self._mmap)
        self._mmap.seek(0)
        self._pool_ptr = 0
        self._pool_pos = 0
        self._is_configured = True
        self.seek(self._pointers()[1])
        self._last = None

    def _close(self):
        if self._is_configured:
            if self._sem.has_deposit_lock():
                self._sem.deposit_unlock()
            if self._sem.has_notification_lock():
                self._sem.notification_unlock()
            del(self._pool_pos)
            del(self._pool_ptr)
            del(self._indx)
            del(self._slaw_version)
            self._mmap.close()
            self._fh.close()
            del(self._mmap)
            del(self._fh)
            del(self._index_capacity)
            del(self._header_size)
            del(self._size)
            del(self._sem)
            del(self._perms)
            self._is_configured = False

    def _pool_size(self):
        return self._size

    def dispose(self):
        self._close()
        fh = self._lock_pool()
        fh.close()
        pool_conf = plasma.slaw.read_slaw_file(self._pool_conf_file).ingests()
        sem = SemaphoreSet(pool_conf['sem-key'])
        sem.destroy()
        os.rmdir(self._notification_directory)
        os.unlink(self._pool_conf_file)
        os.unlink(self._mmap_conf_file)
        os.unlink(self._mmap_file)
        os.rmdir(self._directory)

    def sleep(self):
        fh = self._lock_pool()
        try:
            pool_conf = plasma.slaw.read_slaw_file(self._pool_conf_file).ingests()
            sem = SemaphoreSet(pool_conf['sem-key'])
            sem.destroy()
            fh.close()
        finally:
            fh.close()

class OldV0MMapPool(MMapPool):
    ## junk
    def bootstrap(self, pool_data):
        self.__name           = pool_data['name']
        self.__mmap_file      = pool_data['mmap_file']
        self.__pool_directory = pool_data['directory']
        self.__pool_type      = pool_data['type']
        self.__pool_version   = pool_data['version']
        self.__config_file    = pool_data['config_file']
        self.__mmap_config    = pool_data['mmap_config']
        conf = dict(plasma.slaw.read_slaw_file(pool_data['mmap_config']).Ingests())
        fsize = conf.get('file-size', None)
        if fsize is None:
            raise ConfigError("Pool '%s' config missing 'file-size'" % name)
        hsize = conf.get('header-size', 0)
        if hsize <= 0:
            hsize = 16
        self.__chunks = dict()
        self.__chunks['conf'] = dict()
        self.__chunks['ptrs'] = dict()
        self.__chunks['conf']['file_size'] = fsize
        self.__chunks['conf']['header_size'] = hsize
        self.__chunks['conf']['sem_key'] = pool_data['sem_key']
        self.__chunks['conf']['next_index'] = None
        self.__chunks['perm'] = pool_data['perms']

    def read_header(self):
        self.__chunks['conf']['mmap_version'] = 0
        self.__chunks['conf']['flags'] = 0
        (old, new) = struct.unpack('QQ', self.__read(16))
        self.__chunks['ptrs']['old'] = old
        self.__chunks['ptrs']['new'] = new
        self.__chunks['ptrs']['__pos'] = 0
        if self.__chunks['conf']['header_size'] >= POOL_MMAP_V0_HEADER_SIZE:
            mgc = struct.unpack('Q', self.read(8))
            slaw_version = (mgc >> POOL_MMAP_SLAW_VERSION_SHIFTY) & 0xff
            mgc &= ~(0xff << POOL_MMAP_SLAW_VERSION_SHIFTY)
            if mgc != POOL_MMAP_MAGICV0:
                raise CorruptPool("Magic string is wrong")
            (slaw_version, slaw_type, flags) = struct.unpack('>BBH', self.__read(4))
            self.__slaw_version = slaw_version
            if self.__chunks['conf']['header_size'] > POOL_MMAP_V0_HEADER_SIZE:
                self.__pindex = PoolIndex(self)
            else:
                self.__pindex = None
        else:
            ## pre-0 pool
            self.__slaw_version = 1
            self.__pindex = None

    def write_config_file(self, perms, file_size, header_size, idx_cap):
        conf = Protein(ingests=obmap({ 'file-size': unt64(file_size), 'header-size': unt64(header_size), 'index-capacity': unt64(idx_cap) }))
        plasma.slaw.write_slaw_file(self.__mmap_config, conf, self.__slaw_version)
        os.chmod(self.__mmap_config, 0666)

    def init_pool_index(self, capacity):
        self.seek(POOL_MMAP_V0_HEADER_SIZE)
        PoolIndex.create(self.__pool_mmap_fh, capacity, 0)

    def initialize_header(self, slaw_version, idx_cap):
        self.__slaw_version = slaw_version
        self.seek(0)
        self.write(struct.pack('QQQ', 0, 0, POOL_MMAP_MAGICV0 | (slaw_version << POOL_MMAP_SLAW_VERSION_SHIFTY)))
        if idx_cap > 0:
            self.init_pool_index(idx_cap)

    def save_default_config(self):
        conf = Protein(ingests=obmap({ 'type': 'mmap', 'pool-version': int32(POOL_DIRECTORY_VERSION_CONFIG_IN_FILE), 'sem-key': self.__chunks['conf']['sem_key'], 'perms': v3int32(self.__chunks['perm']['mode'], self.__chunks['perm']['uid'], self.__chunks['perm']['gid']) }))
        plasma.slaw.write_slaw_file(self.__config_file, conf, self.__slaw_version)
        os.chmod(self.__config_file, self.__chunks['perm']['mode'])

    def create(self, name, options):
        size = options.Ingests().get('size', SMALL_POOL_SIZE)
        if isinstance(size, (str, unicode)):
            m = re.match('^([0-9]+)([kMGT]?)$', size)
            if m:
                size = int(m.group(1))
                mult = m.group(2)
            else:
                m = re.match('^([0-9]*\.[0-9]+)([kMGT]?)$', size)
                if m:
                    size = float(m.group(1))
                    mult = m.group(2)
                else:
                    size = 1
                    mult = 'M'
            if mult == 'k':
                size *= 2**10
            elif mult == 'M':
                size *= 2**20
            elif mult == 'G':
                size *= 2**30
            elif mult == 'T':
                size *= 2**40
            size = int(size)
        index_capacity = options.Ingests().get('index-capacity', 0)
        pool_version = 4
        header_size = PoolIndex.size(index_capacity)
        min_size = POOL_MMAP_V0_HEADER_SIZE + header_size
        max_size = POOL_SIZE_MAX
        if size < min_size or size > max_size:
            raise PoolSizeError("Invalid size %d; must be at least %d and no more than %d" % (size, min_size, max_size))
        dirname = self.pool_dir(name)
        self.__mmap_config = os.path.join(dirname, 'mmap.conf')
        self.__config_file = os.path.join(dirname, 'pool.conf')
        self.__mmap_file = os.path.join(dirname, '%s.mmap-pool' % os.path.basename(dirname))
        self.__slaw_version = 2
        self.__mmap_file = ''
        try:
            fh = os.fdopen(os.open(self.__mmap_file, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0666), 'rw')
        except:
            unlink(self.__mmap_file)
            fh = os.fdopen(os.open(self.__mmap_file, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0666), 'rw')
        self.__chunks = {
            'conf': {
                'sem-key': sem_key,
            },
            'perm': {
                'uid': -1,
                'gid': -1,
                'mode': 0666,
            },
        }
        os.ftruncate(fh.fileno(), size)
        self.__pool_mmap_fh = fh
        self.write_config_file(self.__chunks['perm'], size, min_size, index_capacity)
        self.save_default_config()
        
