import os, struct
from loam import *
from plasma.const import *
from plasma.exceptions import *
from plasma.pool.mmap import MMapPool
from plasma.sem_ops import SemaphoreSet
from plasma.pool.util import makedirs, with_umask
from plasma.pool.mmap.chunks import *
from plasma.slaw import BINARY_MAGIC

#DEFAULT_MODE = 0777
DEFAULT_MODE = 0o777
DEFAULT_OWNER = -1
DEFAULT_GROUP = -1

class V1MMapPool(MMapPool):
    pool_type = obstring('mmap')
    _mmap_version = 1

    def __init__(self, name):
        raise AbstractClassError("%s.__init__ must be defined in a subclass" % type(self).__name__)

    def _write_header(self):
        #print 'writing header to %d' % self._fh.fileno()
        self._fh.write(BINARY_MAGIC)
        self._fh.write(struct.pack('>BBH', self._slaw_version, self._slaw_type, self._pool_flags))

    @with_umask
    def _create(self, options=None):
        self._slaw_version = 2
        self._slaw_type = 2
        self._pool_flags = 0
        self._size = self._get_size(options)
        #self._pool_version = self._get_version(options)
        self._perm = self._get_permissions(options)
        self._index_capacity = self._get_index_capacity(options)
        conf = confChunk()
        ptrs = ptrsChunk()
        perm = permChunk()
        header_size = 8 + conf.size() + ptrs.size() + perm.size()
        if self._index_capacity > 0:
            indx = indxChunk()
            indx.signature = indx.signature
            indx.initialize(self._index_capacity)
            header_size += indx.size()
        else:
            indx = None
        if self._size < header_size or self._size > POOL_SIZE_MAX:
            raise PoolInvalidSizeException("Invalid size %d; must be at least %d and no more than %d" % (self._size, header_size, POOL_SIZE_MAX))
        makedirs(self._directory, self._perm['mode'], self._perm['uid'], self._perm['gid'])
        makedirs(self._notification_directory, self._perm['mode'], self._perm['uid'], self._perm['gid'])
        self._sem = SemaphoreSet()
        fh = os.fdopen(os.open(self._mmap_file, os.O_RDWR | os.O_CREAT | os.O_EXCL, self._perm['mode'] & 0o666), 'rb+')
        #fh = os.fdopen(os.open(self._mmap_file, os.O_RDWR | os.O_CREAT | os.O_EXCL, self._perm['mode'] & 0666), 'rb+')
        #print 'fh = %s (%d)' % (self._mmap_file, fh.fileno())
        self._fh = fh
        os.chown(self._mmap_file, self._perm['uid'], self._perm['gid'])
        os.ftruncate(fh.fileno(), self._size)
        fh.seek(0)
        self._write_header()
        #conf.set_pool(fh)
        #ptrs.set_pool(fh)
        #perm.set_pool(fh)
        #if indx is not None:
        #    indx.set_pool(fh)
        conf.mmap_version = 1
        conf.header_size = header_size
        conf.file_size = self._size
        conf.sem_key = self._sem.key()
        self._flags = 0
        if options.get('stop-when-full', False):
            self._flags |= POOL_FLAG_STOP_WHEN_FULL
        if options.get('frozen', False):
            self._flags |= POOL_FLAG_FROZEN
        if options.get('auto-dispose', False):
            self._flags |= POOL_FLAG_AUTO_DISPOSE
        if options.get('checksum', False):
            self._flags |= POOL_FLAG_CHECKSUM
        if options.get('sync', False):
            self._flags |= POOL_FLAG_SYNC
        conf.flags = self._flags
        conf.next_index = 0
        ptrs.oldest_entry = conf.header_size
        ptrs.newest_entry = 0
        perm.mode = self._perm['mode']
        perm.uid = self._perm['uid']
        perm.gid = self._perm['gid']
        conf.set_pool(fh, 8)
        ptrs.set_pool(fh, conf.end())
        perm.set_pool(fh, ptrs.end())
        if indx is not None:
            indx.set_pool(fh, perm.end())
            indx.initialize(self._index_capacity)
        #print 'conf = %s' % conf
        #print 'indx = %s' % indx
        #print 'ptrs = %s' % ptrs
        #print 'perm = %s' % perm
        self._save_default_config()

    def _pointers(self):
        return self._chunks['ptrs'].all()

    def _set_pointers(self, old, new):
        self._chunks['ptrs'].oldest_entry = old
        self._chunks['ptrs'].newest_entry = new

    def _open(self):
        self._mmap_open()
        (magic,sv,st,flags) = struct.unpack('>4sBBH', self._mmap.read(8))
        if magic != BINARY_MAGIC:
            raise PoolCorruptException("invalid magic number")
        self._slaw_version = sv
        self._slaw_flags = flags
        conf = Chunk.load(self._mmap)
        self._header_size = conf.header_size
        header_size = self._header_size - (8 + conf.size())
        chunks = { 'conf': conf }
        while header_size > 0:
            chnk = Chunk.load(self._mmap)
            header_size -= chnk.size()
            chunks[chnk.name()] = chnk
        self._size = conf.file_size
        self._sem = SemaphoreSet(conf.sem_key)
        self._flags = conf.flags
        self._perm = {
            'mode': chunks['perm'].mode,
            'uid': chunks['perm'].uid,
            'gid': chunks['perm'].gid
        }
        if chunks.has_key('indx'):
            self._indx = chunks['indx']
        else:
            self._indx = None
        self._chunks = chunks
        self._mmap.seek(0)
        self._pool_ptr = 0
        self._pool_pos = 0
        self._is_configured = True
        self._pool_index = conf.next_index
        (old, new) = self._pointers()
        #print 'old = %d; new = %d' % (old, new)
        if new >= old:
            self.seek(new)
        else:
            self.seek(old)
        self._current_state = {
            'position':  self.tell(),
            'timestamp': None,
            'index':     -1,
            'size':      0,
            'protein':   None,
        }
        self._last = None

    def _close(self):
        if self._is_configured:
            if self._sem.has_deposit_lock():
                self._sem.deposit_unlock()
            if self._sem.has_notification_lock():
                self._sem.notification_unlock()
            del(self._pool_index)
            del(self._pool_ptr)
            del(self._pool_pos)
            del(self._indx)
            del(self._perm)
            del(self._flags)
            del(self._sem)
            del(self._size)
            del(self._header_size)
            del(self._chunks)
            del(self._slaw_flags)
            del(self._slaw_version)
            self._mmap.close()
            self._fh.close()
            del(self._mmap)
            del(self._fh)
            self._is_configured = False

    def _pool_size(self):
        return self._chunks['conf'].file_size

    def dispose(self):
        self._close()
        fh = self._lock_pool()
        fh.seek(8)
        conf = Chunk.load(fh)
        sem = SemaphoreSet(conf.sem_key)
        sem.destroy()
        fh.close()
        os.rmdir(self._notification_directory)
        if self._pool_conf_file is not None:
            os.unlink(self._pool_conf_file)
        if self._mmap_conf_file is not None:
            os.unlink(self._mmap_conf_file)
        os.unlink(self._mmap_file)
        try:
            os.rmdir(self._directory)
        except:
            pass

    def sleep(self):
        fh = self._lock_pool()
        try:
            fh.seek(8)
            conf = Chunk.load(fh)
            sem = SemaphoreSet(conf.sem_key)
            sem.destroy()
            fh.close()
        finally:
            fh.close()

    #def rename(self, new_name):
    #    pass

    #def sleep(self):
    #    pass

    #def check_in_use(self):
    #    pass

    def participate(self, options):
        self._open()
    #    pass

    #def withdraw(self):
    #    pass

    #def name(self):
    #    return self._name

    #def newest_index(self):
    #    pass

    #def oldest_index(self):
    #    pass

    #def deposit(self, protein):
    #    pass

    #def nth_protein(self, idx):
    #    pass

    #def index_lookup(self, timestamp, whence=TIMESTAMP_RELATIVE, direction=DIRECTION_ABSOLUTE):
    #    pass

    #def probe_back(self, idx, search):
    #    pass

    #def probe_frwd(self, idx, search):
    #    pass

    #def await_next(self, idx, timeout=POOL_WAIT_FOREVER):
    #    pass

    #def await_probe_frwd(self, idx, timeout=POOL_WAIT_FOREVER):
    #    pass


class OldV1MMapPool(MMapPool):
    @classmethod
    def pool_open(cls, pool_data):
        pool = cls()
        pool.bootstrap(pool_data)
        return pool

    def bootstrap(self, pool_data):
        self.__name           = pool_data['name']
        self.__mmap_file      = pool_data['mmap_file']
        self.__pool_directory = pool_data['directory']
        self.__pool_type      = pool_data['type']
        self.__pool_version   = pool_data['version']
        self.__config_file    = pool_data['config_file']

    def read_header(self):
        mgc = self.read(4)
        if mgc != BINARY_MAGIC:
            raise CorruptPool("Magic string (%s) isn't %s" % (' '.join('%02x' % x for x in mgc), ' '.join('%02x' % x for x in BINARY_MAGIC)))
        (slaw_version, slaw_type, flags) = struct.unpack('>BBH', self._read(4))
        self.__slaw_version = slaw_version
        conf = self._read_chunk()
        self.__chunks['conf'] = conf
        while self.tell() < conf['header_size']:
            chunk = self.__read_chunk()
            if chunk['__type'] == 'indx':
                self.__pindex = chunk['index']
            else:
                chunks[chunk['__type']] = chunk
        

    def __read_chunk(self):
        pos = self.tell()
        (x,) = struct.unpack('Q', self.read(8))
        if x >> 32 != 0x1badd00d:
            raise HoseCommandException(POOL_CORRUPT, "bad chunk signature")
        (chunk_type,) = struct.unpack('4s', struct.pack('>I', x & 0xffffffff))
        (chunk_len,) = struct.unpack('Q', self.read(8))
        chunk_data = self.read(8 * (chunk_len - 2))
        if chunk_type == 'conf':
            chunk = self.__parse_conf_chunk(chunk_data)
        elif chunk_type == 'perm':
            chunk = self.__parse_perm_chunk(chunk_data)
        elif chunk_type == 'ptrs':
            chunk = self.__parse_ptrs_chunk(chunk_data)
        elif chunk_type == 'indx':
            self.seek(pos+16)
            chunk = { 'index': PoolIndex(self) }
        else:
            chunk = dict()
        chunk['__type'] = chunk_type
        chunk['__pos'] = pos
        return chunk

    def __parse_conf_chunk(self, data):
        vals = struct.unpack('6q', data)
        return {
            'mmap_version': vals[0],
            'file_size': vals[1],
            'header_size': vals[2],
            'sem_key': vals[3],
            'flags': vals[4],
            'next_index': vals[5]
        }

    def __parse_perm_chunk(self, data):
        vals = struct.unpack('3q', data)
        return {
            'mode': vals[0],
            'uid': vals[1],
            'gid': vals[2]
        }

    def __parse_ptrs_chunk(self, data):
        vals = struct.unpack('2q', data)
        return {
            'old': vals[0],
            'new': vals[1]
        }

    def read_header(self):
        self.__chunks['conf']['mmap_version'] = 0
        self.__chunks['conf']['flags'] = 0
        (old, new) = struct.unpack('QQ', self.__read(16))
        self.__chunks['ptrs']['old'] = old
        self.__chunks['ptrs']['new'] = new
        self.__chunks['ptrs']['__pos'] = 0
        if self.__chunks['conf']['header_size'] >= POOL_MMAP_V0_HEADER_SIZE:
            mgc = self.__read(4)
            if mgc != BINARY_MAGIC:
                raise CorruptPool("Magic string (%s) isn't %s" % (' '.join('%02x' % x for x in mgc), ' '.join('%02x' % x for x in BINARY_MAGIC)))
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

