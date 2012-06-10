import os
from loam import *
from plasma.const import *
from plasma.pool.mmap.v1 import V1MMapPool

class SingleFileMMapPool(V1MMapPool):
    pool_type = obstring('mmap')

    def __init__(self, name):
        self._name = name
        self._pool_version = POOL_DIRECTORY_VERSION_SINGLE_FILE
        self._mmap_file = self._pool_dir(name)
        (self._directory, pool_name) = os.path.split(self._mmap_file)
        self._pool_conf_file = None
        self._mmap_conf_file = None
        self._notification_directory = os.path.join(self._directory, '.notification', pool_name)
        self._fifo_file = None

    def _save_default_config(self):
        return None

    def rename(self, new_name):
        self._close()
        new_pool = type(self)(new_name)
        fh = file(self._mmap_file)
        fh.seek(8)
        conf = Chunk.load(fh)
        header_size = conf.header_size - (8 + conf.size())
        chunks = { 'conf': conf }
        while header_size > 0:
            chnk = Chunk.load(self._mmap)
            header_size -= chnk.size()
            chunks[chnk.name()] = chnk
        perms = {
            'mode': chunks['perm'].mode,
            'uid': chunks['perm'].uid,
            'gid': chunks['perm'].gid
        }
        fh.close()
        makedirs(new_pool._directory, perms['mode'], perms['uid'], perms['gid'])
        (not_pdir, junk) = os.path.split(new_pool._notification_directory)
        makedirs(not_pdir, perms['mode'], perms['uid'], perms['gid'])
        os.rename(self._notification_directory, new_pool._notification_directory)
        self._safe_rename(self._mmap_file, new_pool._mmap_file)
        try:
            os.rmdir(self._directory)
        except:
            pass
        (not_pdir, junk) = os.path.split(self._notification_directory)
        try:
            os.rmdir(not_pdir)
        except:
            pass

    def dispose(self):
        pass

    def rename(self, new_name):
        pass

    def sleep(self):
        pass

    def check_in_use(self):
        pass

    def participate(self, options):
        pass

    def withdraw(self):
        pass

    def name(self):
        return self._name

    def newest_index(self):
        pass

    def oldest_index(self):
        pass

    def deposit(self, protein):
        pass

    def nth_protein(self, idx):
        pass

    def index_lookup(self, timestamp, whence=TIMESTAMP_RELATIVE, direction=DIRECTION_ABSOLUTE):
        pass

    def probe_back(self, idx, search):
        pass

    def probe_frwd(self, idx, search):
        pass

    def await_next(self, idx, timeout=POOL_WAIT_FOREVER):
        pass

    def await_probe_frwd(self, idx, timeout=POOL_WAIT_FOREVER):
        pass


class OldSingleFileMMapPool(V1MMapPool):
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
        if mgc != MAGIC:
            raise CorruptPool("Magic string (%s) isn't %s" % (' '.join('%02x' % x for x in mgc), ' '.join('%02x' % x for x in MAGIC)))
        (slaw_version, slaw_type, flags) = struct.unpack('>BBH', self.__read(4))
        self.__slaw_version = slaw_version
        conf = self.__read_chunk()
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
            if mgc != MAGIC:
                raise CorruptPool("Magic string (%s) isn't %s" % (' '.join('%02x' % x for x in mgc), ' '.join('%02x' % x for x in MAGIC)))
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


