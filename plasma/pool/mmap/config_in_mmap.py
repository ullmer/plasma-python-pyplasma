import os
from loam import *
import plasma.slaw
from plasma.const import *
from plasma.protein import Protein
from plasma.pool.mmap.v1 import V1MMapPool
from plasma.pool.mmap.chunks import *
from plasma.pool.util import makedirs

class ConfigInMMapPool(V1MMapPool):
    pool_type = obstring('mmap')

    def __init__(self, name):
        self._name = name
        self._pool_version = POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP
        self._directory = self._pool_dir(name)
        self._mmap_file = os.path.join(self._directory, 'mmap-pool')
        self._pool_conf_file = os.path.join(self._directory, 'pool.conf')
        self._mmap_conf_file = None
        self._notification_directory = os.path.join(self._directory, 'notification')
        self._fifo_file = None
        self._is_configured = False
        self._slaw_version = None

    def _save_default_config(self):
        config = Protein(ingests={ 'type': 'mmap', 'pool-version': int32(self._pool_version) })
        plasma.slaw.write_slaw_file(self._pool_conf_file, config)

    def rename(self, new_name):
        self._close()
        new_pool = type(self)(new_name)
        fh = file(self._mmap_file)
        fh.seek(8)
        conf = Chunk.load(fh)
        header_size = conf.header_size - (8 + conf.size())
        chunks = { 'conf': conf }
        while header_size > 0:
            chnk = Chunk.load(fh)
            header_size -= chnk.size()
            chunks[chnk.name()] = chnk
        perms = {
            'mode': chunks['perm'].mode,
            'uid': chunks['perm'].uid,
            'gid': chunks['perm'].gid
        }
        fh.close()
        makedirs(new_pool._directory, perms['mode'], perms['uid'], perms['gid'])
        os.rename(self._notification_directory, new_pool._notification_directory)
        self._safe_rename(self._pool_conf_file, new_pool._pool_conf_file)
        #self._safe_rename(self._mmap_conf_file, new_pool._mmap_conf_file)
        self._safe_rename(self._mmap_file, new_pool._mmap_file)
        try:
            os.rmdir(self._directory)
        except:
            pass

