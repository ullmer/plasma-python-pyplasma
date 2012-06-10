import os
from loam import *
import plasma.slaw
from plasma.const import *
from plasma.pool.mmap.v0 import V0MMapPool

POOL_SIZE_SMALL   =   1 * (2**20) ##   1 MB
POOL_SIZE_MEDIUM  =  10 * (2**20) ##  10 MB
POOL_SIZE_LARGE   = 100 * (2**20) ## 100 MB
POOL_SIZE_OBSCENE =   2 * (2**30) ##   2 GB
POOL_SIZE_MAX     =   8 * (2**40) ##   8 TB

class ConfigInFileMMapPool(V0MMapPool):
    pool_type = obstring('mmap')

    def __init__(self, name):
        self._name = name
        self._pool_version = POOL_DIRECTORY_VERSION_CONFIG_IN_FILE
        self._directory = self._pool_dir(name)
        (junk, pool_name) = os.path.split(self._directory)
        self._mmap_file = os.path.join(self._directory, '%s.mmap-pool' % pool_name)
        self._pool_conf_file = os.path.join(self._directory, 'pool.conf')
        self._mmap_conf_file = None
        self._notification_directory = os.path.join(self._directory, 'notification')
        self._fifo_file = None

    def _save_default_config(self):
        config = Protein(ingests={
            'type': 'mmap',
            'pool-version': int32(self._pool_version),
            'perms': v3int32(int32(self._perms['mode']),
                             int32(self._perms['uid']),
                             int32(self._perms['gid'])),
            'sem-key': int32(self._sem.key())
        })
        plasma.slaw.write_slaw_file(self._pool_conf_file, config, self._slaw_version)

    def _save_mmap_config(self):
        header_size = POOL_MMAP_V0_HEADER_SIZE
        if self._indx:
            header_size += self._indx.size()
        config = Protein(ingests={ 'header-size': unt64(header_size), 'file-size': unt64(self._size), 'index-capacity': unt64(self._indx.capacity) })
        plasma.slaw.write_slaw_file(self._mmap_conf_file, config, self._slaw_version)

    def rename(self, new_name):
        self._close()
        new_pool = type(self)(new_name)
        pool_conf = plasma.slaw.read_slaw_file(self._pool_conf_file).ingests()
        perms = pool_conf.ingests().get('perms')
        makedirs(new_pool._directory, perms.x, perms.y, perms.z)
        os.rename(self._notification_directory, new_pool._notification_directory)
        self._safe_rename(self._pool_conf_file, new_pool._pool_conf_file)
        self._safe_rename(self._mmap_conf_file, new_pool._mmap_conf_file)
        self._safe_rename(self._mmap_file, new_pool._mmap_file)
        try:
            os.rmdir(self._directory)
        except:
            pass

