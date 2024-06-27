import sys, os, re
from loam import *
from plasma.const import *
from plasma.exceptions import *
from plasma.protein import Protein
from plasma.hose import Hose
from plasma.pool.mmap import MMapPool
import plasma.config

class LocalHose(Hose):

    def __init__(self):
        self._is_configured = False
        self._state = HOSE_STATE_INITIAL

    ## --------------------- ##
    ## Creation and Disposal ##
    ## --------------------- ##

    def create(self, path, pool_type='mmap', options=None):
        self._check_state('create')
        pool = MMapPool(path)
        if isinstance(options, Protein):
            options = options.ingests()
        args = dict()
        for key in ('resizable', 'single_file', 'size', 'index_capacity', 'mode', 'uid', 'gid', 'stop_when_full', 'frozen', 'auto_dispose', 'checksum', 'sync'):
            xkey = key.replace('_', '-')
            if options.has_key(xkey):
                args[key] = options[xkey]
        retval = pool.create(**args)
        self._close('create')
        return retval

    def dispose(self, path):
        self._check_state('dispose')
        pool = MMapPool(path)
        retval = pool.dispose()
        self._close('dispose')
        return retval

    def rename(self, old_path, new_path):
        self._check_state('rename')
        pool = MMapPool(old_path)
        retval = pool.rename(new_path)
        self._close('rename')
        return retval

    def exists(self, path):
        self._check_state('exists')
        pool = MMapPool(path)
        retval = pool.exists()
        self._close('exists')
        return retval

    def sleep(self, path):
        self._check_state('sleep')
        pool = MMapPool(path)
        retval = pool.sleep()
        self._close('sleep')
        return retval

    def check_in_use(self, path):
        self._check_state('check_in_use')
        pool = MMapPool(path)
        retval = pool.check_in_use()
        self._close('check_in_use')
        return retval

    ## ---------------------------- ##
    ## Connecting and Disconnecting ##
    ## ---------------------------- ##

    def participate(self, path, options=None):
        self._check_state('participate')
        self._pool = MMapPool(path)
        self._pool.participate()
        self._hose_name = path
        self._is_configured = True
        self._state = HOSE_STATE_PARTICIPATE
        try:
            self._hose_index = self.newest_index()
        except PoolNoSuchProteinException:
            self._hose_index = -1
        self._pool.runout()

    def participate_creatingly(self, path, pool_type, create_options, participate_options=None):
        self._check_state('participate_creatingly')
        self._pool = MMapPool(path)
        if isinstance(create_options, Protein):
            create_options = create_options.ingests()
        args = dict()
        for key in ('resizable', 'single_file', 'size', 'index_capacity', 'mode', 'uid', 'gid', 'stop_when_full', 'frozen', 'auto_dispose', 'checksum', 'sync'):
            xkey = key.replace('_', '-')
            if create_options.has_key(xkey):
                args[key] = create_options[xkey]
        self._pool.participate_creatingly(**args)
        self._hose_name = path
        self._is_configured = True
        self._state = HOSE_STATE_PARTICIPATE
        try:
            self._hose_index = self.newest_index()
        except PoolNoSuchProteinException:
            self._hose_index = -1
        self._pool.runout()

    def withdraw(self):
        self._check_state('withdraw')
        self._pool.withdraw()
        del(self._pool)
        del(self._hose_index)
        self._close('withdraw')

    def list_pools(self):
        self._check_state('list')
        retval = self.list_ex(plasma.config.ob_pools_dir())
        self._close('list_pools')
        return retval

    def list_ex(self, path=None):
        self._check_state('list_ex')
        root = plasma.config.ob_pools_dir()
        root = re.sub('/{2,}', '/', root)
        if root.endswith('/'):
            root = root[:-1]
        if path is None:
            path = ''
        addon = ''
        strip = ''
        if path.startswith('local:'):
            path = path[6:]
            addon = 'local:'
        elif not path.startswith('/'):
            path = '%s/%s' % (root, path)
        strip = '%s/' % path
        path = re.sub('/{2,}', '/', path)
        if path.endswith('/'):
            path = path[:-1]
        if not path.startswith('/'):
            raise PoolPoolnameBadthException(path, "doesn't reference an absolute path")
        if not os.path.exists(path):
            raise PoolNoSuchPoolException(path)
        pools = oblist()
        def walker(arg, dirname, fnames):
            if 'pool.conf' in fnames:
                pools.append('%s%s' % (addon, dirname[len(strip):]))
                for i in reversed(range(len(fnames))):
                    if os.path.isfile(os.path.join(dirname, fnames[i])):
                        del(fnames[i])
                return None
            for x in fnames:
                y = os.path.join(dirname, x)
                if os.path.isfile(y) and os.path.getsize(y) % 4096 == 0:
                    pools.append('%s%s' % (addon, y[len(strip):]))
        os.path.walk(path, walker, None)
        self._close('list_ex')
        return pools
        #n = len(path)+1
        #return oblist(x[n:] for x in pools)

    def name(self):
        return self._pool.name()

    def get_hose_name(self):
        return self._hose_name

    def set_hose_name(self, name):
        self._hose_name = name

    def get_info(self, hops=0):
        self._check_state('info')
        return self._pool.get_info()

    def newest_index(self):
        self._check_state('newest_index')
        return self._pool.newest_index()

    def oldest_index(self):
        self._check_state('oldest_index')
        return self._pool.oldest_index()

    def deposit(self, protein):
        self._check_state('deposit')
        index = self._pool.deposit(protein)
        return index

    def deposit_ex(self, protein):
        self._check_state('deposit_ex')
        (index, timestamp) = self._pool.deposit_ex(protein)
        return obmap({ 'index': index, 'timestamp': timestamp })

    def curr(self):
        self._check_state('curr')
        protein = self._pool.curr()
        self._hose_index = protein.index()
        return protein

    def next(self):
        self._check_state('next')
        try:
            protein = self._pool.await_next()
        except PoolAwaitTimedoutException:
            raise PoolNoSuchProteinException("already at newest protein")
        self._hose_index = protein.index()
        return protein

    def prev(self):
        self._check_state('prev')
        protein = self._pool.prev()
        self._hose_index = protein.index()
        return protein

    def fetch(self, ops, clamp=False):
        self._check_state('sub_fetch')
        allowed_keys = set(('idx', 'des', 'ing', 'roff', 'rbytes'))
        resp = oblist()
        for op in ops:
            try:
                repeat = True
                while repeat:
                    repeat = False
                    try:
                        protein = self.nth_protein(op.idx)
                        op.set_protein(protein, self._pool.slaw_version())
                    except PoolNoSuchProteinException:
                        if not clamp:
                            raise
                        old = self.oldest_index
                        new = self.newest_index
                        if op.idx < old:
                            op.idx = old
                            repeat = True
                        elif op.idx > new:
                            op.idx = new
                            repeat = True
                        else:
                            raise
            #except ObException, e:
            except(ObException, e):
                op.set_exception(sys.exc_info())
        return ops

    def nth_protein(self, idx):
        self._check_state('nth_protein')
        protein = self._pool.nth_protein(idx)
        self._hose_index = protein.index()
        return protein

    def index_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        self._check_state('index_lookup')
        return self._pool.index_lookup(timestamp, whence, direction)

    def probe_back(self, search):
        self._check_state('probe_back')
        self._pool.save_last()
        while True:
            try:
                protein = self._pool.prev()
            except PoolNoSuchProteinException:
                self._pool.restore_last()
                raise
            if protein.descrips() and protein.matches(search, SEARCH_GAP):
                self._hose_index = protein.index()
                return protein

    def probe_frwd(self, search):
        self._check_state('probe_frwd')
        self._pool.save_last()
        orig_pos = self._pool.tell()
        while True:
            try:
                protein = self._pool.await_next()
            except PoolAwaitTimedoutException:
                self._pool.restore_last()
                self._pool.seek(orig_pos)
                raise PoolNoSuchProteinException("no matching protein remaining in pool")
            if protein.descrips() and protein.matches(search, SEARCH_GAP):
                self._hose_index = protein.index()
                return protein

    def await_next(self, timeout=POOL_WAIT_FOREVER, interrupt=None):
        self._check_state('await_next')
        return self._pool.await_next(timeout, interrupt=interrupt)

    def await_probe_frwd(self, search, timeout=POOL_WAIT_FOREVER, interrupt=None):
        self._check_state('await_probe_frwd')
        self._pool.save_last()
        #last = self._pool._last
        while True:
            try:
                protein = self._pool.await_next(timeout, interrupt=interrupt)
            except (PoolAwaitTimedoutException, PoolAwaitWokenException):
                self._pool.restore_last()
                #self._pool._last = last
                raise
            if protein.matches(search, SEARCH_GAP):
                self._hose_index = protein.index()
                return protein

    def await_nth(self, idx, timeout=POOL_WAIT_FOREVER, interrupt=None):
        self._check_state('await_next')
        return self._pool.await_nth(idx, timeout, interrupt)

    def close(self):
        if self._is_configured:
            self.withdraw()

    def _close(self, final_command=None):
        self._is_configured = False
        if final_command is not None:
            self._final_command = final_command
            self._state = HOSE_STATE_FINAL

    def awaiter(self):
        return self._pool.add_awaiter()


    def SeekTo(self, idx):
        try:
            self._hose_index = self._pool.seek_to(idx)
        except PoolNoSuchProteinException:
            self._hose_index = self.newest_index()
            raise

