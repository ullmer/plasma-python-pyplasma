import os, struct, select, datetime, pwd, grp, mmap, random, time, logging, fcntl, traceback
from loam import *
from plasma.const import *
from plasma.exceptions import *
from plasma.sem_ops import SemaphoreSet
import plasma.util
from plasma.pool.util import makedirs, pools_dir, with_umask, with_config_lock
import plasma.slaw
from plasma.protein import Protein

DEFAULT_MODE = 0777
DEFAULT_OWNER = -1
DEFAULT_GROUP = -1
NULL_BYTE = chr(0)
CACHE_SIZE = 1000

class MMapPool(object):

    @classmethod
    def _pool_dir(cls, name):
        path = name
        if path.startswith('local:'):
            path = path[6:]
        if not path.startswith('/'):
            path = os.path.join(pools_dir(), path)
        return path

    @classmethod
    def _get_version(cls, options):
        if options is None:
            return POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP
        if type(options) == Protein:
            options = options.ingests()
        if options.get('resizable', True):
            pool_version = POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP
        else:
            pool_version = POOL_DIRECTORY_VERSION_CONFIG_IN_FILE
        if options.get('single-file', False):
            pool_version = POOL_DIRECTORY_VERSION_SINGLE_FILE
        return pool_version

    @classmethod
    @with_config_lock
    def create(cls, name, options=None):
        return cls._xcreate(name, options)

    @classmethod
    def _xcreate(cls, name, options):
        path = cls._pool_dir(name)
        if os.path.exists(path):
            raise PoolExistsException(name, path)
        version = cls._get_version(options)
        if version == POOL_DIRECTORY_VERSION_SINGLE_FILE:
            pool = SingleFileMMapPool(name)
        elif version == POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP:
            pool = ConfigInMMapPool(name)
        elif version == POOL_DIRECTORY_VERSION_CONFIG_IN_FILE:
            pool = ConfigInFileMMapPool(name)
        else:
            raise PoolTypeBadthException("unknown pool version %d" % version)
        pool._create(options)
        return pool

    def __init__(self, name):
        raise AbstractClassError("%s.__init__ must be defined in a subclass" % type(self).__name__)

    def name(self):
        return self._name

    def _get_size(self, options):
        if options is None:
            return POOL_SIZE_SMALL
        if type(options) == Protein:
            options = options.ingests()
        size = options.get('size', POOL_SIZE_SMALL)
        if size == 'small':
            return POOL_SIZE_SMALL
        if size == 'medium':
            return POOL_SIZE_MEDIUM
        if size == 'large':
            return POOL_SIZE_LARGE
        if size == 'obscene':
            return POOL_SIZE_OBSCENE
        if size == 'max':
            return POOL_SIZE_MAX
        size = plasma.util.sizestr_to_bytes(size)
        if size % 4096 != 0:
            size += (4096 - (size % 4096))
        return size

    def _get_index_capacity(self, options):
        if options is None:
            return 0
        if type(options) == Protein:
            options = options.ingests()
        return options.get('index-capacity', 0)

    def _get_permissions(self, options):
        if options is None:
            return {
                'mode': int64(0777),
                'uid': int64(-1),
                'gid': int64(-1)
            }
        if type(options) == Protein:
            options = options.ingests()
        x_mode = options.get('mode', DEFAULT_MODE)
        x_owner = options.get('owner', DEFAULT_OWNER)
        x_group = options.get('group', DEFAULT_GROUP)
        if isinstance(x_mode, (str, unicode)):
            x_mode = int(x_mode, 8)
        if isinstance(x_owner, (str, unicode)):
            pwent = pwd.getpwnam(x_owner)
            x_owner = pwent.pw_uid
        if isinstance(x_group, (str, unicode)):
            grent = grp.getgrnam(x_group)
            x_group = grent.gr_gid
        return {
            'mode': int64(x_mode),
            'uid': int64(x_owner),
            'gid': int64(x_group)
        }


    def _mmap_open(self):
        try:
            fh = self._fh
            mfh = self._mmap
        except:
            fh = None
            mfh = None
        if fh is not None or mfh is not None:
            raise IOError("mmap appears to already be open")
        path = self._mmap_file
        if not os.path.exists(path):
            raise PoolNoSuchPoolException(self._name, path)
        #fh = os.fdopen(os.open(path, os.O_RDWR|os.O_SHLOCK|os.O_BINARY))
        fh = os.fdopen(os.open(path, os.O_RDWR|os.O_SHLOCK))
        mfh = mmap.mmap(fh.fileno(), 0)
        self._fh = fh
        self._mmap = mfh
        return self._mmap

    @classmethod
    def _get_pool(cls, name):
        dirname = cls._pool_dir(name)
        if not os.path.exists(dirname):
            raise PoolNoSuchPoolException(name, dirname)
        if not os.path.isdir(dirname):
            ## single file pool -- version 1
            return SingleFileMMapPool(name)
        conf_file = os.path.join(dirname, 'pool.conf')
        if not os.path.exists(conf_file):
            raise PoolNoSuchPoolException(name, dirname)
        conf = plasma.slaw.read_slaw_file(conf_file).ingests()
        version = conf.get('pool-version', None)
        if version is None:
            raise PoolFileBadthException("Pool '%s' config missing 'pool-version'" % name)
        if version == POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP:
            return ConfigInMMapPool(name)
        if version == POOL_DIRECTORY_VERSION_CONFIG_IN_FILE:
            return ConfigInFileMMapPool(name)
        raise PoolWrongVersionException("'%s' had pool-version %d, but expected either %d or %d" % (version, POOL_DIRECTORY_VERSION_CONFIG_IN_FILE, POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP))

    @classmethod
    def participate(cls, name, options=None):
        pool = cls._get_pool(name)
        pool.participate(options)
        return pool

    @classmethod
    @with_config_lock
    def participate_creatingly(cls, name, create_options, participate_options=None):
        try:
            pool = cls._get_pool(name)
        except PoolNoSuchPoolException:
            pool = cls.create(name, create_options)
        pool.participate(participate_options)
        return pool

    def _max_protein_size(self):
        return self._pool_size() - self._header_size

    def seek(self, pos, whence=0):
        if whence == 0:
            abs_pos = pos
        elif whence == 1:
            abs_pos = self._pool_pos + pos
        if abs_pos < 0:
            traceback.print_stack()
            raise Exception("seeking into negative space")
        ptr = abs_pos % self._pool_size()
        if ptr < self._header_size:
            raise IOError("seeking pool into header (%d / %d)" % (ptr, abs_pos))
        self._mmap.seek(ptr, 0)
        self._pool_pos = abs_pos
        self._pool_ptr = ptr

    def _first_pos(self):
        (old, new) = self._pointers()
        if new < self._pool_size():
            ## pool hasn't wrapped, but oldest pointer may have advanced
            return old
        n = int(new / self._pool_size())
        return n * self._pool_size() + self._header_size

    def _first_index(self):
        pos = self.tell()
        first_pos = self._first_pos()
        self.seek(first_pos+8)
        try:
            (idx,) = struct.unpack('Q', self.read(8))
        except StompledException:
            return self._first_index()
        self.seek(pos)
        (old, new) = self._pointers()
        if new < old:
            ## pool is empty
            return -1, first_pos
        return idx, first_pos

    def read(self, size):
        start = self.tell()
        if start < 0:
            try:
                raise Exception("trying to read in negative space")
            except:
                traceback.print_stack()
                raise
        if size > self._max_protein_size():
            raise PoolProteinBiggerThanPoolException()
        if self._pool_ptr + size > self._pool_size():
            ## this shouldn't ever happen
            s1 = self._pool_size() - self._pool_ptr
            s2 = size - s1
            data = self.read(s1)
            self.seek(self._pool_pos + s1 + self._header_size)
            return data + self.read(s2)
        data = self._mmap.read(size)
        if len(data) != size:
            raise SlawWrongLengthException()
        (old, new) = self._pointers()
        if start < old:
            self.seek(old)
            logging.error("read of %s at %s stompled" % (size, start))
            raise StompledException()
        self._pool_ptr += size
        self._pool_pos += size
        return data

    def tell(self):
        return self._pool_pos

    def _jumpback(self):
        (old, new) = self._pointers()
        if self.tell() <= old:
            self.seek(old)
            raise PoolNoSuchProteinException("already at oldest protein")
        self.seek(-8, 1)
        try:
            jump = struct.unpack('Q', self.read(8))[0]
        except StompledException:
            raise PoolNoSuchProteinException("already at oldest protein")
        self.seek(-1 * jump, 1)
        return self.tell()

    def _lock_pool(self, wait=False):
        try:
            fh = self._fh
        except:
            fh = None
        if fh is None:
            path = self._mmap_file
            #fh = os.fdopen(os.open(path, os.O_RDONLY|os.O_SHLOCK|os.O_BINARY))
            fh = os.fdopen(os.open(path, os.O_RDONLY|os.O_SHLOCK))
        if wait:
            lock_op = fcntl.LOCK_EX
        else:
            lock_op = fcntl.LOCK_EX|fcntl.LOCK_NB
        try:
            fcntl.flock(fh.fileno(), lock_op)
        except IOError:
            raise PoolInUseException()
        return fh

    def _unlock_pool(self, fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_SH)

    @classmethod
    @with_config_lock
    def dispose(cls, name):
        pool = cls._get_pool(name)
        pool.dispose()

    @classmethod
    def check_in_use(cls, name):
        pool = cls._get_pool(name)
        try:
            fh = pool._lock_pool()
        except PoolInUseException:
            return False
        pool._unlock_pool(fh)
        return True

    def _read_entry(self, pos=None):
        (old, new) = self._pointers()
        if pos is None:
            pos = self.tell()
        if pos < old:
            self.seek(old)
            #self._last = None
            raise PoolNoSuchProteinException(pos)
        if pos > new:
            self.seek(new)
            #self._last = None
            raise PoolNoSuchProteinException(pos)
        self.seek(pos)
        (timestamp,index) = struct.unpack('dQ', self.read(16))
        protein = plasma.slaw.parse_slaw(self._slaw_version, self)
        jumpback = struct.unpack('Q', self.read(8))[0]
        self.seek(pos+jumpback)
        timestamp = obtimestamp(timestamp)
        protein.set_origin(self)
        protein.set_index(index)
        protein.set_timestamp(timestamp)
        self._last = {
            'start':     pos,
            'end':       self.tell(),
            'timestamp': timestamp,
            'index':     index,
            'size':      jumpback,
            'protein':   protein,
        }
        self._quick_cache(index, pos, jumpback)
        return protein

    def add_awaiter(self):
        self._sem.notification_lock()
        try:
            fh = self._open_fifo()
            #print 'notification unlock 1'
            #self._sem.notification_unlock()
        finally:
            #print 'notification unlock 2'
            self._sem.notification_unlock()
        return fh

    def remove_awaiter(self):
        self._sem.notification_lock()
        try:
            self._close_fifo()
            #print 'notification unlock 3'
            #self._sem.notification_unlock()
        finally:
            #print 'notification unlock 4'
            self._sem.notification_unlock()

    def _open_fifo(self):
        if self._fifo_file is not None:
            if os.path.exists(self._fifo_file):
                return self._fifo_fh
        fifo_name = ''.join(RANDCHARS[random.randint(0, len(RANDCHARS)-1)] for i in range(12))
        fifo_file = os.path.join(self._notification_directory, fifo_name)
        if os.path.exists(fifo_file):
            return self._open_fifo()
        os.mkfifo(fifo_file, 0622)
        self._fifo_file = fifo_file
        self._fifo_fh = open(self._fifo_file, 'r+b')
        return self._fifo_fh

    def _close_fifo(self):
        if self._fifo_fh is not None:
            self._fifo_fh.close()
            self._fifo_fh = None
        if self._fifo_file is not None:
            if os.path.exists(self._fifo_file):
                os.remove(self._fifo_file)
            self._fifo_file = None

    def _write_fifo(self, fifo):
        try:
            fh = os.fdopen(os.open(fifo, os.O_WRONLY|os.O_NONBLOCK), 'w')
            fh.write(NULL_BYTE)
            fh.close()
            os.unlink(fifo)
        except:
            logging.exception("error while notifying fifo %s" % fifo)
            try:
                os.unlink(fifo)
            except:
                pass

    def _wake_awaiters(self):
        self._sem.notification_lock()
        try:
            for fname in os.listdir(self._notification_directory):
                fifo = os.path.join(self._notification_directory, fname)
                self._write_fifo(fifo)
            #print 'notification unlock 5'
            #self._sem.notification_unlock()
        finally:
            #print 'notification unlock 6'
            self._sem.notification_unlock()

    def withdraw(self):
        try:
            self.remove_awaiter()
        except:
            pass
        try:
            #print 'notification unlock 7'
            if self._sem.has_notification_lock():
                self._sem.notification_unlock()
        except:
            pass
        try:
            if self._sem.has_deposit_lock():
                self._sem.deposit_unlock()
        except:
            pass
        self._close()

    def _safe_rename(self, src, dest):
        os.link(src, dest)
        os.unlink(src)

    @classmethod
    @with_config_lock
    def rename(cls, old_name, new_name):
        pool = cls._get_pool(old_name)
        pool.rename(new_name)

    @classmethod
    def exists(cls, name):
        try:
            pool = cls._get_pool(name)
            return True
        except PoolNoSuchPoolException:
            return False

    @classmethod
    def sleep(self, name):
        pool = cls._get_pool(name)
        pool.sleep()

    def deposit(self, protein):
        (index, timestamp) = self.deposit_ex(protein)
        return index

    def deposit_ex(self, protein):
        self._sem.deposit_lock()
        try:
            (index, timestamp, pos) = self._deposit(protein)
            #self._sem.deposit_unlock()
        finally:
            self._sem.deposit_unlock()
        return (index, timestamp)

    def _has_flag(self, flag):
        if self._flags & flag:
            return True
        return False

    def _quick_cache(self, idx, start, size):
        if not hasattr(self, '_cache_buffer'):
            self._cache_buffer = list((None,) * CACHE_SIZE)
        n = idx % len(self._cache_buffer)
        self._cache_buffer[n] = (idx, start, size)

    def _quick_lookup(self, idx):
        if not hasattr(self, '_cache_buffer'):
            return None
        n = idx % len(self._cache_buffer)
        cache = self._cache_buffer[n]
        if cache is not None and cache[0] == idx:
            return cache[1]
        return None

    def _deposit(self, protein):
        timestamp = obtimestamp()
        (old, new) = self._pointers()
        last = self._last
        curpos = self._pool_pos
        data = protein.to_slaw()
        try:
            if new >= old:
                ## we should have a lock, so no worries about stompling
                self.seek(new)
                last_protein = self._read_entry()
                index = last_protein.index() + 1
            else:
                ## pool is empty
                self.seek(old)
                index = 0
            ## find where to put the new protein
            newnew = self.tell()
            jumpback = len(data) + 24
            sz = self._pool_size()
            ptr = newnew % sz
            if ptr + jumpback > sz:
                #print 'wrapping'
                #time.sleep(2)
                if self._has_flag(POOL_FLAG_STOP_WHEN_FULL):
                    raise PoolFullException()
                newnew = self._pool_pos + (sz - self._pool_ptr) + self._header_size
            (first_index, first_pos) = self._first_index()
            newold = old
            self.seek(old)
            ## check that we're not overwriting old proteins
            ## if so, advance oldest pointer
            while newnew + jumpback - sz > newold:
                #print 'skipping entry at %d (%d)' % (newold, self.tell())
                #print 'new = %d, old = %d, newnew = %d, newold = %d, jumpback = %d, sz = %d' % (new, old, newnew, newold, jumpback, sz)
                ix = self._skip_entry()
                if ix == first_index - 1:
                    #newold = int(newold / sz) + sz + self._header_size
                    newold = first_pos
                    self.seek(newold)
                else:
                    newold = self.tell()
            #print 'new = %d, old = %d, newnew = %d, newold = %d, jumpback = %d, sz = %d' % (new, old, newnew, newold, jumpback, sz)
            self.seek(newnew)
            self._mmap.write(struct.pack('dQ', timestamp.timestamp(), index))
            self._mmap.write(data)
            self._mmap.write(struct.pack('Q', jumpback))
            #print 'set pointers to %d, %d' % (newold, newnew)
            self._set_pointers(newold, newnew)
            #self.seek(curpos)
            #self._last = last
        finally:
            self.seek(curpos)
            self._last = last
        if self._indx is not None:
            self._indx.add_entry(newnew, timestamp, index, newold)
        self._wake_awaiters()
        return (index, timestamp, newnew)

    def _index_at(self, pos):
        orig_pos = self.tell()
        self.seek(pos+8)
        try:
            (idx,) = struct.unpack('Q', self.read(8))
            self.seek(orig_pos)
        finally:
            self.seek(orig_pos)
        return idx

    def _skip_entry(self):
        pos = self.tell()
        try:
            (ts, ix) = struct.unpack('dQ', self.read(16))
            plasma.slaw.skip_slaw(self._slaw_version, self)
            (jumpback,) = struct.unpack('Q', self.read(8))
            self._quick_cache(ix, pos, jumpback)
            return ix
        except StompledException:
            while True:
                try:
                    (ts, ix) = struct.unpack('dQ', self.read(16))
                    self.seek(-16, 1)
                    return ix - 1
                except StompledException:
                    pass

    def seek_to(self, idx):
        if self._last is not None:
            ## little bit of optimization
            offset = None
            if self._last['index'] == idx:
                ## we want the index of the protein we just read
                offset = self._last['start']
            elif idx == self._last['index'] + 1:
                ## we want the index after the protein we just read
                offset = self._last['end']
            elif idx == self._last['index'] - 1:
                ## we want the index before the protein we just read
                if self._first_index()[0] == self._last['index']:
                    ## can't seek backwards from first protein
                    pass
                else:
                    self.seek(self._last['start'] - 8)
                    try:
                        (jumpback,) = struct.unpack('Q', self.read(8))
                        offset = self._last['start'] - jumpback
                    except StompledException:
                        raise PoolNoSuchProteinException("protein %d no longer in pool" % idx)
            if offset is not None:
                if offset < 0:
                    print "seek_to offset %d" % offset
                self.seek(offset)
                return idx
        if idx > self.newest_index():
            self.runout()
            raise PoolNoSuchProteinException("protein %d not yet in pool" % idx)
        if idx < self.oldest_index():
            (old, new) = self._pointers()
            self.seek(old)
            raise PoolNoSuchProteinException("protein %d no longer in pool" % idx)
        ## try the cache
        offset = self._quick_lookup(idx)
        if offset is not None:
            self.seek(offset)
            return idx
        (first_index, first_pos) = self._first_index()
        if idx >= first_index:
            start_index = first_index
            start_pos = first_pos
        else:
            (old, new) = self._pointers()
            start_index = self.oldest_index()
            start_pos = old
            if idx < start_index:
                raise PoolNoSuchProteinException("protein %d no longer in pool" % idx)
        if self._last is not None:
            if idx >= self._last['index'] and idx - self._last['index'] < idx - start_index:
                start_index = self._last['index']
        if self._indx is not None:
            (xidx, offset, ts) = self._indx.find(idx)
            if idx - xidx < idx - start_index:
                start_index = xidx
                start_pos = offset
        self.seek(start_pos)
        while start_index < idx:
            if start_index == first_index - 1:
                self.seek(first_pos)
                start_index = first_index
            else:
                try:
                    self._skip_entry()
                    start_index = self._index_at(self.tell())
                except StompledException:
                    start_index = self._index_at(self.tell())
        return start_index

    def nth_protein(self, idx):
        found = self.seek_to(idx)
        if found == idx:
            try:
                return self._read_entry()
            except StompledException:
                pass
        raise PoolNoSuchProteinException("protein %d no longer in pool" % idx)

    def await_nth(self, idx, timeout=None, interrupt=None):
        (old, new) = self._pointers()
        try:
            protein = self.nth_protein(idx)
            return protein
        except PoolNoSuchProteinException:
            if timeout == POOL_NO_WAIT:
                raise
        self.seek(new)
        while True:
            rem = self.await(timeout, interrupt=interrupt)
            try:
                protein = self.nth_protein(idx)
                return protein
            except PoolNoSuchProteinException:
                pass
            if timeout != POOL_WAIT_FOREVER:
                timeout = rem
                if timeout <= 0:
                    raise PoolAwaitTimedoutException()

    def index_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        (index, timestamp, offset) = self._lookup(timestamp, whence, direction)
        return index

    def offset_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        (index, timestamp, offset) = self._lookup(timestamp, whence, direction)
        return offset

    def _lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        if type(timestamp) == datetime.datetime:
            timestamp = obtimestamp(timestamp).timestamp()
        elif isinstance(timestamp, obtimestamp):
            timestamp = timestamp.timestamp()
        if whence != TIMESTAMP_ABSOLUTE:
            last = self._last
            try:
                p = self.nth_protein(whence)
                timestamp += p.timestamp()
                self._last = last
            except PoolNoSuchProteinException:
                self._last = last
                raise
            #timestamp += obtimestamp().timestamp()
        orig_pos = self.tell()
        if self._indx is not None:
            (offset, ts, idx) = self._indx.timefind(timestamp)
            ts = ts.timestamp()
            self.seek(offset)
        else:
            (old, new) = self._pointers()
            if new < old:
                raise PoolNoSuchProteinException("pool is empty")
            self.seek(new)
            try:
                (ts,idx) = struct.unpack('dQ', self.read(16))
                self.seek(new)
            except StompledException:
                self.seek(orig_pos)
                return self._lookup(timestamp, whence, direction)
            offset = new
        if ts < timestamp and direction == DIRECTION_HIGHER:
            raise PoolNoSuchProteinException("latest protein is earlier than the requested time")
        prev_offset = offset
        prev_ts = ts
        prev_idx = idx
        #print 'lookup %s starting from (%s, %s, %s)' % (timestamp, offset, ts, idx)
        while True:
            if prev_ts == timestamp:
                break
            try:
                self.seek_to(prev_idx - 1)
                offset = self.tell()
                (ts, idx) = struct.unpack('dQ', self.read(16))
            except (StompledException, PoolNoSuchProteinException):
                (ts, idx) = (None, None)
                break
            if ts <= timestamp:
                break
        self.seek(orig_pos)
        if direction == DIRECTION_LOWER:
            if ts is None:
                raise PoolNoSuchProteinException("earliest protein is later than the requested time")
            return (idx, obtimestamp(ts), offset)
        if direction == DIRECTION_HIGHER:
            return (prev_idx, obtimestamp(prev_ts), prev_offset)
        if ts is None:
            return (prev_idx, obtimestamp(prev_ts), prev_offset)
        if abs(timestamp - ts) < abs(timestamp - prev_ts):
            return (idx, obtimestamp(ts), offset)
        return (prev_idx, obtimestamp(prev_ts), prev_offset)

    def newest_index(self):
        pos = self.tell()
        (old, new) = self._pointers()
        if new < old:
            raise PoolNoSuchProteinException("pool is empty")
        self.seek(new)
        try:
            (ts, idx) = struct.unpack('dQ', self.read(16))
            self.seek(pos)
            return idx
        except StompledException:
            return self.newest_index()

    def oldest_index(self):
        pos = self.tell()
        (old, new) = self._pointers()
        if new < old:
            raise PoolNoSuchProteinException("pool is empty")
        self.seek(old)
        try:
            (ts, idx) = struct.unpack('dQ', self.read(16))
            self.seek(pos)
            return idx
        except StompledException:
            return self.oldest_index()

    def rewind(self):
        (old, new) = self._pointers()
        self.seek(old)

    def to_last(self):
        (old, new) = self._pointers()
        self.seek(new)

    def runout(self):
        (old, new) = self._pointers()
        if old < new:
            self.seek(new)
            self._skip_entry()
        else:
            self.seek(old)

    def curr(self):
        if self._last:
            return self._last['protein']
        (old, new) = self._pointers()
        if self.tell() > new:
            raise PoolNoSuchProteinException("no proteins have been deposited since you started participating in the pool")
        try:
            return self._read_entry()
        except StompledException:
            return self.curr()

    def next(self, timeout=POOL_NO_WAIT, interrupt=None):
        (old, new) = self._pointers()
        if self.tell() < old:
            self.seek(old)
        if self.tell() > new:
            if timeout == POOL_NO_WAIT:
                raise PoolAwaitTimedoutException()
            self.await(timeout, interrupt=interrupt)
        (old, new) = self._pointers()
        if self.tell() < old:
            self.seek(old)
        if self.tell() > new:
            raise PoolNoSuchProteinException("beyond end of the pool")
        (first_index, first_pos) = self._first_index()
        pos = self.tell()
        try:
            if pos != first_pos:
                ## make sure we're not at the end of the pool
                self.seek(-8, 1)
                (jumpback,) = struct.unpack('Q', self.read(8))
                self.seek(-1 * jumpback, 1)
                (ts, idx) = struct.unpack('dQ', self.read(16))
                if first_index == idx + 1:
                    ## we are at the end of the pool, so skip to
                    ## the first protein
                    self.seek(first_pos)
                else:
                    self.seek(pos)
            return self._read_entry()
        except StompledException:
            return self.next()

    def prev(self):
        if self._last is None:
            ## we haven't read anything yet, so let's do that
            (old, new) = self._pointers()
            if self.tell() <= old:
                raise PoolNoSuchProteinException("already at oldest protein")
            try:
                p = self.curr()
            except PoolNoSuchProteinException():
                ## nothing has been deposited since we started
                ## participating, so let's use the newest protein
                if new < old:
                    raise PoolNoSuchProteinException("pool is empty")
                self.seek(new)
                try:
                    return self._read_entry()
                except StompledException:
                    return self.prev()
        if self._last['index'] > 0:
            return self.nth_protein(self._last['index'] - 1)
        raise PoolNoSuchProteinException("already at protein zero")

    def await(self, timeout=POOL_WAIT_FOREVER, interrupt=None):
        fh = self.add_awaiter()
        (old, new) = self._pointers()
        if self.tell() <= new:
            self.remove_awaiter()
            return timeout
        if timeout == POOL_NO_WAIT:
            self.remove_awaiter()
            raise PoolAwaitTimedoutException()
        if timeout == POOL_WAIT_FOREVER:
            timeout = None
        start = time.time()
        sfhs = [fh]
        if interrupt is not None:
            sfhs.append(interrupt)
        (rlist, wlist, xlist) = select.select(sfhs, [], [], timeout)
        end = time.time()
        if fh not in rlist:
            self.remove_awaiter()
            if interrupt is not None and interrupt in rlist:
                raise PoolAwaitWokenException()
            raise PoolAwaitTimedoutException()
        self.remove_awaiter()
        if timeout is None:
            return POOL_WAIT_FOREVER
        return timeout - (end - start)

    def get_info(self):
        if self._indx is not None:
            index_capacity = self._indx.capacity
            index_step = self._indx.step
            index_count = self._indx.count
        else:
            index_capacity = 0
            index_step = 1
            index_count = 0
        return Protein(ingests={
            'type':              obstring('mmap'),
            'terminal':          obbool(True),
            'size':              unt64(self._pool_size()),
            'size-used':         unt64(self._size_used()),
            'mmap-pool-version': unt32(self._mmap_version),
            'slaw-version':      unt32(self._slaw_version),
            'index-capacity':    unt64(index_capacity),
            'index-step':        unt64(index_step),
            'index-count':       unt64(index_count),
            'stop-when-full':    obbool(self._has_flag(POOL_FLAG_STOP_WHEN_FULL)),
            'frozen':            obbool(self._has_flag(POOL_FLAG_FROZEN)),
            'auto-dispose':      obbool(self._has_flag(POOL_FLAG_AUTO_DISPOSE)),
            'checksum':          obbool(self._has_flag(POOL_FLAG_CHECKSUM)),
            'sync':              obbool(self._has_flag(POOL_FLAG_SYNC))
        })

    def _size_used(self):
        pos = self.tell()
        last = self._last
        while True:
            (old, new) = self._pointers()
            if new < old:
                return 0
            self.seek(new)
            try:
                newest = self._read_entry()
                size = self._last['end'] - old
                self._last = last
                return size
            except StompledException:
                pass

from plasma.pool.mmap.single_file import SingleFileMMapPool
from plasma.pool.mmap.config_in_mmap import ConfigInMMapPool
from plasma.pool.mmap.config_in_file import ConfigInFileMMapPool
