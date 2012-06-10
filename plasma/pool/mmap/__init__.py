import os, struct, select, datetime, pwd, grp, mmap, random, time, logging, fcntl, traceback
from loam import *
from plasma.const import *
import plasma.config
from plasma.exceptions import *
from plasma.sem_ops import SemaphoreSet
import plasma.util
from plasma.pool.util import makedirs, with_umask
import plasma.slaw
from plasma.protein import Protein
from plasma.slaw import BINARY_MAGIC
from plasma.pool.mmap.chunks import *

DEFAULT_MODE = 0777
DEFAULT_OWNER = -1
DEFAULT_GROUP = -1
NULL_BYTE = chr(0)
CACHE_SIZE = 1000
MY_CONFIG_LOCK = None
POOL_MMAP_V0_HEADER_SIZE = 24
POOL_MMAP_MAGICV0 = 0x00065b0000af4c81
POOL_MMAP_SLAW_VERSION_SHIFTY = 24

class MMapPool(object):

    def __init__(self, name):
        self.closed = True
        self.encoding = None
        self.errors = None
        self.mode = None
        self.newlines = None
        self.softspace = False
        self.__name = name
        self.__is_iterating = False
        self.__iterator_stopped = False
        self.__sem = None
        self.__indx = None
        self.__chunks = None
        self.__mmap = None
        self.__fh = None
        self.__mmap_file = None
        self.__cache_buffer = None

    ## ---------------- ##
    ## file() interface ## 
    ## ---------------- ##

    def __iter__(self):
        self.restart_iterator()
        return self

    def open(self):
        return self.__open()

    def close(self):
        if self.closed:
            return None
        if self.auto_dispose():
            self.__config_lock()
            try:
                if self.auto_dispose():
                    do_dispose = True
                else:
                    do_dispose = False
                self.__close()
                if do_dispose and self.check_in_use():
                    self.dispose()
            finally:
                self.__config_unlock()
        else:
            self.__close()

    def fileno(self):
        return None

    def flush(self):
        return None

    def isatty(self):
        return False

    def next(self):
        if self.__iterator_stopped:
            raise StopIteration()
        try:
            return self.readline()
        except StompledException:
            return self.next()
        except PoolNoSuchProteinException:
            if self.__is_iterating:
                self.__iterator_stopped
                raise StopIteration()
            raise

    def read(self, size):
        start = self.__pool_pos
        if size > self.__max_protein_size():
            raise PoolProteinBiggerThanPoolException()
        pool_size = self.file_size()
        pool_ptr = start % pool_size
        if pool_ptr < self.header_size():
            raise IOError("attempted read from the header")
        if pool_ptr + size > pool_size:
            ## this shouldn't ever happen
            s1 = pool_size - pool_ptr
            s2 = size - s1
            data = self.read(s1)
            self.seek(start + s1 + self.header_size())
            return data + self.read(s2)
        self.__mmap.seek(pool_ptr, 0)
        data = self.__mmap.read(size)
        if len(data) != size:
            raise SlawWrongLengthException()
        (old, new) = self.__pointers()
        if start < old:
            self.seek(old)
            logging.error("read of %s at %s (%d) stompled" % (size, start, old))
            raise StompledException()
        self.__pool_pos += size
        return data

    def readline(self, size=None):
        pos = self.tell()
        (old, new) = self.__pointers()
        if pos < old:
            raise PoolNoSuchProteinException("oldest pointer has advanced beyond pool pointer")
        if pos > new:
            raise PoolNoSuchProteinException("pool pointer has advanced beyond newest pointer")
        try:
            (timestamp, index) = struct.unpack('dQ', self.read(16))
            protein = plasma.slaw.parse_slaw(self.__slaw_version, self)
            (jumpback,) = struct.unpack('Q', self.read(8))
            #if self.tell() % self.file_size() == 0:
            #    self.seek(self.header_size(), 1)
            self.__quick_cache(index, pos, jumpback)
        except StompledException:
            self.seek(pos)
            raise
        try:
            timestamp = obtimestamp(timestamp)
        except:
            print "bad timestamp: %f at %d (%d)" % (timestamp, pos, pos % self.file_size())
            raise
        protein.set_origin(self)
        protein.set_index(index)
        protein.set_timestamp(timestamp)
        self.__last = {
            'start':     pos,
            'end':       self.tell(),
            'timestamp': timestamp,
            'index':     index,
            'size':      jumpback,
            'protein':   protein,
        }
        self.__quick_cache(index, pos, jumpback)
        return protein

    def seek(self, offset, whence=0):
        if whence == 0:
            abs_pos = offset
        elif whence == 1:
            abs_pos = self.__pool_pos + offset
        if abs_pos < 0:
            raise IOError("seeking into negative space")
        ptr = abs_pos % self.file_size()
        if abs_pos > 0 and ptr == 0:
            #print "seek to end of pool, advancing to end of header"
            abs_pos += self.header_size()
            ptr = abs_pos % self.file_size()
        if ptr < self.header_size():
            raise IOError("seeking into pool header (%d / %d) vs %d" % (ptr, abs_pos, self.header_size()))
        self.__pool_pos = abs_pos

    def tell(self):
        return self.__pool_pos

    def truncate(self, size=None):
        return self.resize(size)

    def write(self, protein):
        self.deposit(protein)

    def writelines(self, proteins):
        if self.frozen():
            raise PoolFrozenException()
        self.__deposit_lock()
        try:
            for p in proteins:
                self.__deposit(protein)
        finally:
            self.__deposit_unlock()

    def xreadlines(self):
        return self

    ## ------------------- ##
    ## libPlasma interface ##
    ## ------------------- ##

    def create(self,
               resizable=True,
               single_file=False,
               size=POOL_SIZE_SMALL,
               index_capacity=0,
               mode=0777,
               uid=-1,
               gid=-1,
               stop_when_full=False,
               frozen=False,
               auto_dispose=False,
               checksum=False,
               sync=False):
        self.__config_lock()
        try:
            self.__create(resizable, single_file, size, index_capacity, mode, uid, gid, stop_when_full, frozen, auto_dispose, checksum, sync)
        finally:
            self.__config_unlock()

    def participate(self, *args, **kwargs):
        return self.open(*args, **kwargs)

    def participate_creatingly(self,
                               resizable=True,
                               single_file=False,
                               size=POOL_SIZE_SMALL,
                               index_capacity=0,
                               mode=0777,
                               uid=-1,
                               gid=-1,
                               stop_when_full=False,
                               frozen=False,
                               auto_dispose=False,
                               checksum=False,
                               sync=False):
        self.__config_lock()
        try:
            if not self.exists():
                self.__create(resizable, single_file, size, index_capacity, mode, uid, gid, stop_when_full, frozen, auto_dispose, checksum, sync)
            return self.participate()
        finally:
            self.__config_unlock()

    def withdraw(self):
        return self.close()

    def dispose(self, *args, **kwargs):
        self.__config_lock()
        try:
            if not self.closed:
                self.__close()
            #(pv, dv) = self.__characterize()
            #if pv == 1:
            #    self.__init_v1(dv)
            #else:
            #    self.__init_v0(dv)
            self.__open()
            fh = self.__lock_pool()
            self.__mmap.close()
            self.__sem.destroy()
            os.rmdir(self.__notification_directory)
            if self.__pool_conf_file is not None:
                os.unlink(self.__pool_conf_file)
            if self.__mmap_conf_file is not None:
                os.unlink(self.__mmap_conf_file)
            os.unlink(self.__mmap_file)
            try:
                os.rmdir(self.__pool_directory)
            except:
                pass
            self.__unlock_pool(fh)
            fh.close()
        finally:
            self.__config_unlock()

    def change_options(self, 
               size=None,
               mode=None,
               uid=None,
               gid=None,
               stop_when_full=None,
               frozen=None,
               auto_dispose=None,
               checksum=None,
               sync=None):
        self.__deposit_lock()
        try:
            if size is not None and size != self.file_size():
                self.__resize()
            if self.__pool_version == 1:
                if mode is not None:
                    self.__chunks['perm'].mode = mode
                if uid is not None:
                    self.__chunks['perm'].uid = self.__get_uid(uid)
                if gid is not None:
                    self.__chunks['perm'].gid = self.__get_gid(gid)
                if stop_when_full is not None:
                    self.set_flag(POOL_FLAG_STOP_WHEN_FULL, stop_when_full)
                if frozen is not None:
                    self.set_flag(POOL_FLAG_FROZEN, frozen)
                if auto_dispose:
                    self.set_flag(POOL_FLAG_AUTO_DISPOSE, auto_dispose)
                if checksum is not None:
                    self.set_flag(POOL_FLAG_CHECKSUM, checksum)
                if sync is not None:
                    self.set_flag(POOL_FLAG_SYNC, sync)
            else:
                if size is not None and size != self.file_size():
                    raise PoolInvalidSizeException("version 0 pools cannot be resized")
                if stop_when_full:
                    raise PoolWrongVersionException("version 0 pools cannot set stop-when-full flag")
                if frozen:
                    raise PoolWrongVersionException("version 0 pools cannot set frozen flag")
                if auto_dispose:
                    raise PoolWrongVersionException("version 0 pools cannot set auto-dispose flag")
                if checksum:
                    raise PoolWrongVersionException("version 0 pools cannot set checksum flag")
                if sync:
                    raise PoolWrongVersionException("version 0 pools cannot set sync flag")
                if mode is not None:
                    self.__perm['mode'] = mode
                if uid is not None:
                    self.__perm['uid'] = self.__get_uid(uid)
                if gid is not None:
                    self.__perm['gid'] = self.__get_gid(gid)
                self.__save_default_config()
        finally:
            self.__deposit_unlock()

    def set_flag(self, flag, on=True):
        if on:
            self.__chunks['conf'].flags |= flag
        else:
            mask = 0xffffffffffffffff
            self.__chunks['conf'].flags &= (mask ^ flag)

    def resize(self, new_size):
        raise Exception("resize not yet implemented")
        self.__deposit_lock()
        try:
            old_size = self.file_size()
            if new_size < old_size:
                raise PoolInvalidSizeException("pool may not be made smaller than it already is")
            (old, new) = self.__pointers()
        finally:
            self.__deposit_unlock()
            
    def rename(self, new_name):
        self.__config_lock()
        try:
            if not self.closed:
                raise PoolInUseException()
            fh = self.__lock_pool()
            try:
                self.__rename(new_name)
            finally:
                self.__unlock_pool(fh)
                fh.close()
        finally:
            self.__config_unlock()

    @with_umask
    def __rename(self, new_name):
        (pv, dv) = self.__characterize()
        if pv == 1:
            self.__init_v1(dv)
        else:
            self.__init_v0(dv)
        old = {
            'name': self.__name,
            'pool_directory': self.__pool_directory,
            'pool_conf_file': self.__pool_conf_file,
            'mmap_conf_file': self.__mmap_conf_file,
            'mmap_file': self.__mmap_file,
            'notification_directory': self.__notification_directory,
        }
        self.__name = new_name
        if pv == 1:
            self.__init_v1(dv)
        else:
            self.__init_v0(dv)
        self.__safe_rename(old['pool_conf_file'], self.__pool_conf_file)
        self.__safe_rename(old['mmap_conf_file'], self.__mmap_conf_file)
        self.__safe_rename(old['mmap_file'], self.__mmap_file)
        st = os.stat(old['notification_directory'])
        makedirs(self.__notification_directory, st.st_mode, st.st_uid, st.st_gid)
        os.rmdir(old['notification_directory'])
        try:
            os.rmdir(old['pool_directory'])
        except:
            pass

    @with_umask
    def __safe_rename(self, src, dest):
        if src == dest:
            return False
        if src is None or dest is None:
            return False
        (src_dir, src_file) = os.path.split(src)
        (dest_dir, dest_file) = os.path.split(dest)
        if not os.path.exists(dest_dir):
            st = os.stat(src_dir)
            makedirs(dest_dir, st.st_mode, st.st_uid, st.st_gid)
        if not os.path.isdir(dest_dir):
            raise Exception("can't rename %s to %s: %s is not a directory" % (src, dest, dest_dir))
        os.link(src, dest)
        os.unlink(src)
        return True

    def __init_v0(self, directory_version):
        self.__pool_version = 0
        self.__directory_version = directory_version
        self.__pool_directory = self.__pool_dir()
        (pdir, pname) = os.path.split(self.__pool_directory)
        self.__mmap_file = os.path.join(self.__pool_directory, '%s.mmap-pool' % pname)
        self.__pool_conf_file = os.path.join(self.__pool_directory, 'pool.conf')
        self.__mmap_conf_file = os.path.join(self.__pool_directory, 'mmap.conf')
        self.__notification_directory = os.path.join(self.__pool_directory, 'notification')

    def __init_v1(self, directory_version):
        self.__pool_version = 1
        self.__directory_version = directory_version
        self.__mmap_conf_file = None
        if directory_version == POOL_DIRECTORY_VERSION_SINGLE_FILE:
            return self.__init_v1_single_file(directory_version)
        self.__pool_directory = self.__pool_dir()
        self.__mmap_file = os.path.join(self.__pool_directory, 'mmap-pool')
        self.__pool_conf_file = os.path.join(self.__pool_directory, 'pool.conf')
        self.__mmap_conf_file = None
        self.__notification_directory = os.path.join(self.__pool_directory, 'notification')

    def check_in_use(self):
        try:
            fh = self.__lock_pool()
        except PoolInUseException:
            return False
        self.__unlock_pool()
        return True

    def exists(self):
        #self.__config_lock()
        try:
            (pv, dv) = self.__characterize()
            return True
        except PoolNoSuchPoolException:
            return False
        #finally:
        #    self.__config_unlock()

    def sleep(self):
        if self.check_in_use():
            raise PoolInUseException()
        if self.closed:
            self.open()
        sem = self.__sem
        self.__close()
        fh = self.__lock_pool()
        try:
            sem.destroy()
        finally:
            self.__unlock_pool(fh)
            fh.close()

    def name(self):
        return self.__name

    def deposit(self, protein):
        if self.frozen():
            raise PoolFrozenException()
        self.__deposit_lock()
        try:
            (index, timestamp, offset) = self.__deposit(protein)
        finally:
            self.__deposit_unlock()
        return int64(index)

    def deposit_ex(self, protein):
        if self.frozen():
            raise PoolFrozenException()
        self.__deposit_lock()
        try:
            (index, timestamp, offset) = self.__deposit(protein)
        finally:
            self.__deposit_unlock()
        return (int64(index), timestamp)

    def nth_protein(self, idx):
        found = self.seek_to(idx)
        try:
            pos = self.tell()
            p = self.readline()
            if p.index() != idx:
                raise Exception("expected index %d at %d, got %d" % (idx, pos, p.index()))
            return p
            return self.readline()
        except StompledException:
            raise PoolNoSuchProteinException("protein %d no longer in pool" % idx)

    def await_nth(self, idx, timeout=None, interrupt=None):
        if idx < self.oldest_index():
            raise PoolNoSuchProteinException("protien %d no longer in pool")
        (old, new) = self.__pointers()
        try:
            return self.nth_protein(idx)
        except PoolNoSuchProteinException:
            if timeout == POOL_NO_WAIT:
                raise PoolAwaitTimedoutException()
        self.seek(new)
        while True:
            rem = self.await(timeout, interrupt=interrupt)
            try:
                return self.nth_protein(idx)
            except PoolNoSuchProteinException:
                pass
            if timeout != POOL_WAIT_FOREVER:
                timeout = rem
                if timeout <= 0:
                    raise PoolAwaitTimedoutException()

    def index_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        (index, timestamp, offset) = self.__lookup(timestamp, whence, direction)
        return index

    def offset_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        (index, timestamp, offset) = self.__lookup(timestamp, whence, direction)
        return offset

    def newest_index(self):
        pos = self.tell()
        (old, new) = self.__pointers()
        if new < old:
            raise PoolNoSuchProteinException("pool is empty")
        self.seek(new)
        try:
            (ts, idx) = struct.unpack('dQ', self.read(16))
        except StompledException:
            self.seek(pos)
            return self.newest_index()
        self.seek(pos)
        return int64(idx)

    def oldest_index(self):
        pos = self.tell()
        (old, new) = self.__pointers()
        if new < old:
            raise PoolNoSuchProteinException("pool is empty")
        self.seek(old)
        try:
            (ts, idx) = struct.unpack('dQ', self.read(16))
        except StompledException:
            self.seek(pos)
            return self.oldest_index()
        self.seek(pos)
        return int64(idx)

    def first_index(self):
        idx, pos = self.first_index_and_offset()
        return idx

    def first_index_and_offset(self):
        pos = self.tell()
        first_pos = self.first_offset()
        (old, new) = self.__pointers()
        if new < old:
            ## pool is empty
            return -1, first_pos
        self.seek(first_pos + 8)
        try:
            (idx,) = struct.unpack('Q', self.read(8))
            self.seek(pos)
            return idx, first_pos
        except StompledException:
            self.seek(pos)
            return self.first_index_and_offset()

    def first_offset(self):
        (old, new) = self.__pointers()
        if new < self.file_size():
            ## pool hasn't wrapped, but oldest pointer may have advanced
            return old
        n = int(new / self.file_size())
        return n * self.file_size() + self.header_size()

    def last_index(self):
        return self.first_index() - 1

    def rewind(self):
        (old, new) = self.__pointers()
        self.seek(old)

    def to_last(self):
        (old, new) = self.__pointers()
        if new < old:
            self.seek(old)
        else:
            self.seek(new)

    def runout(self):
        (old, new) = self.__pointers()
        if new < old:
            self.seek(old)
        else:
            self.seek(new)
            try:
                self.skip()
            except StompledException:
                self.runout()

    def curr(self):
        if self.__last is not None:
            return self.__last['protein']
        (old, new) = self.__pointers()
        if self.tell() > new:
            raise PoolNoSuchProteinException("no proteins have been deposited since you started participating in the pool")
        try:
            return self.readline()
        except StompledException:
            return self.curr()

    def prev(self):
        if self.__last is None:
            ## we haven't read anything yet, so let's do that
            (old, new) = self.__pointers()
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
                    return self.readline()
                except StompledException:
                    return self.prev()
        if self.__last['index'] > 0:
            return self.nth_protein(self.__last['index'] - 1)
        raise PoolNoSuchProteinException("already at protein zero")

    def await_next(self, timeout=POOL_NO_WAIT, interrupt=None):
        (old, new) = self.__pointers()
        if self.tell() < old:
            self.seek(old)
        if self.tell() > new:
            if timeout == POOL_NO_WAIT:
                raise PoolAwaitTimedoutException()
            self.await(timeout, interrupt=interrupt)
        (old, new) = self.__pointers()
        if self.tell() < old:
            self.seek(old)
        if self.tell() > new:
            raise PoolNoSuchProteinException("beyond end of the pool")
        (first_index, first_pos) = self.first_index_and_offset()
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
            return self.readline()
        except StompledException:
            return self.await_next()

    def get_info(self):
        if self.__indx is not None:
            index_capacity = self.__indx.capacity
            index_step = self.__indx.step
            index_count = self.__indx.count
        else:
            index_capacity = 0
            index_step = 1
            index_count = 0
        return Protein(ingests={
            'type':              obstring('mmap'),
            'terminal':          obbool(True),
            'size':              unt64(self.file_size()),
            'size-used':         unt64(self.size_used()),
            'mmap-pool-version': unt32(self.__pool_version),
            'slaw-version':      unt32(self.__slaw_version),
            'index-capacity':    unt64(index_capacity),
            'index-step':        unt64(index_step),
            'index-count':       unt64(index_count),
            'stop-when-full':    obbool(self.stop_when_full()),
            'frozen':            obbool(self.frozen()),
            'auto-dispose':      obbool(self.auto_dispose()),
            'checksum':          obbool(self.checksum()),
            'sync':              obbool(self.sync()),
        })
        pass

    ## ---------- ##
    ## extensions ##
    ## ---------- ##

    def await(self, timeout=POOL_WAIT_FOREVER, interrupt=None):
        fh = self.add_awaiter()
        (old, new) = self.__pointers()
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

    def file_size(self):
        return self.__size
        if self.__pool_version == 0:
            return self.__size
        return self.__chunks['conf'].file_size

    def header_size(self):
        return self.__header_size
        if self.__pool_version == 0:
            return self.__header_size
        return self.__chunks['conf'].header_size

    def index_capacity(self):
        if self.__pool_version == 0:
            return self.__index_capacity
        if self.__chunks.has_key('indx'):
            return self.__chunks['indx'].capacity
        return 0

    def size_used(self):
        pos = self.tell()
        last = self.__last
        while True:
            (old, new) = self.__pointers()
            if new < old:
                return 0
            self.seek(new)
            try:
                newest = self.readline()
                size = self.__last['end'] - old
                self.__last = last
                return size
            except StompledException:
                pass

    def add_awaiter(self):
        self.__notification_lock()
        try:
            fh = self.__open_fifo()
        finally:
            self.__notification_unlock()
        return fh

    def remove_awaiter(self):
        self.__notification_lock()
        try:
            self.__close_fifo()
        finally:
            self.__notification_unlock()

    def __open_fifo(self):
        fifo_name = ''.join(RANDCHARS[random.randint(0, len(RANDCHARS)-1)] for i in range(12))
        fifo_file = os.path.join(self.__notification_directory, fifo_name)
        if os.path.exists(fifo_file):
            return self.__open_fifo()
        os.mkfifo(fifo_file, 0622)
        self.__fifo_file = fifo_file
        self.__fifo_fh = open(self.__fifo_file, 'r+b')
        return self.__fifo_fh

    def __close_fifo(self):
        self.__fifo_fh.close()
        self.__fifo_fh = None
        if os.path.exists(self.__fifo_file):
            os.remove(self.__fifo_file)
        self.__fifo_file = None

    def __write_fifo(self, fifo):
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

    def __wake_awaiters(self):
        self.__notification_lock()
        try:
            for fname in os.listdir(self.__notification_directory):
                fifo = os.path.join(self.__notification_directory, fname)
                self.__write_fifo(fifo)
        finally:
            self.__notification_unlock()

    def clear_iterator(self):
        self.__is_iterating = False
        self.__iterator_stopped = False

    def restart_iterator(self):
        self.__is_iterating = True
        self.__iterator_stopped = False

    def skip(self):
        pos = self.tell()
        try:
            (timestamp, index) = struct.unpack('dQ', self.read(16))
            plasma.slaw.skip_slaw(self.__slaw_version, self)
            (jumpback,) = struct.unpack('Q', self.read(8))
            #if self.tell() % self.file_size() == 0:
            #    self.seek(self.header_size(), 1)
            self.__quick_cache(index, pos, jumpback)
            return index
        except StompledException:
            while True:
                try:
                    (timestamp, index) = struct.unpack('dQ', self.read(16))
                    self.seek(-16, 1)
                    return index - 1
                except StompledException:
                    pass

    def seek_to(self, index):
        #print "seek to index %d from position %d" % (index, self.tell())
        if self.__last is not None:
            ## little bit of optimization
            offset = None
            if index == self.first_index():
                offset = self.first_offset()
            elif self.__last['index'] == index:
                ## we want the index of the protein we just read
                offset = self.__last['start']
            elif index == self.__last['index'] + 1:
                ## we want the index after the protein we just read
                offset = self.__last['end']
            elif index == self.__last['index'] - 1:
                ## we want the index before the protein we just read
                if self.first_index() == self.__last['index']:
                    ## can't seek backwards from first protein
                    pass
                else:
                    self.seek(self.__last['start'] - 8)
                    try:
                        (jumpback,) = struct.unpack('Q', self.read(8))
                        offset = self.__last['start'] - jumpback
                    except StompledException:
                        raise PoolNoSuchProteinException("protein %d no longer in pool" % index)
            if offset is not None:
                self.seek(offset)
                return index
        if index > self.newest_index():
            self.runout()
            raise PoolNoSuchProteinException("protein %d not yet in pool" % index)
        if index < self.oldest_index():
            (old, new) = self.__pointers()
            self.seek(old)
            raise PoolNoSuchProteinException("protein %d no longer in pool" % index)
        ## try the cache
        offset = self.__quick_lookup(index)
        if offset is not None:
            self.seek(offset)
            return index
        orig_pos = self.tell()
        (first_index, first_pos) = self.first_index_and_offset()
        (old, new) = self.__pointers()
        if index >= first_index:
            start_index = first_index
            start_pos = first_pos
            #print "(1) start_pos = %d / %d" % (start_pos, start_index)
        else:
            #(old, new) = self.__pointers()
            start_index = self.oldest_index()
            start_pos = old
            #print "(2) start_pos = %d / %d" % (start_pos, start_index)
            if index < start_index:
                raise PoolNoSuchProteinException("protein %d no longer in pool" % index)
        if self.__last is not None:
            if index >= self.__last['index'] and index - self.__last['index'] < index - start_index:
                start_index = self.__last['index']
                start_pos = self.__last['start']
                #print "(3) start_pos = %d / %d" % (start_pos, start_index)
        if self.__indx is not None:
            (xidx, offset, ts) = self.__indx.find(index)
            if xidx is not None and offset >= old and abs(index - xidx) < abs(index - start_index) and xidx <= index:
                yidx = self.__index_at(offset)
                if yidx != xidx:
                    print "pool index corrupt: reported %d at %d, but it's actually %d" % (xidx, offset, yidx)
                else:
                    start_index = xidx
                    start_pos = offset
                    #print "(4) start_pos = %d / %d" % (start_pos, start_index)
        self.seek(start_pos)
        if start_index > index:
            raise Exception("start index (%d) greater than search index (%d)" % (start_index, index))
        while start_index < index:
            if start_index == first_index - 1:
                self.seek(first_pos)
                start_index = first_index
            else:
                try:
                    xstart = start_index
                    xpos = self.tell()
                    self.skip()
                    start_index = self.__index_at(self.tell())
                    if start_index > index:
                        raise Exception("did we skip %d? %d -> %d (%d -> %d)" % (index, xstart, start_index, xpos, self.tell()))
                except StompledException:
                    start_index = self.__index_at(self.tell())
        if start_index != index:
            self.seek(orig_pos)
            raise PoolNoSuchProteinException("can't find protein %d in pool (%d != %d; %d/%d)" % (index, start_index, index, self.oldest_index(), self.newest_index()))
        return start_index

    def has_flag(self, flag):
        if self.__pool_version == 0:
            return False
        if self.__chunks['conf'].flags & flag > 0:
            return True
        return False

    def stop_when_full(self):
        return self.has_flag(POOL_FLAG_STOP_WHEN_FULL)

    def frozen(self):
        return self.has_flag(POOL_FLAG_FROZEN)

    def auto_dispose(self):
        return self.has_flag(POOL_FLAG_AUTO_DISPOSE)

    def checksum(self):
        return self.has_flag(POOL_FLAG_CHECKSUM)

    def sync(self):
        return self.has_flag(POOL_FLAG_SYNC)

    def save_last(self):
        self.__saved_last = self.__last

    def restore_last(self):
        self.__last = self.__saved_last
        del(self.__saved_last)

    def slaw_version(self):
        return self.__slaw_version

    ## --------------- ##
    ## private methods ##
    ## --------------- ##

    def __config_lock(self):
        global MY_CONFIG_LOCK
        if MY_CONFIG_LOCK is not None:
            logging.error("already have config lock")
            return False
        logging.debug("acquiring config lock")
        MY_CONFIG_LOCK = os.open(plasma.config.config_lock_dir(), os.O_EXLOCK|os.O_DIRECTORY)
        logging.debug("config lock acquired")
        return True

    def __config_unlock(self):
        global MY_CONFIG_LOCK
        if MY_CONFIG_LOCK is None:
            logging.exception("config lock is not held")
            return False
        logging.debug("releasing config lock")
        os.close(MY_CONFIG_LOCK)
        MY_CONFIG_LOCK = None
        logging.debug("config lock released")
        return True

    def __deposit_lock(self):
        return self.__sem.deposit_lock()

    def __deposit_unlock(self):
        return self.__sem.deposit_unlock()

    def __notification_lock(self):
        return self.__sem.notification_lock()

    def __notification_unlock(self):
        return self.__sem.notification_unlock()

    def __pool_dir(self):
        path = self.name()
        if path.startswith('local:'):
            path = path[6:]
        if not path.startswith(os.path.sep):
            path = os.path.join(plasma.config.ob_pools_dir(), path)
        return path

    def __get_size(self, size):
        if size is None or size == 0:
            return unt64(POOL_SIZE_SMALL)
        if size == 'tiny':
            return unt64(POOL_SIZE_TINY)
        if size == 'small':
            return unt64(POOL_SIZE_SMALL)
        if size == 'medium':
            return unt64(POOL_SIZE_MEDIUM)
        if size == 'large':
            return unt64(POOL_SIZE_LARGE)
        if size == 'obscene':
            return unt64(POOL_SIZE_OBSCENE)
        if size == 'max':
            return unt64(POOL_SIZE_MAX)
        size = plasma.util.sizestr_to_bytes(size)
        if size % 4096 != 0:
            size += (4096 - (size % 4096))
        return unt64(size)

    def __get_uid(self, uid):
        if isinstance(uid, (str, unicode)):
            pwent = pwd.getpwnam(uid)
            uid = pwent.pw_uid
        return int64(uid)

    def __get_gid(self, gid):
        if isinstance(gid, (str, unicode)):
            grent = grp.getgrnam(gid)
            gid = grent.gr_gid
        return int64(gid)

    def __init_v0(self, directory_version):
        self.__pool_version = 0
        self.__directory_version = directory_version
        self.__pool_directory = self.__pool_dir()
        (pdir, pname) = os.path.split(self.__pool_directory)
        self.__mmap_file = os.path.join(self.__pool_directory, '%s.mmap-pool' % pname)
        self.__pool_conf_file = os.path.join(self.__pool_directory, 'pool.conf')
        self.__mmap_conf_file = os.path.join(self.__pool_directory, 'mmap.conf')
        self.__notification_directory = os.path.join(self.__pool_directory, 'notification')

    def __init_v1(self, directory_version):
        self.__pool_version = 1
        self.__directory_version = directory_version
        self.__mmap_conf_file = None
        if directory_version == POOL_DIRECTORY_VERSION_SINGLE_FILE:
            return self.__init_v1_single_file(directory_version)
        self.__pool_directory = self.__pool_dir()
        self.__mmap_file = os.path.join(self.__pool_directory, 'mmap-pool')
        self.__pool_conf_file = os.path.join(self.__pool_directory, 'pool.conf')
        self.__mmap_conf_file = None
        self.__notification_directory = os.path.join(self.__pool_directory, 'notification')

    def __init_v1_single_file(self, directory_version):
        self.__mmap_file = self.__pool_dir()
        (self.__pool_directory, pname) = os.path.split(self.__mmap_file)
        self.__pool_conf_file = None
        self.__notification_directory = os.path.join(self.__pool_directory, '.notification', pname)

    @with_umask
    def __create(self,
                 resizable=True,
                 single_file=False,
                 size=POOL_SIZE_SMALL,
                 index_capacity=0,
                 mode=0777,
                 uid=-1,
                 gid=-1,
                 stop_when_full=False,
                 frozen=False,
                 auto_dispose=False,
                 checksum=False,
                 sync=False):
        self.__slaw_version = SLAW_VERSION_CURRENT
        self.__slaw_type = PLASMA_BINARY_FILE_TYPE_POOL
        self.__pool_flags = 0
        self.__size = self.__get_size(size)
        self.__perm = { 'mode': int64(mode), 'uid': self.__get_uid(uid), 'gid': self.__get_gid(gid) }
        self.__index_capacity = unt64(index_capacity)
        self.__sem = SemaphoreSet()
        if single_file:
            self.__init_v1(POOL_DIRECTORY_VERSION_SINGLE_FILE)
            self.__create_v1(size, index_capacity, mode, uid, gid, stop_when_full, frozen, auto_dispose, checksum, sync)
        elif resizable:
            self.__init_v1(POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP)
            self.__create_v1(size, index_capacity, mode, uid, gid, stop_when_full, frozen, auto_dispose, checksum, sync)
        else:
            self.__create_v0(size, index_capacity, mode, uid, gid, stop_when_full, frozen, auto_dispose, checksum, sync)

    def __create_v0(self, size=POOL_SIZE_SMALL, index_capacity=0, mode=0777, uid=-1, gid=-1, stop_when_full=False, frozen=False, auto_dispose=False, checksum=False, sync=False):
        self.__init_v0(POOL_DIRECTORY_VERSION_CONFIG_IN_FILE)
        self.__header_size = POOL_MMAP_V0_HEADER_SIZE
        if self.__index_capacity > 0:
            self.__indx = PoolIndex()
            self.__indx.capacity = self.__index_capacity
            self.__header_size += self.__indx.size()
        else:
            self.__indx = None
        magic = POOL_MMAP_MAGICV0 | (self.__slaw_version << POOL_MMAP_SLAW_VERSION_SHIFTY)
        if size < self.__header_size or size > POOL_SIZE_MAX:
            raise PoolInvalidSizeException("Invalid size: %d; must be at least %d and no more than %d" % (size, self.__header_size, POOL_SIZE_MAX))
        makedirs(self.__pool_directory, self.__perm['mode'], self.__perm['uid'], self.__perm['gid'])
        makedirs(self.__notification_directory, self.__perm['mode'], self.__perm['uid'], self.__perm['gid'])
        self.__fh = self.__init_mmap_file()
        self.__fh.write(struct.pack('QQQ', self.__header_size, 0, magic))
        if self.__indx is not None:
            self.__indx.set_pool(self.__fh, POOL_MMAP_V0_HEADER_SIZE)
        self.__save_default_config()
        self.__save_mmap_config()
        self.__fh.close()
        self.__fh = None

    def __create_v1(self, size=POOL_SIZE_SMALL, index_capacity=0, mode=0777, uid=-1, gid=-1, stop_when_full=False, frozen=False, auto_dispose=False, checksum=False, sync=False):
        conf = confChunk()
        ptrs = ptrsChunk()
        perm = permChunk()
        self.__header_size = 8 + conf.size() + ptrs.size() + perm.size()
        if self.__index_capacity > 0:
            indx = indxChunk()
            indx.initialize(self.__index_capacity)
            self.__header_size += indx.size()
        else:
            indx = None
        if self.__size < self.__header_size or self.__size > POOL_SIZE_MAX:
            raise PoolInvalidSizeException("Invalid size: %d; must be at least %d and no more than %d" % (size, self.__header_size, POOL_SIZE_MAX))
        makedirs(self.__pool_directory, self.__perm['mode'], self.__perm['uid'], self.__perm['gid'])
        makedirs(self.__notification_directory, self.__perm['mode'], self.__perm['uid'], self.__perm['gid'])
        self.__fh = self.__init_mmap_file()
        self.__fh.write(BINARY_MAGIC)
        self.__fh.write(struct.pack('>BBH', self.__slaw_version, self.__slaw_type, self.__pool_flags))
        conf.mmap_version = self.__pool_version
        conf.header_size = self.__header_size
        conf.file_size = self.__size
        conf.sem_key = self.__sem.key()
        conf.next_index = 0
        conf.flags = 0
        if stop_when_full:
            conf.flags |= POOL_FLAG_STOP_WHEN_FULL
        if frozen:
            conf.flags |= POOL_FLAG_FROZEN
        if auto_dispose:
            conf.flags |= POOL_FLAG_AUTO_DISPOSE
        if checksum:
            conf.flags |= POOL_FLAG_CHECKSUM
        if sync:
            conf.flags |= POOL_FLAG_SYNC
        ptrs.oldest_entry = conf.header_size
        ptrs.newest_entry = 0
        perm.mode = self.__perm['mode']
        perm.uid = self.__perm['uid']
        perm.gid = self.__perm['gid']
        conf.set_pool(self.__fh, 8)
        ptrs.set_pool(self.__fh, conf.end())
        perm.set_pool(self.__fh, ptrs.end())
        if indx is not None:
            indx.set_pool(self.__fh, perm.end())
            indx.initialize(self.__index_capacity)
        self.__save_default_config()

    def __init_mmap_file(self):
        fh = os.fdopen(os.open(self.__mmap_file, os.O_RDWR|os.O_CREAT|os.O_EXCL, self.__perm['mode'] & 0666), 'rb+')
        os.chown(self.__mmap_file, self.__perm['uid'], self.__perm['gid'])
        os.ftruncate(fh.fileno(), self.__size)
        fh.seek(0)
        return fh

    def __save_default_config(self):
        if self.__pool_conf_file is None:
            return None
        if self.__directory_version == POOL_DIRECTORY_VERSION_CONFIG_IN_FILE:
            config = Protein(ingests={
                'type': 'mmap',
                'pool-version': int32(self.__directory_version),
                'perms': v3int32(int32(self.__perm['mode']),
                                 int32(self.__perm['uid']),
                                 int32(self.__perm['gid'])),
                'sem-key': int32(self.__sem.key())
            })
        else:
            config = Protein(ingests={
                'type': 'mmap',
                'pool-version': int32(self.__directory_version),
            })
        plasma.slaw.write_slaw_file(self.__pool_conf_file, config, self.__slaw_version)

    def __save_mmap_config(self):
        if self.__mmap_conf_file is None:
            return None
        config = Protein(ingests={
            'header-size': unt64(self.__header_size),
            'file-size': unt64(self.__size),
            'index-capacity': unt64(self.__index_capacity),
        })
        plasma.slaw.write_slaw_file(self.__mmap_conf_file, config, self.__slaw_version)

    def __open(self):
        if not self.closed:
            raise Exception("pool already open")
        (pool_version, directory_version) = self.__characterize()
        if pool_version == 0:
            self.__open_v0(directory_version)
        elif pool_version == 1:
            self.__open_v1(directory_version)
        else:
            raise PoolWrongVersionException("unimplemented pool version %d" % pool_version)
        self.__mmap.seek(0)
        self.__pool_pos = 0
        self.closed = False
        self.__last = None
        (old, new) = self.__pointers()
        if new < old:
            ## pool is empty, start at the oldest pointer
            self.seek(old)
        else:
            self.seek(new)
            self.readline()

    def __open_v0(self, directory_version):
        self.__init_v0(directory_version)
        pool_conf = plasma.slaw.read_slaw_file(self.__pool_conf_file).ingests()
        mmap_conf = plasma.slaw.read_slaw_file(self.__mmap_conf_file).ingests()
        if type(pool_conf) == Protein:
            pool_conf = pool_conf.ingests()
        if type(mmap_conf) == Protein:
            mmap_conf = mmap_conf.ingests()
        perms = pool_conf.get('perms')
        self.__perm = {
            'mode': perms.x,
            'uid': perms.y,
            'gid': perms.z
        }
        self.__sem = SemaphoreSet(pool_conf['sem-key'])
        self.__size = mmap_conf['file-size']
        self.__header_size = mmap_conf['header-size']
        self.__index_capacity = mmap_conf.get('index-capacity', 0)
        self.__mmap_open()
        self.__mmap.seek(16)
        (magic,) = struct.unpack('Q', self.__mmap.read(8))
        if magic & 0xffffffff00ffffff != POOL_MMAP_MAGICV0:
            raise PoolCorruptException("invalid magic number (0x%x != 0x%x)", (magic, POOL_MMAP_MAGICV0))
        self.__slaw_version = (magic >> POOL_MMAP_SLAW_VERSION_SHIFTY) & 0xff
        if self.__index_capacity > 0:
            self.__indx = PoolIndex(self.__mmap)
        else:
            self.__indx = None

    def __open_v1(self, directory_version):
        self.__init_v1(directory_version)
        self.__mmap_open()
        (magic, sv, st, flags) = struct.unpack('>4sBBH', self.__mmap.read(8))
        if magic != BINARY_MAGIC:
            raise PoolCorruptException("invalid magic number (0x%s != 0x%s)" % (''.join('%02x' % ord(x) for x in magic), ''.join('%02x' % ord(x) for x in BINARY_MAGIC)))
        self.__slaw_version = sv
        self.__slaw_flags = flags
        conf = Chunk.load(self.__mmap)
        header_size = conf.header_size - 8 - conf.size()
        self.__chunks = { 'conf': conf }
        while header_size > 0:
            chnk = Chunk.load(self.__mmap)
            header_size -= chnk.size()
            self.__chunks[chnk.name()] = chnk
        if self.__chunks.has_key('indx'):
            self.__indx = self.__chunks['indx']
        else:
            self.__indx = None
        self.__sem = SemaphoreSet(conf.sem_key)
        self.__header_size = self.__chunks['conf'].header_size
        self.__size = self.__chunks['conf'].file_size

    def __mmap_open(self):
        path = self.__mmap_file
        if not os.path.exists(path):
            raise PoolNoSuchPoolException(self.name(), path)
        self.__fh = os.fdopen(os.open(path, os.O_RDWR|os.O_SHLOCK))
        try:
            self.__mmap = mmap.mmap(self.__fh.fileno(), 0)
        except mmap.error, e:
            self.__fh.close()
            self.__fh = None
            raise PoolMmapBadthException('%s' % e)
        return self.__mmap

    def __characterize(self):
        path = self.__pool_dir()
        if not os.path.exists(path):
            raise PoolNoSuchPoolException(self.name(), path)
        if not os.path.isdir(path):
            ## single file pool
            return (1, POOL_DIRECTORY_VERSION_SINGLE_FILE)
        conf_file = os.path.join(path, 'pool.conf')
        if not os.path.exists(conf_file):
            raise PoolNoSuchPoolException(self.name(), path)
        conf = plasma.slaw.read_slaw_file(conf_file).ingests()
        version = conf.get('pool-version', None)
        if version is None:
            raise PoolFileBadthException("Pool '%s' config missing 'pool-version'" % self.name())
        if version == POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP:
            return (1, version)
        if version == POOL_DIRECTORY_VERSION_CONFIG_IN_FILE:
            return (0, version)
        raise PoolWrongVersionException("'%s' had pool-version %d, but expected either %d or %d" % (version, POOL_DIRECTORY_VERSION_CONFIG_IN_FILE, POOL_DIRECTORY_VERSION_CONFIG_IN_MMAP))

    def __close(self):
        if self.__sem is not None:
            if self.__sem.has_deposit_lock():
                self.__sem.deposit_unlock()
            if self.__sem.has_notification_lock():
                self.__sem.notification_unlock()
        self.__indx = None
        self.__chunks = None
        self.__mmap_file = None
        self.__cache_buffer = None
        if self.__mmap is not None:
            try:
                self.__mmap.close()
            except:
                pass
            self.__mmap = None
        if self.__fh is not None:
            try:
                self.__fh.close()
            except:
                pass
            self.__fh = None
        self.closed = True

    def __lock_pool(self, wait=False):
        if self.__fh is not None:
            fh = self.__fh
        else:
            if self.__mmap_file is None:
                (pv, dv) = self.__characterize()
                if pv == 1:
                    self.__init_v1(dv)
                elif pv == 0:
                    self.__init_v0(dv)
            fh = os.fdopen(os.open(self.__mmap_file, os.O_RDONLY|os.O_SHLOCK))
        if wait:
            lock_op = fcntl.LOCK_EX
        else:
            lock_op = fcntl.LOCK_EX|fcntl.LOCK_NB
        try:
            fcntl.flock(fh.fileno(), lock_op)
        except IOError:
            raise PoolInUseException()
        return fh

    def __unlock_pool(self, fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_SH)

    def __pointers(self):
        if self.__pool_version == 0:
            pos = self.__mmap.tell()
            self.__mmap.seek(0)
            (old, new) = struct.unpack('qq', self.__mmap.read(16))
            self.__mmap.seek(pos)
            return (old, new)
        return self.__chunks['ptrs'].all()

    def __set_pointers(self, old, new):
        #(xold, xnew) = self.__pointers()
        #if old > new:
        #    raise Exception("setting pointers to something bizzarre: (%d, %d) from (%d, %d)" % (old, new, xold, xnew))
        #if xnew - new > 2 * self.file_size():
        #    raise Exception("big jump in pointers: (%d, %d) to (%d, %d)" % (xold, xnew, olf, new))
        if self.__pool_version == 0:
            pos = self.__mmap.tell()
            self.__mmap.seek(0)
            self.__mmap.write(struct.pack('qq', old, new))
            self.__mmap.seek(pos)
        else:
            self.__chunks['ptrs'].oldest_entry = old
            self.__chunks['ptrs'].newest_entry = new

    def __index_at(self, pos=None):
        orig_pos = self.tell()
        if pos is None:
            pos = self.tell()
        else:
            self.seek(pos)
        try:
            (timestamp, index) = struct.unpack('dQ', self.read(16))
        finally:
            self.seek(orig_pos)
        return index

    def __quick_cache(self, index, start, size):
        if self.__cache_buffer is None:
            self.__cache_buffer = list((None,) * CACHE_SIZE)
        n = index % len(self.__cache_buffer)
        self.__cache_buffer[n] = (index, start, size)

    def __quick_lookup(self, index):
        if self.__cache_buffer is None:
            return None
        n = index % len(self.__cache_buffer)
        cache = self.__cache_buffer[n]
        if cache is not None and cache[0] == index:
            return cache[1]
        return None

    def __deposit(self, protein):
        if self.frozen():
            raise PoolFrozenException()
        timestamp = obtimestamp()
        (old, new) = self.__pointers()
        last = self.__last
        curpos = self.tell()
        data = protein.to_slaw()
        jumpback = len(data) + 24
        if jumpback > self.__max_protein_size():
            raise PoolProteinBiggerThanPoolException()
        try:
            if new >= old:
                self.seek(new)
                last_protein = self.readline()
                index = last_protein.index() + 1
            else:
                ## pool is empty
                self.seek(old)
                index = 0
            if self.__chunks is not None:
                index = self.__chunks['conf'].next_index
            newnew = self.tell()
            sz = self.file_size()
            if newnew % sz == 0:
                newnew += self.header_size()
                self.seek(newnew)
            ptr = newnew % sz
            if ptr + jumpback > sz:
                ## protein will need to wrap
                if self.stop_when_full():
                    raise PoolFullException()
                newnew = newnew + (sz - (newnew % sz)) + self.header_size()
                ptr = newnew % sz
            (first_index, first_pos) = self.first_index_and_offset()
            newold = old
            self.seek(old)
            ## check that we're not overwriting old proteins
            ## if so, advance oldest pointer
            while newnew + jumpback - sz > newold:
                ix = self.skip()
                if ix == first_index - 1:
                    ## we're overwriting the remainder of the pool
                    newold = first_pos
                    self.seek(newold)
                else:
                    newold = self.tell()
            ## advance oldest pointer before we write
            self.__set_pointers(newold, new)
            self.__mmap.seek(newnew % sz)
            self.__mmap.write(struct.pack('dQ', timestamp.timestamp(), index))
            self.__mmap.write(data)
            self.__mmap.write(struct.pack('Q', jumpback))
            ## advance newest pointer after write
            self.__set_pointers(newold, newnew)
            if self.__chunks is not None:
                self.__chunks['conf'].next_index = index + 1
        finally:
            self.seek(curpos)
            self.__last = last
        if self.__indx is not None:
            self.__indx.add_entry(newnew, timestamp, index, newold)
        self.__wake_awaiters()
        return (index, timestamp, newnew)

    def __lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        if type(timestamp) == datetime.datetime:
            timestamp = obtimestamp(timestamp).timestamp()
        elif isinstance(timestamp, obtimestamp):
            timestamp = timestamp.timestamp()
        orig_pos = self.tell()
        if whence != TIMESTAMP_ABSOLUTE:
            last = self.__last
            try:
                p = self.nth_protein(whence)
                timestamp += p.timestamp()
            finally:
                self.seek(orig_pos)
                self.__last = last
        if self.__indx is not None:
            (offset, ts, idx) = self.__indx.timefind(timestamp)
            ts = ts.timestamp()
            self.seek(offset)
        else:
            (old, new) = self.__pointers()
            if new < old:
                raise PoolNoSuchProteinException("pool is empty")
            self.seek(new)
            try:
                (ts, idx) = struct.unpack('dQ', self.read(16))
                self.seek(new)
            except StompledException:
                self.seek(orig_pos)
                return self.__lookup(timestamp, TIMESTAMP_ABSOLUTE, direction)
            offset = new
        if ts < timestamp and direction == DIRECTION_HIGHER:
            raise PoolNoSuchProteinException("latest protein is earlier than the requested time")
        prev_offset = offset
        prev_ts = ts
        prev_idx = idx
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

    def __max_protein_size(self):
        return self.file_size() - self.header_size()

