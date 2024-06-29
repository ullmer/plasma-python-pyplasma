import struct
from loam.obstruct import obcons
from loam.obtime import obtimestamp
from plasma.const import *
from plasma.exceptions import *
import datetime

CHUNK_HEADER = 0x1badd00d

class Chunk(object):
    @classmethod
    def load(cls, fh):
        pos = fh.tell()
        (x,) = struct.unpack('Q', fh.read(8))
        if x >> 32 != CHUNK_HEADER:
            raise PoolCorruptException("header chunk at %d doesn't start with 0x%x (0x%x)" % (pos, CHUNK_HEADER, x >> 32))
        (chunk_type,) = struct.unpack('4s', struct.pack('>I', x & 0xffffffff))
        (chunk_len,) = struct.unpack('Q', fh.read(8))
        fh.seek(8 * (chunk_len - 2), 1)
        if chunk_type == 'conf' or chunk_type == b'conf': #TODO: probably should be generalized
            return confChunk(fh, pos)
        if chunk_type == 'ptrs' or chunk_type == b'ptrs':
            return ptrsChunk(fh, pos)
        if chunk_type == 'perm' or chunk_type == b'perm':
            return permChunk(fh, pos)
        if chunk_type == 'indx' or chunk_type == b'indx':
            return indxChunk(fh, pos)
        raise PoolCorruptException("unknown chunk type '%s'" % chunk_type)

    def __init__(self, fh=None, pos=None):
        self._fh = fh
        if pos is None:
            if fh is None:
                self._pos = 0
            else:
                self._pos = fh.tell()
        else:
            self._pos = pos
        self._cache = dict()
        self._keydata = dict()
        x = self._pos + 16
        for key in self._keys:
            size = struct.calcsize(key['format'])
            self._keydata[key['name']] = {
                'name':   key['name'],
                'format': key['format'],
                'pos':    x,
                'size':   size,
                'default': key['default']
            }
            if fh is None:
                setattr(self, key['name'], key['default'])
            x += size

    def __str__(self):
        return '<%s: %s>' % (type(self).__name__, '; '.join('%s = %s' % (k['name'], getattr(self, k['name'])) for k in self._keys))

    def set_pool(self, fh, pos=None):
        if pos is None:
            pos = fh.tell()
        self._fh = fh
        self._pos = pos
        #print 'setting %s start to %d' % (type(self).__name__, pos)
        for kd in self._keydata.values():
            kd['pos'] += self._pos
        for k, v in self._cache.iteritems():
            setattr(self, k, v)
        self.write_header()
        self._fh.seek(self.datasize(), 1)

    def __getattribute__(self, key):
        if key.startswith('_'):
            return super(Chunk, self).__getattribute__(key)
        if self._keydata.has_key(key):
            if self._fh is None:
                return self._cache.get(key, self._keydata[key]['default'])
            startpos = self._fh.tell()
            keydata = self._seekto(key)
            pos = self._fh.tell()
            (val,) = struct.unpack(keydata['format'], self._fh.read(keydata['size']))
            #print 'get %s (at %d): %s' % (key, pos, val)
            self._fh.seek(startpos)
            return val
        return super(Chunk, self).__getattribute__(key)

    def __setattr__(self, key, val):
        #print '%s.__setattr__(%s, %s)' % (type(self).__name__, key, val)
        if key.startswith('_'):
            return super(Chunk, self).__setattr__(key, val)
        if self._keydata.has_key(key):
            if self._fh is None:
                #print 'no fh yet, caching %s = %s' % (key, val)
                self._cache[key] = val
                return None
            startpos = self._fh.tell()
            keydata = self._seekto(key)
            #print 'set %s to %s (%s at %d)' % (key, val, ' '.join('%02x' % ord(x) for x in struct.pack(keydata['format'], val)), self._fh.tell())
            self._fh.write(struct.pack(keydata['format'], val))
            self._fh.seek(startpos)
            return None
        #print '%s not a special key' % key
        return super(Chunk, self).__setattr__(key, val)

    def _seekto(self, key):
        #print '_seekto %s' % key
        keydata = self._keydata[key]
        self._fh.seek(keydata['pos'])
        return keydata

    def size(self):
        return self.datasize() + 16

    def datasize(self):
        sz = 0
        for key in self._keydata.values():
            sz += key['size']
        return sz

    def all(self):
        fmt = ''
        for key in self._keys:
            fmt += key['format']
        startpos = self._fh.tell()
        self._fh.seek(self._pos + 16)
        readlen = self.datasize()
        retval = struct.unpack(fmt, self._fh.read(readlen))
        self._fh.seek(startpos)
        return retval

    def end(self):
        return self._pos + self.size()

    def write_header(self):
        self._fh.seek(self._pos)
        hdr = (0x1badd00d << 32) | struct.unpack('>I', self._name)[0]
        self._fh.write(struct.pack('QQ', hdr, self.size() / 8))

    def name(self):
        return self._name

class confChunk(Chunk):
    _name = 'conf'
    _keys = [
        { 'name': 'mmap_version', 'format': 'q', 'default': 0 },
        { 'name': 'file_size',    'format': 'q', 'default': 0 },
        { 'name': 'header_size',  'format': 'q', 'default': 0 },
        { 'name': 'sem_key',      'format': 'q', 'default': 0 },
        { 'name': 'flags',        'format': 'q', 'default': 0 },
        { 'name': 'next_index',   'format': 'q', 'default': 0 }
    ]

class ptrsChunk(Chunk):
    _name = 'ptrs'
    _keys = [
        { 'name': 'oldest_entry', 'format': 'q', 'default': 0 },
        { 'name': 'newest_entry', 'format': 'q', 'default': 0 }
    ]

class permChunk(Chunk):
    _name = 'perm'
    _keys = [
        #{ 'name': 'mode', 'format': 'q', 'default': 0666 },
        { 'name': 'mode', 'format': 'q', 'default': 0o666 },
        { 'name': 'uid',  'format': 'q', 'default': -1 },
        { 'name': 'gid',  'format': 'q', 'default': -1 }
    ]

class PoolIndexBase(object):
    _keys = [
        { 'name': 'signature', 'format': 'Q', 'default': 0x00BEEF00FEED0011 },
        { 'name': 'capacity',  'format': 'Q', 'default': 0 },
        { 'name': 'count',     'format': 'Q', 'default': 0 },
        { 'name': 'start',     'format': 'Q', 'default': 0 },
        { 'name': 'step',      'format': 'Q', 'default': 1 },
        { 'name': 'first',     'format': 'q', 'default': 0 },
        { 'name': 'last',      'format': 'q', 'default': 0 }
    ]

    def __getitem__(self, idx):
        if self._fh is None:
            return self._itemcache[idx]
        nth_pos = (self.start + idx) % self.capacity
        #self._fh.seek(self._pos + self._entries_offset + (16 * idx))
        self._fh.seek(self._pos + self._entries_offset + (16 * nth_pos))
        #print "indx: get(%d) -> %d" % (idx, self._fh.tell())
        return struct.unpack('qd', self._fh.read(16))

    def __setitem__(self, idx, val):
        if not isinstance(val, (obcons, list, tuple)):
            raise ValueError("%s may only contain (index, timestamp) tuples" % type(self).__name__)
        nth_pos = (self.start + idx) % self.capacity
        offset = val[0]
        if isinstance(val[1], obtimestamp):
            ts = val[1].timestamp()
        elif isinstance(val[1], datetime.datetime):
            ts = obtimestamp(val[1]).timestamp()
        else:
            ts = val[1]
        if self._fh is None:
            #self._itemcache[idx] = (offset, ts)
            self._itemcache[nth_pos] = (offset, ts)
            return None
        #self._fh.seek(self._pos + self._entries_offset + (16 * idx))
        self._fh.seek(self._pos + self._entries_offset + (16 * nth_pos))
        self._fh.write(struct.pack('qd', offset, ts))
        return None

    def initialize(self, capacity):
        self.signature = self.signature
        self.capacity = capacity
        self._itemcache = list((None,)*capacity)
        self[0] = (-1, -1.0)
        #for i in range(1, capacity):
        #    self[i] = (-1, 0.0)
        self.capacity = capacity

    def idx_for_n(self, n):
        x = n - self.start
        if x < 0:
            x += self.capacity
        idx = self.first + (x * self.step)
        if idx > self.last:
            idx = self.last
        return idx

    def find(self, idx):
        orig_first = self.first
        orig_last = self.last
        orig_step = self.step
        orig_capacity = self.capacity
        orig_start = self.start
        if idx < orig_first:
            return (None, None, None)
        n_for_idx = (idx - orig_first) / orig_step
        #nth_pos = (orig_start + n_for_idx) % orig_capacity
        idx_for_n = orig_first + (n_for_idx * orig_step)
        #(offset, timestamp) = self[nth_pos]
        (offset, timestamp) = self[n_for_idx]
        #print "%d -> %d: %d, %f" % (idx, nth_pos, offset, timestamp)
        if offset == 0:
            return (None, None, None)
        ## check that the index bounds haven't been overwritten
        if idx < self.first:
            return (None, None, None)
        if orig_step != self.step:
            return (None, None, None)
        if idx_for_n > orig_last:
            #print "%d: idx (%d) for n (%d) > last (%d)" % (idx, idx_for_n, nth_pos, orig_last)
            #print "count=%d; step=%d; start=%d; first=%d; last=%d" % (self.count, self.step, self.start, self.first, self.last)
            idx_for_n = orig_last
        #print "looking for %d, got %d" % (idx, idx_for_n)
        #print "count=%d; step=%d; start=%d; first=%d; last=%d" % (self.count, self.step, self.start, self.first, self.last)
        return idx_for_n, offset, obtimestamp(timestamp)

    def timefind(self, ts):
        if type(ts) == datetime.datetime:
            ts = obtimestamp(ts).timestamp()
        elif isinstance(ts, obtimestamp):
            ts = ts.timestamp()
        orig_first = self.first
        orig_last = self.last
        orig_start = self.start
        orig_step = self.step
        orig_capacity = self.capacity
        for i in range(self.count):
            #n = (orig_start + i) % orig_capacity
            #(offset, timestamp) = self[n]
            (offset, timestamp) = self[i]
            if timestamp >= ts:
                #return (offset, obtimestamp(timestamp), self.idx_for_n(n))
                return (offset, obtimestamp(timestamp), self.idx_for_n(i))
            if orig_step != self.step:
                logging.warning("index compacted while looking for timestamp")
                return self.timefind(ts)
        #n = (self.start + self.count) % self.capacity
        #(offset, timestamp) = self[n]
        (offset, timestamp) = self[self.count]
        #return (offset, obtimestamp(timestamp), self.idx_for_n(n))
        return (offset, obtimestamp(timestamp), self.idx_for_n(self.count))

    def garbage_collect(self, oldest):
        num_to_kill = 0
        count = self.count
        while num_to_kill < count and self[num_to_kill][0] < oldest:
            num_to_kill += 1
        #print "garbage collect: num_to_kill = %d" % num_to_kill
        self.count -= num_to_kill
        self.start += num_to_kill
        self.first += num_to_kill * self.step

    def compact(self):
        if self.count >= self.capacity:
            #print "compact: count %d -> %d; step %d -> %d" % (self.count, self.count / 2, self.step, self.step * 2)
            self.count /= 2
            self.step *= 2
            for i in range(1, self.count):
                self[i] = self[2*i]
                #n = (i + self.start) % self.capacity
                #n2 = ((2*i) + self.start) % self.capacity
                #self[n] = self[n2]

    def add_entry(self, offset, timestamp, idx, oldest):
        if self.count == 0:
            self.first = idx
        n = (idx - self.first) / self.step
        #print "add_entry(%d, %d) -> %d" % (idx, offset, n)
        if n >= self.count:
            while n > self.count:
                #print 'add_entry(%d, %d): zeroing out %d' % (offset, idx, self.count)
                self[self.count] = (0, 0.0)
                self.count += 1
            if self.count == self.capacity:
                #print 'add_entry(%d, %d): garbage collect(%d)' % (offset, idx, oldest)
                self.garbage_collect(oldest)
            if self.count >= self.capacity:
                #print 'add_entry(%d, %d): compact()' % (offset, idx)
                self.compact()
            #print 'add_entry(%d, %d): %d' % (idx, offset, self.count)
            self[self.count] = (offset, timestamp)
            self.count += 1
        #else:
        #    print "n (%d) < count (%d)" % (n, self.count)
        self.last = idx
        #print "count=%d; step=%d; start=%d; first=%d; last=%d" % (self.count, self.step, self.start, self.first, self.last)

class PoolIndex(Chunk, PoolIndexBase):
    def __init__(self, *args, **kwargs):
        super(PoolIndex, self).__init__(*args, **kwargs)
        offset = 0
        for key in self._keydata.values():
            offset += key['size']
        self._entries_offset = offset

    def size(self):
        sz = 0
        for key in self._keydata.values():
            sz += key['size']
        return sz + (16 * self.capacity)

    def write_header(self):
        return None

    def set_pool(self, *args, **kwargs):
        super(PoolIndex, self).set_pool(*args, **kwargs)
        for i in range(self.capacity):
            self[i] = self._itemcache[i]

class indxChunk(Chunk, PoolIndexBase):
    _name = 'indx'

    def __init__(self, *args, **kwargs):
        super(indxChunk, self).__init__(*args, **kwargs)
        offset = 16
        for key in self._keydata.values():
            offset += key['size']
        self._entries_offset = offset

    def size(self):
        sz = 16
        for key in self._keydata.values():
            sz += key['size']
        return sz + (16 * self.capacity)

