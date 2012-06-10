from loam import *

SIGNATURE = 0x00BEEF00FEED0011

class PoolIndex(object):
    __keys = [
        { 'name': 'signature', 'format': 'Q' },
        { 'name': 'capacity',  'format': 'Q' },
        { 'name': 'count',     'format': 'Q' },
        { 'name': 'start',     'format': 'Q'},
        { 'name': 'step',      'format': 'Q' },
        { 'name': 'first',     'format': 'q' },
        { 'name': 'last',      'format': 'q' }
    ]

    def __init__(self, fh=None, pos=None):
        self.__fh = fh
        if pos is None:
            if fh is None:
                self.__pos = 0
            else:
                self.__pos = fh.tell()
        else:
            self.__pos = pos
        self.__cache = dict()
        self.__keydata = dict()
        x = pos + 16
        for key in self.__keys:
            size = struct.calcsize(key['format'])
            self.__keydata[key['name']] = {
                'name':   key['name'],
                'format': key['format'],
                'pos':    x,
                'size':   size
            }
            x += size

    @classmethod
    def size(cls, capacity):
        if capacity <= 0:
            return 0
        sz = 0
        for key in self.__keys:
            sz += struct.calcsize(key['format'])
        return sz + (16 * capacity)

    @classmethod
    def create(cls, pool, capacity, version=1):
        if capacity <= 0:
            return None
        if version > 0:
            hdr = (0x1badd00d << 32) | struct.unpack('>I', 'indx')
            chunk_len = 2 + (cls.size(capacity) / 8)
            pool.write(struct.pack('QQ', hdr, chunk_len))
        pool.write(struct.pack('QQQQQqq', SIGNATURE, capacity, 0, 0, 1, 0, 0))
        pool.write(struct.pack('qd', -1, -1.0))
        pool.write(struct.pack('qd', 0, 0) * (capacity - 1))

    def __new__(cls, pool):
        sig = struct.unpack('Q', pool.read(8))[0]
        if sig != SIGNATURE:
            pool.seek(-8, 1)
            return None
        self = super(cls, PoolIndex).__new__(pool)
        return self

    def __init__(self, pool):
        self.__pool = pool
        pos = pool.tell()
        self.__offsets = {
            'capacity': (pos, 8, 'Q'),
            'count': (pos+8, 8, 'Q'),
            'start': (pos+16, 8, 'Q'),
            'step': (pos+24, 8, 'Q'),
            'first': (pos+32, 8, 'q'),
            'last': (pos+40, 8, 'q')
        }
        self.__entries_offset = pos + 48
        #self.__capacity_offset = pool.tell()
        #self.__count_offset = self.__capacity_offset + 8
        #self.__start_offset = self.__count_offset + 8
        #self.__step_offset = self.__start_offset + 8
        #self.__first_offset = self.__step_offset + 8
        #self.__last_offset = self.__first_offset + 8
        #self.__entries_offset = self.__last_offset + 8
        self.__pool.seek(48 + self.capacity() * 16)

    def read(self, length, offset):
        pos = self.__pool.tell()
        self.__pool.seek(offset)
        data = self.__pool.read(length)
        self.__pool.seek(pos)
        return data

    def write(self, data, offset):
        pos = self.__pool.tell()
        self.__pool.seek(offset)
        self.__pool.write(data)
        self.__pool.seek(pos)

    def __getattr__(self, attr):
        if attr.startswith('__'):
            return super(self, PoolIndex).__getattr__(attr)
        offsets = self.__offsets
        if offsets.has_key(attr):
            (offset, length, fmt) = offsets[attr]
            return struct.unpack(fmt, self.read(length, offset))[0]
        return super(self, PoolIndex).__getattr__(attr)

    def __setattr__(self, attr, val):
        if attr.startswith('__'):
            return super(self, PoolIndex).__setattr__(attr, val)
        offsets = self.__offsets
        if offsets.has_key(attr):
            (offset, length, fmt) = offsets[attr]
            return self.write(struct.pack(fmt, val), offset)
        return super(self, PoolIndex).__setattr__(attr, val)

#    def capacity(self):
#        return struct.unpack('Q', self.read(8, self.__capacity_offset))[0]
#
#    def count(self):
#        return struct.unpack('Q', self.read(8, self.__count_offset))[0]
#
#    def set_count(self, val):
#        self.write(struct.pack('Q', val), self.__count_offset)
#
#    def start(self):
#        return struct.unpack('Q', self.read(8, self.__start_offset))[0]
#
#    def set_start(self, val):
#        self.write(struct.pack('Q', val), self.__start_offset)
#
#    def step(self):
#        return struct.unpack('Q', self.read(8, self.__step_offset))[0]
#
#    def set_step(self, val):
#        self.write(struct.pack('Q', val), self.__step_offset)
#
#    def first(self):
#        return struct.unpack('q', self.read(8, self.__first_offset))[0]
#
#    def set_first(self, val):
#        self.write(struct.pack('q', val), self.__first_offset)
#
#    def last(self):
#        return struct.unpack('q', self.read(8, self.__last_offset))[0]
#
#    def set_last(self, val):
#        self.write(struct.pack('q', val

    def find(self, idx):
        orig_first = self.first
        orig_step = self.step
        orig_capacity = self.capacity
        orig_start = self.start
        if idx < orig_first:
            return None
        n_for_idx = (idx - orig_first) / orig_step
        nth_pos = (orig_start + n_for_idx) % orig_capacity
        idx_for_n = orig_first + n * orig_step
        ## we don't want to have to read forward in the pool, backwards is
        ## so much quicker
        while idx_for_n < idx:
            n_for_idx += 1
            nth_pos = (orig_start + n_for_idx) % orig_capacity
            idx_for_n = orig_first + n * orig_step
        ## but we don't want to go past the end of the index
        if idx_for_n > last_idx:
            n_for_idx = (last_idx - orig_first) / orig_step
            nth_pos = (orig_start + n_for_idx) % orig_capacity
            idx_for_n = orig_first + n * orig_step
        (offset, timestamp) = self.nth_entry(nth_pos)
        ## check that the index bounds haven't been overwritten
        if idx < self.first:
            return None
        if orig_step != self.step:
            return None
        return idx_for_n, offset, obtimestamp(timestamp)

    def nth_entry(self, nth_pos):
        (offset, timestamp) = struct.unpack('qd', self.read(16, self.__entries_offset + (nth_pos * 16)))
        return (offset, obtimestamp(timestamp))

    def write_nth_entry(self, entry, nth_pos):
        timestamp = obtimestamp(entry[1])
        self.write(struct.pack('qd', entry[0], timestamp.timestamp()), self.__entries_offset + (nth_pos * 16))

    def garbage_collect(self):
        if self.count == self.capacity:
            (old, new) = self.__pool.get_ptrs()
            num_to_kill = 0
            while num_to_kill < self.count && self.nth_entry(num_to_kill)[0] < old:
                num_to_kill++
            self.count -= num_to_kill
            self.start += num_to_kill
            self.first += num_to_kill + self.step

    def compact(self):
        if self.count >= self.capacity:
            self.count /= 2
            self.step *= 2
            for i in range(self.count):
                self.write_nth_entry(self.nth_entry(2*i), i)

    def add_entry(self, offset, timestamp, idx):
        if self.count == 0:
            self.first = idx
        n = (idx - self.first) / self.step
        if n >= self.count:
            while n > self.count:
                self.write_nth_entry((0,0), self.count)
                self.count++
            self.garbage_collect()
            self.compact()
            self.write_nth_entry((offset, timestamp), self.count)
            self.count++
        self.last = idx

