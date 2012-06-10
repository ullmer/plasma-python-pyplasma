import struct
from loam.util import get_prefix, AM_I_BIG_ENDIAN
from loam.obnum import unt64, int64

class obstring(unicode):
    """
    obstrings behave just like unicode strings, but have a to_slaw
    method attached to them.  All of the usual string methods work here,
    and return obstring objects (instead of unicode or str objects) or
    int64/unt64 objects (instead of int objects).  Methods that usually
    return bool values still return bool values, not obbools, however.
    """

    def __add__(self, *args):
        return obstring(unicode.__add__(self, *args))

    def __format__(self, *args):
        return obstring(unicode.__format__(self, *args))

    def __getitem__(self, *args):
        return obstring(unicode.__getitem__(self, *args))

    def __getslice__(self, *args):
        return obstring(unicode.__getslice__(self, *args))

    def __len__(self, *args):
        return unt64(unicode.__len__(self))

    def __sizeof__(self, *args):
        return unt64(unicode.__sizeof__(self))

    def __mod__(self, *args):
        return obstring(unicode.__mod__(self, *args))

    def __rmod__(self, *args):
        return obstring(unicode.__rmod__(self, *args))

    def __mul__(self, *args):
        return obstring(unicode.__mul__(self, *args))

    def __rmul__(self, *args):
        return obstring(unicode.__rmul__(self, *args))

    def capitalize(self, *args):
        return obstring(unicode.capitalize(self, *args))

    def center(self, *args):
        return obstring(unicode.center(self, *args))

    def count(self, *args):
        return unt64(unicode.count(self, *args))

    def expandtabs(self, *args):
        return obstring(unicode.expandtabs(self, *args))

    def find(self, *args):
        return int64(unicode.find(self, *args))

    def format(self, *args):
        return obstring(unicode.format(self, *args))

    def index(self, *args):
        return unt64(unicode.index(self, *args))

    def join(self, *args):
        return obstring(unicode.join(self, *args))

    def ljust(self, *args):
        return obstring(unicode.ljust(self, *args))

    def lower(self, *args):
        return obstring(unicode.lower(self, *args))

    def lstrip(self, *args):
        return obstring(unicode.lstrip(self, *args))

    def partition(self, *args):
        return (obstring(x) for x in unicode.partition(self, *args))

    def replace(self, *args):
        return obstring(unicode.replace(self, *args))

    def rfind(self, *args):
        return int64(unicode.find(self, *args))

    def rindex(self, *args):
        return unt64(unicode.find(self, *args))

    def rjust(self, *args):
        return obstring(unicode.rjust(self, *args))

    def rpartition(self, *args):
        return (obstring(x) for x in unicode.rpartition(self, *args))

    def rsplit(self, *args):
        return obstruct.oblist(obstring(x) for x in unicode.rsplit(self, *args))

    def rstrip(self, *args):
        return obstring(unicode.rstrip(self, *args))

    def split(self, *args):
        return obstruct.oblist(obstring(x) for x in unicode.split(self, *args))

    def splitlines(self, *args):
        return obstruct.oblist(obstring(x) for x in unicode.splitlines(self, *args))

    def strip(self, *args):
        return obstring(unicode.strip(self, *args))

    def swapcase(self, *args):
        return obstring(unicode.swapcase(self, *args))

    def title(self, *args):
        return obstring(unicode.title(self, *args))

    def translate(self, *args):
        return obstring(unicode.translate(self, *args))

    def upper(self, *args):
        return obstring(unicode.upper(self, *args))

    def zfill(self, *args):
        return obstring(unicode.zfill(self, *args))

    def to_slaw(self, version=2):
        if version == 2:
            return self.to_slaw_v2()
        if version == 1:
            return self.to_slaw_v1()
        raise SlawWrongVersionException('Slaw version %d not supported' % version)

    def to_slaw_v1(self):
        v = self.encode('utf8') + '\x00'
        q = len(v)
        if q % 4:
            v += '\x00' * (4 - (q % 4))
            q += 4 - (q % 4)
        q = q / 4
        if q <= 0x1fffffff:
            header = struct.pack('I', (2 << 28) | q)
        elif q <= 0xffffffff:
            header = struct.pack('II', 0xa0000001, q)
        else:
            header = struct.pack('IQ', 0xe0000001, q)
        return header + v

    def to_slaw_v2(self):
        x = self.encode('utf8')+'\x00'
        n = len(x)
        if n <= 7:
            ## pack as wee string
            if AM_I_BIG_ENDIAN:
                x = '%s%s' % ('\x00' * (8 - len(x)), x)
            (special,) = struct.unpack('q', struct.pack('8s', x))
            first = 0x30 | n
            packed = struct.pack('q', (first << 56) | special)
        else:
            ## pack as full string
            octlen = 1 + (n / 8)
            if n % 8 > 0:
                octlen += 1
                padding = 8 - (n % 8)
            else:
                padding = 0
            first = 0x70 | padding
            packed = struct.pack('q%ds' % ((octlen-1)*8), (first << 56) | octlen, x)
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def to_json(self, degrade=False):
        if degrade:
            return unicode(self)
        return { 'json_class': 'obstring', 'v': unicode(self) }

    def to_yaml(self, indent=''):
        return '%s' % self

import obstruct
