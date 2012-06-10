from loam.util import make_obnumber
from loam.obnum import obnumber
import struct

class numeric_array(list):

    def __init__(self, vals, obtype):
        list.__init__(self, list(None for x in vals))
        self.__obtype = obtype
        for i in range(len(vals)):
            if isinstance(vals[i], obnumber):
                if type(vals[i]) != self.__obtype:
                    raise ObInadequateClassException("Can't use %s(%s) in %s array" % (type(vals[i]).__name__, vals[i], self.__obtype.__name__))
                self[i] = vals[i]
            else:
                self[i] = obtype(vals[i])
        self.is_signed = obtype.is_signed
        self.is_float = obtype.is_float
        self.is_complex = obtype.is_complex
        self.vtype = obtype.vtype
        self.bits = obtype.bits
        self.size = obtype.size

    def __setitem__(self, index, value):
        if isinstance(value, obnumber):
            if type(value) != self.__obtype:
                raise ObInadequateClassException("Can't use %s(%s) in %s array" % (type(value).__name__, value, self.__obtype.__name__))
        else:
            value = self.__obtype(value)
        list.__setitem__(self, index, value)

    def __setslice__(self, i, j, values):
        for k in range(len(values)):
            if isinstance(values[k], obnumber):
                if type(values[k]) != self.__obtype:
                    raise ObInadequateClassException("Can't use %s(%s) in %s array" % (type(values[k]).__name__, values[k], self.__obtype.__name__))
            else:
                values[k] = self.__obtype(values[k])
        list.__setslice__(self, i, j, values)

    def append(self, value):
        if isinstance(value, obnumber):
            if type(value) != self.__obtype:
                raise ObInadequateClassException("Can't use %s(%s) in %s array" % (type(value).__name__, value, self.__obtype.__name__))
        else:
            value = self.__obtype(value)
        list.append(self, value)

    def extend(self, vals):
        values = list()
        for val in vals:
            if isinstance(val, obnumber):
                if type(val) != self.__obtype:
                    raise ObInadequateClassException("Can't use %s(%s) in %s array" % (type(val).__name__, val, self.__obtype.__name__))
            else:
                val = self.__obtype(val)
            values.append(val)
        list.extend(self, values)

    def to_slaw(self, version=2):
        if version == 2:
            return self.to_slaw_v2()
        if version == 1:
            return self.to_slaw_v1()
        raise SlawWrongVersionException('Slaw version %d not supported' % version)

    def to_slaw_v1(self):
        f = 0
        if self.is_float:
            f = 1
        c = 0
        if self.is_complex:
            c = 1
        u = 1
        if self.is_signed:
            u = 0
        s = 0
        if self.bits == 8:
            s = 1
        elif self.bits == 16:
            s = 3
        elif self.bits == 64:
            s = 2
        v = self.vtype
        b = (self.bits * self.size * (c + 1) / 8) - 1
        n = len(self) + 1
        h = (1 << 27) | (f << 26) | (c << 25) | (u << 24) | (s << 22) | (v << 19) | b
        if n > 0xffffffff:
            h = h | (3 << 17)
            nx = struct.pack('Q', n)
        elif n > 0x3ff:
            h = h | (2 << 17)
            nx = struct.pack('I', n)
        else:
            h = h | (n << 8)
            nx = ''
        data = ''.join(x.encode() for x in self)
        if len(data) % 4:
            padding = 4 - (len(data) % 4)
            data += struct.pack('%ds' % padding, '')
        return struct.pack('Iss', h, nx, data)

    def to_slaw_v2(self):
        f = 0
        u = 1
        if self.is_signed:
            u = 0
        if self.bits == 8:
            s = 0
        elif self.bits == 16:
            s = 1
        elif self.bits == 32:
            s = 2
        elif self.bits == 64:
            s = 3
        c = 0
        if self.is_complex:
            c = 1
        v = self.vtype
        b = (self.size * self.bits * (self.is_complex + 1) / 8) - 1
        first = 0xc0 | (f << 5) | (u << 4) | (s << 2) | (c << 1)
        l = (first << 24) | (v << 22) | (b << 14)
        data = ''.join(x.encode() for x in self)
        padding = 8 - (len(data) % 8)
        if padding != 8:
            data += struct.pack('%ds' % padding, '')
        packed = struct.pack('Q', (l << 32) | len(self)) + data
        return packed

    def to_json(self, degrade=False):
        val = list(x.to_json(True) for x in self)
        if degrade:
            return val
        if len(self) == 0:
            cls = 'float64_array'
        else:
            cls = '%s_array' % type(self[0]).__name__
        return { 'json_class': cls, 'v': val }

    def to_yaml(self, indent=''):
        return '!array\n%s' % '\n'.join('%s  - %s' % (indent, x.to_yaml('%s  ' % indent)) for x in self)

