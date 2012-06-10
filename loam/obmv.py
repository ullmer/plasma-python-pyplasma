from loam.const import *
from loam.exceptions import *
import loam.obnum
## multi-vectors

class obmvpointer(list):
    def __init__(self, underlier, sc):
        (start, count) = sc
        list.__init__(self, underlier[start:start+count])
        self.__underlier = underlier
        self.__indeces = range(start, start+count)

    def __getitem__(self, index):
        ix = self.__indeces[index]
        return self.__underlier[ix]

    def __setitem__(self, index, value):
        ix = self.__indeces[index]
        self.__underlier[ix] = value
        list.__setitem__(self, index, value)

class obmv(loam.obnum.obnumber):

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join('%s' % x for x in self._items))

    def __repr__(self):
        return self.__str__()

    def __new__(cls, *args):
        if cls == obmv:
            if len(args) == 4:
                n = 2
            elif len(args) == 8:
                n = 3
            elif len(args) == 16:
                n = 4
            elif len(args) == 32:
                n = 5
            else:
                raise ObInvalidArgumentException("multivector must have 4, 8, 16 or 32 values")
            args = loam.util.make_obnumbers(*args)
            kls = 'm%d%s' % (n, type(args[0]).__name__)
            return globals()[kls](*args)
        return super(obmv, cls).__new__(cls, *args)

    def __init__(self, *args):
        args = list(args)
        if len(args) != 2**self.size:
            raise ObInvalidArgumentException("%s must have %d values" % (type(self).__name__, 2**self.size))
        if self.is_float:
            cls = 'float'
        elif self.is_signed:
            cls = 'int'
        else:
            cls = 'unt'
        cls += '%d' % self.bits
        if self.is_complex:
            cls += 'c'
        klass = getattr(loam.obnum, cls)
        for i in range(len(args)):
            if not isinstance(args[i], loam.obnum.obnumber):
                args[i] = klass(args[i])
        for i in range(len(args)):
            if type(args[i]) != klass:
                raise ObInadequateClassException("cannot use a %s in a %s" % (type(args[i]).__name__, type(self).__name__))
        if args[0].is_float != self.is_float:
            raise ObInadequateClassException("cannot use a %s in a %s" % (type(args[i]).__name__, type(self).__name__))
        if args[0].is_signed != self.is_signed:
            raise ObInadequateClassException("cannot use a %s in a %s" % (type(args[i]).__name__, type(self).__name__))
        if args[0].bits != self.bits:
            raise ObInadequateClassException("cannot use a %s in a %s" % (type(args[i]).__name__, type(self).__name__))
        if args[0].is_complex != self.is_complex:
            raise ObInadequateClassException("cannot use a %s in a %s" % (type(args[i]).__name__, type(self).__name__))
        self._items = args
        self._klass = klass

    #def __init__(self, *args):
    #    obvals = list(loam.util.make_obnumber(x) for x in args)
    #    if self.size is None:
    #        if len(args) == 4:
    #            self.size = 2
    #        elif len(args) == 8:
    #            self.size = 3
    #        elif len(args) == 16:
    #            self.size = 4
    #        elif len(args) == 32:
    #            self.size = 5
    #    if len(args) != (2**self.size):
    #        raise TypeError('%s() takes %d arguments (%d given)' % (type(self).__name__, (2**self.size)+1, len(args)+1))
    #    if self.bits is None:
    #        for val in obvals:
    #            if self.bits is None or val.bits > self.bits:
    #                self.bits = val.bits
    #    if self.is_signed is None:
    #        self.is_signed = False
    #        for val in obvals:
    #            if val.is_signed:
    #                self.is_signed = True
    #                break
    #    if self.is_float is None:
    #        self.is_float = False
    #        for val in obvals:
    #            if val.is_float:
    #                self.is_float = True
    #                break
    #    if self.is_complex is None:
    #        self.is_complex = False
    #        for val in obvals:
    #            if val.is_complex:
    #                self.is_complex = True
    #                break
    #    if self.is_float:
    #        cls = 'float'
    #    elif self.is_signed:
    #        cls = 'int'
    #    else:
    #        cls = 'unt'
    #    cls += '%d' % (2**self.bits)
    #    if self.is_complex:
    #        cls += 'c'
    #    self._klass = loam.obnumber[cls]
    #    items = list(None for i in range(2**self.size))
    #    for i in range(2**self.size):
    #        if isinstance(args[i], loam.obnumber.obnumber):
    #            if type(args[i]) != self._klass:
    #                raise ValueError("Can't use %s(%s) in m%d%s" % (type(args[i]).__name__, args[i], self.size, cls))
    #            items[i] = args[i]
    #        else:
    #            items[i] = klass(args[i])
    #    self._items = items

    def encode(self):
        data = ''
        for i in range(2**self.size):
            data += self._items[i].encode()
        return data

    def __getattribute__(self, key):
        if key.startswith('_'):
            return super(obmv, self).__getattribute__(key)
        if self._itemkeys.has_key(key):
            return self._items[self._itemkeys[key]]
        if self._rangekeys.has_key(key):
            return obmvpointer(self, self._rangekeys[key])
        if key == 'coeff':
            return self._items
        return super(obmv, self).__getattribute__(key)

    def __setattribute__(self, key, value):
        if self._itemkeys.has_key(key):
            self[self._itemkeys[key]] = value
        elif self._rangekeys.has_key(key):
            p = self.__getattribute__(key)
            for i in range(len(p)):
                p[i] = value[i]
        elif key == 'coeff':
            for i in range(2**self.size):
                self[i] = value[i]
        else:
            super(obmv, self).__setattribute__(key, value)

    def __getitem__(self, index):
        return self._items[index]

    def __getslice__(self, i, j):
        return self._items[i:j]

    def __setitem__(self, index, value):
        if isinstance(value, loam.obnum.obnumber):
            if type(value) != self._klass:
                raise ObInadequateClassException("Can't use %s(%s) in m%d%s" % (type(value).__name__, value, self.size, self._klass.__name__))
        else:
            value = self._klass(value)
        self._items[index] = value

    @classmethod
    def underlying_class(cls):
        if cls.is_float:
            kls = 'float'
        elif cls.is_signed:
            kls = 'int'
        else:
            kls = 'unt'
        kls += '%d' % cls.bits
        if cls.is_complex:
            kls += 'c'
        return getattr(loam.obnum, kls)

    def bytesize(self):
        return (self.bits / 8) * (2**self.size) * (self.is_complex + 1)

    def to_json(self, degrade=False):
        val = list(x.to_json(True) for x in self._items)
        if degrade:
            return val
        return { 'json_class': type(self).__name__, 'v': val }

    def to_yaml(self, indent=''):
        return '!multivector [%s]' % ', '.join(x.to_yaml() for x in self._items)

class mv2(obmv):
    """
    2 dimensional multi-vectors have 4 values.  They can be accessed by index:

    >>> foo = mv2int8(10, 20, 30, 40)
    >>> foo[1]
    int8(20)

    They can be accessed by name.  u0, a and e all reference the first value;
    x and e1 reference the second value; y and e2 reference the third value;
    i and e12 reference the fourth value:

    >>> foo.x
    int8(20)
    >>> foo.e1
    int8(20)

    >>> foo[0] = foo.u0 = foo.a = foo.e
    >>> foo[1] = foo.x = foo.e1
    >>> foo[2] = foo.y = foo.e2
    >>> foo[3] = foo.u2 = foo.i = foo.e12
    >>> foo.u1 = foo[1:3]
    """

    size = 2
    vtype = 4
    _itemkeys = {
        'u0': 0, 'a': 0, 'e': 0,
        'x': 1, 'e1': 1,
        'y': 2, 'e2': 2,
        'i': 3, 'e12': 3
    }
    _rangekeys = {
        'u1': (1,2)
    }

class mv3(obmv):
    """
    3 dimensional multi-vectors have 8 values.  They can be accessed by index:

    >>> foo = mv3int8(10, 20, 30, 40, 50, 60, 70, 80)
    >>> foo[6]
    int8(70)

    They can be accessed by name.  u0, a and e all reference the first value;
    x and e1 reference the second value; y and e2 reference the third value;
    z and e3 reference the fourth value; xy and e12 reference the fifth value;
    yz and e23 reference the sixth value; zx and e31 reference the seventh
    value; u3, i and e123 reference the eighth value.

    >>> foo.yz
    int8(60)
    >>> foo.e123
    int8(80)

    >>> foo[0] = foo.u0 = foo.a = foo.e
    >>> foo[1] = foo.x = foo.e1
    >>> foo[2] = foo.y = foo.e2
    >>> foo[3] = foo.z = foo.e3
    >>> foo[4] = foo.xy = foo.e12
    >>> foo[5] = foo.yz = foo.e23
    >>> foo[6] = foo.zx = foo.e31
    >>> foo[7] = foo.u3 = foo.i = foo.e123
    >>> foo.u1 = foo[1:4]
    >>> foo.u2 = foo[4:7]
    """
    size = 3
    vtype = 5
    _itemkeys = {
        'u0': 0, 'a': 0, 'e': 0,
        'x': 1, 'e1': 1,
        'y': 2, 'e2': 2,
        'z': 3, 'e3': 3,
        'xy': 4, 'e12': 4,
        'yz': 5, 'e23': 5,
        'zx': 6, 'e31': 6,
        'u3': 7, 'i': 7, 'e123': 7
    }
    _rangekeys = {
        'u1': (1,3),
        'u2': (4,3),
    }

class mv4(obmv):
    """
    4 dimensional multi-vectors have 16 values.  They can be accessed by index:

    >>> foo = mv3int8(10, 20, 30, 40, 50, 60, 70, 80, 5, 15, 25, 35, 45, 65, 75)
    >>> foo[12]
    int8(45)

    They can also be accessed by name:

    >>> foo[0]  = foo.u0 = foo.a = foo.e
    >>> foo[1]  = foo.x = foo.e1
    >>> foo[2]  = foo.y = foo.e2
    >>> foo[3]  = foo.z = foo.e3
    >>> foo[4]  = foo.v = foo.e4
    >>> foo[5]  = foo.xy = foo.e12
    >>> foo[6]  = foo.yz = foo.e23
    >>> foo[7]  = foo.zv = foo.e34
    >>> foo[8]  = foo.vx = foo.e41
    >>> foo[9]  = foo.xz = foo.e13
    >>> foo[10] = foo.yv = foo.e24
    >>> foo[11] = foo.xyz = foo.e123
    >>> foo[12] = foo.yzv = foo.e234
    >>> foo[13] = foo.zvx = foo.e341
    >>> foo[14] = foo.vxy = foo.e412
    >>> foo[15] = foo.u4 = foo.i = foo.e1234
    >>> foo.u1 = foo[1:5]
    >>> foo.u2 = foo[5:11]
    >>> foo.u3 = foo[11:15]
    """

    size = 4
    vtype = 6
    _itemkeys = {
        'u0': 0, 'a': 0, 'e': 0,
        'x': 1, 'e1': 1,
        'y': 2, 'e2': 2,
        'z': 3, 'e3': 3,
        'v': 4, 'e4': 4,
        'xy': 5, 'e12': 5,
        'yz': 6, 'e23': 6,
        'zv': 7, 'e34': 7,
        'vx': 8, 'e41': 8,
        'xz': 9, 'e13': 9,
        'yv': 10, 'e24': 10,
        'xyz': 11, 'e123': 11,
        'yzv': 12, 'e234': 12,
        'zvx': 13, 'e341': 13,
        'vxy': 14, 'e412': 14,
        'i': 15, 'e1234': 15, 'u4': 15
    }
    _rangekeys = {
        'u1': (1, 4),
        'u2': (5, 6),
        'u3': (11, 4),
    }

class mv5(obmv):
    """
    5 dimensional multi-vectors have 32 values.  They can be accessed by index
    or by name:

    >>> foo[0]  = foo.u0 = foo.a = foo.e
    >>> foo[1]  = foo.x = foo.e1
    >>> foo[2]  = foo.y = foo.e2
    >>> foo[3]  = foo.z = foo.e3
    >>> foo[4]  = foo.v = foo.e4
    >>> foo[5]  = foo.w = foo.e5
    >>> foo[6]  = foo.xy = foo.e12
    >>> foo[7]  = foo.yz = foo.e23
    >>> foo[8]  = foo.zv = foo.e34
    >>> foo[9]  = foo.vw = foo.e45
    >>> foo[10] = foo.wx = foo.e51
    >>> foo[11] = foo.xz = foo.e13
    >>> foo[12] = foo.yv = foo.e24
    >>> foo[13] = foo.zw = foo.e35
    >>> foo[14] = foo.vx = foo.e41
    >>> foo[15] = foo.wy = foo.e52
    >>> foo[16] = foo.xyz = foo.e123
    >>> foo[17] = foo.yzv = foo.e234
    >>> foo[18] = foo.zvw = foo.e345
    >>> foo[19] = foo.vwx = foo.e451
    >>> foo[20] = foo.wxy = foo.e512
    >>> foo[21] = foo.xyv = foo.e124
    >>> foo[22] = foo.yzw = foo.e235
    >>> foo[23] = foo.zvx = foo.e341
    >>> foo[24] = foo.vwy = foo.e452
    >>> foo[25] = foo.wxz = foo.e513
    >>> foo[26] = foo.xyzv = foo.e1234
    >>> foo[27] = foo.yzvw = foo.e2345
    >>> foo[28] = foo.zvwx = foo.e3451
    >>> foo[29] = foo.vwxy = foo.e4512
    >>> foo[30] = foo.wxyz = foo.e5123
    >>> foo[31] = foo.u5 = foo.i = foo.e12345
    >>> foo.u1 = foo[1:6]
    >>> foo.u2 = foo[6:16]
    >>> foo.u3 = foo[16:26]
    >>> foo.u4 = foo[26:31]
    """
    size = 5
    vtype = 7
    _itemkeys = {
        'u0': 0, 'a': 0, 'e': 0,
        'x': 1, 'e1': 1,
        'y': 2, 'e2': 2,
        'z': 3, 'e3': 3,
        'v': 4, 'e4': 4,
        'w': 5, 'e5': 5,
        'xy': 6, 'e12': 6,
        'yz': 7, 'e23': 7,
        'zv': 8, 'e34': 8,
        'vw': 9, 'e45': 9,
        'wx': 10, 'e51': 10,
        'xz': 11, 'e13': 11,
        'yv': 12, 'e24': 12,
        'zw': 13, 'e35': 13,
        'vx': 14, 'e41': 14,
        'wy': 15, 'e52': 15,
        'xyz': 16, 'e123': 16,
        'yzv': 17, 'e234': 17,
        'zvw': 18, 'e345': 18,
        'vwx': 19, 'e451': 19,
        'wxy': 20, 'e512': 20,
        'xyv': 21, 'e124': 21,
        'yzw': 22, 'e235': 22,
        'zvx': 23, 'e341': 23,
        'vwy': 24, 'e452': 24,
        'wxz': 25, 'e513': 25,
        'xyzv': 26, 'e1234': 26,
        'yzvw': 27, 'e2345': 27,
        'zvwx': 28, 'e3451': 28,
        'vwxy': 29, 'e4512': 29,
        'wxyz': 30, 'e5123': 30,
        'i': 31, 'e12345': 31
    }
    _rangekeys = {
        'u1': (1, 5),
        'u2': (6, 10),
        'u3': (16, 10),
        'u4': (26, 5),
    }

class mv2int(mv2):
    is_float = False

class mv2unt(mv2int):
    is_signed = False

class mv2float(mv2):
    is_float = True
    is_signed = True

class mv2int8(mv2int):
    bits = 8

class mv2unt8(mv2unt):
    bits = 8

class mv2int16(mv2int):
    bits = 16

class mv2unt16(mv2unt):
    bits = 16

class mv2int32(mv2int):
    bits = 32

class mv2unt32(mv2unt):
    bits = 32

class mv2int64(mv2int):
    bits = 64

class mv2unt64(mv2unt):
    bits = 64

class mv2float32(mv2float):
    bits = 32

class mv2float64(mv2float):
    bits = 64


class mv3int(mv3):
    is_float = False

class mv3unt(mv3int):
    is_signed = False

class mv3float(mv3):
    is_float = True
    is_signed = True

class mv3int8(mv3int):
    bits = 8

class mv3unt8(mv3unt):
    bits = 8

class mv3int16(mv3int):
    bits = 16

class mv3unt16(mv3unt):
    bits = 16

class mv3int32(mv3int):
    bits = 32

class mv3unt32(mv3unt):
    bits = 32

class mv3int64(mv3int):
    bits = 64

class mv3unt64(mv3unt):
    bits = 64

class mv3float32(mv3float):
    bits = 32

class mv3float64(mv3float):
    bits = 64


class mv4int(mv4):
    is_float = False

class mv4unt(mv4int):
    is_signed = False

class mv4float(mv4):
    is_float = True
    is_signed = True

class mv4int8(mv4int):
    bits = 8

class mv4unt8(mv4unt):
    bits = 8

class mv4int16(mv4int):
    bits = 16

class mv4unt16(mv4unt):
    bits = 16

class mv4int32(mv4int):
    bits = 32

class mv4unt32(mv4unt):
    bits = 32

class mv4int64(mv4int):
    bits = 64

class mv4unt64(mv4unt):
    bits = 64

class mv4float32(mv4float):
    bits = 32

class mv4float64(mv4float):
    bits = 64


class mv5int(mv5):
    is_float = False

class mv5unt(mv5int):
    is_signed = False

class mv5float(mv5):
    is_float = True
    is_signed = True

class mv5int8(mv5int):
    bits = 8

class mv5unt8(mv5unt):
    bits = 8

class mv5int16(mv5int):
    bits = 16

class mv5unt16(mv5unt):
    bits = 16

class mv5int32(mv5int):
    bits = 32

class mv5unt32(mv5unt):
    bits = 32

class mv5int64(mv5int):
    bits = 64

class mv5unt64(mv5unt):
    bits = 64

class mv5float32(mv5float):
    bits = 32

class mv5float64(mv5float):
    bits = 64


import loam.util
