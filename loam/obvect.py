import math
import loam.obnum

def common_class(*args, **kwargs):
    bits = kwargs.get('bits', None)
    is_float = kwargs.get('is_float', None)
    is_signed = kwargs.get('is_signed', None)
    is_complex = kwargs.get('is_complex', None)
    size = kwargs.get('size', None)
    _is_float = False
    _is_signed = False
    _is_complex = False
    _bits = 8
    _size = 1
    for val in args:
        if val is None:
            continue
        if not isinstance(val, loam.obnum.obnumber):
            val = loam.util.make_obnumber(val)
        if val.is_float:
            _is_float = True
        if val.is_signed:
            _is_signed = True
        if val.is_complex:
            _is_complex = True
        if val.bits > _bits:
            _bits = val.bits
        if val.size > _size:
            _size = val.size
    if is_float is None:
        is_float = _is_float
    if is_signed is None:
        is_signed = _is_signed
    if is_complex is None:
        is_complex = _is_complex
    if bits is None:
        bits = _bits
    if size is None:
        size = _size
    if is_float:
        cls = 'float'
    elif is_signed:
        cls = 'int'
    else:
        cls = 'unt'
    cls += '%d' % bits
    if is_complex:
        cls += 'c'
    if size > 1:
        cls = 'v%d%s' % (size, cls)
        return globals()[cls]
    return getattr(loam.obnum, cls)

class obvector(loam.obnum.obnumber):
    """
    Vectors have 2, 3 or 4 elements, accessed by index or key (x, y, z, w),
    which are typed as obint, obfloat or obcomplex numbers.

    >>> foo = obvector(1, 2, 3)
    >>> foo.x
    1
    >>> foo[1]
    2

    """

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join('%s' % x for x in self._items))

    def __repr__(self):
        return self.__str__()

    def __new__(cls, *args):
        if cls == obvector:
            args = loam.util.make_obnumbersc(*args)
            kls = 'v%d%s' % (len(args), type(args[0]).__name__)
            return globals()[kls](*args)
        return super(obvector, cls).__new__(cls, *args)

    def __init__(self, *args):
        args = list(args)
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
        #items = list(None for i in range(self.size))
        #for i in range(self.size):
        #    if isinstance(args[i], loam.obnum.obnumber):
        #        if type(args[i]) != self._klass:
        #            raise ValueError("Can't use %s(%s) in v%d%s" % (type(args[i]).__name__, args[i], self.size, cls))
        #        items[i] = args[i]
        #    else:
        #        items[i] = klass(args[i])
        #self._items = items

    def __getattribute__(self, key):
        if key == 'x':
            return self._items[0]
        if key == 'y':
            return self._items[1]
        if key == 'z':
            return self._items[2]
        if key == 'w':
            return self._items[3]
        return super(obvector, self).__getattribute__(key)

    def __setattribute__(self, key, value):
        if key in 'xyzw':
            if isinstance(value, loam.obnum.obnumber):
                if type(value) != self._klass:
                    raise ObInadequateClassException("Can't use %s(%s) in v%d%s" % (type(value).__name__, value, self.size, cls))
            else:
                value = self._klass(value)
            if key == 'x':
                self._items[0] = value
            if key == 'y':
                self._items[1] = value
            if key == 'z':
                self._items[2] = value
            if key == 'w':
                self._items[3] = value
        return super(obvector, self).__setattribute__(key, value)

    def __getitem__(self, index):
        return self._items[index]

    def __setitem__(self, index, value):
        if isinstance(value, loam.obnum.obnumber):
            if type(value) != self._klass:
                raise ObInadequateClassException("Can't use %s(%s) in v%d%s" % (type(value).__name__, value, self.size, cls))
        else:
            value = self._klass(value)
        self._items[index] = value

    def bytesize(self):
        return (self.bits / 8) * (self.is_complex + 1) * self.size

    def encode(self):
        data = ''
        for i in range(self.size):
            data += self._items[i].encode()
        return data

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

    def __abs__(self):
        """
        The length of the vector: ||a||
        """
        r2 = 0
        for i in range(self.size):
            r2 = r2 + (self[i] * self[i])
        if isinstance(r2, loam.obnum.obcomplex):
            return type(r2)(r2**0.5)
        return type(r2)(math.sqrt(r2))

    def __add__(self, other):
        """
        Add two vectors.  You cannot add a scalar or a multi-vector to a vector.
        """
        if not isinstance(other, obvector):
            raise TypeError("cannot add %s to %s" % (type(other).__name__, type(self).__name__))
        cls = common_class(self, other)
        args = [0,] * cls.size
        res = cls(*args)
        ucls = res.underlying_class()
        for i in range(max(self.size, other.size)):
            if i >= self.size:
                x = 0
            else:
                x = self[i]
            if i >= other.size:
                y = 0
            else:
                y = other[i]
            res[i] = x + y
        return res

    def __iadd__(self, other):
        return self + other

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        """
        Subtract one vector from another.  You cannot subtract a scalar or a
        multi-vector from a vector.
        """
        if not isinstance(other, obvector):
            raise TypeError("cannot add %s to %s" % (type(other).__name__, type(self).__name__))
        cls = common_class(self, other)
        args = [0,] * cls.size
        res = cls(*args)
        ucls = res.underlying_class()
        for i in range(max(self.size, other.size)):
            if i >= self.size:
                x = 0
            else:
                x = self[i]
            if i >= other.size:
                y = 0
            else:
                y = other[i]
            res[i] = x - y
        return res

    def __isub__(self, other):
        return self - other

    def __rsub__(self, other):
        return (-1 * self) + other

    def __mul__(self, other):
        """
        Multiply a vector by a scalar.  To multiply by a vector, use dot()
        or cross()
        """
        if isinstance(other, loam.obnum.obnumber):
            if other.size != 1:
                raise TypeError("%s can only be multiplied by a scalar value.  Use dot() or cross() for vector multiplication")
        else:
            other = loam.util.make_obnumber(other)
        ulcls = common_class(self[0], other)
        args = (ulcls(0),) * self.size
        res = obvector(*args)
        for i in range(self.size):
            res[i] = self[i] * other
        return res

    def __imul__(self, other):
        return self * other

    def __rmul__(self, other):
        return self * other

    def __div__(self, other):
        """
        Divide a vector by a scalar.  You cannot divide a vector by another
        vector or a multi-vector.
        """
        if isinstance(other, loam.obnum.obnumber):
            if other.size != 1:
                raise TypeError("%s can only be divided by a scalar value.")
        else:
            other = loam.util.make_obnumber(other)
        ulcls = common_class(self[0], other)
        args = (ulcls(0),) * self.size
        res = obvector(*args)
        for i in range(self.size):
            res[i] = self[i] / other
        return res

    def __idiv__(self, other):
        return self / other

    def __floordiv__(self, other):
        if isinstance(other, loam.obnum.obnumber):
            if other.size != 1:
                raise TypeError("%s can only be divided by a scalar value.")
        else:
            other = loam.util.make_obnumber(other)
        ulcls = common_class(self[0], other)
        args = (ulcls(0),) * self.size
        res = obvector(*args)
        for i in range(self.size):
            res[i] = self[i] // other
        return res

    def __ifloordiv__(self, other):
        return self // other

    def __pos__(self):
        return self

    def __neg__(self):
        return self * -1

    def __nonzero__(self):
        """
        True if any element of the vector is non-zero.
        """
        for i in self.size:
            if self[i] != 0:
                return True
        return False

    def __eq__(self, other):
        """
        True if all elements of the vectors are equal.  If the vectors are
        different sizes, the remaining elements of the larger vector must
        be zero.
        """
        if not isinstance(other, obvector):
            return False
        for i in range(max(self.size, other.size)):
            if i >= self.size:
                x = 0
            else:
                x = self[i]
            if i >= other.size:
                y = 0
            else:
                y = other[i]
            if x != y:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def dot(self, other):
        """
        Dot product of two vectors.
        """
        if not isinstance(other, obvector):
            raise TypeError("%s cannot be dotted with %s" % (type(self).__name__, type(other).__name__))
        res = 0
        for i in range(min(self.size, other.size)):
            res = res + (self[i] * other[i])
        return res

    def cross(self, other):
        """
        Cross product of two vectors.  Only meaningful for 2- or 3-dimensions.
        Returns a 3-dimensional vector.
        """
        if not isinstance(other, obvector):
            raise TypeError("%s cannot be crossed with %s" % (type(self).__name__, type(other).__name__))
        if max(self.size, other.size) > 3:
            raise TypeError("cross products only supported for 2 or 3 dimensional vectors")
        ulcls = common_class(self[0], other[0])
        x = [0,0,0]
        y = [0,0,0]
        for i in range(3):
            if i >= self.size:
                x[i] = 0
            else:
                x[i] = self[i]
            if i >= other.size:
                y[i] = 0
            else:
                y[i] = other[i]
        return obvector(ulcls(x[1]*y[2] - x[2]*y[1]),
                      ulcls(x[2]*y[0] - x[0]*y[2]),
                      ulcls(x[0]*y[1] - x[1]*y[0]))

    def angle(self, other):
        """
        The angle between two vectors.
        """
        return loam.obnum.float64(math.acos(self.dot(other) / (abs(self)*abs(other))))

    def normal(self, other):
        """
        Unit vector perpendicular to self and another vector.  Only meaningful
        for 2- or 3-dimensions.  Returns a 3-dimensional vector.
        """
        return self.cross(other) / (abs(self)*abs(other)*math.sin(self.angle(other)))

    def to_json(self, degrade=False):
        val = list(x.to_json(degrade) for x in self)
        if degrade:
            return val
        return { 'json_class': type(self).__name__, 'v': val }

    def to_yaml(self, indent=''):
        return '!vector [%s]' % ', '.join(x.to_yaml() for x in self)

class v2(obvector):
    size = 2
    vtype = 1

class v2int(v2):
    is_float = False

class v2unt(v2int):
    is_signed = False

class v2float(v2):
    is_float = True
    is_signed = True

class v2intc(v2int):
    is_complex = True

class v2untc(v2unt):
    is_complex = True

class v2floatc(v2float):
    is_complex = True

class v2int8(v2int):
    bits = 8

class v2unt8(v2unt):
    bits = 8

class v2int16(v2int):
    bits = 16

class v2unt16(v2unt):
    bits = 16

class v2int32(v2int):
    bits = 32

class v2unt32(v2unt):
    bits = 32

class v2int64(v2int):
    bits = 64

class v2unt64(v2unt):
    bits = 64

class v2float32(v2float):
    bits = 32

class v2float64(v2float):
    bits = 64

class v2int8c(v2intc):
    bits = 8

class v2unt8c(v2untc):
    bits = 8

class v2int16c(v2intc):
    bits = 16

class v2unt16c(v2untc):
    bits = 16

class v2int32c(v2intc):
    bits = 32

class v2unt32c(v2untc):
    bits = 32

class v2int64c(v2intc):
    bits = 64

class v2unt64c(v2untc):
    bits = 64

class v2float32c(v2floatc):
    bits = 32

class v2float64c(v2floatc):
    bits = 64


class v3(obvector):
    size = 3
    vtype = 2

class v3int(v3):
    is_float = False

class v3unt(v3int):
    is_signed = False

class v3float(v3):
    is_float = True
    is_signed = True

class v3intc(v3int):
    is_complex = True

class v3untc(v3unt):
    is_complex = True

class v3floatc(v3float):
    is_complex = True

class v3int8(v3int):
    bits = 8

class v3unt8(v3unt):
    bits = 8

class v3int16(v3int):
    bits = 16

class v3unt16(v3unt):
    bits = 16

class v3int32(v3int):
    bits = 32

class v3unt32(v3unt):
    bits = 32

class v3int64(v3int):
    bits = 64

class v3unt64(v3unt):
    bits = 64

class v3float32(v3float):
    bits = 32

class v3float64(v3float):
    bits = 64

class v3int8c(v3intc):
    bits = 8

class v3unt8c(v3untc):
    bits = 8

class v3int16c(v3intc):
    bits = 16

class v3unt16c(v3untc):
    bits = 16

class v3int32c(v3intc):
    bits = 32

class v3unt32c(v3untc):
    bits = 32

class v3int64c(v3intc):
    bits = 64

class v3unt64c(v3untc):
    bits = 64

class v3float32c(v3floatc):
    bits = 32

class v3float64c(v3floatc):
    bits = 64


class v4(obvector):
    size = 4
    vtype = 3

class v4int(v4):
    is_float = False

class v4unt(v4int):
    is_signed = False

class v4float(v4):
    is_float = True
    is_signed = True

class v4intc(v4int):
    is_complex = True

class v4untc(v4unt):
    is_complex = True

class v4floatc(v4float):
    is_complex = True

class v4int8(v4int):
    bits = 8

class v4unt8(v4unt):
    bits = 8

class v4int16(v4int):
    bits = 16

class v4unt16(v4unt):
    bits = 16

class v4int32(v4int):
    bits = 32

class v4unt32(v4unt):
    bits = 32

class v4int64(v4int):
    bits = 64

class v4unt64(v4unt):
    bits = 64

class v4float32(v4float):
    bits = 32

class v4float64(v4float):
    bits = 64

class v4int8c(v4intc):
    bits = 8

class v4unt8c(v4untc):
    bits = 8

class v4int16c(v4intc):
    bits = 16

class v4unt16c(v4untc):
    bits = 16

class v4int32c(v4intc):
    bits = 32

class v4unt32c(v4untc):
    bits = 32

class v4int64c(v4intc):
    bits = 64

class v4unt64c(v4untc):
    bits = 64

class v4float32c(v4floatc):
    bits = 32

class v4float64c(v4floatc):
    bits = 64



import loam.util
