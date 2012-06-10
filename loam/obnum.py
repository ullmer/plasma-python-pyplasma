import decimal, datetime, time, struct, math
from loam.exceptions import *

def common_class(*args, **kwargs):
    bits = kwargs.get('bits', None)
    is_float = kwargs.get('is_float', None)
    is_signed = kwargs.get('is_signed', None)
    is_complex = kwargs.get('is_complex', None)
    _is_float = False
    _is_signed = False
    _is_complex = False
    _bits = 8
    for val in args:
        if val is None:
            continue
        if not isinstance(val, obnumber):
            val = loam.util.make_obnumber(val)
        if val.is_float:
            _is_float = True
        if val.is_signed:
            _is_signed = True
        if val.is_complex:
            _is_complex = True
        if val.bits > _bits:
            _bits = val.bits
    if is_float is None:
        is_float = _is_float
    if is_signed is None:
        is_signed = _is_signed
    if is_complex is None:
        is_complex = _is_complex
    if bits is None:
        bits = _bits
    if is_float:
        cls = 'float'
    elif is_signed:
        cls = 'int'
    else:
        cls = 'unt'
    cls += '%d' % bits
    if is_complex:
        cls += 'c'
    return globals()[cls]

class obnumber(object):
    """
    Abstract base class for loam numeric types.
    """

    __readonly = set(('is_float', 'is_signed', 'is_complex', 'bits', 'size', 'vtype'))

    is_float = True
    """
    True if the object represents a floating point number, False otherwise
    """

    is_signed = True
    """
    True if the object represents a signed number, False otherwise
    """

    is_complex = False
    """
    True if the object represents a complex number, False otherwise
    """

    bits = 64
    """
    Size, in bits, of the underlying number.  In the case of multi-valued objects (complex, vector and multi-vector types), this is the bit size of one of those values.
    """

    size = 1
    """
    Number of values contained by this object.  For the basic integer, float and complex types, this is always 1.  Vectors and multi-vectors contain multiple values.
    """

    vtype = 0
    """
    A number representing what kind of vector this is.  Zero for non-vector types.  1, 2 and 3 for 2-, 3- and 4-dimensional vectors, respectively.  4, 5, 6, 7 for 2-, 3-, 4- and 5-dimensional multi-vectors, respectively.
    """

    def __setattribute__(self, key, val):
        if key in self.__readonly:
            raise TypeError("readonly attribute")
        super(obnumber, self).__setattribute__(key, val)

    def bytesize(self):
        """
        Number of bytes needed to represent all of the values contained in
        the object.
        """
        return self.size * self.bits * (self.is_complex + 1) / 8

    def encode(self, prefix=''):
        """
        Packs object into binary data.  This is *not* the same as to_slaw().
        This method only packs up the objects values, it does not include
        any information about the object's type and is not suitible for use
        in pools, etc.  See struct.pack()
        """
        fmt = self.get_format()
        return struct.pack('%s%s' % (prefix, fmt), self)

    @classmethod
    def get_format(cls):
        """
        Packing format for encode() and decode().  See struct documentation
        for more info.
        """
        if cls.is_float:
            if cls.bits == 32:
                fmt = 'f'
            else:
                fmt = 'd'
        elif cls.is_signed:
            if cls.bits == 64:
                fmt = 'q'
            elif cls.bits == 32:
                fmt = 'i'
            elif cls.bits == 16:
                fmt = 'h'
            else:
                fmt = 'b'
        else:
            if cls.bits == 64:
                fmt = 'Q'
            elif cls.bits == 32:
                fmt = 'I'
            elif cls.bits == 16:
                fmt = 'H'
            else:
                fmt = 'B'
        n = 1
        if cls.is_complex:
            n = n * 2
        if cls.vtype < 4:
            n = n * (cls.vtype + 1)
        else:
            n = n * (2**(cls.vtype - 2))
        if n == 1:
            return fmt
        return '%d%s' % (n, fmt)

    @classmethod
    def decode(cls, data, prefix=''):
        """
        Unpack binary data into an object.  Note, this is not the same as
        from_slaw(), as it requires that the object's class already be
        determined, and only unpacks the raw values from the slaw.  See
        struct.unpack()
        """
        fmt = cls.get_format()
        fsize = struct.calcsize(fmt)
        if len(data) != fsize:
            raise SlawWrongLengthException('%s expected %d bytes of data, but got %d bytes (%s) (%s)' % (cls, fsize, len(data), ' '.join('%02x' % ord(x) for x in data), fmt))
        values = struct.unpack('%s%s' % (prefix, fmt), data)
        if cls.size > 1 and cls.is_complex:
            ucls = cls.underlying_class()
            values = list(ucls(values[i], values[i+1]) for i in range(0, len(values), 2))
        return cls(*values)

    def to_slaw(self, version=2):
        """
        Returns a value suitible for depositing in a pool.
        """
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
        h = (1 << 27) | (f << 26) | (c << 25) | (u << 24) | (s << 22) | (v << 19) | b
        data = self.encode()
        if len(data) % 4:
            padding = 4 - (len(data) % 4)
            data += struct.pack('%ds' % padding, '')
        return struct.pack('Is', h, data)

    def to_slaw_v2(self):
        f = 0
        if self.is_float:
            f = 1
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
        if v >= 4:
            xsize = 2**self.size
        else:
            xsize = self.size
        b = (self.bits * xsize * (c + 1) / 8) - 1
        first = 0x80 | (f << 5) | (u << 4) | (s << 2) | (c << 1)
        l = (first << 24) | (v << 22) | (b << 14)
        if b <= 3:
            e = self.encode()
            if len(e) % 4:
                e += '\0' * (4 - (len(e) % 4))
            (n,) = struct.unpack('I', e)
            packed = struct.pack('Q', (l << 32) | n)
        else:
            data_bytes = self.encode()
            padding = 8 - (len(data_bytes) % 8)
            if padding != 8:
                data_bytes += struct.pack('%ds' % padding, '')
            packed = struct.pack('Q', l << 32) + data_bytes
        (h,) = struct.unpack('Q', packed[:8])
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def __abs__(self):
        """
        x.__abs__() <==> abs(x)
        """
        try:
            cls = common_class(self, is_signed=False, is_complex=False)
        except ObInvalidArgumentException:
            return NotImplemented
        if self.is_complex:
            return cls(math.sqrt((self.real * self.real) + (self.imag * self.imag)))
        if self.is_float:
            return cls(abs(float(self)))
        return cls(abs(int(self)))

    def __add__(self, other):
        """
        x.__add__(y) <==> x+y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            return cls(x.real + y.real, x.imag + y.imag)
        if cls.is_float:
            return cls(float(self) + float(other))
        return cls(int(self) + int(other))

    def __iadd__(self, other):
        """
        x = x.__iadd__(y) <==> x+=y
        """
        return self + other

    def __radd__(self, other):
        """
        x.__radd__(y) <==> y+x
        """
        return self + other

    def __sub__(self, other):
        """
        x.__sub__(y) <==> x-y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            return cls(x.real - y.real, x.imag - y.imag)
        if cls.is_float:
            return cls(float(self) + float(other))
        return cls(int(self) - int(other))

    def __isub__(self, other):
        """
        x = x.__isub__(y) <==> x-=y
        """
        return self - other

    def __rsub__(self, other):
        """
        x.__rsub__(y) <==> y-x
        """
        return -self + other

    def __mul__(self, other):
        """
        x.__mul__(y) <==> x*y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            real = (x.real * y.real) - (x.imag * y.imag)
            imag = (x.real * y.imag) + (x.imag * y.real)
            return cls(real, imag)
        if cls.is_float:
            return cls(float(self) * float(other))
        return cls(int(self) * int(other))

    def __imul__(self, other):
        """
        x = x.__imul__(y) <==> x*=y
        """
        return self * other

    def __rmul__(self, other):
        """
        x.__rmul__(y) <==> y*x
        """
        return self * other

    def __div__(self, other):
        """
        x.__div__(y) <==> x/y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            num = x * y.conjugate()
            den = y * y.conjugate()
            return cls(num.real / den.real, num.imag / den.real)
        if cls.is_float:
            return cls(float(self) / float(other))
        return cls(int(self) / int(other))

    def __idiv__(self, other):
        """
        x = x.__idiv__(y) <==> x/=y
        """
        return self / other

    def __rdiv__(self, other):
        """
        x.__rdiv__(y) <==> y/x
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other) / cls(self)

    def __floordiv__(self, other):
        """
        x.__floordiv__(y) <==> x//y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("floor() is not well defined for complex numbers")
        if cls.is_float:
            return cls(float(self) // float(other))
        return cls(int(self) // int(other))

    def __ifloordiv__(self, other):
        """
        x = x.__ifloordiv__(y) <==> x//=y
        """
        return self // other

    def __rfloordiv__(self, other):
        """
        x.__rfloordiv__(y) <==> y//x
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other) // cls(self)

    def __truediv__(self, other):
        """
        x.__truediv__(y) <==> x/y
        """
        try:
            cls = common_class(self, other, is_float=True)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            return cls(self) / cls(other)
        return cls(float(self) / float(other))

    def __itruediv__(self, other):
        """
        x = x.__itruediv__(y) <==> x/=y
        """
        return self.__truediv__(other)

    def __rtruediv__(self, other):
        """
        x.__rtruediv__(y) <==> y/x
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other).__truediv__(cls(self))

    def __mod__(self, other):
        """
        x.__mod__(y) <==> x%y
        """
        return self - (other * (self // other))

    def __rmod__(self, other):
        """
        x.__rmod__(y) <==> y%x
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other) % cls(self)

    def __divmod__(self, other):
        """
        x.__divmod__(y) <==> divmod(x, y)
        """
        return ((self - (self % other)) / other, self % other)

    def __rdivmod__(self, other):
        """
        x.__rdivmod__(y) <==> divmod(y, x)
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other).__rdivmod__(cls(self))

    def __pos__(self):
        """
        x.__pos__() <==> +x
        """
        try:
            cls = common_class(self)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            return cls(+self.real, +self.imag)
        if cls.is_float:
            return cls(+float(self))
        return cls(+int(self))

    def __neg__(self):
        """
        x.__neg__() <==> -x
        """
        try:
            cls = common_class(self, is_signed=True)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            return cls(-self.real, -self.imag)
        if cls.is_float:
            return cls(-float(self))
        return cls(-int(self))

    def __pow__(self, other):
        """
        x.__pow__(y) <==> x**y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            ## complicated
            ## w = r * math.exp(i * theta)
            x = cls(self)
            y = cls(other)
            r = math.sqrt((x.real * x.real) + (x.imag * x.imag))
            theta = math.atan2(x.imag, x.real)
            dlogr = other.imag * math.log(r)
            ctheta = other.real * theta
            mult = (r**other.real) * math.exp(-other.imag * theta)
            real = mult * math.cos(dlogr + ctheta)
            imag = mult * math.sin(dlogr + ctheta)
            return cls(mult * math.cos(dlogr+ctheta), mult * math.sin(dlogr+ctheta))
        if cls.is_float:
            return cls(float(self)**float(other))
        return cls(int(self)**int(other))

    def __rpow__(self, other):
        """
        x.__rpow__(y) <==> y**x
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        return cls(other)**cls(self)

    def __trunc__(self):
        """
        Returns the Integral closes to x beterrn 0 and x
        (Actual python int, not an obint)
        """
        try:
            cls = common_class(self, is_float=False)
        except ObInvalidArgumentException:
            return NotImplemented
        if self.is_complex:
            raise TypeError("can't truncate a complex")
        if self.is_float:
            return float(self).__trunc__()
        return int(self)

    def __nonzero__(self):
        """
        x.__nonzero__() <==> x != 0
        """

        if self.is_complex:
            return self.real != 0 and self.imag != 0
        if self.is_float:
            return float(self) != 0
        return int(self) != 0

    def __eq__(self, other):
        """
        x.__eq__(y) <==> x == y
        """
        if self is None and other is not None:
            return False
        if other is None and self is not None:
            return False
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            return x.real == y.real and x.imag == y.imag
        if cls.is_float:
            return float(self) == float(other)
        return int(self) == int(other)

    def __ne__(self, other):
        """
        x.__ne__(y) <==> x != y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            x = cls(self)
            y = cls(other)
            return x.real != y.real or x.imag != y.imag
        if cls.is_float:
            return float(self) != float(other)
        return int(self) != int(other)

    def __gt__(self, other):
        """
        x.__gt__(y) <==> x>y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("no ordering relation is defined for complex numbers")
        if cls.is_float:
            return float(self) > float(other)
        return int(self) > int(other)

    def __lt__(self, other):
        """
        x.__lt__(y) <==> x<y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("no ordering relation is defined for complex numbers")
        if cls.is_float:
            return float(self) < float(other)
        return int(self) < int(other)

    def __ge__(self, other):
        """
        x.__ge__(y) <==> x>=y
        """
        return self > other or self == other

    def __le__(self, other):
        """
        x.__le__(y) <==> x<=y
        """
        return self < other or self == other


class obint(obnumber, int):
    """
    Abstract base class for loam integer types.  Behaves like a python int.
    """

    is_float = False

    def __str__(self):
        """
        x.__str__() <==> str(x)
        """
        return '%s(%s)' % (type(self).__name__, int(self))

    def __repr__(self):
        """
        x.__repr__() <==> repr(x)
        """
        return '%s(%s)' % (type(self).__name__, int(self).__repr__())

    def __new__(cls, *args):
        """
        Constructor takes a number as its argument.  The number must fit
        within the bounds defined by bits() and is_signed().
        """
        maxint = 2**cls.bits
        if cls.is_signed:
            if abs(int(args[0])) >= maxint / 2:
                raise ObInadequateClassException('%s(%d) must be between %d and %d' % (cls.__name__, int(args[0]), -1 * ((maxint / 2) - 1), (maxint / 2) - 1))
        else:
            if int(args[0]) < 0 or int(args[0]) >= maxint:
                raise ObInadequateClassException('%s(%d) must be between 0 and %d' % (cls.__name__, int(args[0]), maxint - 1))
        self = super(obint, cls).__new__(cls, *args)
        return self

    def __and__(self, other):
        """
        x.__and__(y) <==> x&y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-and a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-and a float")
        return cls(int(self) & int(other))

    def __iand__(self, other):
        """
        x = x.__iand__(y) <==> x&=y
        """
        return self & other

    def __rand__(self, other):
        """
        x.__rand__(y) <==> y&x
        """
        return self & other

    def __or__(self, other):
        """
        x.__or__(y) <==> x|y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-or a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-or a float")
        return cls(int(self) | int(other))

    def __ior__(self, other):
        """
        x = x.__ior__(y) <==> x|=y
        """
        return self | other

    def __ror__(self, other):
        """
        x.__ror__(y) <==> y|x
        """
        return self | other

    def __xor__(self, other):
        """
        x.__xor__(y) <==> x^y
        """
        try:
            cls = common_class(self, other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-xor a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-xor a float")
        return cls(int(self) ^ int(other))

    def __ixor__(self, other):
        """
        x = x.__ixor__(y) <==> x^=y
        """
        return self ^ other

    def __rxor__(self, other):
        """
        x.__rxor__(y) <==> y^x
        """
        return self ^ other

    def __lshift__(self, other):
        """
        x.__lshift__(y) <==> x<<y
        """
        try:
            cls = common_class(self)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-xor a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-xor a float")
        return cls(int(self) << int(other))

    def __rlshift__(self, other):
        """
        x.__rlshift__(y) <==> y<<x
        """
        try:
            cls = common_class(other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-xor a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-xor a float")
        return cls(int(other) << int(self))

    def __rshift__(self, other):
        """
        x.__rshift__(y) <==> x>>y
        """
        try:
            cls = common_class(self)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-xor a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-xor a float")
        return cls(int(self) >> int(other))

    def __rrshift__(self, other):
        """
        x.__rlshift__(y) <==> y>>x
        """
        try:
            cls = common_class(other)
        except ObInvalidArgumentException:
            return NotImplemented
        if cls.is_complex:
            raise TypeError("can't bitwise-xor a complex")
        if cls.is_float:
            raise TypeError("can't bitwise-xor a float")
        return cls(int(other) >> int(self))

    def __invert__(self):
        """
        x.__invert__() <==> ~x
        """
        cls = type(self)
        return cls(~int(self))

    def __oct__(self):
        """
        x.__oct__() <==> oct(x)
        """
        return oct(int(self))

    def __hex__(self):
        """
        x.__hex__() <==> hex(x)
        """
        return hex(int(self))

    def conjugate(self):
        """
        Returns self, the complex conjugate of any int.
        """
        return self

    def to_json(self, degrade=False):
        if degrade:
            return int(self)
        return { 'json_class': type(self).__name__, 'v': int(self) }

    def to_yaml(self, indent=''):
        return '!i%d %d' % (self.bits, self)

class int8(obint):
    bits = 8

class unt8(int8):
    is_signed = False

class int16(obint):
    bits = 16

class unt16(int16):
    is_signed = False

class int32(obint):
    bits = 32

class unt32(int32):
    is_signed = False

class int64(obint):
    bits = 64

class unt64(int64):
    is_signed = False

class obfloat(float, obnumber):

    def __str__(self):
        """
        x.__str__() <==> str(x)
        """
        return '%s(%s)' % (type(self).__name__, float(self))

    def __repr__(self):
        """
        x.__repr__() <==> repr(x)
        """
        return '%s(%s)' % (type(self).__name__, float(self).__repr__())

    def conjugate(self):
        """
        Returns self, the complex conjugate of any float.
        """
        return self

    def as_integer_ratio(self):
        """
        x.as_integer_ratio() -> (int64, int64)

        Returns a pair of integers, whose ratio is exactly equal to the original
        float and with a positive denominator.
        Raises OverflowError on infinities and a ValueError on NaNs.
        
        >>> float32(10.0).as_integer_ratio()
        (int64(10), int64(1))
        >>> float32(0.0).as_integer_ratio()
        (int64(0), int64(1))
        >>> float64(-.25).as_integer_ratio()
        (int64(-1), int64(4))
        >>> float64(math.sqrt(2)).as_integer_ratio()
        (int64(6369051672525773), int64(4503599627370496))
        """
        (num, den) = float(self).as_integer_ratio()
        return (int64(num), int64(den))

    def hex(self):
        """
        float.hex() -> string

        Return a hexadecimal representation of a floating-point number.
        >>> float32(-0.1).hex()
        '-0x1.999999999999ap-4'
        >>> float64(3.14159).hex()
        '0x1.921f9f01b866ep+1'
        """
        return float(self).hex()

    def is_integer(self):
        """
        Returns True if the float is an integer.
        """
        return float(self).is_integer()

    def to_json(self, degrade=False):
        if degrade:
            return float(self)
        return { 'json_class': type(self).__name__, 'v': float(self) }

    def to_yaml(self):
        return '!f%d %s' % (self.bits, float(self))

class float32(obfloat):
    bits = 32

class float64(obfloat):
    bits = 64

class obcomplex(obnumber):
    is_complex = True

    def __new__(cls, real=None, imag=None):
        if imag is None:
            if type(real) == complex or isinstance(real, obcomplex):
                imag = real.imag
                real = real.real
        if cls == obcomplex:
            if real is None and imag is None:
                real = float64(0)
                imag = float64(0)
            elif real is None:
                real = type(imag)(0)
            elif imag is None:
                imag = type(real)(0)
            xcls = common_class(real, imag)
            (real, imag) = loam.util.make_obnumbers(real, imag)
            kls = '%sc' % type(real).__name__
            return globals()[kls](real, imag)
        return super(obcomplex, cls).__new__(cls, real, imag)

    def __init__(self, real=0, imag=None):
        if imag is None:
            if type(real) == complex or isinstance(real, obcomplex):
                imag = real.imag
                real = real.real
        if real is None:
            real = 0
        if imag is None:
            imag = 0
        cls = self.underlying_class()
        self.__real = cls(real)
        self.__imag = cls(imag)

    def __getattribute__(self, key):
        if key == 'real':
            return self.__real
        if key == 'imag':
            return self.__imag
        return super(obcomplex, self).__getattribute__(key)

    @classmethod
    def underlying_class(cls):
        if cls.is_float:
            kls = 'float'
        elif cls.is_signed:
            kls = 'int'
        else:
            kls = 'unt'
        kls += '%d' % cls.bits
        return globals()[kls]

    def encode(self):
        return self.real.encode() + self.imag.encode()

    def to_yaml(self):
        return '!complex [%s, %s]' % (self.real.to_yaml(), self.imag.to_yaml())

class intc(obcomplex):
    is_float = False
    is_signed = True

    def __str__(self):
        return '%s(%s+%sj)' % (type(self).__name__, int(self.real), int(self.imag))

    def __repr__(self):
        return '%s(%s+%sj)' % (type(self).__name__, int(self.real).__repr__(), int(self.imag).__repr__())

    def to_json(self, degrade=False):
        return { 'json_class': type(self).__name__, 'v': [int(self.real), int(self.imag)] }

class int64c(intc):
    bits = 64

class unt64c(int64c):
    is_signed = False

class int32c(intc):
    bits = 32

class unt32c(int32c):
    is_signed = False

class int16c(intc):
    bits = 16

class unt16c(int16c):
    is_signed = False

class int8c(intc):
    bits = 8

class unt8c(int8c):
    is_signed = False

class floatc(obcomplex):
    is_float = True
    is_signed = True

    def __str__(self):
        return '%s(%s+%sj)' % (type(self).__name__, float(self.real), float(self.imag))

    def __repr__(self):
        return '%s(%s+%sj)' % (type(self).__name__, float(self.real).__repr__(), float(self.imag).__repr__())

    def to_json(self, degrade=False):
        return { 'json_class': type(self).__name__, 'v': [float(self.real), float(self.imag)] }

class float64c(floatc):
    bits = 64

class float32c(floatc):
    bits = 32

import loam.util
