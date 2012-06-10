import struct

class obnil(object):
    """
    Represents a null object.  Since python's None cannot be subclassed,
    this is not as useful as it might be.
    """

    def __init__(self):
        """
        Constructor takes no arguments
        """
        pass

    def __str__(self):
        return 'obnil'

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
        return struct.pack('I', 0x01010101)

    def to_slaw_v2(self):
        packed = struct.pack('Q', 0x2000000000000002)
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def to_json(self, degrade=False):
        if degrade:
            return None
        return { 'json_class': 'obnil', 'v': None }

    def to_yaml(self, indent=''):
        return '~'

class obbool(int):
    """
    Represents a boolean object.  Python's bool type cannot be subclassed
    (True and False are singletons), this object is a subclass of int (as
    is python's bool type).  It should still behave as a boolean, however.
    """

    def __new__(cls, *args):
        """
        Constructor takes a True-ish (True, non-zero number, etc) or False-ish
        (False, zero, None, etc) value, and is immutable.
        """
        self = super(obbool, cls).__new__(cls, *args)
        return self

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
        return struct.pack('I', 2 | self)

    def to_slaw_v2(self):
        packed = struct.pack('Q', 0x2000000000000000 | int(self))
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def to_json(self, degrade=False):
        if degrade:
            return bool(self)
        return { 'json_class': 'obbool', 'v': bool(self) }

    def to_yaml(self, indent=''):
        return ('%s' % bool(self)).lower()

    def __and__(self, other):
        """
        x.__and__(y) <==> x&y
        """
        return obbool(bool(self).__and__(bool(other)))

    def __or__(self, other):
        """
        x.__or__(y) <==> x|y
        """
        return obbool(bool(self).__or__(bool(other)))

    def __rand__(self, other):
        """
        x.__rand__(y) <==> y&x
        """
        return obbool(bool(self).__rand__(bool(other)))

    def __ror__(self, other):
        """
        x.__ror__(y) <==> y|x
        """
        return obbool(bool(self).__ror__(bool(other)))

    def __rxor__(self, other):
        """
        x.__rxor__(y) <==> y^x
        """
        return obbool(bool(self).__rxor__(bool(other)))

    def __xor__(self, other):
        """
        x.__xor__(y) <==> x^y
        """
        return obbool(bool(self).__xor__(bool(other)))

    def __str__(self):
        """
        x.__str__() <==> str(x)
        (ObTrue or ObFalse)
        """
        return 'Ob%s' % bool(self)

    def __repr__(self):
        return self.__str__()

