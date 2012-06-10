import struct
from loam.const import *
from loam.exceptions import *
from loam.obnum import int64

class obcons(tuple):
    """
    Two item tuple.  Items can be accessed as with a python tuple, and also
    as self.left and self.right.  Like python tuples, these are immutable.
    """

    def __new__(cls, *args):
        self = tuple.__new__(cls, (make_loamtype(x) for x in args[0]))
        if len(self) != 2:
            raise ObInvalidArgumentException('obcons must be a 2-tuple')
        return self

    def __getattribute__(self, key, *args):
        if key == 'left':
            return self[0]
        if key == 'right':
            return self[1]
        return tuple.__getattribute__(self, key, *args)

    def __add__(self, *args):
        """
        Since obcons objects have exactly two members, this tuple method is
        not applicable
        """
        return NotImplemented

    def __mul__(self, *args):
        """
        Since obcons objects have exactly two members, this tuple method is
        not applicable
        """
        return NotImplemented

    def __rmul__(self, *args):
        """
        Since obcons objects have exactly two members, this tuple method is
        not applicable
        """
        return NotImplemented

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
        data_bytes = self.left.to_slaw_v1() + self.right.to_slaw_v1()
        q = len(data_bytes)
        if q % 4:
            data_bytes += '\x00' * (4 - (q % 4))
            q += 4 - (q % 4)
        if q <= 0x3fffffff:
            header = struct.pack('I', (1 << 30) | q)
        elif q <= 0xffffffff:
            header = struct.pack('II', 0xa0000002, q)
        else:
            header = struct.pack('IQ', 0xe0000002, q)
        return header + data_bytes

    def to_slaw_v2(self):
        data_bytes = self.left.to_slaw_v2() + self.right.to_slaw_v2()
        octlen = 1 + (len(data_bytes) / 8)
        first = 0x62
        packed = struct.pack('q', (first << 56) | octlen) + data_bytes
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def to_json(self, degrade=False):
        val = [self.left.to_json(degrade), self.right.to_json(degrade)]
        if degrade:
            return val
        return { 'json_class': 'obcons', 'v': val }

    def to_yaml(self, indent=''):
        if indent == '':
            return '!cons\n    %s: %s' % (self.left.to_yaml('  '), self.right.to_yaml('  '))
        else:
            return '!cons\n%s  %s: %s' % (indent, self.left.to_yaml('%s  ' % indent), self.right.to_yaml('%s  ' % indent))

class oblist(list):
    """
    Loam version of a list.  Like python lists, these can be composed of
    arbitrarily typed values (though they should all be loam types;
    non-loam types will be converted).  All the usual list operations should
    work with oblists.
    """

    def __init__(self, *args):
        super(oblist, self).__init__(*args)
        for i in range(len(self)):
            self[i] = self[i]

    def __setitem__(self, index, value):
        """
        Like list.__setitem__, but tries to ensure that the value is a
        loam type.
        """
        return list.__setitem__(self, index, make_loamtype(value))

    def __setslice__(self, i, j, y):
        """
        like list.__setslice__, but tries to ensure that the values are
        loam types.
        """
        return list.__setslice__(self, i, j, list(make_loamtype(x) for x in y))

    def __add__(self, *args):
        """
        oblist version of list.__add__
        """
        if not isinstance(args[0], list):
            return NotImplemented
        return oblist(list.__add__(self, args[0]))

    def __getslice__(self, *args):
        """
        oblist version of list.__getslice__
        """
        return oblist(list.__getslice__(self, *args))

    def __iadd__(self, *args):
        """
        oblist version of list.__iadd__
        """
        return oblist(list.__iadd__(self, *args))

    def __imul__(self, *args):
        """
        oblist version of list.__imul__
        """
        return oblist(list.__imul__(self, *args))

    def __mul__(self, *args):
        """
        oblist version of list__mul__
        """
        return oblist(list.__mul__(self, *args))

    def __rmul__(self, *args):
        """
        oblist version of list.__rmul__
        """
        return oblist(list.__rmul__(self, *args))

    def append(self, val):
        """
        Like list.append, but tries to ensure that the value is a
        loam types.
        """
        return list.append(self, make_loamtype(val))

    def extend(self, vals):
        """
        Like list.extend, but tries to ensure that the values are
        loam types.
        """
        return list.extend(self, (make_loamtype(x) for x in vals))

    def insert(self, index, val):
        """
        Like list.insert, but tries to ensure that the value is a
        loam type.
        """
        return list.insert(self, index, make_loamtype(val))

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
        n = len(self)
        data_bytes = ''.join(e.to_slaw_v1() for e in self)
        if n <= 0x7ffffff:
            header = struct.pack('I', (2 << 27) | n)
        elif n <= 0xffffffff:
            header = struct.pack('II', 0xa0000004, n)
        else:
            header = struct.pack('IQ', 0xe0000004, n)
        return header + data_bytes

    def to_slaw_v2(self):
        n = len(self)
        data_bytes = ''.join(e.to_slaw_v2() for e in self)
        octlen = 1 + (len(data_bytes) / 8)
        extra = ''
        if n > 14:
            octlen += 1
            extra = struct.pack('q', n)
            n = 15
        first = 0x40 | n
        packed = struct.pack('q', (first << 56) | octlen) + extra + data_bytes
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def search_ex(self, search, how=SEARCH_GAP):
        """
        If search is a list, performs the type of search specified by how 
        (SEARCH_GAP or SEARCH_CONTIG).  Otherwise, returns the index of the
        first match (or -1 if no match) as an int64.
        """
        if not isinstance(search, list):
            try:
                return int64(self.index(search))
            except ValueError:
                return int64(-1)
        if how == SEARCH_GAP:
            return self.gapsearch(search)
        if how == SEARCH_CONTIG:
            return self.contigsearch(search)
        raise ObInvalidArgumentException("how must be %d (SEARCH_GAP) or %d (SEARCH_CONTIG)" % (SEARCH_GAP, SEARCH_CONTIG))

    def contigsearch(self, search):
        """
        Searches the list for the items in search in consecutive order, and
        returns the index of the first item (or -1 if no match) as an int64.
        """
        first = search[0]
        for i in range(len(self)):
            if self[i] == first:
                found = True
                for j in range(i+1, i+len(search)):
                    if self[j] != search[j-i]:
                        found = False
                        break
                if found:
                    return int64(i)
        return int64(-1)

    def gapsearch(self, search):
        """
        Searches the list for the items in search, which must be in the same
        order, but not necessarily consecutive.  Returns the index of the
        first item (or -1 if no match) as an int64.
        """
        hit = -1
        found = []
        needle = 0
        for i in range(len(self)):
            if self[i] == search[needle]:
                found.append(True)
                if hit < 0:
                    hit = i
                needle += 1
                if needle >= len(search):
                    break
        if len(found) != len(search):
            return int64(-1)
        return int64(hit)

    def to_json(self, degrade=True):
        val = list(x.to_json(degrade) for x in self)
        if degrade:
            return val
        return { 'json_class': 'oblist', 'v': val }

    def to_yaml(self, indent=''):
        if indent == '':
            return '\n%s' % '\n'.join('  - %s' % x.to_yaml('  ') for x in self)
        else:
            joinstr = '\n%s  - ' % indent
            return '- %s' % joinstr.join(x.to_yaml('%s  ' % indent) for x in self)

class obmap(dict):
    """
    Loam version of a dict.  Like python dicts, these can be composed of
    arbitrarily typed values (though they should all be loam types;
    non-loam types will be converted).  All the usual dict operations should
    work with obdicts.
    """

    def __init__(self, *args, **kwargs):
        self.__key_positions = dict()
        self.__key_count = 0
        dict.__init__(self, *args, **kwargs)
        for k in dict.keys(self):
            self[k] = self[k]

    def __setitem__(self, key, value):
        """
        Like dict.__setitem__, but tries to ensure that the value is a
        loam type.
        """
        if not self.__key_positions.has_key(key):
            self.__key_positions[key] = self.__key_count
            self.__key_count += 1
        dict.__setitem__(self, key, make_loamtype(value))

    def __delitem__(self, key):
        dict.__del__(self, key)
        del(self.__key_positions[key])

    def update(self, other, **more):
        """
        Like dict.update, but tries to ensure that the values are loam types.
        """
        if hasattr(other, 'keys'):
            for k in other.keys():
                self[k] = make_loamtype(other[k])
        else:
            for k,v in other:
                self[k] = make_loamtype(v)
        for k in more.keys():
            self[k] = make_loamtype(more[k])

    def iteritems(self, *args):
        """
        Like dict.iteritems, but as obmaps are represented as obconses,
        this method generates obconses, with the left element being the
        key and the right element being the value.
        """
        for k in self.iterkeys(*args):
            yield obcons((k,self[k]))
        #for k,v in dict.iteritems(self, *args):
        #    yield obcons((k,v))

    def items(self, *args):
        """
        Like dict.items, but as obmaps are represented as obconses, this
        method returns a list of obconses, with the left element being the
        key and the right element being the value.
        """
        return oblist(self.iteritems(*args))
        #return oblist(obcons((k,v)) for k,v in dict.items(self, *args))

    def iterkeys(self, *args):
        for k in sorted(self.__key_positions.keys(), key=lambda x: self.__key_positions[x]):
            yield k

    def keys(self, *args):
        """
        Like dict.keys, but returns an oblist instead of a list.
        """
        return oblist(self.iterkeys(*args))
        #return oblist(dict.keys(self, *args))

    def itervalues(self, *args):
        for k in self.iterkeys(*args):
            yield self[k]

    def values(self, *args):
        """
        Like dict.values, but returns an oblist instead of a list.
        """
        return oblist(self.itervalues(*args))
        #return oblist(dict.values(self, *args))

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
        n = len(self)
        data_bytes = ''.join(c.to_slaw_v1() for c in self.iteritems())
        if n <= 0x7ffffff:
            header = struct.pack('I', (3 << 27) | n)
        elif n <= 0xffffffff:
            header = struct.pack('II', 0xa0000005, n)
        else:
            header = struct.pack('IQ', 0xe0000005, n)
        return header + data_bytes

    def to_slaw_v2(self):
        n = len(self)
        data_bytes = ''.join(c.to_slaw_v2() for c in self.iteritems())
        octlen = 1 + (len(data_bytes) / 8)
        extra = ''
        if n > 14:
            octlen += 1
            extra = struct.pack('q', n)
            n = 15
        first = 0x50 | n
        header = (first << 56) | octlen
        packed = struct.pack('q', header) + extra + data_bytes
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self))
        return packed

    def to_json(self, degrade=False):
        val = dict((c.left.to_json(True),c.right.to_json(degrade)) for c in self.items())
        if degrade:
            return val
        return { 'json_class': 'obmap', 'v': val }

    def to_yaml(self, indent=''):
        return '!!omap%s' % ''.join('\n%s- %s: %s' % (indent, k.to_yaml('%s  ' % indent), v.to_yaml('%s  ' % indent)) for k,v in self.iteritems())

from loam.util import make_loamtype, get_prefix
