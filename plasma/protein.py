import struct, datetime
from loam import *
from plasma.const import *
from loam.util import get_prefix, AM_I_BIG_ENDIAN

class Protein(object):
    """
    Proteins are the main unit of data in plasma.  They generally consist of
    descrips (usually a list of strings), ingests (a set of key value pairs),
    and sometimes unstructured "rude data".

    Like loam types, proteins are transferred about as slaw, so these objects
    also have a to_slaw method that works like the others.
    """

    NO_TIME = datetime.datetime.min
    NO_INDEX = -1

    def __init__(self, descrips=None, ingests=None, rude_data='', timestamp=None, index=None, origin=None):
        """
        descrips should be an oblist or None, ingests should be an obmap
        or None, rude_data should be a bytestring, timestamp should be a
        datetime.datetime or None, index should be an int64 or None,
        origin should be a plasma.pool or None.

        timestamp, index and origin are only relevant when coming from a
        pool.  You probably shouldn't be setting these yourself.

        If descrips is a list, it will be converted to an oblist, and its
        contents will be converted to loam types.  Likewise, if ingests is
        a dict, it will be converted to an obmap and its contents will be
        converted to loam types.
        """
        if type(descrips) == list:
            descrips = oblist(descrips)
        if type(ingests) == dict:
            ingests = obmap(ingests)
        if rude_data is None:
            rude_data = ''
        if isinstance(timestamp, (int, long, float, datetime.date)):
            timestamp = obtimestamp(timestamp)
        self.__descrips = descrips
        self.__ingests = ingests
        self.__rude_data = rude_data
        self.__timestamp = timestamp
        self.__index = index
        self.__origin = origin

    def __str__(self):
        return 'Protein(descrips=%s, ingests=%s, rude_data="%s")' % (self.__descrips, self.__ingests, self.__rude_data)

    def is_null(self):
        pass

    def is_empty(self):
        """
        True if the protein has no descrips, ingests or rude data.
        """
        if self.__descrips is not None and len(self.__descrips) > 0:
            return False
        if self.__ingests is not None and len(self.__ingests) > 0:
            return False
        if self.rude_data is not None and self.rude_data != '':
            return False
        return True

    def spew(self, fh):
        fh.write('%s\n' % self)


    def hash(self):
        return self.__hash__()

    def descrips(self):
        """
        The protein's descrips.  Usually an oblist of obstrings.  This
        is by convention, though, and is not enforced.  If a protein
        has no descrips, this will return None.

        In typical usage, descrips is like a set of tags to filter on.
        """
        return self.__descrips

    def ingests(self):
        """
        The protein's ingests.  Usually an obmap with obstring keys.  This
        is by convention, though, and is not enforced.  If a protein has
        no ingests, this will return None.

        In typical usage, ingests is where the protein's payload is stored.
        """
        return self.__ingests

    def rude_data(self):
        """
        Unstructured data included with the protein.
        """
        return self.__rude_data

    ## descrips matching
    def search(self, needle, how=SEARCH_GAP):
        """
        Descrips search.

        This function looks for needle in this protein's descrips, using
        the plasma semantics. That means that the search will only succeed
        if the descrips are an oblist, in which case this functions returns:

        * if needle is a list, self.descrips().search_ex(needle,how)
        * if needle is not a list, self.descrips().search_ex([needle,], how)

        If this protein's descrips are not a list, this function returns a
        negative value. The optional how argument works the same as in the
        oblist method search_ex().
        """
        ## Slaw or bslaw, Protein_Search_Type
        ## return int64
        if type(self.__descrips) != oblist:
            print 'descrips is not a list (%s)' % type(self.__descrips)
            return int64(-1)
        if isinstance(needle, list):
            if len(needle) == 0:
                return 0
            return self.__descrips.search_ex(needle, how)
        else:
            return self.__descrips.search_ex(oblist([needle,]), how)

    def matches(self, needle, how=SEARCH_GAP):
        """
        Convenience function checking if self.search(needle, how) > -1
        """
        ## Slaw or bslaw, Protein_Search_Type
        ## return obbool
        return obbool(self.search(needle, how) > -1)

    def timestamp(self):
        """
        If the protein was retrieved from a pool, this returns the time
        at which it was deposited in the pool as a python datetime.datetime.
        Otherwise, returns None
        """
        return self.__timestamp

    def index(self):
        """
        If the protein was retrieved from a pool, this returns the index
        at which it was deposited in the pool (int64).  Otherwise, returns
        None
        """
        return self.__index

    def origin(self):
        """
        If the protein was retrieved from a pool, this returns the pool
        from which it was retrieved.  Otherwise, returns None.
        """
        return self.__origin

    def set_timestamp(self, when):
        self.__timestamp = when

    def set_index(self, index):
        self.__index = int64(index)

    def set_origin(self, origin):
        self.__origin = origin

    @classmethod
    def null(cls):
        pass

    def to_slaw(self, version=2):
        if version == 2:
            return self.to_slaw_v2()
        if version == 1:
            return self.to_slaw_v1()
        raise SlawWrongVersionException('Slaw version %d not supported' % version)

    def to_slaw_v1(self):
        pass
        n = 0
        d = int(self.descrips() is not None)
        i = int(self.ingests() is not None)
        f = 0
        r = int(len(self.rude_data()) > 0)
        if len(self.rude_data()) % 4:
            p = 4 - (len(self.rude_data()) % 4)
        data = ''
        if d:
            data += self.descrips().to_slaw(1)
        if i:
            data += self.ingests().to_slaw(1)
        data += self.__rude_data
        if p:
            data += '\x00' * p
        q = len(data) / 4
        if q > 2**32:
            h = (1 << 31) | (1 << 30) | (n << 27) | (d << 26) | (i << 25) | (r << 24) | (1 << 23) | (p << 8) | (1 << 7) | f
            h = struct.pack('IQ', h, q)
        elif q > 2**7:
            h = (1 << 31) | (n << 27) | (d << 26) | (i << 25) | (r << 24) | (1 << 23) | (p << 8) | (1 << 7) | f
            h = struct.pack('II', h, q)
        else:
            h = (1 << 31) | (1 << 28) | (n << 27) | (d << 26) | (i << 25) | (r << 24) | (1 << 23) | (q << 16) | (p << 8) | (1 << 7) | f
            h = struct.pack('I', h)
        return h+data

    def to_slaw_v2(self):
        n = 0
        d = int(self.descrips() is not None)
        i = int(self.ingests() is not None)
        f = 0
        r = len(self.rude_data()) 
        x = int(r > 7)
        data = ''
        if d:
            data += self.descrips().to_slaw(2)
        if i:
            data += self.ingests().to_slaw(2)
        if len(self.rude_data()) > 7:
            data += self.rude_data()
            padding = 8 - (r % 8)
            if padding < 8:
                data += '\x00' * padding
            h2 = struct.pack('q', (n << 63) | (d << 62) | (i << 61) | (f << 60) | (x << 59) | r)
        else:
            s = self.rude_data()
            if AM_I_BIG_ENDIAN:
                s = '%s%s' % ('\x00' * (8 - len(s)), s)
            (special,) = struct.unpack('q', struct.pack('8s', s))
            h2 = struct.pack('Q', (n << 63) | (d << 62) | (i << 61) | (f << 60) | (x << 59) | (r << 56) | special)
        octlen = (len(data) / 8) + 2
        header = (1 << 60) | ((octlen & 0xfffffffffffff0) << 4) | (octlen & 0xf)
        packed = struct.pack('Q8s%ds' % len(data), header, h2, data)
        #print ' '.join('%02x' % ord(c) for c in packed)
        if len(packed) % 8 > 0:
            raise SlawWrongLengthException('%s packing not a multiple of 8 bytes (%d): %s' % (type(self).__name__, len(packed), self.deconvert()))
        return packed

    def has_descrips(self, *args):
        dset = set(self.descrips())
        for d in args:
            if d not in dset:
                return False
        return True

    def unset_descrips(self):
        self.__descrips = None

    def set_descrips(self, *args):
        self.__descrips = oblist(args)

    def add_descrips(self, *args):
        if self.__descrips is None:
            self.__descrips = oblist()
        if type(self.__descrips) != oblist:
            raise TypeError("descrips is not an oblist")
        for d in args:
            self.__descrips.append(d)

    def unset_ingests(self):
        self.__ingests = None

    def set_ingests(self, **kwargs):
        self.__ingests = obmap(kwargs)

    def update_ingests(self, **kwargs):
        if self.__ingests is None:
            self.__ingests = obmap()
        if type(self.__ingests) != obmap:
            raise TypeError("ingests is not an obmap")
        for k,v in kwargs.iteritems():
            self.__ingests[k] = v

    def delete_ingests(self, *args):
        if type(self.__ingests) != obmap:
            raise TypeError("ingests is not an obmap")
        for k in args:
            if self.__ingests.has_key(k):
                del(self.__ingests[k])

    def unset_rude_data(self):
        self.__rude_data = ''

    def set_rude_data(self, data):
        if data is None:
            data = ''
        self.__rude_data = data

    def IsNull(self, *args, **kwargs):
        """
        Plasma++ version of is_null()
        """
        return self.is_null(*args, **kwargs)

    def IsEmpty(self, *args, **kwargs):
        """
        Plasma++ version of is_empty()
        """
        return self.is_empty(*args, **kwargs)

    def Spew(self, *args, **kwargs):
        """
        Plasma++ version of spew()
        """
        return self.spew(*args, **kwargs)

    def Hash(self, *args, **kwargs):
        """
        Plasma++ version of hash()
        """
        return self.hash(*args, **kwargs)

    def Descrips(self):
        """
        Plasma++ version of descrips()
        """
        return self.descrips()

    def Ingests(self):
        """
        Plasma++ version of ingests()
        """
        return self.ingests()

    def ProteinValue(self):
        """
        Plasma++ function that returns the underlying C-level object.
        Here, we just return self.
        """
        return self

    def ToSlaw(self):
        """
        Plasma++ version of to_slaw()
        """
        return self.to_slaw()

    def Search(self, *args, **kwargs):
        """
        Plasma++ version of search()
        """
        return self.search(*args, **kwargs)

    def Matches(self, *args, **kwargs):
        """
        Plasma++ version of matches()
        """
        return self.matches(*args, **kwargs)

    def Timestamp(self, *args, **kwargs):
        """
        Plasma++ version of timestamp()
        """
        return self.timestamp(*args, **kwargs)

    def Index(self, *args, **kwargs):
        """
        Plasma++ version of index()
        """
        return self.index(*args, **kwargs)

    def Origin(self, *args, **kwargs):
        """
        Plasma++ version of origin()
        """
        return self.origin(*args, **kwargs)

    def Null(self, *args, **kwargs):
        """
        Plasma++ version of null()
        """
        return self.num(*args, **kwargs)

    def to_json(self, degrade=False):
        val = dict()
        if self.descrips() is None:
            val['descrips'] = None
        else:
            val['descrips'] = self.descrips().to_json(degrade)
        if self.ingests() is None:
            val['ingests'] = None
        else:
            val['ingests'] = self.ingests().to_json(degrade)
        val['rude_data'] = self.rude_data()
        if self.timestamp() is None:
            val['timestamp'] = None
        else:
            val['timestamp'] = self.timestamp().to_json(degrade)
        if self.index() is None:
            val['index'] = None
        else:
            val['index'] = self.index().to_json(degrade)
        if self.origin() is None:
            val['origin'] = None
        else:
            val['origin'] = self.origin().name()
        if degrade:
            return val
        return { 'json_class': 'protein', 'v': val }

    def to_yaml(self, indent=''):
        yamlstr = '!protein\n'
        if self.descrips() is not None:
            yamlstr += '%sdescrips: %s' % (indent, self.descrips().to_yaml(indent))
        if self.ingests() is not None:
            yamlstr += '%singests: %s' % (indent, self.ingests().to_yaml(indent))
        return yamlstr

