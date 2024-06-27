#import struct, cStringIO, re
import struct, re
from io import StringIO

try:
    import yaml
except:
    yaml = None
from plasma.slaw.v1 import parse_slaw1, skip_slaw1
from plasma.slaw.v2 import parse_slaw2, skip_slaw2
from loam import *
from plasma.protein import Protein
from plasma.const import *
from plasma.exceptions import *

BINARY_MAGIC = struct.pack('@BBBB', 0xff, 0xff, 0x0b, 0x10)
YAML_MAGIC = '!<ta'

def read_slaw_file(filename):
    """
    Returns a slaw-decoded data structure stored in the file referenced by
    filename.  Slaw version is autodetected.
    """
    fh = open(filename, 'rb')
    obj = read_slaw_fh(fh)
    fh.close()
    return obj

def read_slaw_fh(fh):
    """
    Returns a slaw-decoded data structure from the file object fh.  Slaw
    version is autodetected.
    """
    if fh.read(4) != BINARY_MAGIC:
        fh.close()
        raise SlawWrongFormatException( "not an oblong protein")
    (version, ptype, xflags) = struct.unpack('@BB2s', fh.read(4))
    return parse_slaw(version, fh)

def write_slaw_file(filename, obj, version=2):
    """
    Writes obj as slaw-encoded data (using the object's to_slaw() method)
    to the file referenced by filename.  version is the slaw version to use,
    defaults to version 2.
    """
    fh = open(filename, 'wb')
    write_slaw_fh(fh, obj, version)
    fh.close()

def write_slaw_fh(fh, obj, version=2):
    """
    Writes obj as slaw-encoded data (using the object's to_slaw() method)
    to the file object fh.  version is the slaw version to use, defaults
    to version 2.
    """
    fh.write(BINARY_MAGIC)
    fh.write(struct.pack('@BB2s', version, 1, struct.pack('>H', 0)))
    fh.write(obj.to_slaw(version))

def parse_slaw(version, fh, backward=False):
    """
    Reads the next slaw-encoded data structure from the file object fh,
    and decodes it into an object.  version is the slaw version in which
    the file was encoded, and backward is used internally to fix endianness
    issues.
    """
    if version == 2:
        return parse_slaw2(fh, backward)
    if version == 1:
        return parse_slaw1(fh, backward)
    raise SlawWrongVersionException('Slaw version %d not supported' % version)

def skip_slaw(version, fh, backward=False):
    """
    Parses just enough of the slaw-encoded data structure to advance the
    file pointer past the end of the slaw object.  version is the slaw
    version in which the file was encoded, and backward is used internally
    to fix endianness issues.
    """
    if version == 2:
        return skip_slaw2(fh, backward)
    if version == 1:
        return skip_slaw2(fh, backward)
    raise SlawWrongVersionException('Slaw version %d not supported' % version)

def parse_yaml_slaw(fh):
    tag = fh.readline()
    data = yaml.load(fh)
    protein = Protein(descrips=data.get('descrips', None), ingests=data.get('ingests', None))
    return protein

def parse_slaw_data(version, data, backward=False):
    """
    Decodes the slaw-encoded data structure in data.  version is the slaw
    version in which the data was encoded, and backward is used internally to
    fix endianness issues.
    """
    #return parse_slaw(version, cStringIO.StringIO(data), backward)
    result = parse_slaw(version, StringIO.StringIO(data), backward)
    return result

def degrade_json(obj):
    if type(obj) == list:
        return list(degrade_json(x) for x in obj)
    if type(obj) == dict:
        if obj.has_key('json_class') and obj.has_key('v'):
            return degrade_json(obj['v'])
        return dict( (degrade_json(k), degrade_json(v)) for k, v in obj)
    return obj

protein_keys = set(('descrips', 'ingests', 'rude_data', 'timestamp', 'index'))
def from_json(obj):
    if obj is None:
        return obnil()
    elif type(obj) == dict:
        if obj.has_key('json_class'):
            kls = obj['json_class']
            val = obj['v']
            m = re.match('^(m?v[2-4])?(unt|int|float)(8|16|32|64)(c)?(_array)?$', kls)
            if m is not None:
                vtype = m.group(1)
                ctype = m.group(4)
                atype = m.group(5)
                if vtype is None:
                    vtype = ''
                if ctype is None:
                    ctype = ''
                if atype is None:
                    atype = ''
                base_type = '%s%s' % (m.group(2), m.group(3))
                val = degrade_json(val)
                if atype != '':
                    akls = '%s%s%s' % (vtype, base_type, ctype)
                    acls = globals()[akls]
                    ## numeric array
                    ret = numeric_array([], acls)
                    if vtype != '':
                        ## vector or multi-vector
                        vcls = globals()['%s%s%s' % (vtype, base_type, ctype)]
                        if ctype != '':
                            ## complex
                            acls = globals()[base_type+'c']
                            for v in val:
                                nums = list()
                                for num in v:
                                    nums.append(acls(num[0], num[1]))
                                ret.append(vcls(*nums))
                        else:
                            ## real
                            acls = globals()[base_type]
                            for v in val:
                                nums = list()
                                for num in v:
                                    nums.append(acls(num))
                                ret.append(vcls(*nums))
                    else:
                        ## scalar
                        if ctype != '':
                            ## complex
                            acls = globals()[base_type+'c']
                            for num in val:
                                ret.append(acls(num[0], num[1]))
                        else:
                            ## real
                            acls = globals()[base_type]
                            for num in val:
                                ret.append(acls(num))
                    return ret
                else:
                    ## singleton
                    if vtype != '':
                        ## vector or multi-vector
                        vcls = globals()['%s%s%s' % (vtype, base_type, ctype)]
                        nums = list()
                        if m.group(4):
                            ## complex
                            acls = globals()[base_type+'c']
                            for num in val:
                                nums.append(acls(num[0], num[1]))
                        else:
                            ## real
                            acls = globals()[base_type]
                            for num in val:
                                nums.append(acls(num))
                        return vcls(*nums)
                    else:
                        ## scalar
                        if ctype != '':
                            ## complex
                            acls = globals()[base_type+'c']
                            return acls(val[0], val[1])
                        else:
                            ## real
                            acls = globals()[base_type]
                            return acls(val)
            elif kls == 'protein':
                kwargs = dict()
                for k in protein_keys:
                    v = val.get(k, None)
                    if v is not None:
                        kwargs[k] = from_json(v)
                return Protein(**kwargs)
            elif kls == 'obstring':
                return obstring(val)
            elif kls == 'obbool':
                return obbool(val)
            elif kls == 'obnil':
                return obnil()
            elif kls == 'obcons':
                return obcons(from_json(val[0]), from_json(val[1]))
            elif kls == 'oblist':
                args = list(from_json(x) for x in val)
                return oblist(*args)
            elif kls == 'obmap':
                args = list( (from_json(k), from_json(v)) for k,v in val.items() )
                return obmap(*args)
            elif kls == 'obtimestamp':
                return obtimestamp(from_json(val))
        else:
            for key in obj.keys():
                if key not in protein_keys:
                    return obmap( (from_json(k), from_json(v)) for k,v in obj.iteritems() )
            return from_json({ 'json_class': 'protein', 'v': obj })
    elif type(obj) == list:
        return oblist(from_json(x) for x in obj)
    elif type(obj) in (str, unicode):
        return obstring(obj)
    elif type(obj) == bool:
        return obbool(obj)
    elif type(obj) == float:
        return float64(obj)
    elif type(obj) == int:
        return int64(obj)

def dump_yaml(obj):
    return '%YAML 1.1\n%TAG ! tag:oblong.com,2009:slaw/\n--- %s' % obj.to_yaml()

