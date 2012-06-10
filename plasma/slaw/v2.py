import struct
from loam.util import get_prefix, flip_prefix, AM_I_BIG_ENDIAN
from loam.exceptions import *
from loam.obsimple import obnil, obbool
from loam.obnum import *
from loam.obvect import *
from loam.obmv import *
from loam.numarr import numeric_array
from loam.obstr import obstring
from loam.obstruct import obcons, oblist, obmap
from plasma.protein import Protein
from plasma.const import *
from plasma.exceptions import *

def str2hex(data, prefix=''):
    if isinstance(data, (int, long)):
        data = struct.pack('%sQ' % prefix, data)
    return ' '.join('%02x' % ord(x) for x in data)

def parse_header2(fh, backward=False):
    prefix = get_prefix(backward)
    header_data = fh.read(8)
    try:
        (header,) = struct.unpack('%sQ' % prefix, header_data)
    except struct.error:
        raise SlawCorruptSlawException("unexpected end-of-file")
    stype = header >> 60
    if stype == 0:
        if header & 0x10:
            ## backward protein
            orig_prefix = prefix
            prefix = flip_prefix(orig_prefix)
            try:
                (header,) = struct.unpack('%sQ' % prefix, struct.pack('%sQ' % orig_prefix, header))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
            return ('protein', header, fh, backward ^ True)
        raise SlawCorruptProteinException("header oct %s does not represent a protein" % str2hex(header, prefix))
    if stype == 1:
        if header & 0xf0 != 0:
            raise SlawCorruptProteinException("header oct %s does not represent a protein" % str2hex(header_data))
        return ('protein', header, fh, backward)
    if stype == 2:
        if header & 2:
            return ('obnil', header, fh, backward)
        return ('obbool', header, fh, backward)
    if stype == 3:
        return ('obstring', header, fh, backward)
    if stype == 4:
        return ('oblist', header, fh, backward)
    if stype == 5:
        return ('obmap', header, fh, backward)
    if stype == 6:
        return ('obcons', header, fh, backward)
    if stype == 7:
        return ('obstring', header, fh, backward)
    if stype & 12 == 8:
        return ('obnumber', header, fh, backward)
    if stype & 12 == 12:
        return ('numeric_array', header, fh, backward)
    raise SlawCorruptSlawException("header oct %s does not represent a valid slaw type", str2hex(header, prefix))

def parse_slaw2(fh, backward=False):
    """
    Decode the next version 2 slaw from the file object fh, and return an
    object representation of the data.
    """
    #pos = fh.tell()
    (slaw_type, header, fh, backward) = parse_header2(fh, backward)
    #print '%d: %s (%s)' % (pos, slaw_type, ' '.join('%02x' % ord(x) for x in struct.pack('Q', header)))
    return v2parsers[slaw_type](header, fh, backward)

def skip_slaw2(fh, backward=False):
    """
    Parse just enough of the next version 2 slaw from the file object fh to
    skip past this slaw object.
    """
    (slaw_type, header, fh, backward) = parse_header2(fh, backward)
    try:
        return v2skippers[slaw_type](header, fh, backward)
    except:
        print "error skipping slaw %s (with header 0x%016x / %s)" % (slaw_type, header, backward)
        raise

def parse_obnil(header, fh, backward=False):
    return obnil()

def skip_obnil(header, fh, backward=False):
    return fh.tell()

def parse_obbool(header, fh, backward=False):
    return obbool(header & 1)

def skip_obbool(header, fh, backward=False):
    return fh.tell()

def parse_obnumber(header, fh, backward=False):
    prefix = get_prefix(backward)
    count = header & 0x3fffffffffff
    config = header >> 46
    b = (header >> 46) & 0xff
    v = (header >> 54) & 7
    c = (header >> 57) & 1
    s = (header >> 58) & 3
    u = (header >> 60) & 1
    f = (header >> 61) & 1
    vtypes = ['', 'v2', 'v3', 'v4', 'mv2', 'mv3', 'mv4', 'mv5']
    cls = vtypes[v]
    if u:
        cls += 'unt'
    elif f:
        cls += 'float'
    else:
        cls += 'int'
    cls += '%s' % (2**(s+3))
    if c:
        cls += 'c'
    klass = globals()[cls]
    if b == 3:
        data = struct.pack('%sI' % prefix, header & 0xffffffff)
    elif b == 2:
        data = struct.pack('%sI' % prefix, header & 0xffffffff)[:3]
    #    xdata = list((header >> x) & 0xff for x in range(0, 64, 8))
    #    print '3 byte data has header %s' % ' '.join('%02x' % x for x in xdata) 
    #    data = struct.pack('%sBBB' % prefix, header & 0xff, (header >> 8) & 0xff, (header >> 16) & 0xff)
    elif b == 1:
        data = struct.pack('%sH' % prefix, header & 0xffff)
    elif b == 0:
        data = struct.pack('%sB' % prefix, header & 0xff)
    else:
        data = fh.read(b+1)
        if (b+1) % 8 != 0:
            fh.read(8 - ((b+1) % 8))
    return klass.decode(data, prefix)

def skip_obnumber(header, fh, backward=False):
    b = (header >> 46) & 0xff
    if b > 3:
        n = b + 1
        if n % 8 != 0:
            n += (8 - (n % 8))
        fh.seek(n, 1)
    return fh.tell()

def parse_numeric_array(header, fh, backward=False):
    prefix = get_prefix(backward)
    count = header & 0x3fffffffffff
    config = header >> 46
    b = ((header >> 46) & 0xff) + 1
    v = (header >> 54) & 7
    c = (header >> 57) & 1
    s = (header >> 58) & 3
    u = (header >> 60) & 1
    f = (header >> 61) & 1
    vtypes = ['', 'v2', 'v3', 'v4', 'mv2', 'mv3', 'mv4', 'mv5']
    kls = vtypes[v]
    if u:
        kls += 'unt'
    elif f:
        kls += 'float'
    else:
        kls += 'int'
    kls += '%d' % (2**(s+3))
    if c:
        kls += 'c'
    klass = globals()[kls]
    #print 'numarr(%s, b=%d, v=%d, c=%d, s=%d, u=%d, f=%d, B=%d)' % (kls, b, v, c, s, u, f, count)
    data = fh.read(b*count)
    if (b*count) % 8:
        fh.read(8 - ((b*count) % 8))
    return numeric_array(list(klass.decode(data[i:i+b], prefix) for i in range(0, len(data), b)), klass)

def skip_numeric_array(header, fh, backward=False):
    count = header & 0x3fffffffffff
    b = (header >> 46) & 0xff + 1
    n = b * count
    if n % 8 != 0:
        n += (8 - (n % 8))
    fh.seek(n, 1)
    return fh.tell()

def parse_obstring(header, fh, backward=False):
    if (header >> 60) == 3:
        ## wee string
        strbytes = ((header >> 56) & 7) - 1
        if backward:
            if AM_I_BIG_ENDIAN:
                value = struct.pack('>q', header)[8-strbytes:7].decode('utf8')
            else:
                value = struct.pack('<q', header)[0:strbytes].decode('utf8')
        else:
            if AM_I_BIG_ENDIAN:
                value = struct.pack('>q', header)[8-strbytes:7].decode('utf8')
            else:
                value = struct.pack('<q', header)[0:strbytes].decode('utf8')
    else:
        ## full string
        padding = (header >> 56) & 7
        octlen = header & 0xffffffffffffff
        data = fh.read(8 * (octlen-1))
        n = (8 * (octlen-1)) - (padding + 1)
        value = data[:n].decode('utf8')
    #print 'obstring(%s)' % value
    return obstring(value)

def skip_obstring(header, fh, backward=False):
    if (header >> 60) != 3:
        octlen = header & 0xffffffffffffff
        n = 8 * (octlen - 1)
        fh.seek(n, 1)
    return fh.tell()

def parse_obcons(header, fh, backward=False):
    octlen = header & 0xffffffffffffff
    value = (parse_slaw2(fh, backward), parse_slaw2(fh, backward))
    return obcons(value)

def skip_obcons(header, fh, backward=False):
    octlen = header & 0xffffffffffffff
    n = 8 * (octlen - 1)
    fh.seek(n, 1)
    return fh.tell()

def parse_oblist(header, fh, backward=False):
    prefix = get_prefix(backward)
    n = (header >> 56) & 15
    octlen = header & 0xffffffffffffff
    if n >= 15:
        try:
            (n,) = struct.unpack('%sq' % prefix, fh.read(8))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    return oblist(parse_slaw2(fh, backward) for i in range(n))

def skip_oblist(header, fh, backward=False):
    octlen = header & 0xffffffffffffff
    n = 8 * (octlen - 1)
    fh.seek(n, 1)
    return fh.tell()

def parse_obmap(header, fh, backward=False):
    prefix = get_prefix(backward)
    n = (header >> 56) & 15
    octlen = header & 0xffffffffffffff
    if n >= 15:
        try:
            (n,) = struct.unpack('%sq' % prefix, fh.read(8))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    value = obmap()
    for i in range(n):
        x = parse_slaw2(fh, backward)
        value[x.left] = x.right
    return value

def skip_obmap(header, fh, backward=False):
    octlen = header & 0xffffffffffffff
    n = 8 * (octlen - 1)
    fh.seek(n, 1)
    return fh.tell()

def parse_protein(header, fh, backward=False):
    prefix = get_prefix(backward)
    octlen = ((header >> 4) & 0xffffffffffffff) | (header & 0xf)
    try:
        (h2,) = struct.unpack('q', fh.read(8))
    except struct.error:
        raise SlawCorruptSlawException("unexpected end-of-file")
    n = h2 >> 63
    d = (h2 >> 62) & 1
    i = (h2 >> 61) & 1
    f = (h2 >> 60) & 1
    x = (h2 >> 59) & 1
    #print 'octlen = %d' % self.octlen
    #print 'ndifx = (%d, %d, %d, %d, %d)' % (n, d, i, f, x)
    if x:
        r = h2 & 0x7ffffffffffffff
    else:
        r = (h2 >> 56) & 7
    if d:
        descrips = parse_slaw2(fh, backward)
    else:
        descrips = None
    if i:
        ingests = parse_slaw2(fh, backward)
    else:
        ingests = None
    if x:
        padding = 8 - (r % 8)
        rude_data = fh.read(r)
        if padding < 8:
            fh.read(padding)
    else:
        if r == 0:
            rude_data = ''
        elif AM_I_BIG_ENDIAN:
            rude_data = struct.pack('>q', h2)[8-r:8]
        else:
            rude_data = struct.pack('<q', h2)[0:r]
    return Protein(descrips, ingests, rude_data)

def skip_protein(header, fh, backward=False):
    octlen = ((header >> 4) & 0xffffffffffffff) | (header & 0xf)
    n = 8 * (octlen - 1)
    fh.seek(n, 1)
    return fh.tell()

v2parsers = {
    'protein': parse_protein,
    'obnil': parse_obnil,
    'obbool': parse_obbool,
    'obstring': parse_obstring,
    'oblist': parse_oblist,
    'obmap': parse_obmap,
    'obcons': parse_obcons,
    'obnumber': parse_obnumber,
    'numeric_array': parse_numeric_array,
}

v2skippers = {
    'protein': skip_protein,
    'obnil': skip_obnil,
    'obbool': skip_obbool,
    'obstring': skip_obstring,
    'oblist': skip_oblist,
    'obmap': skip_obmap,
    'obcons': skip_obcons,
    'obnumber': skip_obnumber,
    'numeric_array': skip_numeric_array,
}
