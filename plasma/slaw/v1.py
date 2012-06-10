import struct, cStringIO
from loam.util import get_prefix
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

def parse_header1(fh, backward=False):
    prefix = get_prefix(backward)
    try:
        (header,) = struct.unpack('%sI' % prefix, fh.read(4))
    except struct.error:
        raise SlawCorruptSlawException("unexpected end-of-file")
    if header == 0x01010101:
        ## nil
        return ('obnil', header, fh, backward)
    if header >> 30 == 1:
        ## wee cons
        return ('obcons', header, fh, backward)
    if header >> 29 == 1:
        ## wee string
        return ('obstring', header, fh, backward)
    if header >> 27 == 2:
        ## wee list
        return ('oblist', header, fh, backward)
    if header >> 27 == 3:
        ## wee map
        return ('obmap', header, fh, backward)
    if header >> 27 == 1:
        ## numeric
        return ('obnumber', header, fh, backward)
    if header & 0xbffffff8 == 0xa0000000:
        stype = header & 7
        if stype == 1:
            ## full string
            return ('obstring', header, fh, backward)
        if stype == 2:
            ## full cons
            return ('obcons', header, fh, backward)
        if stype == 4:
            ## full list
            return ('oblist', header, fh, backward)
        if stype == 5:
            ## full map
            return ('obmap', header, fh, backward)
        raise SlawCorruptSlawException('header quad %s does not indicate a valid slaw type' % ' '.join('%02x' % ord(x) for x in struct.pack('%sI' % prefix, header)))
    if header & 0xfffffffe == 0:
        ## boolean
        return ('obbool', header, fh, backward)
    if header & 0xa08080e0 == 0x80800080:
        ## protein
        return ('protein', header, fh, backward)
    if header & 0xe08080a0 == 0x80008080:
        ## backward protein?
        header = struct.unpack('%sI' % get_prefix(backward ^ True), struct.pack('%sI' % prefix, header))[0]
        return ('protein', header, fh, backward ^ True)
    raise SlawCorruptSlawException('header quad %s does not indicate a valid slaw type' % ' '.join('%02x' % ord(x) for x in struct.pack('%sI' % prefix, header)))

def parse_slaw1(fh, backward=False):
    """
    Decode the next version 1 slaw from the file object fh, and return an
    object representation of the data.
    """
    (slaw_type, header, fh, backward) = parse_header1(fh, backward)
    #print 'slaw_type = %s' % slaw_type
    return v1parsers[slaw_type](header, fh, backward)

def skip_slaw1(fh, backward=False):
    (slaw_type, header, fh, backward) = parse_header1(fh, backward)
    return v1skippers[slaw_type](header, fh, backward)

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
    f = (header >> 26) & 1
    c = (header >> 25) & 1
    u = (header >> 24) & 1
    s = (header >> 22) & 3
    v = (header >> 19) & 7
    b = (header & 0xff) + 1
    if (header >> 18) & 1:
        if (header >> 17) & 1:
            try:
                n = struct.unpack('%sQ' % prefix, fh.read(8))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
        else:
            try:
                n = struct.unpack('%sI' % prefix, fh.read(4))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
    else:
        n = (header >> 8) & 0x3ff
    stypes = [32, 8, 64, 16]
    vtypes = ['', 'v2', 'v3', 'v4', 'mv2', 'mv3', 'mv4', 'mv5']
    cls = vtypes[v]
    if u:
        cls += 'unt'
    elif f:
        cls += 'float'
    else:
        cls = 'int'
    cls += '%d' % stypes[s]
    if c:
        cls += 'c'
    klass = globals()[cls]
    if n == 0:
        ## singleton
        data = fh.read(b)
        if b % 4:
            fh.read(4 - (b % 4))
        try:
            return klass.decode(data, prefix)
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    else:
        ## array(n-1)
        n -= 1
        data = fh.read(b*n)
        if (b*n) % 4:
            fh.read(4 - ((b*n) % 4))
        try:
            return numeric_array(list(klass.decode(data[i:i+b], prefix) for i in range(0, len(data), b)), klass)
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")

def skip_obnumber(header, fh, backward=False):
    prefix = get_prefix(backward)
    b = (header & 0xff) + 1
    if (header >> 18) & 1:
        if (header >> 17) & 1:
            try:
                n = struct.unpack('%sQ' % prefix, fh.read(8))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
        else:
            try:
                n = struct.unpack('%sI' % prefix, fh.read(4))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
    else:
        n = (header >> 8) & 0x3ff
    if n == 0:
        x = b
    else:
        n -= 1
        x = b * n
        if x % 4 != 0:
            x += (4 - (x % 4))
    fh.seek(x, 1)
    return fh.tell()

def parse_obstring(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 31 == 0:
        ## wee string
        #quadlen = header & 0x1fffffff - 1
        quadlen = header & 0x1fffffff
    elif header >> 30 == 3:
        ## 8-byte quadlen
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            #quadlen -= 2
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 30 == 2:
        ## 4-byte quadlen
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            #quadlen -= 3
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    value = fh.read(quadlen * 4).decode('utf8').replace('\x00', '')
    return obstring(value)

def skip_obstring(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 31 == 0:
        ## wee string
        quadlen = header & 0x1fffffff - 1
    elif header >> 30 == 3:
        ## 8-byte quadlen
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 2
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 30 == 2:
        ## 4-byte quadlen
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 3
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    fh.seek(quadlen*4, 1)
    return fh.tell()

def parse_obcons(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 30 == 1:
        ## wee cons
        quadlen = header & 0x3fffffff - 1
    elif header >> 29 == 5:
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 2
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 29 == 7:
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 3
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    value = (parse_slaw1(fh, backward), parse_slaw1(fh, backward))
    return obcons(value)

def skip_obcons(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 30 == 1:
        ## wee cons
        quadlen = header & 0x3fffffff - 1
    elif header >> 29 == 5:
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 2
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 29 == 7:
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 3
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    fh.seek(quadlen*4, 1)
    return fh.tell()

def parse_oblist(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 28 == 1:
        ## wee list
        count = header & 0x7ffffff
        (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
        quadlen -= 2
    elif header >> 29 == 5:
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 3
            (count,) = struct.unpack('%sI' % prefix, fh.read(4))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 29 == 7:
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 5
            (count,) = struct.unpack('%sQ' % prefix, fh.read(8))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    return oblist(parse_slaw1(fh, backward) for i in range(count))

def skip_oblist(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 28 == 1:
        ## wee list
        count = header & 0x7ffffff
        (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
        quadlen -= 2
    elif header >> 29 == 5:
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 3
            (count,) = struct.unpack('%sI' % prefix, fh.read(4))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 29 == 7:
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 5
            (count,) = struct.unpack('%sQ' % prefix, fh.read(8))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    fh.seek(quadlen*4, 1)
    return fh.tell()

def parse_obmap(header, fh, backward=False):
    prefix = get_prefix(backward)
    if header >> 28 == 1:
        ## wee list
        count = header & 0x7ffffff
        (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
        quadlen -= 2
    elif header >> 29 == 5:
        try:
            (quadlen,) = struct.unpack('%sI' % prefix, fh.read(4))
            quadlen -= 3
            (count,) = struct.unpack('%sI' % prefix, fh.read(4))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    elif header >> 29 == 7:
        try:
            (quadlen,) = struct.unpack('%sQ' % prefix, fh.read(8))
            quadlen -= 5
            (count,) = struct.unpack('%sQ' % prefix, fh.read(8))
        except struct.error:
            raise SlawCorruptSlawException("unexpected end-of-file")
    value = obmap()
    for i in range(count):
        x = parse_slaw1(fh, backward)
        value[x.left] = x.right
    return value

def skip_obmap(header, fh, backward=False):
    return skip_oblist(header, fh, backward)

def parse_protein(header, fh, backward=False):
    prefix = get_prefix(backward)
    w = (header >> 28) & 1
    n = (header >> 27) & 1
    d = (header >> 26) & 1
    i = (header >> 25) & 1
    r = (header >> 24) & 1
    q = (header >> 16) & 0x7f
    p = (header >>  8) & 0x7f
    f = header & 0x1f
    qr = 0
    if not w:
        if (header >> 30) & 1:
            try:
                (q,) = struct.unpack('%sQ' % prefix, fh.read(8))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
            qr += 2
        else:
            try:
                (q,) = struct.unpack('%sI' % prefix, fh.read(4))
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
            qr += 1
    #print 'prefix = %s; w=%s;n=%s;d=%s;i=%s;r=%s;q=%s;p=%s;f=%s' % (prefix, w, n, d, i, r, q, p, f)
    pdata = fh.read((q - qr)*4)
    xfh = cStringIO.StringIO(pdata)
    if d:
        descrips = parse_slaw1(xfh, backward)
    else:
        descrips = None
    if i:
        ingests = parse_slaw1(xfh, backward)
    else:
        ingests = None
    if r:
        rude_data = xfh.read()
    else:
        rude_data = ''
    return Protein(descrips, ingests, rude_data)

def skip_protein(header, fh, backward=False):
    prefix = get_prefix(backward)
    w = (header >> 28) & 1
    q = (header >> 16) & 0x7f - 1
    if not w:
        if (header >> 30) & 1:
            try:
                (q,) = struct.unpack('%sQ' % prefix, fh.read(8))
                q -= 3
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
        else:
            try:
                (q,) = struct.unpack('%sI' % prefix, fh.read(4))
                q -= 2
            except struct.error:
                raise SlawCorruptSlawException("unexpected end-of-file")
            qr += 1
    fh.seek(q*4, 1)
    return fh.tell()

v1parsers = {
    'protein': parse_protein,
    'obnil': parse_obnil,
    'obbool': parse_obbool,
    'obstring': parse_obstring,
    'oblist': parse_oblist,
    'obmap': parse_obmap,
    'obcons': parse_obcons,
    'obnumber': parse_obnumber,
    #'numeric_array': parse_numeric_array,
}

v1skippers = {
    'protein': skip_protein,
    'obnil': skip_obnil,
    'obbool': skip_obbool,
    'obstring': skip_obstring,
    'oblist': skip_oblist,
    'obmap': skip_obmap,
    'obcons': skip_obcons,
    'obnumber': skip_obnumber,
    #'numeric_array': skip_numeric_array,
}
