import decimal, time, datetime, struct
from loam.exceptions import *

AM_I_BIG_ENDIAN = (struct.pack('>I', 1) == struct.pack('I', 1))

def get_prefix(backward=False):
    if backward:
        if AM_I_BIG_ENDIAN:
            return '<'
        return '>'
    return ''

def flip_prefix(prefix):
    if prefix == '':
        if AM_I_BIG_ENDIAN:
            return '<'
        else:
            return '>'
    elif prefix == '>':
        return '<'
    elif prefix == '<':
        return '>'

def make_loamtype(value):
    #if type(value) in (int, float, long, complex, decimal.Decimal):
    if type(value) in (int, float, complex, decimal.Decimal):
        return make_obnumber(value)
    if type(value) in (datetime.date, datetime.datetime):
        return loam.obtime.obtimestamp(value)
    #if type(value) in (str, unicode):
    if isinstance(value, str):
        return loam.obstr.obstring(value)
    if type(value) == tuple and len(value) == 2:
        return loam.obstruct.obcons(value)
    if type(value) == list:
        types = set()
        for x in value:
            types.add(type(x))
            if len(types) > 1:
                break
        if len(types) == 1 and (type(value[0]) in (int, float, long, complex) or isinstance(value[0], loam.obnum.obnumber)):
            value = list(make_obnumbersc(*value))
            return loam.numarr.numeric_array(value, type(value[0]))
        return loam.obstruct.oblist(value)
    if type(value) == dict:
        return loam.obstruct.obmap(value)
    return value

def make_obnumber(value):
    if isinstance(value, loam.obnum.obnumber):
        return value
    if type(value) == int:
        if value < 0:
            if abs(value) <= 0x7f:
                return loam.obnum.int8(value)
            if abs(value) <= 0x7fff:
                return loam.obnum.int16(value)
            if abs(value) <= 0x7fffffff:
                return loam.obnum.int32(value)
            return loam.obnum.int64(value)
        if value <= 0xff:
            return loam.obnum.unt8(value)
        if value <= 0xffff:
            return loam.obnum.unt16(value)
        if value <= 0xffffffff:
            return loam.obnum.unt32(value)
        return loam.obnum.unt64(value)
    if type(value) == float:
        return loam.obnum.float32(value)
    if type(value) == decimal.Decimal:
        return loam.obnum.float64(float(value))
    if type(value) == datetime.datetime:
        return loam.obnum.float64(time.mktime(value.timetuple()))
    if type(value) == complex:
        return loam.obnum.float64c(value)
    raise ObInvalidArgumentException("%s is not a numeric type (%s)" % (value, type(value).__name__))

def make_obnumbers(*values):
    is_float = False
    is_signed = False
    bits = 8
    maxint = 0
    for val in values:
        if isinstance(val, complex):
            raise TypeError("Can't use complex numbers with make_obnumbers()")
        elif isinstance(val, (loam.obnum.obint, loam.obnum.obfloat)):
            if val.is_float:
                is_float = True
            if val.is_signed:
                is_signed = True
            if val.bits > bits:
                bits = val.bits
        elif isinstance(val, float):
            is_float = True
            is_signed = True
            bits = 64
        elif isinstance(val, int):
            if val < 0:
                is_signed = True
            if abs(val) > maxint:
                maxint = abs(val)
    if not is_float:
        if is_signed:
            if maxint > 2**31:
                bits = 64
            elif maxint > 2**15 and bits < 32:
                bits = 32
            elif maxint > 2**7 and bits < 16:
                bits = 16
        else:
            if maxint > 2**32:
                bits = 64
            elif maxint > 2**16 and bits < 32:
                bits = 32
            elif maxint > 2**8 and bits < 16:
                bits = 16
    if is_float:
        cls = 'float'
    elif is_signed:
        cls = 'int'
    else:
        cls = 'unt'
    cls = '%s%d' % (cls, bits)
    obtype = getattr(loam.obnum, cls)
    return tuple(obtype(x) for x in values)

def make_obnumbersc(*values):
    is_float = False
    is_signed = False
    is_complex = False
    bits = 8
    maxint = 0
    for val in values:
        if isinstance(val, (loam.obnum.obint, loam.obnum.obfloat, loam.obnum.obcomplex)):
            if val.is_float:
                is_float = True
            if val.is_signed:
                is_signed = True
            if val.bits > bits:
                bits = val.bits
            if val.is_complex:
                is_complex = True
        elif isinstance(val, complex):
            is_float = True
            is_signed = True
            is_complex = True
            bits = 64
        elif isinstance(val, float):
            is_float = True
            is_signed = True
            bits = 64
        elif isinstance(val, int):
            if val < 0:
                is_signed = True
            if abs(val) > maxint:
                maxint = abs(val)
    if not is_float:
        if is_signed:
            if maxint > 2**31:
                bits = 64
            elif maxint > 2**15 and bits < 32:
                bits = 32
            elif maxint > 2**7 and bits < 16:
                bits = 16
        else:
            if maxint > 2**32:
                bits = 64
            elif maxint > 2**16 and bits < 32:
                bits = 32
            elif maxint > 2**8 and bits < 16:
                bits = 16
    if is_float:
        cls = 'float'
    elif is_signed:
        cls = 'int'
    else:
        cls = 'unt'
    cls = '%s%d' % (cls, bits)
    if is_complex:
        cls += 'c'
    obtype = getattr(loam.obnum, cls)
    return tuple(obtype(x) for x in values)

def make_obnumerics(*values):
    is_float = False
    is_signed = False
    is_complex = False
    vtype = 0
    bits = 8
    maxint = 0

import loam.obnum, loam.obvect, loam.obmv, loam.obstr, loam.obstruct, loam.numarr, loam.obtime
