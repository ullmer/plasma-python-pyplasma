import re

def sizestr_to_bytes(val):
    if isinstance(val, (str, unicode)):
        m = re.match('^([0-9]+)([kMGT]?)$', val)
        if m:   
            size = int(m.group(1))
            mult = m.group(2)
        else:
            m = re.match('^([0-9]*\.[0-9]+)([kMGT]?)$', val)
            if m:
                size = float(m.group(1))
                mult = m.group(2)
            else:
                size = 1
                mult = 'M'
        if mult == 'k':
            size *= 2**10
        elif mult == 'M':
            size *= 2**20
        elif mult == 'G':
            size *= 2**30
        elif mult == 'T':
            size *= 2**40
        return int(size)
    if isinstance(val, (int, float)):
        return int(val)

