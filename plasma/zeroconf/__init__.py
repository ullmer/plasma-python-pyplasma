import sys, select, socket, pybonjour, re, logging

def log_error(msg, error_code):
    try:
        raise pybonjour.BonjourException(error_code)
    except:
        logging.exception(msg)

def unescape(m):
    return chr(int(m.group(1), 10))

class Zeroconf(object):
    def __init__(self):
        self.registered_services = []

    def canonicalize_regtype(self, regtype):
        if ',' in regtype:
            (regtype, subtype) = regtype.split(',', 1)
        else:
            subtype = None
        if not regtype.startswith('_'):
            regtype = '_%s' % regtype
        if not re.search('\\._(tcp|udp)\\.?$', regtype):
            regtype += '._tcp.'
        if subtype is not None and subtype != '':
            sts = subtype.split(',')
            for i in range(len(sts)):
                if not sts[i].startswith('_'):
                    sts[i] = '_%s' % sts[i]
            regtype += ',%s' % ','.join(sts)
        return regtype

    def resolve(self, regtype, timeout=2, callback=None):
        regtype = self.canonicalize_regtype(regtype)
        #print regtype
        self.__resolve_timeout = timeout
        self.__resolve_callback = callback
        self.services = dict() 
        fh = pybonjour.DNSServiceBrowse(regtype = regtype,
                                        callBack = self.browse_callback)
        self.__resolve_fh = fh
        self.__services = []
        #while True:
        #    ready = select.select([fh], [], [], self.__resolve_timeout)
        #    if fh not in ready[0]:
        #        break
        #    pybonjour.DNSServiceProcessResult(fh)
        #fh.close()
        #return self.services

    def __iter__(self):
        return self

    def next(self):
        if self.__services:
            return self.__services.pop(0)
        while True:
            ready = select.select([self.__resolve_fh], [], [], self.__resolve_timeout)
            if self.__resolve_fh not in ready[0]:
                self.__resolve_fh.close()
                raise StopIteration()
            pybonjour.DNSServiceProcessResult(self.__resolve_fh)
            break
        if self.__services:
            return self.__services.pop(0)
        raise StopIteration()

    def browse_callback(self, browse, flags, index, err, service, regtype, domain):
        if err != pybonjour.kDNSServiceErr_NoError:
            log_error('browse error', err)
        elif not (flags & pybonjour.kDNSServiceFlagsAdd):
            ## service removed
            pass
        else:
            fh = pybonjour.DNSServiceResolve(0, index, service, regtype, domain, self.resolve_callback)
            self.service = {
                'flags': flags,
                'index': index,
                'service': service,
                'regtype': regtype,
                'domain': domain,
                'resolved': False,
            }
            while not self.service['resolved']:
                ready = select.select([fh], [], [], self.__resolve_timeout)
                if fh not in ready[0]:
                    break
                pybonjour.DNSServiceProcessResult(fh)
            fh.close()

    def resolve_callback(self, resolve, flags, index, err, name, host, port, txt):
        if err != pybonjour.kDNSServiceErr_NoError:
            log_error('resolve error', err)
        else:
            self.service['fullname'] = re.sub('\\\\([0-9]{3})', unescape, name)
            self.service['host'] = host
            self.service['port'] = port
            self.service['text'] = txt #.replace('\0', '')
            self.service['resolved'] = True
            self.service['queried'] = False
            fh = pybonjour.DNSServiceQueryRecord(0, index, host, pybonjour.kDNSServiceType_A, pybonjour.kDNSServiceClass_IN, self.query_callback)
            while not self.service['queried']:
                ready = select.select([fh], [], [], self.__resolve_timeout)
                if fh not in ready[0]:
                    break
                pybonjour.DNSServiceProcessResult(fh)
            fh.close()

    def query_callback(self, query, flags, index, err, name, rrtype, rrclass, rdata, ttl):
        if err != pybonjour.kDNSServiceErr_NoError:
            log_error('resolve error', err)
        else:
            svc = self.service
            self.service['hostname'] = re.sub('\\\\([0-9]{3})', unescape, name)
            self.service['type'] = rrtype
            self.service['class'] = rrclass
            self.service['data'] = ' '.join('%02X' % ord(x) for x in rdata)
            self.service['ip'] = socket.inet_ntoa(rdata)
            self.service['ttl'] = ttl
            self.service['queried'] = True
            if not self.services.has_key(self.service['fullname']):
                self.services[self.service['fullname']] = dict()
            self.services[self.service['fullname']][self.service['index']] = self.service
            self.__services.append(self.service)
            if self.__resolve_callback is not None:
                self.__resolve_callback(self.service)

    def register(self, regtype, port=0, name=None, txt=''):
        flags = 0
        index = 0
        if name is not None:
            name = name.encode('utf8')
        regtype = self.canonicalize_regtype(regtype)
        if ',' in regtype:
            (junk, subtype) = regtype.split(',', 1)
            txt = pybonjour.TXTRecord()
            for st in subtype.split(','):
                txt[st] = None
        else:
            txt = txt.encode('utf8')
        domain = None
        host = None
        #print 'regtype = %s, txt = %s' % (regtype, txt)
        fh = pybonjour.DNSServiceRegister(name=name, regtype=regtype, port=port, txtRecord=txt, callBack=self.register_callback)
        #print 'fh = %s' % fh
        ready = select.select([fh], [], [])
        pybonjour.DNSServiceProcessResult(fh)

    def register_callback(self, register, flags, err, name, regtype, domain):
        #print 'register_callback(%s, %s, %s, %s, %s, %s' % (register, flags, err, name, regtype, domain)
        if err != pybonjour.kDNSServiceErr_NoError:
            print 'register error: %d' % err
            log_error('register error', err)
        self.registered_services.append( (register, regtype, name, domain) )

    def unregister(self, regtype=None):
        if regtype is not None:
            regtype = self.canonicalize_regtype(regtype)
            if ',' in regtype:
                (regtype, subtype) = regtype.split(',', 1)
        for i in reversed(range(len(self.registered_services))):
            if regtype is None or regtype == self.registered_services[i][1]:
                self.registered_services[i][0].close()
                self.registered_services.pop(i)

if '__main__' == __name__:
    def cb(service):
        print "Service:"
        for k,v in service.iteritems():
            print '  %8s: %s' % (k, v)
    zc = Zeroconf()
    for regtype in sys.argv[1:]:
        services = zc.resolve(sys.argv[1], callback=cb)
        for name in sorted(services.keys()):
            print '%s = [' % name
            for idx in sorted(services[name].keys()):
                print '    {'
                for k,v in services[name][idx].iteritems():
                    print '        %8s: %s,' % (k, v)
                print '    },'
            print ']'

