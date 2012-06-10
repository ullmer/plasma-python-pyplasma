import unittest, os, random, hashlib, time
import plasma.zeroconf

TEST_PORT = 65459

class ZeroconfTestCase(unittest.TestCase):

    def setUp(self):
        self.ident = hashlib.sha1('%s' % random.random()).hexdigest()[:8]

    def testNoSubtype(self):
        regtype = 'pyplasma-%s' % self.ident
        xregtype = '_%s._tcp.' % regtype
        zc = plasma.zeroconf.Zeroconf()
        zc.register(regtype, TEST_PORT)
        try:
            services = list()
            zc.resolve(regtype)
            for svc in zc:
                services.append(svc)
            self.assertGreaterEqual(len(services), 1)
            self.assertEquals(services[0]['regtype'], xregtype)
            self.assertEquals(services[0]['port'], TEST_PORT)
            services = list()
            zc.resolve('%s,foo' % regtype)
            for svc in zc:
                services.append(svc)
            self.assertEquals(len(services), 0)
        finally:
            zc.unregister(regtype)
        zc.unregister(regtype)
        time.sleep(5)
        services = list()
        zc.resolve(regtype)
        for svc in zc:
            services.append(svc)
        self.assertEquals(len(services), 0)

    def testWithSubtype(self):
        regtype = 'pyplasma-%s' % self.ident
        xregtype = '_%s._tcp.' % regtype
        zc = plasma.zeroconf.Zeroconf()
        zc.register('%s,foo,bar,baz' % regtype, TEST_PORT)
        try:
            services = list()
            zc.resolve(regtype)
            for svc in zc:
                services.append(svc)
            self.assertGreaterEqual(len(services), 1)
            self.assertEquals(services[0]['regtype'], xregtype)
            self.assertEquals(services[0]['port'], TEST_PORT)
            n = len(services)
            services = list()
            zc.resolve('%s,bar' % regtype)
            for svc in zc:
                services.append(svc)
            self.assertEquals(len(services), n)
            self.assertEquals(services[0]['regtype'], xregtype)
            self.assertEquals(services[0]['port'], TEST_PORT)
            services = list()
            zc.resolve('%s,junk' % regtype)
            for svc in zc:
                services.append(svc)
            self.assertEquals(len(services), 0)
        finally:
            zc.unregister(regtype)
        zc.unregister(regtype)
        time.sleep(5)
        services = list()
        zc.resolve(regtype)
        for svc in zc:
            services.append(svc)
        self.assertEquals(len(services), 0)
        services = list()
        zc.resolve('%s,bar' % regtype)
        for svc in zc:
            services.append(svc)
        self.assertEquals(len(services), 0)

if '__main__' == __name__:
    unittest.main()

