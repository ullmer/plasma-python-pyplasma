import unittest, os, random, hashlib, time, socket, sys, time, subprocess
from loam import *
from plasma.protein import Protein
from plasma.const import *
from plasma.exceptions import *
from plasma.hose import Hose, PoolFetchOp
from plasma.hose.local import LocalHose
from plasma.hose.tcp import TCPHose

SERVER_ALREADY_RUNNING = False
POOL_SERVER_BIN = None
POOL_SERVER_PID = None
USE_TLS = False

class BaseHoseTestCase(unittest.TestCase):

    def testValidateName(self):
        parsed = Hose.validate_name('pyplasma-test')
        self.assertEquals(parsed['transport'], 'local')
        self.assertEquals(parsed['host'], 'localhost')
        self.assertIsNone(parsed['port'])
        self.assertEquals(len(parsed['components']), 1)
        self.assertEquals(parsed['path'], 'pyplasma-test')
        self.assertEquals(parsed['name'], 'pyplasma-test')
        parsed = Hose.validate_name('local:/var/ob/pools/pyplasma-test')
        self.assertEquals(parsed['transport'], 'local')
        self.assertEquals(len(parsed['components']), 5)
        self.assertEquals(parsed['path'], 'local:/var/ob/pools/pyplasma-test')
        parsed = Hose.validate_name('pyplasma/test')
        self.assertEquals(parsed['transport'], 'local')
        self.assertEquals(len(parsed['components']), 2)
        self.assertEquals(parsed['path'], 'pyplasma/test')
        parsed = Hose.validate_name('tcp://localhost/pyplasma-test')
        self.assertEquals(parsed['transport'], 'tcp')
        self.assertEquals(parsed['host'], 'localhost')
        self.assertIsNone(parsed['port'])
        self.assertEquals(len(parsed['components']), 1)
        self.assertEquals(parsed['path'], 'pyplasma-test')
        parsed = Hose.validate_name('tcps://some.other.host:12345/path/to/pool')
        self.assertEquals(parsed['transport'], 'tcps')
        self.assertEquals(parsed['host'], 'some.other.host')
        self.assertEquals(parsed['port'], 12345)
        self.assertEquals(len(parsed['components']), 3)
        self.assertEquals(parsed['path'], 'path/to/pool')
        bad_names = ['', 'abcdef' * 20, 'tcp://localhost/', 'some//path', 'some"name', 'some/.path', 'some./path', 'some /path', 'some$/path', 'some/con/path', 'some/con.path']
        for name in bad_names:
            self.assertRaises(PoolPoolnameBadthException, Hose.validate_name, name)

    def testGetHose(self):
        hose = Hose._get_hose_for_name('pyplasma-test')[0]
        self.assertIsInstance(hose, LocalHose)
        hose = Hose._get_hose_for_name('local:/var/ob/pools/pyplasma-test')[0]
        self.assertIsInstance(hose, LocalHose)
        self.assertRaises(PoolPoolnameBadthException, Hose._get_hose_for_name, 'http://www.oblong-pointing.com/pyplasma-test')

class TCPHoseTestCase(unittest.TestCase):
    prefix = 'tcp://localhost/'

    @classmethod
    def setUpClass(cls):
        raise Exception("no tcp server")
        global SERVER_ALREADY_RUNNING
        global POOL_SERVER_BIN
        global POOL_SERVER_PID
        global USE_TLS
        ## Do we already have a pool server running?
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', DEFAULT_PORT))
            sock.close()
            SERVER_ALREADY_RUNNING = True
            POOL_SERVER_BIN = None
            POOL_SERVER_PID = None
            ## Yes
            ## Does it support TLS?
            ## Let's just assume so
            USE_TLS = True
        except socket.error:
            #print 'pool server not running'
            print('pool server not running')
            SERVER_ALREADY_RUNNING = False
            ## No
            ## Can we run the C pool_tcp_server?
            POOL_SERVER_BIN = None
            for dn in os.getenv('PATH').split(os.pathsep):
                en = os.path.join(dn, 'pool_tcp_server')
                if os.path.exists(en) and os.access(en, os.X_OK):
                    POOL_SERVER_BIN = en
                    break
            if POOL_SERVER_BIN is not None:
                ## Yes
                ## Does it support TLS?
                try:
                    output = subprocess.check_output([POOL_SERVER_BIN, '--help'])
                #except subprocess.CalledProcessError, e:
                except (subprocess.CalledProcessError, e):
                    output = e.output
                if re.search('only allow secure connections', output):
                    ## Yes
                    USE_TLS = True
                else:
                    ## No
                    USE_TLS = False
                pid = os.fork()
                if pid:
                    POOL_SERVER_PID = pid
                else:
                    os.execl(POOL_SERVER_BIN, '-n')
                    sys.exit(0)
            else:
                ## No
                ## Start up bundled pool server as a last resort
                server = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'py-pool-tcp-server')
                POOL_SERVER_BIN = server
                pid = os.fork()
                if pid:
                    POOL_SERVER_PID = pid
                else:
                    os.execl(POOL_SERVER_BIN, '-n')
                    sys.exit(0)
                USE_TLS = True

    def setUp(self):
        self.ident = hashlib.sha1('%s' % random.random()).hexdigest()[:8]
        self.create_options = obmap({'size': unt64(102400), 'index-capacity': unt64(100)})
        self.pool = None
        self.hose = None

    def tearDown(self):
        try:
            if self.hose is not None:
                pool = self.hose.get_hose_name()
                self.hose.withdraw()
                Hose.dispose(pool)
            elif self.pool is not None:
                Hose.dispose(self.pool)
        except:
            pass

    @classmethod
    def tearDownClass(cls):
        root = cls.prefix
        for pool in Hose.list_ex(root):
            if pool.startswith('pyplasma-test-'):
                Hose.dispose('%s%s' % (root, pool))
        if not SERVER_ALREADY_RUNNING:
            os.kill(POOL_SERVER_PID, signal.SIGTERM)

    def testServer(self):
        hose = Hose._get_hose_for_name('%spyplasma-test' % self.prefix)[0]
        self.assertIsInstance(hose, TCPHose)
        self.assertEquals(hose._protocol_name, 'tcp')
        self.assertEquals(hose._protocol_version, 3)
        self.assertEquals(hose._slaw_version, 2)

    def testCreateDispose(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        self.assertIsNone(Hose.create(pool, 'mmap', self.create_options))
        self.assertIsNone(Hose.dispose(pool))

    def testRename(self):
        orig = '%spyplasma-test-orig-%s' % (self.prefix, self.ident)
        new = '%spyplasma-test-new-%s' % (self.prefix, self.ident)
        Hose.create(orig, 'mmap', self.create_options)
        self.assertIsNone(Hose.rename(orig, new))
        Hose.dispose(new)

    def testExists(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.assertTrue(Hose.exists(pool))
        Hose.dispose(pool)
        self.assertFalse(Hose.exists(pool))

    def testSleep(self):
        pass

    def testCheckInUse(self):
        pass

    def testParticipateWithdraw(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(self.pool, 'mmap', self.create_options)
        hose = Hose.participate(self.pool)
        self.assertIsInstance(hose, TCPHose)
        self.assertIsNone(hose.withdraw())

    def testParticipateCreatingly(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        hose = Hose.participate_creatingly(self.pool, 'mmap', self.create_options)
        self.assertIsInstance(hose, TCPHose)
        self.assertIsNone(hose.withdraw())

    def testListPools(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(self.pool, 'mmap', self.create_options)
        pools = Hose.list_ex(self.prefix)
        self.assertGreaterEqual(len(pools), 1)
        self.assertIn('pyplasma-test-%s' % self.ident, pools)

    def testListEx(self):
        pool = '%spyplasma/testsub-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        pools = Hose.list_ex('%spyplasma' % self.prefix)
        self.assertGreaterEqual(len(pools), 1)
        self.assertIn('testsub-%s' % self.ident, pools)

    def testHoseName(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        hose = Hose.participate(pool)
        self.assertEquals(hose.name(), 'pyplasma-test-%s' % self.ident)
        self.assertEquals(hose.get_hose_name(), pool)
        self.assertIsNone(hose.set_hose_name('something'))
        self.assertEquals(hose.get_hose_name(), 'something')
        hose.withdraw()
        Hose.dispose(pool)

    def testGetInfo(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        p = self.hose.get_info()
        i = p.ingests()
        self.assertIsNone(p.descrips())
        self.assertEquals(i['index-step'], 1)
        self.assertFalse(i['frozen'])
        self.assertFalse(i['checksum'])
        self.assertEquals(i['slaw-version'], 2)
        self.assertEquals(i['index-count'], 0)
        self.assertFalse(i['stop-when-full'])
        self.assertTrue(i['terminal'])
        self.assertFalse(i['auto-dispose'])
        self.assertEquals(i['size-used'], 0)
        self.assertEquals(i['index-capacity'], 100)
        self.assertFalse(i['sync'])
        self.assertEquals(i['mmap-pool-version'], 1)
        self.assertEquals(i['type'], 'mmap')
        self.assertEquals(i['size'], 102400)

    def testIndeces(self):
        ## newest_index
        ## oldest_index
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.assertRaises(PoolNoSuchProteinException, self.hose.oldest_index)
        self.assertRaises(PoolNoSuchProteinException, self.hose.newest_index)
        self.hose.deposit(Protein())
        self.assertEquals(self.hose.oldest_index(), 0)
        self.assertEquals(self.hose.newest_index(), 0)
        self.hose.deposit(Protein())
        self.hose.deposit(Protein())
        self.hose.deposit(Protein())
        self.assertEquals(self.hose.oldest_index(), 0)
        self.assertEquals(self.hose.newest_index(), 3)

    def testDeposit(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        p = Protein()
        self.assertEquals(self.hose.deposit(p), 0)
        p = Protein(descrips=oblist(['some', 'stuff', 'and', 'junk']))
        self.assertEquals(self.hose.deposit(p), 1)
        p = Protein(ingests=obmap({'foo': 1, 'bar': 'baz'}))
        self.assertEquals(self.hose.deposit(p), 2)
        p = Protein(descrips=oblist(['some', 'stuff']), ingests=obmap({'foo': 2, 'bar': 'blah'}))
        self.assertEquals(self.hose.deposit(p), 3)
        now = obtimestamp()
        ex = self.hose.deposit_ex(p)
        self.assertIsInstance(ex, obmap)
        self.assertEquals(ex['index'], 4)
        self.assertIsInstance(ex['timestamp'], obtimestamp)
        self.assertAlmostEqual(ex['timestamp'].timestamp(), now.timestamp(), 0, 10)

    def testCurr(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.assertRaises(PoolNoSuchProteinException, self.hose.curr)
        p = Protein(descrips=['test', 'curr'], ingests={'n': 0})
        self.hose.deposit(p)
        self.assertEquals(self.hose.curr().ingests()['n'], 0)
        p.ingests()['n'] = 1
        self.hose.deposit(p)
        self.assertEquals(self.hose.curr().ingests()['n'], 0)

    def testNext(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.assertRaises(PoolNoSuchProteinException, self.hose.next)
        p = Protein(descrips=['test', 'curr'], ingests={'n': 0})
        self.hose.deposit(p)
        p.ingests()['n'] = 1
        self.hose.deposit(p)
        p.ingests()['n'] = 2
        self.hose.deposit(p)
        self.assertEquals(self.hose.next().ingests()['n'], 0)
        self.assertEquals(self.hose.next().ingests()['n'], 1)
        self.assertEquals(self.hose.next().ingests()['n'], 2)
        self.assertRaises(PoolNoSuchProteinException, self.hose.next)

    def testPrev(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.assertRaises(PoolNoSuchProteinException, self.hose.prev)
        p = Protein(descrips=['test', 'curr'], ingests={'n': 0})
        self.hose.deposit(p)
        p.ingests()['n'] = 1
        self.hose.deposit(p)
        p.ingests()['n'] = 2
        self.hose.deposit(p)
        self.assertRaises(PoolNoSuchProteinException, self.hose.prev)
        p = self.hose.next()
        p = self.hose.next()
        p = self.hose.next()
        self.assertEquals(self.hose.prev().ingests()['n'], 1)
        self.assertEquals(self.hose.prev().ingests()['n'], 0)
        self.assertRaises(PoolNoSuchProteinException, self.hose.prev)

    def testFetch(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.hose.deposit(Protein(descrips=oblist(['abc', 'def']), ingests=obmap({ 'key': 'value', 'n': int64(0) })))
        self.hose.deposit(Protein(ingests=obmap({ 'key': 'value', 'n': int64(1) })))
        self.hose.deposit(Protein(descrips=oblist(['abc', 'def'])))
        self.hose.deposit(Protein(descrips=oblist(['abc', 'def']), ingests=obmap({ 'key': 'value', 'n': int64(3) })))
        self.hose.deposit(Protein(descrips=oblist(['some', 'stuff']), ingests=obmap({ 'key': 'value', 'n': int32(4) })))
        self.hose.deposit(Protein(descrips=oblist(['and', 'junk']), ingests=obmap({ 'key': 'value', 'n': int8(5) })))
        ops = [
            PoolFetchOp(idx=1, want_descrips=True, want_ingests=True),
            PoolFetchOp(idx=2, want_descrips=True, want_ingests=True),
            PoolFetchOp(idx=0, want_descrips=True, want_ingests=True),
            PoolFetchOp(idx=4, want_descrips=False, want_ingests=True),
            PoolFetchOp(idx=5, want_descrips=True, want_ingests=False),
            PoolFetchOp(idx=3, want_descrips=False, want_ingests=False),
            PoolFetchOp(idx=6, want_descrips=True, want_ingests=True),
        ]
        xops = self.hose.fetch(ops)
        self.assertEquals(ops[0].idx, 1)
        self.assertEquals(ops[2].num_descrips, 2)
        self.assertEquals(ops[2].num_ingests, 2)
        self.assertEquals(len(ops[2].p.descrips()), 2)
        self.assertEquals(len(ops[2].p.ingests()), 2)
        self.assertEquals(ops[0].num_descrips, -1)
        self.assertEquals(ops[1].num_ingests, -1)
        self.assertEquals(ops[3].num_descrips, 2)
        self.assertIsNone(ops[3].p.descrips())
        self.assertEquals(ops[4].num_ingests, 2)
        self.assertIsNone(ops[4].p.ingests())
        self.assertEquals(ops[6].exception[0], PoolNoSuchProteinException)
        self.assertEquals(ops[6].total_bytes, 0)

    def __n_deposits(self, n, *words, **kwargs):
        delay = kwargs.get('delay', None)
        if kwargs.has_key('delay'):
            del(kwargs['delay'])
        proteins = list()
        if not hasattr(self, 'hose') or self.hose is None:
            pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
            Hose.create(pool, 'mmap', self.create_options)
            self.hose = Hose.participate(pool)
        if len(words) == 0:
            words = ['test',]
        for i in range(n):
            if delay is not None:
                time.sleep(delay)
            kwargs['n'] = i
            ingests = obmap(kwargs)
            n = i % len(words)
            if words[n] is None:
                descrips = None
            elif isinstance(words[n], list):
                descrips = oblist(words[n])
            else:
                descrips = oblist([words[n],])
            p = Protein(descrips=descrips, ingests=ingests)
            retval = self.hose.deposit_ex(p)
            proteins.append( (p, retval) )
        return proteins

    def testNthProtein(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        self.assertRaises(PoolNoSuchProteinException, self.hose.nth_protein, 0)
        p = Protein(descrips=['test', 'curr'], ingests={'n': 0})
        self.hose.deposit(p)
        p.ingests()['n'] = 1
        self.hose.deposit(p)
        p.ingests()['n'] = 2
        self.hose.deposit(p)
        self.assertEquals(self.hose.nth_protein(1).ingests()['n'], 1)
        self.assertEquals(self.hose.nth_protein(2).ingests()['n'], 2)
        self.assertEquals(self.hose.nth_protein(0).ingests()['n'], 0)
        self.assertRaises(PoolNoSuchProteinException, self.hose.nth_protein, 3)

    def testIndexLookup(self):
        return True
        proteins = self.__n_deposits(10, 'test', 'index', 'lookup', delay=1)
        self.assertEquals(self.hose.index_lookup(proteins[0][1]['timestamp']), 0)
        self.assertEquals(self.hose.index_lookup(proteins[9][1]['timestamp']), 9)
        self.assertEquals(self.hose.index_lookup(proteins[4][1]['timestamp']), 4)
        self.assertRaises(PoolNoSuchProteinException, self.hose.index_lookup, 10)

    def testProbeBack(self):
        self.__n_deposits(10, 'test', 'probe', 'back', 'and', 'forth', 'or', 'fifth')
        self.hose.nth_protein(9)
        p = self.hose.probe_back('fifth')
        self.assertEquals(p.ingests()['n'], 6)
        p = self.hose.probe_back('probe')
        self.assertEquals(p.ingests()['n'], 1)
        self.assertRaises(PoolNoSuchProteinException, self.hose.probe_back, 'fifth')

    def testProbeFrwd(self):
        self.__n_deposits(10, 'test', 'probe', ['forward', 'but', 'not', 'backward'], 'to', 'the', 'end', 'of', 'time')
        self.assertRaises(PoolNoSuchProteinException, self.hose.probe_frwd, 'fifth')
        p = self.hose.probe_frwd('forward')
        self.assertEquals(p.ingests()['n'], 2)
        self.hose.nth_protein(0)
        p = self.hose.probe_frwd(['forward', 'backward'])
        self.assertEquals(p.ingests()['n'], 2)
        self.hose.nth_protein(0)
        self.assertRaises(PoolNoSuchProteinException, self.hose.probe_frwd, ['backward', 'forward'])
        self.hose.nth_protein(4)
        p = self.hose.probe_frwd('time')
        self.assertEquals(p.ingests()['n'], 7)
        self.assertRaises(PoolNoSuchProteinException, self.hose.probe_frwd, 'end')

    def testAwaitNext(self):
        return True
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        pid = os.fork()
        if pid:
            self.hose = Hose.participate(pool)
            self.hose.deposit(Protein(ingests=obmap({'n': 10})))
            p = self.hose.await_next(timeout=POOL_NO_WAIT)
            self.assertEquals(p.ingests()['n'], 10)
            p = self.hose.await_next(timeout=int64(5))
            self.assertEquals(p.ingests()['n'], 20)
            p = self.hose.await_next()
            self.assertEquals(p.ingests()['n'], 30)
            self.assertRaises(PoolAwaitTimedoutException, self.hose.await_next, timeout=POOL_NO_WAIT)
            self.assertRaises(PoolAwaitTimedoutException, self.hose.await_next, timeout=int64(5))
            os.waitpid(pid, 0)
        else:
            try:
                hose = Hose.participate(pool)
                time.sleep(3)
                hose.deposit(Protein(ingests=obmap({'n': 20})))
                time.sleep(10)
                hose.deposit(Protein(ingests=obmap({'n': 30})))
                time.sleep(10)
                hose.deposit(Protein(ingests=obmap({'n': 40})))
                hose.withdraw()
            except:
                pass
            sys.exit(0)

    def testAwaitProbeFrwd(self):
        return True
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        pid = os.fork()
        if pid:
            self.hose = Hose.participate(pool)
            self.hose.deposit(Protein(ingests=obmap({'n': 10})))
            p = self.hose.await_next(timeout=POOL_NO_WAIT)
            self.assertEquals(p.ingests()['n'], 10)
            p = self.hose.await_next(timeout=int64(5))
            self.assertEquals(p.ingests()['n'], 20)
            p = self.hose.await_next()
            self.assertEquals(p.ingests()['n'], 30)
            self.assertRaises(PoolAwaitTimedoutException, self.hose.await_next, timeout=POOL_NO_WAIT)
            self.assertRaises(PoolAwaitTimedoutException, self.hose.await_next, timeout=int64(5))
            os.waitpid(pid, 0)
        else:
            try:
                hose = Hose.participate(pool)
                time.sleep(3)
                hose.deposit(Protein(descrips=[], ingests=obmap({'n': 20})))
                time.sleep(10)
                hose.deposit(Protein(ingests=obmap({'n': 30})))
                time.sleep(10)
                hose.deposit(Protein(ingests=obmap({'n': 40})))
                hose.withdraw()
            except:
                pass
            sys.exit(0)
        pass

    def testWakeup(self):
        pass

    def testIsConfigured(self):
        pass

class LocalHoseTestCase(TCPHoseTestCase):
    prefix = ''

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        root = cls.prefix
        for pool in Hose.list_ex(root):
            if pool.startswith('pyplasma-test-'):
                Hose.dispose('%s%s' % (root, pool))

    def testServer(self):
        hose = Hose._get_hose_for_name('%spyplasma-test' % self.prefix)[0]
        self.assertIsInstance(hose, LocalHose)

    def testCheckInUse(self):
        pass

    def testParticipateWithdraw(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(self.pool, 'mmap', self.create_options)
        hose = Hose.participate(self.pool)
        self.assertIsInstance(hose, LocalHose)
        self.assertIsNone(hose.withdraw())

    def testParticipateCreatingly(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        hose = Hose.participate_creatingly(self.pool, 'mmap', self.create_options)
        self.assertIsInstance(hose, LocalHose)
        self.assertIsNone(hose.withdraw())

    def testListPools(self):
        self.pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(self.pool, 'mmap', self.create_options)
        pools = Hose.list_pools()
        self.assertGreaterEqual(len(pools), 1)
        self.assertIn('pyplasma-test-%s' % self.ident, pools)

    def testGetInfo(self):
        pool = '%spyplasma-test-%s' % (self.prefix, self.ident)
        Hose.create(pool, 'mmap', self.create_options)
        self.hose = Hose.participate(pool)
        p = self.hose.get_info()
        i = p.ingests()
        self.assertIsNone(p.descrips())
        self.assertEquals(i['index-step'], 1)
        self.assertFalse(i['frozen'])
        self.assertFalse(i['checksum'])
        self.assertEquals(i['slaw-version'], 2)
        self.assertEquals(i['index-count'], 0)
        self.assertFalse(i['stop-when-full'])
        self.assertTrue(i['terminal'])
        self.assertFalse(i['auto-dispose'])
        self.assertEquals(i['size-used'], 0)
        self.assertEquals(i['index-capacity'], 100)
        self.assertFalse(i['sync'])
        self.assertEquals(i['mmap-pool-version'], 1)
        self.assertEquals(i['type'], 'mmap')
        self.assertEquals(i['size'], 102400)

    def testWakeup(self):
        pass

    def testIsConfigured(self):
        pass

if '__main__' == __name__:
    unittest.main()

