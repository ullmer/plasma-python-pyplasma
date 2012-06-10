import sys, os, socket, signal, struct, select, cStringIO
try:
    import ssl
except:
    ssl = None
from loam import *
from plasma.const import *
from plasma.exceptions import *
import plasma.config
import plasma.slaw
from plasma.protein import Protein
from plasma.hose import Hose

TCP_PROTOCOL_VERSION = 3
SLAW_VERSION = 2
TCP_PROTOCOL_VERSION_INDEX = 76
SLAW_VERSION_INDEX = 77
HANDSHAKE = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
    0x93, 0x93, 0x00, 0x80, 0x18, 0x00, 0x00, 0x02,
    0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x04,
    0x20, 0x00, 0x00, 0x01, 0x6f, 0x70, 0x00, 0x00,
    0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
    0x40, 0x00, 0x00, 0x08, 0x20, 0x00, 0x00, 0x02,
    0x61, 0x72, 0x67, 0x73, 0x00, 0x00, 0x00, 0x00,
    0x10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05,
    0x20, 0x00, 0x00, 0x02, 0x5e, 0x2f, 0x5e, 0x2f,
    0x5e, 0x2f, 0x5e, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]

OP_CREATE                 =  0
OP_DISPOSE                =  1
OP_PARTICIPATE            =  2
OP_PARTICIPATE_CREATINGLY =  3
OP_WITHDRAW               =  4
OP_DEPOSIT                =  5
OP_NTH_PROTEIN            =  6
OP_NEXT                   =  7
OP_PROBE_FRWD             =  8
OP_NEWEST_INDEX           =  9
OP_OLDEST_INDEX           = 10
OP_AWAIT_NEXT_SINGLE      = 11
OP_MULTI_ADD_AWAITER      = 12
OP_INFO                   = 15
OP_LIST                   = 16
OP_INDEX_LOOKUP           = 17
OP_PROBE_BACK             = 18
OP_PREV                   = 19
OP_FANCY_ADD_AWAITER      = 20
OP_SET_HOSE_NAME          = 21
OP_SUB_FETCH              = 22
OP_RENAME                 = 23
OP_ADVANCE_OLDEST         = 24
OP_SLEEP                  = 25
OP_CHANGE_OPTIONS         = 27
OP_LIST_EX                = 28
OP_SUB_FETCH_EX           = 29
OP_STARTTLS               = 30

POOL_CMD_RESULT           = 14
POOL_CMD_FANCY_RESULT_1   = 64
POOL_CMD_FANCY_RESULT_2   = 65
POOL_CMD_FANCY_RESULT_3   = 66

OLD_RETORTS = [
    (OB_UNKNOWN_ERR,          0, 2,    -203),
    (OB_ARGUMENT_WAS_NULL,    0, 2, -210000),
    (SLAW_CORRUPT_PROTEIN,    0, 2, -210001),
    (SLAW_CORRUPT_SLAW,       0, 2, -210002),
    (SLAW_FABRICATOR_BADNESS, 0, 2, -210003),
    (SLAW_NOT_NUMERIC,        0, 2, -210004),
    (SLAW_RANGE_ERR,          0, 2, -210005),
    (SLAW_UNIDENTIFIED_SLAW,  0, 2, -210006),
    (SLAW_WRONG_LENGTH,       0, 2, -210007),
    (SLAW_NOT_FOUND,          0, 2, -210008),
    (POOL_NO_SUCH_PROTEIN,    0, 2, -200010), ## POOL_EMPTY
    (OB_NO_MEM,               0, 2, -200530), ## POOL_MALLOC_BADTH
    (POOL_INVALID_SIZE,       0, 2, -200590), ## POOL_ARG_BADTH
    (POOL_NO_SUCH_PROTEIN,    0, 2, -200610), ## POOL_DISCARDED_PROTEIN
    (POOL_NO_SUCH_PROTEIN,    0, 2, -200620), ## POOL_FUTURE_PROTEIN
]
OLD_RETORTS_LOOKUP = dict()
for x in OLD_RETORTS:
    for i in range(x[1], x[2]+1):
        OLD_RETORTS_LOOKUP[(i, x[3])] = x[0]

class TCPHose(Hose):
    _flipped_wait = False
    _protocol_name = 'tcp'
    _protocol_version = None
    _slaw_version = None

    @classmethod
    def opensocket(cls, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return sock

    @classmethod
    def handshake(cls, sock, short=False):
        if short:
            sendbytes = struct.pack('BB', TCP_PROTOCOL_VERSION, SLAW_VERSION)
        else:
            HANDSHAKE[TCP_PROTOCOL_VERSION_INDEX] = TCP_PROTOCOL_VERSION
            HANDSHAKE[SLAW_VERSION_INDEX] = SLAW_VERSION
            sendbytes = ''.join(chr(x) for x in HANDSHAKE)
        sock.sendall(sendbytes)
        recvbytes = sock.recv(1024)
        (protocol_version, slaw_version, ops_bytes) = struct.unpack('BBB', recvbytes[:3])
        if protocol_version == 0:
            return (0, 0, [])
        if protocol_version > POOL_TCP_VERSION_CURRENT:
            if protocol_version == ord('H') and slaw_version == ord('T'):
                logging.error('%s:%s looks like it might be an http server, not a pool server!' % (host, port))
            else:
                logging.error('%s:%s server claims protocol version %d/slaw %d, but we only know protocol %d/slaw %d' % (host, port, protocol_version, slaw_version, POOL_TCP_VERSION_CURRENT, SLAW_VERSION_CURRENT))
            raise PoolWrongVersion("unsupported version of tcp protocol")
        maskbytes = recvbytes[3:]
        ops = list(False for i in range(ops_bytes * 8))
        for i in range(ops_bytes * 8):
            B = int(i / 8)
            b = i % 8
            #print 'bit %d (byte %d of %d (%d))' % (i, B, len(maskbytes), ops_bytes)
            (byte,) = struct.unpack('B', maskbytes[B])
            ops[i] = bool((byte >> b) & 1)
        return (protocol_version, slaw_version, ops)

    @classmethod
    def connect(cls, host, port=None, secure=None):
        if not port:
            port = DEFAULT_PORT
        sock = cls.opensocket(host, port)
        (protocol_version, slaw_version, ops) = cls.handshake(sock)
        if protocol_version == 0:
            ## version 0 pools don't support TLS
            if secure is not None and secure != 'optional':
                raise PoolNoTlsException()
            return TCPHoseV0(host, port, sock)
        return TCPHose(host, port, sock, protocol_version, slaw_version, ops, secure)

    def __init__(self, host=None, port=None, sock=None, protocol_version=None, slaw_version=None, ops=None, secure=None):
        self._host = host
        self._port = port
        self._socket = sock
        self._protocol_version = protocol_version
        self._slaw_version = slaw_version
        self._operations = ops
        self._state = HOSE_STATE_INITIAL
        self._hose_name = None
        self._encrypted = False
        self._secure = False
        #if port != DEFAULT_PORT:
        #    self._hose_name = 'tcp://%s:%d/%s' % (host, port, name)
        #else:
        #    self._hose_name = 'tcp://%s/%s' % (host, name)
        self.__buffer = ''
        if self._protocol_version == 1:
            self._flipped_wait = True
        if secure is not None:
            if secure == 'optional':
                self.starttls(True)
            else:
                self.starttls(False)
        elif not self._operation_supported(OP_PARTICIPATE) and self._operation_supported(OP_STARTTLS):
            raise PoolTlsRequiredException()

    def _wait_forever(self):
        if self._flipped_wait:
            return POOL_WAIT_FOREVER_OLD
        return POOL_WAIT_FOREVER

    def _no_wait(self):
        if self._flipped_wait:
            return POOL_NO_WAIT_OLD
        return POOL_NO_WAIT

#    def host(self):
#        return self._host
#
#    def port(self):
#        return self._port
#
#    def name(self):
#        return self._name
#
#    def slaw_version(self):
#        return self._slaw_version
#
#    def protocol_version(self):
#        return self._protocol_version

    def _operation_supported(self, id):
        if id >= len(self._operations):
            return False
        return self._operations[id]

    def _send_request_noresp(self, command, *args):
        ing = obmap({ 'op': int32(command) })
        if len(args):
            ing['args'] = oblist(args)
        p = Protein(ingests=ing)
        #print 'send: %s' % p
        data = p.to_slaw(self._slaw_version)
        self._socket.sendall(data)

    def _get_response(self):
        #fh = self._socket
        #if self.__buffer == '':
        #    ready = select.select([fh], [], [], None)
        #while True:
        #    ready = select.select([fh], [], [], 0)
        #    if fh in ready[0]:
        #        self.__buffer += self.read(8)
        #    else:
        #        break
        #xfh = cStringIO.StringIO(self.__buffer)
        #try:
        #    x = plasma.slaw.parse_slaw(self._slaw_version, xfh)
        #    self.__buffer = xfh.read()
        #except:
        #    file('/tmp/bad_data', 'w').write(self.__buffer)
        #    #print >>sys.stderr, ' '.join('%02x' % ord(x) for x in self.__buffer)
        #    self.__buffer = xfh.read()
        #    file('/tmp/bad_data.remainder', 'w').write(self.__buffer)
        #    raise
        x = plasma.slaw.parse_slaw(self._slaw_version, self)
        #print 'recv: %s' % x
        ing = x.ingests()
        return (ing['op'], ing['args'])

    def _send_request(self, command, *args):
        self._send_request_noresp(command, *args)
        return self._get_response()

    def close(self):
        pass

    def _close(self, final_command=None):
        self._socket.close()
        del(self._socket)
        self._name = None
        if final_command is not None:
            self._final_command = final_command
            self._state = HOSE_STATE_FINAL

    def _check_retort(self, retort, ok_retorts=OB_OK):
        ## remap old retorts
        key = (self._protocol_version, retort)
        if OLD_RETORTS_LOOKUP.has_key(key):
            retort = OLD_RETORTS_LOOKUP[key]
        self._last_retort = retort
        if isinstance(ok_retorts, (int, long)):
            if retort == ok_retorts:
                return True
        else:
            if retort in ok_retorts:
                return True
        raise get_retort_exception(retort)(retort=retort)

    def read(self, size):
        #print 'sock read %d' % size
        data = ''
        while size > 0:
            read_size = 8192
            if read_size > size:
                read_size = size
            read_data = self._socket.recv(read_size)
            data += read_data
            size -= len(read_data)
        return data
        #return self._socket.recv(size)

    ## --------------------- ##
    ## Creation and Disposal ##
    ## --------------------- ##

    def create(self, path, pool_type, options):
        self._check_state('create')
        if not isinstance(options, Protein):
            options = Protein(ingests=options)
        (op, args) = self._send_request(OP_CREATE, path, pool_type, options)
        self._check_retort(args[0])
        self._close('create')

    def dispose(self, path):
        self._check_state('dispose')
        (op, args) = self._send_request(OP_DISPOSE, path)
        self._check_retort(args[0])
        self._close('dispose')

    def rename(self, old_path, new_path):
        self._check_state('rename')
        (op, args) = self._send_request(OP_RENAME, old_path, new_path)
        self._check_retort(args[0])
        self._close('rename')

    def exists(self, path):
        self._check_state('exists')
        pools = self.list_pools()
        if path in pools:
            return True
        return False

    def sleep(self, path):
        self._check_state('sleep')
        (opt, args) = self._send_request(OP_SLEEP, path)
        self._check_retort(args[0])
        self._close('sleep')

    ## ---------------------------- ##
    ## Connecting and Disconnecting ##
    ## ---------------------------- ##

    def participate(self, path, options=None):
        self._check_state('participate')
        if options is None:
            options = NIL
        (op, args) = self._send_request(OP_PARTICIPATE, path, options)
        self._check_retort(args[0])
        self._set_name(path)
        self._state = HOSE_STATE_PARTICIPATE
        try:
            self._hose_index = self.newest_index()
        except PoolNoSuchProteinException:
            self._hose_index = int64(-1)

    def participate_creatingly(self, path, pool_type, create_options, participate_options=None):
        self._check_state('participate_creatingly')
        if participate_options is None:
            participate_options = NIL
        if not isinstance(create_options, Protein):
            create_options = Protein(ingests=create_options)
        (op, args) = self._send_request(OP_PARTICIPATE_CREATINGLY, path, pool_type, create_options, participate_options)
        self._check_retort(args[0], (OB_OK, POOL_CREATED))
        self._set_name(path)
        self._state = HOSE_STATE_PARTICIPATE
        try:
            self._hose_index = self.newest_index()
        except PoolNoSuchProteinException:
            self._hose_index = int64(-1)

    def withdraw(self):
        self._check_state('withdraw')
        (op, args) = self._send_request(OP_WITHDRAW)
        self._check_retort(args[0])
        self._close('withdraw')

    def starttls(self, optional=False):
        if self._encrypted:
            return True
        if not self._operation_supported(OP_STARTTLS):
            if optional:
                return False
            raise PoolNoTlsException()
        self._check_state('starttls')
        (op, args) = self._send_request(OP_STARTTLS, obmap())
        self._check_retort(args[0])
        if optional and ssl is not None:
            sock = ssl.wrap_socket(self._socket, ssl_version=ssl.PROTOCOL_TLSv1)
            self._raw_socket = self._socket
            self._socket = socket
            self._encrypted = True
        elif ssl is not None:
            cafile = None
            for d in plasma.config.ob_etc_path():
                fn = os.path.join(d, 'certificate-authorities.pem')
                if os.path.exists(fn):
                    cafile = fn
                    break
            if cafile is None:
                raise PoolTlsErrorException("no certificate-authorities.pem file found in ob_etc_path")
            try:
                sock = ssl.wrap_socket(self._socket, cert_reqs=ssl.CERT_REQUIRED, ssl_version=ssl.PROTOCOL_TLSv1, ca_certs=cafile)
                self._raw_socket = self._socket
                self._socket = sock
                self._encrypted = True
                self._secure = True
            except ssl.SSLError, e:
                raise PoolTlsErrorException('%s' % e)
        else:
            raise PoolNoTlsException("TLS support not available from your Python installation")
        (protocol_version, slaw_version, ops) = self.handshake(self._socket, True)
        self._protocol_version = protocol_version
        self._slaw_version = slaw_version
        self._operations = ops
        self._state = HOSE_STATE_INITIAL
        self._hose_name = None
        return True

    ## ------------------------- ##
    ## Pool and Hose Information ##
    ## ------------------------- ##

    def list_pools(self):
        self._check_state('list')
        (op, args) = self._send_request(OP_LIST)
        self._check_retort(args[0])
        self._close()
        return args[1] ## oblist?

    def list_ex(self, uri=None):
        if uri is None or uri == '':
            return self.list_pools()
        if not self._operation_supported(OP_LIST_EX):
            pools = oblist()
            for pool in self.list_pools():
                if pool.startswith('%s/' % uri):
                    pools.append(pool[len(uri)+1:])
            return pools
        self._check_state('list_ex')
        (op, args) = self._send_request(OP_LIST_EX, obstring(uri))
        self._check_retort(args[0])
        self._close()
        return args[1]

    def _set_name(self, path):
        self._name = path
        if self._hose_name is None:
            if self._port != DEFAULT_PORT:
                self._hose_name = '%s://%s:%d/%s' % (self._protocol_name, self._host, self._port, self._name)
            else:
                self._hose_name = '%s://%s/%s' % (self._protocol_name, self._host, self._name)

    def name(self):
        return self._name

    def get_hose_name(self):
        return self._hose_name

    def set_hose_name(self, name):
        self._send_request_noresp(OP_SET_HOSE_NAME, name, sys.argv[0], os.getpid())
        self._hose_name = name

    def get_info(self, hops=0):
        self._check_state('info')
        if hops == 0:
            return Protein(ingests={
                'type': 'tcp',
                'terminal': False,
                'host': self._host,
                'port': unt32(self._port),
                'net-pool-version': unt32(self._protocol_version),
                'slaw-version': unt32(self._slaw_version),
            })
        (op, args) = self._send_request(OP_INFO, int64(hops-1))
        self._check_retort(args[0])
        return args[1] ## protein

    def newest_index(self):
        self._check_state('newest_index')
        (op, args) = self._send_request(OP_NEWEST_INDEX)
        self._check_retort(args[1])
        return args[0] ## index

    def oldest_index(self):
        self._check_state('oldest_index')
        (op, args) = self._send_request(OP_OLDEST_INDEX)
        self._check_retort(args[1])
        return args[0] ## index

    ## ----------------------------- ##
    ## Depositing (Writing) to Pools ##
    ## ----------------------------- ##

    def deposit(self, protein):
        self._check_state('deposit')
        ret = self.deposit_ex(protein)
        return ret['index']

    def deposit_ex(self, protein):
        self._check_state('deposit_ex')
        (op, args) = self._send_request(OP_DEPOSIT, protein)
        self._check_retort(args[1])
        return obmap({ 'index':     args[0],
                       'timestamp': obtimestamp(args[2]) })

    ## ------------------ ##
    ## Reading from Pools ##
    ## ------------------ ##

    def curr(self):
        self._check_state('curr')
        if self._hose_index < 0:
            return self.nth_protein(0)
        return self.nth_protein(self._hose_index)

    def next(self, idx=None):
        self._check_state('next')
        if idx is None:
            idx = self._hose_index + 1
        if type(idx) != int64:
            idx = int64(idx)
        (op, args) = self._send_request(OP_NEXT, idx)
        self._check_retort(args[3])
        protein = args[0]
        protein.set_index(args[2])
        protein.set_timestamp(obtimestamp(args[1]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def prev(self, idx=None):
        self._check_state('prev')
        if idx is None:
            idx = self._hose_index
        if type(idx) != int64:
            idx = int64(idx)
        (op, args) = self._send_request(OP_PREV, idx)
        self._check_retort(args[3])
        protein = args[0]
        protein.set_index(args[2])
        protein.set_timestamp(obtimestamp(args[1]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein
        #return self.nth_protein(self._hose_index - 1)

    def fetch(self, ops, clamp=False):
        self._check_state('sub_fetch')
        #allowed_keys = set(('idx', 'des', 'ing', 'roff', 'rbytes'))
        tcpops = oblist()
        for op in ops:
            top = obmap({
                'idx': op.idx,
                'des': op.want_descrips,
                'ing': op.want_ingests,
                'roff': op.rude_offset,
                'rbytes': op.rude_length
            })
            tcpops.append(top)
        ops_by_idx = dict((x.idx, x) for x in ops)
        if self._operation_supported(OP_SUB_FETCH_EX):
            (op, args) = self._send_request(OP_SUB_FETCH_EX, tcpops, int64(clamp))
        else:
            (op, args) = self._send_request(OP_SUB_FETCH, tcpops)
        for x in args[0]:
            op = ops_by_idx[x['idx']]
            try:
                self._check_retort(x['retort'])
                op.ts = obtimestamp(x['time'])
                op.total_bytes = x['tbytes']
                op.descrip_bytes = x['dbytes']
                op.ingest_bytes = x['ibytes']
                op.rude_bytes = x['rbytes']
                op.num_descrips = x['ndes']
                op.num_ingests = x['ning']
                op.p = x.get('prot', None)
            except ObException:
                op.set_exception(sys.exc_info())
        return ops

    def nth_protein(self, idx):
        self._check_state('nth_protein')
        if type(idx) != int64:
            idx = int64(idx)
        (op, args) = self._send_request(OP_NTH_PROTEIN, idx)
        self._check_retort(args[2])
        protein = args[0]
        protein.set_index(idx)
        protein.set_timestamp(obtimestamp(args[1]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def index_lookup(self, timestamp, whence=TIMESTAMP_ABSOLUTE, direction=DIRECTION_ABSOLUTE):
        self._check_state('index_lookup')
        (op, args) = self._send_request(OP_INDEX_LOOKUP, timestamp, whence, direction)
        self._check_retort(args[1])
        return args[0] ## index

    def probe_back(self, search):
        self._check_state('probe_back')
        (op, args) = self._send_request(OP_PROBE_BACK, self._last_index, search)
        self._check_retort(args[3])
        protein = args[0]
        protein.set_index(args[2])
        protein.set_timestamp(obtimestamp(args[1]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def probe_frwd(self, search):
        self._check_state('probe_frwd')
        (op, args) = self._send_request(OP_PROBE_FRWD, self._hose_index, search)
        self._check_retort(args[3])
        protein = args[0]
        protein.set_index(args[2])
        protein.set_timestamp(obtimestamp(args[1]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def await_next(self, timeout=POOL_WAIT_FOREVER, idx=None):
        self._check_state('await_next')
        if idx is None:
            idx = self._hose_index + 1
        if self._operation_supported(OP_FANCY_ADD_AWAITER):
            return self._fancy_add_awaiter(timeout=timeout, idx=idx)
        if self._operation_supported(OP_MULTI_ADD_AWAITER):
            return self._multi_add_awaiter(timeout)
        if self._operation_supported(OP_AWAIT_NEXT_SINGLE):
            return self._await_next_single(timeout)
        raise PoolUnsupportedOperationException()

    def await_probe_frwd(self, search, timeout=POOL_WAIT_FOREVER, interrupt=None):
        self._check_state('await_probe_frwd')
        if self._operation_supported(OP_FANCY_ADD_AWAITER):
            return self._fancy_add_awaiter(timeout, idx=self._hose_index+1, pattern=search, interrupt=interrupt)
        if self._operation_supported(OP_MULTI_ADD_AWAITER):
            if timeout == POOL_WAIT_FOREVER:
                while True:
                    ret = self._multi_add_awaiter(POOL_NO_WAIT, interrupt=interrupt)
                    if ret.descrips.match(search):
                        return ret
            elif timeout == POOL_NO_WAIT:
                while True:
                    ret = self.next()
                    if ret.descrips.match(search):
                        return ret
            else:
                end = time.time() + timeout
                while timeout > 0:
                    ret = self._multi_add_awaiter(timeout, interrupt=interrupt)
                    if ret.descrips.match(search):
                        return ret
                    timeout = end - time.time()
                raise PoolAwaitTimedoutException()
        raise PoolUnsupportedOperationException()

    def await_nth(self, idx, timeout=POOL_WAIT_FOREVER):
        return self.await_next(timeout, idx=idx)

    def enable_wakeup(self):
        if self._wakeup_enabled:
            return True
        if self._operation_supported(OP_FANCY_ADD_AWAITER):
            self._send_request_noresp(OP_FANCY_ADD_AWAITER, self._last_index)
            (op, args) = self._get_response()
            retort = args[0]
            self._last_retort = retort
            self._wakeup_enabled = True
        elif self._operation_supported(OP_MULTI_ADD_AWAITER):
            self._send_request_noresp(OP_MULTI_ADD_AWAITER)
            self._wakeup_enabled = True
        #elif self._operation_supported(OP_AWAIT_NEXT_SINGLE):
        #    self._send_request_noresp(OP_AWAIT_NEXT_SINGLE)
        #    self._wakeup_enabled = True
        else:
            raise PoolUnsupportedOperationException()
        return self._wakeup_enabled

    def wake_up(self):
        if not self._wakeup_enabled:
            raise WakeUpException(self)
        if self._operation_supported(OP_FANCY_ADD_AWAITER):
            self._interrupt()
            self._wakeup_enabled = False
        elif self._operation_supported(OP_MULTI_ADD_AWAITER):
            self._interrupt()
            self._wakeup_enabled = False
        #elif self._operation_supported(OP_AWAIT_NEXT_SINGLE):
        #    self._wakeup_enabled = False
        else:
            raise PoolUnsupportedOperationException()

    def start_awaiter(self):
        #self.enable_wakeup()
        #return self._socket.fileno()
        self._send_request_noresp(OP_FANCY_ADD_AWAITER, self._hose_index+1, NIL)
        (op, args) = self._get_response()
        retort = args[0]
        self._check_retort(retort, (OB_OK, POOL_NO_SUCH_PROTEIN))
        self._awaiting = True
        return self._socket

    def read_awaiter(self):
        if not self._awaiting:
            return None
        (op, args) = self._get_response()
        if op == POOL_CMD_FANCY_RESULT_2:
            retort = args[0]
            self._check_retort(retort)
            (op, args) = self._get_response()
        self._awaiting = False
        protein = args[2]
        protein.set_index(args[1])
        protein.set_timestamp(obtimestamp(args[0]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def cancel_awaiter(self):
        if self._awaiting:
            self._interrupt()
            self._awaiting = False

    def _fancy_add_awaiter(self, timeout=POOL_WAIT_FOREVER, idx=None, pattern=NIL, interrupt=None):
        if idx is None:
            idx = self._hose_index+1
        self._send_request_noresp(OP_FANCY_ADD_AWAITER, idx, pattern)
        try:
            (op, args) = self._get_response()
            retort = args[0]
            #print '1. op = %d, args = %s' % (op, args)
            self._check_retort(retort, (OB_OK, POOL_NO_SUCH_PROTEIN))
            if retort == POOL_NO_SUCH_PROTEIN:
                if timeout == POOL_NO_WAIT:
                    raise PoolAwaitTimedoutException()
                if timeout != POOL_WAIT_FOREVER:
                    sfhs = [self._socket]
                    if interrupt is not None:
                        sfhs.append(interrupt)
                    ready = select.select(sfhs, [], [], timeout)
                    if self._socket not in ready[0]:
                        raise PoolAwaitTimedoutException()
                elif interrupt is not None:
                    ready = select.select([self._socket, interrupt], [], [])
                    if self._socket not in ready[0]:
                        raise PoolAwaitTimedoutException()
                (op, args) = self._get_response()
                retort = args[0]
                #print '2. op = %d, args = %s' % (op, args)
        except PoolAwaitTimedoutException:
            self._interrupt()
            raise
        self._check_retort(retort)
        (op, args) = self._get_response()
        #print '3. op = %d, args = %s' % (op, args)
        protein = args[2]
        protein.set_index(args[1])
        protein.set_timestamp(obtimestamp(args[0]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def _multi_add_awaiter(self, timeout=POOL_WAIT_FOREVER, interrupt=None):
        if timeout == POOL_NO_WAIT:
            return self.next()
        self._send_request_noresp(OP_MULTI_ADD_AWAITER)
        try:
            if timeout != POOL_WAIT_FOREVER:
                sfhs = [self._socket]
                if interrupt is not None:
                    sfhs.append(interrupt)
                ready = select.select(sfhs, [], [], timeout)
                if self._socket not in ready[0]:
                    raise PoolAwaitTimedoutException()
            elif interrupt is not None:
                ready = select.select([self._socket, interrupt], [], [])
                if self._socket not in ready[0]:
                    raise PoolAwaitTimedoutException()
            (op, args) = self._get_response()
        except PoolAwaitTimedoutException:
            self._interrupt()
            raise
        #def alarm_handler(signum, frame):
        #    raise PoolAwaitTimedoutException()
        #self._send_request_noresp(OP_MULTI_ADD_AWAITER)
        #old_alarm = signal.alarm(0)
        #old_handler = signal.signal(signal.SIGALRM, alarm_handler)
        #try:
        #    if timeout != POOL_WAIT_FOREVER:
        #        signal.alarm(timeout)
        #    (op, args) = self._get_response()
        #    signal.alarm(0)
        #except PoolAwaitTimedoutException:
        #    signal.alarm(0)
        #    self._interrupt()
        #    signal.signal(signal.SIGALRM, old_handler)
        #    signal.alarm(old_alarm)
        #    raise
        #except:
        #    signal.alarm(0)
        #    signal.signal(signal.SIGALRM, old_handler)
        #    signal.alarm(old_alarm)
        #    raise
        #signal.alarm(0)
        #signal.signal(signal.SIGALRM, old_handler)
        #signal.alarm(old_alarm)
        self._check_retort(args[0])
        protein = args[1]
        protein.set_index(args[3])
        protein.set_timestamp(obtimestamp(args[2]))
        protein.set_origin(self)
        return protein

    def _await_next_single(self, timeout=POOL_WAIT_FOREVER):
        if timeout == POOL_WAIT_FOREVER:
            timeout = self._wait_forever()
        elif timeout == POOL_NO_WAIT:
            timeout = self._no_wait()
        (op, args) = self._send_request(OP_AWAIT_NEXT_SINGLE, timeout)
        self._check_retort(args[0])
        protein = args[1]
        protein.set_index(args[3])
        protein.set_timestamp(obtimestamp(args[2]))
        protein.set_origin(self)
        self._hose_index = protein.index()
        return protein

    def _interrupt(self):
        self._send_request_noresp(OP_INFO, int64(0))
        (op, args) = self._get_response()
        (op, args) = self._get_response()

class TCPHoseV0(TCPHose):
    _protocol_version = 0
    _slaw_version = 1
    _flipped_wait = True
    _secure = False
    _encrypted = False
    _operations = [True, True, True, True,
                    True, True, True, True,
                    True, True, True, True,
                    True, False, False, True,
                    True]

    def __init__(self, host, port, sock, protocol_version=0, slaw_version=1, ops=None, secure=None):
        sock.close()
        self._state = HOSE_STATE_INITIAL
        self._host = host
        self._port = port
        self._socket = self.opensocket(host, port)

    def _send_request_noresp(self, command, *args):
        ing = obmap({ 'op': int32(command) })
        if len(args):
            ing['args'] = oblist(args)
        p = Protein(ingests=ing)
        #print 'send: %s' % p
        data = p.to_slaw(self._slaw_version)
        datalen = struct.pack('>Qs' % len(data), data)
        self._socket.sendall(datalen)

    def _get_response(self):
        size = struct.unpack('>Q', self.read(8))
        x = plasma.slaw.parse_slaw(self._slaw_version, self)
        ing = x.ingests()
        return (ing['op'], ing['args'])
