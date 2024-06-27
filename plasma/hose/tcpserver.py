import socket, math, time, os
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
from plasma.hose import Hose, COMMAND_REQUIRED_STATE
from plasma.hose.tcp import *
import logging

class TCPServer(object):

    def __init__(self, conn, addr, secure=None):
        self._socket = conn
        self._addr = addr
        self._secure = secure
        self.__last_buffer = ''
        self.__name = None
        self.__process = addr
        self.__pid = os.getpid()
        self.__op_count = list(0 for x in range(32))
        ops = list(None for x in range(32))
        allops = list(None for x in range(32))
        allops[OP_CREATE]                 = self.op_create
        allops[OP_DISPOSE]                = self.op_dispose
        allops[OP_PARTICIPATE]            = self.op_participate
        allops[OP_PARTICIPATE_CREATINGLY] = self.op_participate_creatingly
        allops[OP_WITHDRAW]               = self.op_withdraw
        allops[OP_DEPOSIT]                = self.op_deposit
        allops[OP_NTH_PROTEIN]            = self.op_nth_protein
        allops[OP_NEXT]                   = self.op_next
        allops[OP_PROBE_FRWD]             = self.op_probe_frwd
        allops[OP_NEWEST_INDEX]           = self.op_newest_index
        allops[OP_OLDEST_INDEX]           = self.op_oldest_index
        allops[OP_AWAIT_NEXT_SINGLE]      = self.op_await_next_single
        allops[OP_MULTI_ADD_AWAITER]      = self.op_multi_add_awaiter
        allops[OP_INFO]                   = self.op_info
        allops[OP_LIST]                   = self.op_list
        allops[OP_INDEX_LOOKUP]           = self.op_index_lookup
        allops[OP_PROBE_BACK]             = self.op_probe_back
        allops[OP_PREV]                   = self.op_prev
        allops[OP_FANCY_ADD_AWAITER]      = self.op_fancy_add_awaiter
        allops[OP_SET_HOSE_NAME]          = self.op_set_hose_name
        allops[OP_SUB_FETCH]              = self.op_sub_fetch
        allops[OP_RENAME]                 = self.op_rename
        #allops[OP_ADVANCE_OLDEST]         = self.op_advance_oldest
        allops[OP_SLEEP]                  = self.op_sleep
        #allops[OP_CHANGE_OPTIONS]         = self.op_change_options
        allops[OP_LIST_EX]                = self.op_list_ex
        allops[OP_SUB_FETCH_EX]           = self.op_sub_fetch_ex
        if self._secure is not None:
            if self._secure == 'optional':
                for i in range(len(allops)):
                    ops[i] = allops[i]
            elif ssl is None:
                raise PoolNoTlsException("TLS support not available from your Python installation")
            ops[OP_STARTTLS] = self.op_starttls
        else:
            for i in range(len(allops)):
                ops[i] = allops[i]
        self.ops = ops
        self.__allops = allops
        self._state = HOSE_STATE_INITIAL
        self.handshake()

    def op_counts(self):
        return self.__op_count

    def name(self):
        return self.__name

    def process(self):
        return self.__process

    def pid(self):
        return self.__pid

    def read(self, size=4096):
        (rrs, rws, res) = select.select([self._socket,], [], [])
        retval = self._socket.recv(size, socket.MSG_WAITALL)
        if len(retval) == 0:
            raise HoseDisconnectException()
        return retval

    def handshake(self, short=False):
        if not short:
            plen = struct.unpack('>Q', self.read(8))
            p = plasma.slaw.parse_slaw(1, self)
            ing = p.ingests()
            if ing['op'] == OP_DISPOSE and ing['args'][0] == '^/^/^/^':
                (pv, sv) = struct.unpack('BB', p.rude_data()[:2])
                self._protocol_version = min(pv, TCP_PROTOCOL_VERSION)
                self._slaw_version = min(sv, SLAW_VERSION)
            else:
                self._protocol_version = 0
                self._slaw_version = 1
                return self.handle(p)
        else:
            (pv, sv) = struct.unpack('BB', self._socket.read(2))
            self._protocol_version = min(pv, TCP_PROTOCOL_VERSION)
            self._slaw_version = min(sv, SLAW_VERSION)
        oplen = int(math.ceil(len(self.ops)/8.0))
        maskbytes = list((0,) * oplen)
        for i in range(len(self.ops)):
            if self.ops[i] is not None:
                n = i / 8
                maskbytes[n] |= (2**(i%8))
        opdata = struct.pack('%dB' % (len(maskbytes) + 3), pv, sv, len(maskbytes), *maskbytes)
        self._socket.sendall(opdata)
        return True

    def runloop(self):
        while True:
            if not self.runonce():
                break

    def runonce(self):
        if self._protocol_version == 0:
            size = struct.unpack('>Q', self._socket.read(8))[0]
        try:
            p = plasma.slaw.parse_slaw(self._slaw_version, self)
            self.__op_count[p.ingests()['op']] += 1
        except HoseDisconnectException:
            return False
        return self.handle(p)

    def handle(self, p):
        op = p.ingests()['op']
        if op < len(self.ops) and self.ops[op] is not None:
            cmd = self.ops[op]
            cmd_name = cmd.__name__[3:]
            try:
                self._check_state(cmd_name)
            #except HoseStateException, e:
            except(HoseStateException, e):
                return False
            args = p.ingests().get('args', [])
            return cmd(*args)
        else:
            logging.error("Client requested unsupported operation %d: %s (%s)" % (op, p, self.ops))
            return False

    def respond(self, op, *args):
        p = Protein(ingests={ 'op': int32(op), 'args': oblist(args) })
        data = p.to_slaw(self._slaw_version)
        if self._protocol_version == 0:
            data = struct.pack('>Qs', len(data), data)
        self._socket.sendall(data)

    def op_create(self, name, pool_type, options):
        try:
            Hose.create(name, pool_type, options)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_FINAL
        return False

    def op_dispose(self, name):
        try:
            Hose.dispose(name)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_FINAL
        return False

    def op_rename(self, old_name, new_name):
        try:
            Hose.rename(old_name, new_name)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_FINAL
        return False

    def op_sleep(self, name):
        try:
            Hose.sleep(name)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_FINAL
        return False

    def op_participate(self, name, options):
        try:
            self.__name = name
            self.hose = Hose.participate(name, options)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_PARTICIPATE
        return True

    def op_participate_creatingly(self, name, pool_type, options, particpate_opts):
        try:
            self.__name = name
            self.hose = Hose.participate_creatingly(name, pool_type, options, participate_options)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_PARTICIPATE
        return True

    def op_withdraw(self):
        try:
            self.hose.withdraw()
            self.hose = None
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        self._state = HOSE_STATE_FINAL
        return False

    def op_starttls(self, args):
        if ssl is None:
            e = PoolNoTlsException()
            self.respond(POOL_CMD_RESULT, int64(e.retort()), obmap())
            return False
        self.respond(POOL_CMD_RESULT, int64(OB_OK), obmap())
        sock = ssl.wrap_socket(self._socket, keyfile=self.keyfile, certfile=self.certfile, server_side=True, cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_TLSv1)
        self._raw_socket = self._socket
        self._socket = sock
        self.ops = list(x for x in self.__allops)
        self._state = HOSE_STATE_INITIAL
        self.handshake()
        return True

    def op_list(self):
        try:
            pools = Hose.list_pools()
            self.respond(POOL_CMD_RESULT, int64(OB_OK), pools)
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), oblist())
        self._state = HOSE_STATE_FINAL
        return False

    def op_list_ex(self, subdir):
        try:
            pools = Hose.list_ex(subdir)
            self.respond(POOL_CMD_RESULT, int64(OB_OK), pools)
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), oblist())
        self._state = HOSE_STATE_FINAL
        return False

    def op_set_hose_name(self, name, prog, pid):
        self.__name = name
        self.__process = process
        self.__pid = pid
        self.hose.set_hose_name('%s:%d-%s' % (name, pid, prog))
        return True

    def op_info(self, hops):
        if hops > -1:
            hops -= 1
        try:
            info = self.hose.get_info(hops)
            self.respond(POOL_CMD_RESULT, int64(OB_OK), info)
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), Protein())
        return True

    def op_change_options(self, options):
        try:
            pass
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            pass
        return True

    def op_newest_index(self):
        try:
            idx = self.hose.newest_index()
            self.respond(POOL_CMD_RESULT, idx, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, unt64(0), int64(e.retort()))
        return True

    def op_oldest_index(self):
        try:
            idx = self.hose.oldest_index()
            self.respond(POOL_CMD_RESULT, idx, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, unt64(0), int64(e.retort()))
        return True

    def op_advance_oldest(self, idx):
        try:
            self.hose.advance_oldest(idx)
            self.respond(POOL_CMD_RESULT, int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()))
        return True

    def op_deposit(self, p):
        try:
            x = self.hose.deposit_ex(p)
            self.respond(POOL_CMD_RESULT, x['index'], int64(OB_OK), x['timestamp'])
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, unt64(0), int64(e.retort()), float64(0))
        return True

    def op_next(self, idx):
        try:
            self.hose.SeekTo(idx)
            p = self.hose.next()
            self.respond(POOL_CMD_RESULT, p, p.timestamp(), p.index(), int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, Protein(), float64(0), unt64(0), int64(e.retort()))
        return True

    def op_prev(self, idx):
        try:
            self.hose.SeekTo(idx)
            p = self.hose.prev()
            self.respond(POOL_CMD_RESULT, p, p.timestamp(), p.index(), int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, Protein(), float64(0), unt64(0), int64(e.retort()))
        return True

    def op_sub_fetch(self, ops):
        return self.op_sub_fetch_ex(ops, int64(0))

    def op_sub_fetch_ex(self, ops, clamp):
        try:
            xops = oblist()
            for op in ops:
                xop = PoolFetchOp(idx=op['idx'], want_descrips=op['des'], want_ingests=op['ing'], rude_offset=op['roff'], rude_length=op['rbytes'])
                xops.append(xop)
            self.hose.fetch(xops, clamp)
            oldest = None
            newest = None
            rops = oblist()
            for xop in xops:
                rop = obmap()
                rop['idx'] = xop.idx
                if xop.exception is not None:
                    rop['retort'] = xop.exception.retort()
                else:
                    if oldest is None or xop.idx < oldest:
                        oldest = xop.idx
                    if newest is None or xop.idx > newest:
                        newest = xop.idx
                    rop['retort'] = int64(OB_OK)
                    rop['time'] = xop.ts
                    rop['tbytes'] = xop.total_bytes
                    rop['dbytes'] = xop.descrip_bytes
                    rop['ibytes'] = xop.ingest_bytes
                    rop['rbytes'] = xop.rude_bytes
                    rop['ndes'] = xop.num_descrips
                    rop['ning'] = xop.num_ingests
                    rop['prot'] = xop.p
                rops.append(rop)
            if oldest is None:
                oldest = int64(-1)
            if newest is None:
                newest = int64(-1)
            self.respond(POOL_CMD_RESULT, rops, oldest, newest)
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, oblist, int64(-1), int64(-1))
        return True

    def op_nth_protein(self, idx):
        try:
            p = self.hose.nth_protein(idx)
            self.respond(POOL_CMD_RESULT, p, p.timestamp(), int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, Protein(), float64(0), int64(e.retort()))
        return True

    def op_index_lookup(self, timestamp, whence, direction):
        try:
            idx = self.hose.index_lookup(timestamp, whence, direction)
            self.respond(POOL_CMD_RESULT, int64(OB_OK), idx)
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), unt64(0))
        return True

    def op_probe_back(self, idx, search):
        try:
            self.hose.SeekTo(idx)
            p = self.hose.probe_back(search)
            self.respond(POOL_CMD_RESULT, p, p.timestamp(), p.index(), int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, Protein(), float64(0), unt64(0), int64(e.retort()))
        return True

    def op_probe_frwd(self, idx, search):
        try:
            self.hose.SeekTo(idx)
            p = self.hose.probe_frwd(search)
            self.respond(POOL_CMD_RESULT, p, p.timestamp(), p.index(), int64(OB_OK))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, Protein(), float64(0), unt64(0), int64(e.retort()))
        return True

    def op_await_next_single(self, timeout):
        try:
            p = self.hose.await_next(timeout=timeout)
            self.respond(POOL_CMD_RESULT, int64(OB_OK), p, p.timestamp(), p.index())
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), Protein(), float64(0), unt64(0))
        return True

    def op_multi_add_awaiter(self):
        try:
            p = self.hose.await_next(interrupt=self._socket)
            self.respond(POOL_CMD_RESULT, int64(OB_OK), p, p.timestamp(), p.index())
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_RESULT, int64(e.retort()), Protein(), float64(0), unt64(0))
        return True

    def op_fancy_add_awaiter(self, idx, search):
        try:
            if search == NIL or isinstance(search, obnil):
                p = self.hose.nth_protein(idx)
            else:
                self.hose.SeekTo(idx)
                p = self.hose.probe_frwd(search)
            self.respond(POOL_CMD_FANCY_RESULT_1, int64(OB_OK), p.timestamp(), p.index())
            self.respond(POOL_CMD_FANCY_RESULT_3, p.timestamp(), p.index(), p)
        #except PoolNoSuchProteinException, e:
        except PoolNoSuchProteinException as e:
            self.respond(POOL_CMD_FANCY_RESULT_1, int64(e.retort()), float64(-1), int64(-1))
            try:
                if search == NIL or isinstance(search, obnil):
                    p = self.hose.await_nth(idx, interrupt=self._socket)
                else:
                    try:
                        self.hose.SeekTo(idx)
                    except PoolNoSuchProteinException:
                        pass
                    p = self.hose.await_probe_frwd(search, interrupt=self._socket)
                self.respond(POOL_CMD_FANCY_RESULT_2, int64(OB_OK), p.timestamp(), p.index())
                self.respond(POOL_CMD_FANCY_RESULT_3, p.timestamp(), p.index(), p)
            #except (PlasmaException, LoamException, ObErrnoException), e:
            except (PlasmaException, LoamException, ObErrnoException) as e:
                self.respond(POOL_CMD_FANCY_RESULT_2, int64(e.retort()), float64(0), unt64(0))
        #except (PlasmaException, LoamException, ObErrnoException), e:
        except (PlasmaException, LoamException, ObErrnoException) as e:
            self.respond(POOL_CMD_FANCY_RESULT_1, int64(e.retort()), float64(0), unt64(0))
        return True

    def _check_state(self, command, required_state=None):
        if required_state is None:
            required_state = COMMAND_REQUIRED_STATE[command]
        if self._state != required_state:
            if self._state == HOSE_STATE_FINAL:
                msg = '%s may not be called after %s, which must be the final command sent on a hose' % (command, self._final_command)
            elif required_state == HOSE_STATE_INITIAL:
                msg = '%s must be the first command sent on a hose' % command
            elif required_state == HOSE_STATE_PARTICIPATE:
                msg = '%s may only be called on a hose that is participating in a pool' % command
            raise HoseStateException(msg)
        return True

class HoseDisconnectException(Exception):
    def __init__(self, msg=None):
        self.__msg = msg

