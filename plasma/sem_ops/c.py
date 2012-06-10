import ctypes, ctypes.util
from plasma.sem_ops.const import *
from plasma.exceptions import *

libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)

class sembuf(ctypes.Structure):
    _fields_ = [("sem_num", ctypes.c_ushort),
                ("sem_op", ctypes.c_short),
                ("sem_flg", ctypes.c_short)]

class _ipc_perm_new(ctypes.Structure):
    _fields_ = [("uid", uid_t),
                ("gid", gid_t),
                ("cuid", uid_t),
                ("cgid", gid_t),
                ("mode", mode_t),
                ("_seq", ctypes.c_ushort),
                ("_key", key_t)]

class semid_ds(ctypes.Structure):
    _fields_ = [("sem_perm", _ipc_perm_new),
                ("sem_base", ctypes.c_int),
                ("sem_nsems", ctypes.c_ushort),
                ("sem_otime", time_t),
                ("sem_pad1", ctypes.c_int),
                ("sem_ctime", time_t),
                ("sem_pad2", ctypes.c_int),
                ("sem_pad3", ctypes.c_int * 4)]

class semun(ctypes.Union):
    _fields_ = [("val", ctypes.c_int),
                ("buf", ctypes.POINTER(semid_ds)),
                ("array", ctypes.POINTER(ctypes.c_ushort))]

def semget(key, nsems, semflg):
    ret = libc.semget(ctypes.c_int(key), ctypes.c_int(nsems), ctypes.c_int(semflg))
    if ret == -1:
        errno = ctypes.get_errno()
        if errno == EACCES:
            raise SemPermissionException('', errno, 'semget', key, nsems, semflg)
        if errno == EEXIST:
            raise SemExistsException('', errno, 'semget', key, nsems, semflg)
        if errno == EINVAL:
            raise SemInvalidException('', errno, 'semget', key, nsems, semflg)
        if errno == ENOENT:
            raise SemDoesNotExistException('', errno, 'semget', key, nsems, semflg)
        if errno == ENOSPC:
            raise SemNoSpaceException('', errno, 'semget', key, nsems, semflg)
        if errno == EINTR:
            raise SemInterruptException('', errno, 'semget', key, nsems, semflg)
        raise PoolSemaphoresBadthException('', errno, 'semget', key, nsems, semflg)
    return ret

def semctl(semid, semnum, cmd, args=None):
    if args is not None:
        xargs = dict()
        if args.has_key('val'):
            xargs['val'] = ctypes.c_int(args['val'])
        if args.has_key('buf'):
            if type(args['buf']['sem_perm']) == dict:
                args['buf']['sem_perm'] = _ipc_perm_new(**pargs)
            buf = args['buf']
            xargs['buf'] = semid_ds(**buf)
        if args.has_key('array'):
            arr = tuple(ctypes.c_ushort(x) for x in args['array'])
            c_arr = (ctypes.c_ushort * len(arr))(*arr)
            xargs['array'] = ctypes.pointer(c_arr)
        c_args = semun(**xargs)
        ret = libc.semctl(ctypes.c_int(semid), ctypes.c_int(semnum), ctypes.c_int(cmd), c_args)
    else:
        ret = libc.semctl(ctypes.c_int(semid), ctypes.c_int(semnum), ctypes.c_int(cmd))
    if ret == -1:
        errno = ctypes.get_errno()
        if errno == EACCES:
            raise SemPermissionException('', errno, 'semctl', semid, semnum, cmd, args)
        if errno == EINVAL:
            raise SemInvalidException('', errno, 'semctl', semid, semnum, cmd, args)
        if errno == EPERM:
            raise SemPermissionException('', errno, 'semctl', semid, semnum, cmd, args)
        if errno == ERANGE:
            raise SemRangeException('', errno, 'semctl', semid, semnum, cmd, args)
        raise PoolSemaphoresBadthException('', errno, 'semctl', semid, semnum, cmd, args)
    return ret

def semop(semid, sops):
    nsops = len(sops)
    arr = tuple(sembuf(**x) for x in sops)
    c_sops = ctypes.pointer((sembuf * nsops)(*arr))
    ret = libc.semop(ctypes.c_int(semid), c_sops, ctypes.c_int(nsops))
    if ret == -1:
        errno = ctypes.get_errno()
        if errno == E2BIG:
            raise SemTooBigException('', errno, 'semop', semid, sops)
        if errno == EACCES:
            raise SemPermissionException('', errno, 'semop', semid, sops)
        if errno == EAGAIN:
            raise SemWaitException('', errno, 'semop', semid, sops)
        if errno == EFBIG:
            raise SemRangeException('', errno, 'semop', semid, sops)
        if errno == EIDRM:
            raise SemDoesNotExistException('', errno, 'semop', semid, sops)
        if errno == EINTR:
            raise SemInterruptException('', errno, 'semop', semid, sops)
        if errno == EINVAL:
            raise SemInvalidException('', errno, 'semop', semid, sops)
        if errno == ENOSPC:
            raise SemUndoException('', errno, 'semop', semid, sops)
        if errno == ERANGE:
            raise SemNoSpaceException('', errno, 'semop', semid, sops)
        raise PoolSemaphoresBadthException('', errno, 'semop', semid, sops)
    return ret


