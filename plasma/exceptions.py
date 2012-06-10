import re
from plasma.const import *
from loam.exceptions import *

class AbstractClassError(Exception):
    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        return '%s: %s' % (type(self).__name__, self.msg)

    def __repr__(self):
        return self.__str__()

class HoseStateException(Exception):
    """
    Not a libPlasma-defined exception.  This exception is raised by hose
    methods when a given method is called in an invalid sequence.  For
    example, we cannot await on a hose that is not participating in a pool.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, self.msg)

    def __repr__(self):
        return self.__str__()

class PlasmaException(ObException):
    def retort(self):
        """
        Returns the numeric retort associated with this exception
        """
        if self._retort is not None:
            return self._retort
        const = re.sub('([A-Za-z])([A-Z])', '\\1_\\2', type(self).__name__[:-9]).upper()
        return globals()[const]

class PoolNoPoolsDirException(PlasmaException):
    """
    Couldn't find a directory to put pools in
    """
    pass

class PoolFileBadthException(PlasmaException):
    """
    Some file-related op failed
    """
    pass

class PoolNullHoseException(PlasmaException):
    """
    pool_hose passed was NULL
    """
    pass

class PoolSemaphoresBadthException(PlasmaException, ObErrnoException):
    """
    Problem with semaphores
    """

    def __init__(self, msg='', errno=None, cmd=None, *args):
        self.msg = msg
        self.errno = errno
        self.command = cmd
        self.args = args

    def __str__(self):
        if self.command is not None:
            return '%s(%d/%s): %s [%s(%s)]' % (type(self).__name__, self.retort(), self.errno, self.msg, self.command, ', '.join('%s' % x for x in self.args))
        return '%s(%d/%s): %s' % (type(self).__name__, self.retort(), self.errno, self.msg)

    def __repr__(self):
        return self.__str__()

    def retort(self):
        if self.errno is None:
            return POOL_SEMAPHORES_BADTH_EXCEPTION
        if self.errno < OB_MIN_ERRNO or self.errno > OB_MAX_ERRNO:
            return POOL_SEMAPHORES_BADTH_EXCEPTION
        if self.errno < 64 and (OB_SHARED_ERRNOS & (1 << self.errno)):
            return -1 * (OB_RETORTS_ERRNO_SHARED + self.errno)
        system = platform.system()
        if system == 'Linux':
            return -1 * (OB_RETORTS_ERRNO_LINUX + self.errno)
        if system == 'Darwin':
            return -1 * (OB_RETORTS_ERRNO_MACOSX + self.errno)
        if system == 'Windows':
            return -1 * (OB_RETORTS_ERRNO_WINDOWS + self.errno)
        return POOL_SEMAPHORES_BADTH_EXCEPTION

class PoolMmapBadthException(PlasmaException):
    """
    mmap didn't work
    """
    pass

class PoolInappropriateFilesystemException(PlasmaException):
    """
    User tried to create an mmap pool on NFS
    """
    pass

class PoolInUseException(PlasmaException):
    """
    Tried to delete (or rename) a pool that was still in use
    """
    pass

class PoolTypeBadthException(PlasmaException):
    """
    Unknown pool type
    """
    pass

class PoolConfigBadthException(PlasmaException):
    """
    Pool config file problem
    """
    pass

class PoolWrongVersionException(PlasmaException):
    """
    Unexpected pool-version in config file
    """
    pass

class PoolCorruptException(PlasmaException):
    """
    Something about the pool itself is bad/invalid
    """
    pass

class PoolPoolnameBadthException(PlasmaException):
    """
    Invalid pool name
    """
    pass

class PoolImpossibleRenameException(PlasmaException):
    """
    Trying to rename a local pool to a network pool, or similar nonsense.
    """
    pass

class PoolFifoBadthException(PlasmaException):
    """
    Problem with fifos
    """
    pass

class PoolInvalidSizeException(PlasmaException):
    """
    The size specified for a pool was not a number or beyond bounds
    """
    pass

class PoolNoSuchPoolException(PlasmaException):
    """
    No pool with this name
    """
    pass

class PoolExistsException(PlasmaException):
    """
    Attempted to create existing pool.
    """
    pass

class PoolIllegalNestingException(PlasmaException):
    """
    Attempted to create pool "foo/bar" when pool "foo" exists, or vice versa.
    """
    pass

class PoolProtocolErrorException(PlasmaException):
    """
    Something unexpected happened in the network pool protocol.
    """
    pass

class PoolNoSuchProteinException(PlasmaException):
    """
    The requested protein was not available
    """
    pass

class PoolAwaitTimedoutException(PlasmaException):
    """
    Await period expired
    """
    pass

class PoolAwaitWokenException(PlasmaException):
    """
    Await cancelled by wake()
    """
    pass

class PoolWakeupNotEnabledException(PlasmaException):
    """
    Attempted to wake a hose without having previously enabled wakeup.
    """
    pass

class PoolProteinBiggerThanPoolException(PlasmaException):
    """
    Protein bigger than pool
    """
    pass

class PoolFrozenException(PlasmaException):
    """
    Tried to deposit to a "frozen" pool
    """
    pass

class PoolFullException(PlasmaException):
    """
    Tried to deposit to full pool that does not allow wrapping
    """
    pass

class PoolNotAProteinException(PlasmaException):
    """
    Tried to deposit a non-protein slaw
    """
    pass

class PoolNotAProteinOrMapException(PlasmaException):
    """
    The options slaw was not a protein or map
    """
    pass

class PoolConfWriteBadthException(PlasmaException):
    """
    Writing config file failed
    """
    pass

class PoolConfReadBadthException(PlasmaException):
    """
    Reading config file failed
    """
    pass

class PoolSendBadthException(PlasmaException):
    """
    Problem sending over network
    """
    pass

class PoolRecvBadthException(PlasmaException):
    """
    Problem reading over network
    """
    pass

class PoolSockBadthException(PlasmaException):
    """
    Problem making network socket
    """
    pass

class PoolServerBusyException(PlasmaException):
    """
    Network pool server busy
    """
    pass

class PoolServerUnreachException(PlasmaException):
    """
    Network pool server unreachable
    """
    pass

class PoolAlreadyGangMemberException(PlasmaException):
    """
    Pool hose already part of a gang
    """
    pass

class PoolNotAGangMemberException(PlasmaException):
    """
    Pool hose is not a member of a given gang
    """
    pass

class PoolEmptyGangException(PlasmaException):
    """
    pool_next_multi() called on an empty gang
    """
    pass

class PoolNullGangException(PlasmaException):
    """
    A NULL gang was passed to any of the gang functions
    """
    pass

class PoolUnsupportedOperationException(PlasmaException):
    """
    The pool type does not support what you want to do to it.
    """
    pass

class PoolInvalidatedByForkException(PlasmaException):
    """
    A hose created before a fork is no longer valid in the child.
    """
    pass

class PoolNoTlsException(PlasmaException):
    """
    Server does not support TLS
    """

class PoolTlsRequiredException(PlasmaException):
    """
    Client does not want to use TLS, but server requires it
    """

class PoolTlsErrorException(PlasmaException):
    """
    Something went wrong with TLS... not very specific
    """

class SemPermissionException(PoolSemaphoresBadthException):
    pass

class SemExistsException(PoolSemaphoresBadthException):
    pass

class SemInvalidException(PoolSemaphoresBadthException):
    pass

class SemDoesNotExistException(PoolSemaphoresBadthException):
    pass

class SemNoSpaceException(PoolSemaphoresBadthException):
    pass

class SemInterruptException(PoolSemaphoresBadthException):
    pass

class SemRangeException(PoolSemaphoresBadthException):
    pass

class SemWaitException(PoolSemaphoresBadthException):
    pass

class SemUndoException(PoolSemaphoresBadthException):
    pass

class SemTooBigException(PoolSemaphoresBadthException):
    pass


PLASMA_RETORT_EXCEPTIONS = {

    POOL_NO_POOLS_DIR:             PoolNoPoolsDirException,
    POOL_FILE_BADTH:               PoolFileBadthException,
    POOL_NULL_HOSE:                PoolNullHoseException,
    POOL_SEMAPHORES_BADTH:         PoolSemaphoresBadthException,
    POOL_MMAP_BADTH:               PoolMmapBadthException,
    POOL_INAPPROPRIATE_FILESYSTEM: PoolInappropriateFilesystemException,
    POOL_IN_USE:                   PoolInUseException,
    POOL_TYPE_BADTH:               PoolTypeBadthException,
    POOL_CONFIG_BADTH:             PoolConfigBadthException,
    POOL_WRONG_VERSION:            PoolWrongVersionException,
    POOL_CORRUPT:                  PoolCorruptException,
    POOL_POOLNAME_BADTH:           PoolPoolnameBadthException,
    POOL_IMPOSSIBLE_RENAME:        PoolImpossibleRenameException,
    POOL_FIFO_BADTH:               PoolFifoBadthException,
    POOL_INVALID_SIZE:             PoolInvalidSizeException,
    POOL_NO_SUCH_POOL:             PoolNoSuchPoolException,
    POOL_EXISTS:                   PoolExistsException,
    POOL_ILLEGAL_NESTING:          PoolIllegalNestingException,
    POOL_PROTOCOL_ERROR:           PoolProtocolErrorException,
    POOL_NO_SUCH_PROTEIN:          PoolNoSuchProteinException,
    POOL_AWAIT_TIMEDOUT:           PoolAwaitTimedoutException,
    POOL_AWAIT_WOKEN:              PoolAwaitWokenException,
    POOL_WAKEUP_NOT_ENABLED:       PoolWakeupNotEnabledException,
    POOL_PROTEIN_BIGGER_THAN_POOL: PoolProteinBiggerThanPoolException,
    POOL_FROZEN:                   PoolFrozenException,
    POOL_FULL:                     PoolFullException,
    POOL_NOT_A_PROTEIN:            PoolNotAProteinException,
    POOL_NOT_A_PROTEIN_OR_MAP:     PoolNotAProteinOrMapException,
    POOL_CONF_WRITE_BADTH:         PoolConfWriteBadthException,
    POOL_CONF_READ_BADTH:          PoolConfReadBadthException,
    POOL_SEND_BADTH:               PoolSendBadthException,
    POOL_RECV_BADTH:               PoolRecvBadthException,
    POOL_SOCK_BADTH:               PoolSockBadthException,
    POOL_SERVER_BUSY:              PoolServerBusyException,
    POOL_SERVER_UNREACH:           PoolServerUnreachException,
    POOL_ALREADY_GANG_MEMBER:      PoolAlreadyGangMemberException,
    POOL_NOT_A_GANG_MEMBER:        PoolNotAGangMemberException,
    POOL_EMPTY_GANG:               PoolEmptyGangException,
    POOL_NULL_GANG:                PoolNullGangException,
    POOL_UNSUPPORTED_OPERATION:    PoolUnsupportedOperationException,
    POOL_INVALIDATED_BY_FORK:      PoolInvalidatedByForkException,
    POOL_NO_TLS:                   PoolNoTlsException,
    POOL_TLS_REQUIRED:             PoolTlsRequiredException,
    POOL_TLS_ERROR:                PoolTlsErrorException,
}

def get_retort_exception(retort):
    """
    Returns the exception class corresponding to the numeric retort.
    TCP pools return a numeric (int64) status code that we can use to
    infer that an exception should be raised.
    """
    iret = int(retort)
    return PLASMA_RETORT_EXCEPTIONS.get(iret, LOAM_RETORT_EXCEPTIONS.get(iret, ObUnknownErrException))

