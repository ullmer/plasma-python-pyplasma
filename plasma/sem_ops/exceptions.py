class ErrnoError(Exception):
    def __init__(self, errno, cmd, *args):
        self.errno = errno
        self.command = cmd
        self.args = args

    def __str__(self):
        return '%s(%d, %s, %s)' % (type(self).__name__, self.errno, self.command, self.args)

    def __repr__(self):
        return self.__str__()

class PermissionError(ErrnoError):
    pass

class ExistsError(ErrnoError):
    pass

class InvalidError(ErrnoError):
    pass

class DoesNotExistError(ErrnoError):
    pass

class NoSpaceError(ErrnoError):
    pass

class InterruptError(ErrnoError):
    pass

class RangeError(ErrnoError):
    pass

class WaitError(ErrnoError):
    pass

class UndoError(ErrnoError):
    pass

class LockingException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, self.msg)

    def __repr__(self):
        return self.__str__()

