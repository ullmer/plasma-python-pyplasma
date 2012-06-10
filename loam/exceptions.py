from loam.const import *
import re

class ObException(Exception):
    """
    Base exception class for loam and plasma.  In the C versions of these
    libraries, exceptions are indicated by numeric (int64) return values
    from functions, but we have the luxury of raising exceptions.  These
    exceptions should corespond one to one with the libLoam and libPlasma
    "retorts".  If you are already familiar with those implementations,
    the exception names we define here should merely be StudlyCap translations
    of the C constants, with the word "Exception" tacked onto the end.
    """

    def __init__(self, msg=None, retort=None):
        self.msg = msg
        self._retort = retort

    def __str__(self):
        try:
            return '%s(%d): %s' % (type(self).__name__, self.retort(), self.msg)
        except:
            return self.msg

    def __repr__(self):
        return self.__str__()

    def name(self):
        ex = re.sub('([A-Z])', '_\\1', type(self).__name__)[1:].upper()
        return ex.replace('_EXCEPTION', '')

class ObErrnoException(ObException):
    """
    Wrapper exception for system defined errno-style exceptions.
    """
    def __init__(self, msg='', errno=None, retort=None):
        self.msg = msg
        self.errno = errno
        self._retort = retort

    def retort(self):
        """
        Returns the numeric retort associated with this exception
        """
        if self._retort is not None:
            return self._retort
        if self.errno is None:
            return OB_UNKNOWN_ERR
        if self.errno < OB_MIN_ERRNO or self.errno > OB_MAX_ERRNO:
            return OB_UNKNOWN_ERR
        if self.errno < 64 and (OB_SHARED_ERRNOS & (1 << self.errno)):
            return -1 * (OB_RETORTS_ERRNO_SHARED + self.errno)
        system = platform.system()
        if system == 'Linux':
            return -1 * (OB_RETORTS_ERRNO_LINUX + self.errno)
        if system == 'Darwin':
            return -1 * (OB_RETORTS_ERRNO_MACOSX + self.errno)
        if system == 'Windows':
            return -1 * (OB_RETORTS_ERRNO_WINDOWS + self.errno)
        return OB_UNKNOWN_ERR

class LoamException(ObException):
    def retort(self):
        """
        Returns the numeric retort associated with this exception
        """
        if self._retort is not None:
            return self._retort
        const = re.sub('([A-Za-z])([A-Z])', '\\1_\\2', type(self).__name__[:-9]).upper()
        return globals()[const]

#---------------------------------------------------
class DataPackingException(LoamException):
    pass

class AlarmException(LoamException):
    pass

class HoseCommandException(LoamException):
    pass

class HoseStateException(LoamException):
    pass

class PackingException(LoamException):
    pass

class PoolCommandException(LoamException):
    pass

class PoolInvalidName(LoamException):
    pass

class SlawError(LoamException):
    pass

class StompledException(LoamException):
    pass

class UnsupportedPoolType(LoamException):
    pass

class VersionError(LoamException):
    pass

class WakeUpException(LoamException):
    pass

#----------------------

class ObNoMemException(LoamException):
    """
    malloc failed, or similar
    """
    pass

class ObBadIndexException(LoamException):
    """
    out-of-bounds access
    """
    pass

class ObArgumentWasNullException(LoamException):
    """
    function was not expecting a NULL argument, but it was nice enough to
    tell you instead of segfaulting.
    """
    pass

class ObNotFoundException(LoamException):
    """
    not the droids you're looking for
    """
    pass

class ObInvalidArgumentException(LoamException):
    """
    argument badness other than NULL or out-of-bounds
    """
    pass

class ObUnknownErrException(LoamException):
    """
    There was no way to determine what the error was, or the error is
    so esoteric that nobody has bothered allocating a code for it yet.
    """
    pass

class ObInadequateClassException(LoamException):
    """
    wrong parentage
    """
    pass

class ObAlreadyPresentException(LoamException):
    """
    You tried to add something that was already there.
    """
    pass

class ObEmptyException(LoamException):
    """
    There was nothing there.  (e. g. popping from an empty stack)
    """
    pass

class ObInvalidOperationException(LoamException):
    """
    You tried to do something that was not allowed.
    """
    pass

class ObDisconnectedException(LoamException):
    """
    The link to whatever-you-were-talking-to has been severed
    """
    pass

class ObVersionMismatchException(LoamException):
    """
    Illegal mixing of different versions of g-speak headers and shared libs.
    """
    pass

class SlawCorruptProteinException(LoamException):
    """
    """
    def __str__(self):
        return '%s: %s' % (type(self).__name__, self.msg)

class SlawCorruptSlawException(LoamException):
    """
    """
    pass

class SlawFabricatorBadnessException(LoamException):
    """
    """
    pass

class SlawNotNumericException(LoamException):
    """
    """
    pass

class SlawRangeErrException(LoamException):
    """
    """
    pass

class SlawUnidentifiedSlawException(LoamException):
    """
    """
    pass

class SlawWrongLengthException(LoamException):
    """
    """
    pass

class SlawNotFoundException(LoamException):
    """
    """
    pass

class SlawAliasNotSupportedException(LoamException):
    """
    """
    pass

class SlawBadTagException(LoamException):
    """
    """
    pass

class SlawEndOfFileException(LoamException):
    """
    """
    pass

class SlawParsingBadnessException(LoamException):
    """
    """
    pass

class SlawWrongFormatException(LoamException):
    """
    """
    pass

class SlawWrongVersionException(LoamException):
    """
    """
    pass

class SlawYamlErrException(LoamException):
    """
    """
    pass

class SlawNoYamlException(LoamException):
    """
    """
    pass


LOAM_RETORT_EXCEPTIONS = {
    OB_NO_MEM:            ObNoMemException,
    OB_BAD_INDEX:         ObBadIndexException,
    OB_ARGUMENT_WAS_NULL: ObArgumentWasNullException,
    OB_NOT_FOUND:         ObNotFoundException,
    OB_INVALID_ARGUMENT:  ObInvalidArgumentException,
    OB_UNKNOWN_ERR:       ObUnknownErrException,
    OB_INADEQUATE_CLASS:  ObInadequateClassException,
    OB_ALREADY_PRESENT:   ObAlreadyPresentException,
    OB_EMPTY:             ObEmptyException,
    OB_INVALID_OPERATION: ObInvalidOperationException,
    OB_DISCONNECTED:      ObDisconnectedException,
    OB_VERSION_MISMATCH:  ObVersionMismatchException,

    SLAW_CORRUPT_PROTEIN:          SlawCorruptProteinException,
    SLAW_CORRUPT_SLAW:             SlawCorruptSlawException,
    SLAW_FABRICATOR_BADNESS:       SlawFabricatorBadnessException,
    SLAW_NOT_NUMERIC:              SlawNotNumericException,
    SLAW_RANGE_ERR:                SlawRangeErrException,
    SLAW_UNIDENTIFIED_SLAW:        SlawUnidentifiedSlawException,
    SLAW_WRONG_LENGTH:             SlawWrongLengthException,
    SLAW_NOT_FOUND:                SlawNotFoundException,
    SLAW_ALIAS_NOT_SUPPORTED:      SlawAliasNotSupportedException,
    SLAW_BAD_TAG:                  SlawBadTagException,
    SLAW_END_OF_FILE:              SlawEndOfFileException,
    SLAW_PARSING_BADNESS:          SlawParsingBadnessException,
    SLAW_WRONG_FORMAT:             SlawWrongFormatException,
    SLAW_WRONG_VERSION:            SlawWrongVersionException,
    SLAW_YAML_ERR:                 SlawYamlErrException,
    SLAW_NO_YAML:                  SlawNoYamlException,
}
