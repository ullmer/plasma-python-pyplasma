import re
from loam import *
from plasma.const import *
from plasma.exceptions import *

COMMAND_REQUIRED_STATE = {
    'create':                 HOSE_STATE_INITIAL,
    'dispose':                HOSE_STATE_INITIAL,
    'rename':                 HOSE_STATE_INITIAL,
    'exists':                 HOSE_STATE_INITIAL,
    'sleep':                  HOSE_STATE_INITIAL,
    'check_in_use':           HOSE_STATE_INITIAL,
    'participate':            HOSE_STATE_INITIAL,
    'participate_creatingly': HOSE_STATE_INITIAL,
    'withdraw':               HOSE_STATE_PARTICIPATE,
    'list':                   HOSE_STATE_INITIAL,
    'list_ex':                HOSE_STATE_INITIAL,
    'info':                   HOSE_STATE_PARTICIPATE,
    'newest_index':           HOSE_STATE_PARTICIPATE,
    'oldest_index':           HOSE_STATE_PARTICIPATE,
    'deposit':                HOSE_STATE_PARTICIPATE,
    'deposit_ex':             HOSE_STATE_PARTICIPATE,
    'curr':                   HOSE_STATE_PARTICIPATE,
    'next':                   HOSE_STATE_PARTICIPATE,
    'prev':                   HOSE_STATE_PARTICIPATE,
    'sub_fetch':              HOSE_STATE_PARTICIPATE,
    'nth_protein':            HOSE_STATE_PARTICIPATE,
    'index_lookup':           HOSE_STATE_PARTICIPATE,
    'probe_back':             HOSE_STATE_PARTICIPATE,
    'probe_frwd':             HOSE_STATE_PARTICIPATE,
    'await_next':             HOSE_STATE_PARTICIPATE,
    'await_probe_frwd':       HOSE_STATE_PARTICIPATE,
    'await_next_single':      HOSE_STATE_PARTICIPATE,
    'multi_add_awaiter':      HOSE_STATE_PARTICIPATE,
    'fancy_add_awaiter':      HOSE_STATE_PARTICIPATE,
    'sub_fetch_ex':           HOSE_STATE_PARTICIPATE,
    'advance_oldest':         HOSE_STATE_PARTICIPATE,
    'starttls':               HOSE_STATE_INITIAL,
    'set_hose_name':          HOSE_STATE_PARTICIPATE,
}

class Hose(object):
    def __new__(cls, *args):
        if cls == Hose:
            return cls._get_hose_for_name(args[0])[0]
            #if args[0].startswith('tcp://'):
            #    return TCPHose(*args)
            #return MMapHose(*args)
        return super(Hose, cls).__new__(cls, *args)

    def __init__(self, name):
        self._last_retort = None
        self._is_configured = False
        self._state = HOSE_STATE_INITIAL
        pass

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

    @classmethod
    def _get_hose_for_name(cls, name):
        parsed = cls.validate_name(name)
        if parsed['transport'] == 'tcp':
            hose = TCPHose.connect(parsed['host'], parsed['port'])
        elif parsed['transport'] == 'tcps':
            hose = TCPHose.connect(parsed['host'], parsed['port'], secure='required')
        elif parsed['transport'] == 'tcpo':
            hose = TCPHose.connect(parsed['host'], parsed['port'], secure='optional')
        elif parsed['transport'] == 'local':
            hose = LocalHose()
        else:
            raise PoolPoolnameBadthException("Unknown transport %s" % parsed['transport'])
        return (hose, parsed)

    ## --------------------- ##
    ## Creation and Disposal ##
    ## --------------------- ##

    @classmethod
    def create(cls, name, pool_type, options):
        """
        Create a new pool.

        The pool_type string specifies what kind of pool you want to create,
        e.g., "mmap". This refers to the type of the pool on the host
        machine, _not_ the transport (e.g., "tcp"). Currently, the transport
        is specified by the pool hose.

        The options argument may be either an obmap or a protein, and
        describes any parameters needed to create the pool, which will vary
        by the type of pool. The option format is a protein containing
        multiple ingests, where the ingest key is the parameter to set and
        the ingest value is (unsurprisingly) the value of the parameter.
        (Or just an obmap or dict with similar key/value pairs.) For
        example, mmap pools need to know what size of pool to create, so to
        create a pool of size 1048576, you'd pass it an option protein with
        the ingest "size:1048576". If no parameters are needed, the options
        argument is None.

        Possible options are:
            +----------------+-----------------------------------+----------+
            | key            | type                              | default  |
            +================+===================================+==========+
            | resizable      | obbool                            | True     |
            +----------------+-----------------------------------+----------+
            | single-file    | obbool                            | False    |
            +----------------+-----------------------------------+----------+
            | size           | unt64 (bytes)                     | required |
            +----------------+-----------------------------------+----------+
            | index-capacity | unt64 (proteins)                  | 0        |
            +----------------+-----------------------------------+----------+
            | stop-when-full | obbool                            | False    |
            +----------------+-----------------------------------+----------+
            | frozen         | obbool                            | False    |
            +----------------+-----------------------------------+----------+
            | auto-dispose   | obbool                            | False    |
            +----------------+-----------------------------------+----------+
            | sync           | obbool                            | False    |
            +----------------+-----------------------------------+----------+
            | mode           | string (octal) or int32           | -1       |
            +----------------+-----------------------------------+----------+
            | owner          | string (username) or int32 (uid)  | -1       |
            +----------------+-----------------------------------+----------+
            | group          | string (groupname) or int32 (gid) | -1       |
            +----------------+-----------------------------------+----------+

        On success, this method returns None.  On error, it raises the
        following exceptions:

        * ObNoMemException
          (memory allocation errors)
        * PoolInvalidSizeException
          (the size specified in options is below or above limits,
          or is not coercible to an integer)
        * PoolPoolnameBadthException
          (name is ill-formed)
        * PoolTypeBadthException
          (type does not name a known pool type)
        * PoolExistsException
          (a pool with this name already exists)
        * ObErrnoException
          (system error such as a failure to acquire an OS lock)
        """
        (hose, parsed) = cls._get_hose_for_name(name)

        #print("plasma/hose/__init__: hose, parsed==", hose, parsed) 
        if hose is None:
          print("plasma/hose/__init__: hose return from cls._get_hose_for_name", name, "is None;")
          print(" optimistically punting and proceeding")
          return None #TODO: this seems unlikely to be "right," but keeping toes crossed and swashbuckling forward

        hose.create(parsed['path'], pool_type, options)
        hose.close()
        return None

    @classmethod
    def dispose(cls, name):
        """
        Destroy a pool utterly.

        Possible exceptions:

        * PoolNoSuchPoolException
          (a pool by that name pool doesn't exist)
        * PoolInUseException
          (there is still a hose open to this pool)
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        hose.dispose(parsed['path'])
        hose.close()
        return None

    @classmethod
    def rename(cls, old_name, new_name):
        """
        Rename a pool.

        Like dispose(), raises PoolInUseException if you call it while there
        are any open hoses to old_name.
        """
        (hose, old_parsed) = cls._get_hose_for_name(old_name)
        (junk, new_parsed) = cls._get_hose_for_name(new_name)
        junk.close()
        if old_parsed['transport'] != new_parsed['transport']:
            hose.close()
            raise PoolImpossibleRenameException(new_name, "must be a %s pool" % old_parsed['transport'])
        if old_parsed['host'] != new_parsed['host']:
            hose.close()
            raise PoolImpossibleRenameException(new_name, "must be on %s" % old_parsed['host'])
        hose.rename(old_parsed['path'], new_parsed['path'])
        hose.close()
        return None

    @classmethod
    def exists(cls, name):
        """
        Returns True if name exists, and returns False if name does not exist.

        (And, of course, raises an exception if an error occurs.) Beware of
        TOCTOU! In most cases, it would be more robust to just use
        participate(), because then if it does exist, you'll have a hose to
        it. With exists(), it might go away between now and when you
        participate in it.
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        retval = hose.exists(parsed['path'])
        hose.close()
        return retval

    @classmethod
    def validate_name(cls, name):
        """
        Check that a pool name is valid.  Returns True if the name is just
        fine, and raises an PoolPoolnameBadthException if the name is no good.

        Pool names may contain most printable characters, including '/',
        although '/' is special since it indicates a subdirectory, just
        like on the filesystem. Some other restrictions on pool name:

        * The total pool name must be between 1 and 100 characters in length
        * A pool name consists of one or more components, separated by
          slashes
        * A component may not be the empty string
        * A component may only contain the characters:
          "!#$%&'()+,-.0123456789;=@ ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "[]^_`abcdefghijklmnopqrstuvwxyz{}~"
          (the double quotes are not part of the legal character set)
        * A component may not begin with '.', and may not end with '.',
          ' ', or '$'
        * A component may not be any of the following names,
          case-insensitively, nor may it begin with one of these
          immediately followed by a dot: CON, PRN, AUX, NUL, COM1, COM2,
          COM3, COM4, COM5, COM6, COM7, COM8, COM9, LPT1, LPT2, LPT3,
          LPT4, LPT5, LPT6, LPT7, LPT8, and LPT9
        """
        if len(name) < 1:
            raise PoolPoolnameBadthException(name, 'must not be an empty string')
        if len(name) > 100:
            raise PoolPoolnameBadthException(name, 'must be no more than 100 characters')
        m = re.match('^([a-z0-9]+)://([\w\.\-]+)(?::(\d+))?/(.+)$', name)
        if m:
            transport = m.group(1)
            host = m.group(2)
            if m.group(3):
                port = int(m.group(3))
            else:
                port = None
            components = list(m.group(4).split('/'))
        else:
            transport = 'local'
            host = 'localhost'
            port = None
            components = list(name.split('/'))
        if transport == 'local' and components[0] == 'local:':
            local = True
            del(components[0])
        else:
            local = False
        for comp in components:
            if len(comp) == 0:
                raise PoolPoolnameBadthException(name, 'path components must not be empty strings')
            if comp.startswith('.'):
                raise PoolPoolnameBadthException(name, 'path components may not start with "."')
            if comp.endswith('.'):
                raise PoolPoolnameBadthException(name, 'path components may not end with "."')
            if comp.endswith(' '):
                raise PoolPoolnameBadthException(name, 'path components may not end with a space')
            if comp.endswith('$'):
                raise PoolPoolnameBadthException(name, 'path components may not end with a "$"')
            if re.search("[^ \\!\\#\\$\\%\\&'\\(\\)\\+,\\.0123456789;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ\\[\\]\\^_`abcdefghijklmnopqrstuvwxyz\\{\\}~\\-]", comp):
                raise PoolPoolnameBadthException(name, "path components may only contain the following characters:  !#$%&'()+,-.0123456789;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{}~")
            m = re.match('^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?', comp, re.IGNORECASE)
            if m:
                if m.group(2):
                    raise PoolPoolnameBadthException(name, 'path components may not begin with "%s."' % m.group(1))
                raise PoolPoolnameBadthException(name, 'path component may not be %s' % comp)
        if local:
            components.insert(0, 'local:')
        return {
            'transport': transport,
            'host': host,
            'port': port,
            'components': components,
            'path': '/'.join(components),
            'name': name
        }

    @classmethod
    def sleep(cls, name):
        """
        Put a pool "to sleep", which means allowing the pool implementation
        to free certain resources used by the pool, in the expectation that
        it won't be used in a while.

        A pool can only be put to sleep if there are no open hoses to it;
        a PoolInUseException will be raised if this condition is not met.
        The pool will automatically "wake up" (reacquire the resources it
        needs) the next time it is in participated in.

        In practice, in the current implementation, "resources" means
        "semaphores". This function is only useful/necessary if you intend
        to have a large number (more than 32768) of pools.
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        hose.sleep(parsed['path'])
        hose.close()
        return None

    @classmethod
    def check_in_use(cls, name):
        """
        If the named pool exists and there are currently no hoses open to it,
        returns True.

        If the named pool currently has one or more hoses open to it, raises
        a PoolInUseException.  Can also raise other exceptions, such as
        PoolNoSuchPoolException if the pool does not exist.

        Note:
          Beware of TOCTOU issues, though:
          http://cwe.mitre.org/data/definitions/367.html
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        try:
            hose.check_in_use(parsed['path'])
        except:
            hose.close()
            raise
        hose.close()
        return True

    ## ---------------------------- ##
    ## Connecting and Disconnecting ##
    ## ---------------------------- ##

    @classmethod
    def participate(cls, name, options=None):
        """
        Create a connection to a pool - a pool hose.

        The pool hose can only be used by one thread at a time, very similar
        to a file descriptor. The options slaw, which can be either an
        obmap or a protein (or can be None) describes any parameters needed
        to connect to the pool. At present, nothing is required, but in the
        future it may have uses, e.g., for authentication.

        If the connection is successful, the associated index will be set to
        its newest value, and a Hose is returned. Possible exceptions include:

        * ObNoMemException
          (memory allocation errors)
        * PoolPoolnameBadthException
          (name is not a legal pool name)
        * ObErrnoException
          (system error such as a failure to acquire an OS lock or a network
          resource)
        * PoolNoSuchPoolException
          (a pool with this name does not exist)
        * PoolCorruptException, PoolWrongVersionException,
          SlawWrongVersionException
          (the pool data does not have the expected format)

        For local pools we have also:

        * PoolInvalidSizeException
          (the size in an mmap pool's configuration is incorrect)
        * PoolInappropriateFilesystemException
          (the pool is backed by a filesystem not supported by plasma, eg. NFS)
        * PoolMMapBadthException
          (errors accessing mapped memory in mmap pools)

        And, for remote ones:

        * PoolSockBadthException, PoolServerUnreachException
          (connectivity problems)
        * PoolProtocolErrorException
          (unexpected responses from the pool server)
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        if hose is None:
           print("plasma/hose/participate:: cls._get_hose_for_name", name, "returned an empty hose")
           print("optimistically ignoring and pushing on")
           return None

        hose.participate(parsed['path'], options)
        return hose

    @classmethod
    def participate_creatingly(cls, name, pool_type, create_options, participate_options=None):
        """
        Combines create() and participate() in a single call, returning a hose
        to the newly created pool.
        """
        (hose, parsed) = cls._get_hose_for_name(name)
        hose.participate_creatingly(parsed['path'], pool_type, create_options, participate_options)
        return hose

    def withdraw(self):
        """
        Close your connection to the pool and free all resources associated
        with it.

        All (successful) calls to participate() or participate_creatingly()
        must be followed by a withdraw(), eventually.  For remote pools,
        possible exceptions are:

        * PoolSockBadthException, PoolServerUnreachException
          (connectivity problems)
        * PoolProtocolErrorException
          (unexpected responses from the pool server)
        """
        pass

    ## ------------------------- ##
    ## Pool and Hose Information ##
    ## ------------------------- ##

    @classmethod
    def list_pools(cls):
        """
        List all the pools on the local system, returning their names as an
        oblist of obstrings.
        """
        hose = LocalHose()
        retval = hose.list_pools()
        hose.close()
        return retval

    @classmethod
    def list_ex(cls, uri=None):
        """
        List all the pools under a specified URI.

        If uri is NULL, then lists all local pools under OB_POOLS_DIR
        (behaves like list()). A subset of those pools, underneath a
        specified subdirectory of OB_POOLS_DIR, can be requested with a
        uri of the form "some/dir". Pools underneath an arbitrary local
        directory can be listed with "local:/an/absolute/dir". uri should
        be a string like "tcp://chives.la923.oblong.net:1234/" if you want
        to list pools on a remote server.

        Returns an oblist of obstrings, one for each pool name.
        """
        try:
            (hose, parsed) = cls._get_hose_for_name(uri)
        except PoolPoolnameBadthException:
            (hose, parsed) = cls._get_hose_for_name('%sDUMMY' % uri)
            parsed['path'] = parsed['path'][:-5]
        retval = hose.list_ex(parsed['path'])
        hose.close()
        return retval

    def name(self):
        """
        Get the name of the pool this pool hose is connected to.
        """
        pass

    def get_hose_name(self):
        pass

    def set_hose_name(self, name):
        """
        Set the name of this hose.

        Hose names have no effect on the functioning of libPlasma, and
        exist only as a debugging aid. Hose names may be used in various
        messages, so you should set it to something that's meaningful to
        a human. Besides OB_OK on success, this method can raise an
        ObNoMemException in out of memory conditions.
        """
        pass

    def get_info(self, hops=0):
        """
        Returns a protein with information about a pool.

        Should always include an ingest "type", which is a string naming the
        pool type, and "terminal", which is a boolean which is true if this
        is a terminal pool type like "mmap", or false if this is a transport
        pool type like "tcp". For mmap pools, should include an ingest
        "size", which is an integer giving the size of the pool in bytes.
        For tcp pools, should include an ingest "host", which is a string
        naming the host the pool is on, and "port" which is an integer
        giving the port. For other pool types, ingests with other relevant
        info can be included. If "hops" is 0, means return information
        about this pool hose. If "hops" is 1, means return information
        about the pool beyond this hose (assuming this hose is a nonterminal
        type like TCP). And higher values of "hops" mean go further down
        the line, if multiple nonterminal types are chained together. If
        "hops" is -1, means return information about the terminal pool, no
        matter how far it is.
        """
        pass

    def newest_index(self):
        """
        Get the index of the newest protein in this pool.

        Raises PoolNoSuchProteinException if no proteins are in the pool.
        """
        pass

    def oldest_index(self):
        """
        Get the index of the oldest protein in this pool.

        Raises PoolNoSuchProteinException if no proteins are in the pool.
        """
        pass

    ## ----------------------------- ##
    ## Depositing (Writing) to Pools ##
    ## ----------------------------- ##

    def deposit(self, protein):
        """
        Deposit (write) a protein into this pool.

        Returns the index of the deposited protein.

        Possible exceptions:

        * PoolNotAProteinException
          (argument is not a protein)
        * ObErrnoException
          (system error such as a failure to acquire an OS lock)
        * PoolSemaphoresBadthException
          (the required locks couldn't be acquired)
        * PoolProteinBiggerThanPoolException
          (the protein won't fit in the pool)
        * PoolCorruptException
          (the pool data does not have the expected format)

        And for remote pools:

        * PoolSockBadthException, PoolServerUnreachException
          (connectivity problems)
        * PoolProtocolErrorException
          (unexpected responses from the pool server)
        """
        pass

    def deposit_ex(self, protein):
        """
        Alias for deposit() that returns an obmap with index and timestamp
        of the deposited protein.
        """
        return self.deposit(protein)

    ## ------------------ ##
    ## Reading from Pools ##
    ## ------------------ ##

    def curr(self):
        """
        Retrieve the protein at the pool hose's index.  The returned
        protein will have its timestamp and index properties set.

        This method may raise the same exceptions as next().
        """
        pass

    def next(self):
        """
        Retrieve the next available protein at or following the pool hose's
        index and advance the index to position following.

        May skip proteins since the protein immediately following the last
        read protein may have been discarded.  The protein will have its
        timestamp and index properties set.

        Possible exceptions:

        * PoolNoSuchProteinException
          (no new proteins are available.  In this case, you may be
          interested in await_next())
        * ObStompledException
          (the protein was overwritten while it was being read)
        * ObErrnoException
          (system error such as a failure to acquire an OS lock)
        * PoolCorruptException
          (the pool data does not have the expected format)

        And for remote pools:

        * PoolSockBadthException, PoolServerUnreachException
          (connectivity problems)
        * PoolProtocolErrorException
          (unexpected responses from the pool server)
        """
        pass

    def prev(self):
        """
        Retrieve the protein just previous to the pool hose's current index,
        and move the pool hose's index to this position.  The returnd
        protein will have its timetamp and index properties set.

        This method may raise the same exceptions as next().
        """
        pass

    def fetch(self, ops, clamp=False):
        """
        Fetch all or some of one or more proteins.

        The ops array, of length nops, describes the index of each protein
        to be fetched, and which parts of it should be fetched. On return,
        the ops structures will be filled out with additional information,
        such as the result (retort) of the operation, metadata about the
        protein, and the protein itself, which, depending on the options
        specified, may be a cut-down version of the actual protein in the
        pool.

        As an added bonus, if non-NULL, oldest_idx_out and newest_idx_out
        will be filled out with the oldest and newest indexes, respectively.
        If the pool is empty or an error occurs, they will be set to a
        negative number-- specifically, the retort indicating the error.
        """
        pass

    def nth_protein(self, idx):
        """
        Retrieve the protein with the given index.

        The protein will have its timestamp and index properties set.

        Possible exceptions:

        * PoolNoSuchProtein
          (the index is previous to that of the oldest index, or it is
          after the newest index)
        * ObStompled
          (the protein was overwritten while it was being read)
        * ObErrno
          (system error such as a failure to acquire an OS lock)
        * PoolCorrupt
          (the pool data does not have the expected format)

        And for remote pools:
    
        * PoolSockBadth, PoolServerUnreach
          (connectivity problems)
        * PoolProtocolError
          (unexpected responses from the pool server)
        """
        pass

    def index_lookup(self, timestamp, whence=TIMESTAMP_RELATIVE, direction=DIRECTION_ABSOLUTE):
        """
        Find the index of a protein in the pool by timestamp.  If whence is
        TIMESTAMP_ABSOLUTE or timestamp is a datetime (or obtimestamp),
        we search for a protein at that timestamp, otherwise, we search for
        a protein that many seconds in the past.  If direction is
        DIRECTION_ABSOLUTE, we'll search for the protein closest to the
        search time, if it's DIRECTION_HIGHER; we'll search for the protein
        closest to, but no earlier than, the search time; and if it's
        DIRECTION_LOWER, we'll search for the protein closest to, but no
        later than, the search time.  The index of the matched protein
        (unt64) will be returned.

        If no protein matches, this method will raise a
        PoolNoSuchProteinException.  Note that you may get the oldest or the
        newest index, depending on the value of direction.
        """
        pass

    def probe_back(self, search):
        """
        Search backward in the pool for a protein with a descrip matching
        that of the search argument.  The returned protein will have its
        timestamp and index properties set.

        If the beginning of the pool is reached without finding a match,
        a PoolNoSuchProteinException will be raised.  On success, the
        hose's current index will be set to that of the matched protein.
        """
        pass

    def probe_frwd(self, search):
        """
        Search forward in the pool for a protein with a descrip matching
        that of the search argument.

        If the end of the pool is reached without finding a match, a
        PoolNoSuchProteinException will be raised.  On success, the hose's
        currnet index will be set to one more than that of the matched
        protein.
        """
        pass

    def await_next(self, timeout=POOL_WAIT_FOREVER):
        """
        The same as next(), but wait for the next protein if none are
        available now.

        The timeout argument specifies how long to wait for a protein:
            * timeout == POOL_WAIT_FOREVER (-1): Wait forever
            * timeout == POOL_NO_WAIT (0): Don't wait at all
            * timeout > 0: only wait this many seconds.

        In addition to the exceptions raised by next(), this method may
        also raise:

        * PoolAwaitTimedoutException
          (no protein arrived before the timeout expired)
        """
        pass

    def await_probe_frwd(self, search, timeout=POOL_WAIT_FOREVER):
        """
        The same as probe_frwd(), but wait if necessary.  See await_next()
        comments for the meaning of the timeout argument,

        Note:
            timeout is overall, and does not restart when a non-matching
            protein is found.
        """
        pass

    def enable_wakeup(self):
        """
        Enable wake_up() for this hose.

        Calling this function multiple times on a hose is the same as
        calling it once on a hose. Wakeup is mediated by a system-dependent
        IPC mechanism: a failure on acquiring the necessary system resources
        is signalled by raising an ObErrnoException.
        """
        pass

    def wake_up(self):
        """
        A signal-safe and thread-safe function to interrupt any call to
        await_next() on this hose.

        For each time that this function is called, one call to await() will
        raise a PoolAwaitWokenException. (XXX That's not really true if
        enough wakeup requests pile up -- they will eventually be eaten if
        no one looks at them. See bug 771.)

        It is an error to call this function without previously having
        called enable_wakeup() on this hose: in that case, this function
        will raise a PoolWakeupNotEnabledException.
        """
        pass

    def awaiter(self):
        ## return a fileno for select()
        pass

    def is_configured(self):
        """
        A boolean giving an indication -- a best guess -- of whether this
        Hose is properly set up and participating.

        If pool construction fails, is_configured() returns false. withdraw()
        sets is_configured () to false.

        Note that in the common case of using the Pool::Participate
        functions, which return a NULL pointer on failure, there's never
        any need to call IsConfigured().
        """
        return self._is_configured

    def join_gang(self, gang):
        """
        Convenience method for HoseGang.add_hose
        """
        gang.add_hose(self)

    def leave_gang(self, gang):
        """
        Convenience method for HoseGang.remove_hose
        """
        gang.remove_hose(self)

    def IsConfigured(self):
        """
        Plasma++ alias for is_configured()
        """
        return self.is_configured()

    def last_retort(self):
        """
        The result of the most recent operation that generates an ObRetort.
        """
        return self.last_retort

    def LastRetort(self):
        """
        Plasma++ alias for last_retort()
        """
        return self.last_retort()

    def Withdraw(self):
        """
        Plasma++ alias for withdraw()
        """
        pass

    def Deposit(self, protein):
        """
        Plasma++ alias for deposit()
        """
        ## Protein
        pass

    def Next(self, timeout=POOL_WAIT_FOREVER):
        """
        Plasma++ alias for await_next()
        """
        ## pool_timestamp
        pass

    def Current(self):
        """
        Plasma++ alias for curr()
        """
        pass

    def Previous(self):
        """
        Plasma++ alias for prev()
        """
        pass

    def Nth(self, idx):
        """
        Plasma++ alias for nth_protein()
        """
        ## int64
        pass

    def ProbeForward(self, search, timeout=POOL_WAIT_FOREVER):
        """
        Plasma++ alias for probe_frwd()
        """
        ## Slaw, pool_timestamp
        pass

    def ProbeBackward(self, search):
        """
        Plasma++ alias for probe_back()
        """
        ## Slaw
        pass

    def EnableWakeup(self):
        """
        Plasma++ alias for enable_wakeup()
        """
        pass

    def WakeUp(self):
        """
        Plasma++ alias for wake_up()
        """
        pass

    def CurrentIndex(self):
        """
        Query the index pointer for this hose, and the index number of the
        oldest and newest proteins currently found in the pool.

        Upon connection to an empty pool, all of these methods would return
        0. The index sequence increases monotonically, so OldestIndex()
        will always be less than or equal to NewestIndex().
        """
        pass

    def OldestIndex(self):
        """
        Plasma++ alias for oldest_index()
        """
        pass

    def NewestIndex(self):
        """
        Plasma++ alias for newest_index()
        """
        pass

    def SeekTo(self, index):
        """
        Sets the index pointer to the given index.
        """
        ## int64
        pass

    def SeekToTime(self, timestamp, bound):
        """
        Sets the index pointer to the index given by
        index_lookup(timestamp, whence=TIMESTAMP_ABSOLUTE, direction=bound)
        """
        ## pool_timestamp, time_comparisson
        pass

    def SeekBy(self, offset):
        """
        Sets the index pointer to the relative offset from the current
        index pointer.
        """
        ## int64
        pass

    def SeekByTime(self, laps, bound):
        """
        Sets the index pointer to the index given by
        index_lookup(timestamp, whence=TIMESTAMP_RELATIVE, direction=bound)
        """
        ## pool_timestmap, time_comparisson
        pass

    def ToLast(self):
        """
        Sets the index pointer so that Current() will return the very
        newest protein in the pool.
        """
        pass

    def Runout(self):
        """
        Sets the index pointer just past the end of the pool, so that a call
        to Next() will wait, returning the protein deposited by another
        participant.
        """        
        pass

    def Rewind(self):
        """
        Sets the index pointer so that Current() will return the oldest
        protein in the pool.
        """
        pass

    def PoolName(self):
        """
        Plasma++ alias for name()
        """
        pass

    def Name(self):
        """
        Plasma++ alias for get_hose_name()
        """
        pass

    def SetName(self, name):
        """
        Plasma++ alias for set_hose_name()
        """
        ## Str
        pass

    def ResetName(self):
        pass

    def RawHose(self):
        pass

class PoolFetchOp(object):
    def __init__(self, **kwargs):
        self.idx = int64(kwargs['idx'])
        self.want_descrips = obbool(kwargs.get('want_descrips', False))
        self.want_ingests = obbool(kwargs.get('want_ingests', False))
        self.rude_offset = int64(kwargs.get('rude_offset', -1))
        self.rude_length = int64(kwargs.get('rude_length', -1))
        self.exception = None
        self.tort = None
        self.ts = None
        self.total_bytes = int64(0)
        self.descrip_bytes = int64(0)
        self.ingest_bytes = int64(0)
        self.rude_bytes = int64(0)
        self.num_descrips = int64(-1)
        self.num_ingests = int64(-1)
        self.p = None

    def set_protein(self, p, sv):
        self.tort = OB_OK
        self.ts = p.timestamp()
        self.total_bytes = int64(len(p.to_slaw(sv)))
        if p.descrips():
            self.descrip_bytes = int64(len(p.descrips().to_slaw(sv)))
            if isinstance(p.descrips(), oblist):
                self.num_descrips = int64(len(p.descrips()))
        if p.ingests():
            self.ingest_bytes = int64(len(p.ingests().to_slaw(sv)))
            if isinstance(p.ingests(), obmap):
                self.num_ingests = int64(len(p.ingests()))
        self.rude_bytes = int64(len(p.rude_data()))
        if not self.want_descrips and not self.want_ingests and self.rude_offset < 0:
            return None
        if not self.want_descrips:
            p.unset_descrips()
        if not self.want_ingests:
            p.unset_ingests()
        rd = p.rude_data
        if self.rude_offset < 0 or self.rude_offset > len(rd):
            p.unset_rude_data()
        else:
            start = self.rude_offset
            if self.rude_length < 0:
                p.set_rude_data(rd[start:])
            else:
                end = start + self.rude_length
                if end > len(rd):
                    end = len(rd)
                p.set_rude_data(rd[start:end])
        self.p = p

    def set_exception(self, e):
        self.exception = e
        if isinstance(e[1], ObException):
            self.tort = e[1].retort()

from plasma.hose.local import LocalHose
from plasma.hose.tcp import TCPHose

