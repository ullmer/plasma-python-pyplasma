import select
from plasma.const import *
from plasma.exceptions import *
from loam import *
import plasma.hose

class HoseGang(object):
    """
    HoseGangs allow you to await on multiple hoses simultaneously, without
    having to poll.
    """
    def __init__(self):
        self.__hoses = list()
        self.__last_retort = None

    def add_hose(self, hose):
        """
        Add a hose to the gang.  If the argument is a string, create a new
        hose to participate in the named pool.
        """
        if isinstance(hose, (str, unicode, obstring)):
            hose = plasma.hose.Hose.participate(hose)
        self.__hoses.append(hose)

    def remove_hose(self, hose):
        """
        Remove a hose from the gang.  If the argument is a string, remove
        the hose connected to the named pool.
        """
        if not isinstance(hose, (str, unicode, obstring)):
            hose = hose.name()
        for i in range(len(self.__hoses)):
            if self.__hoses[i].name() == hose:
                rethose = self.__hoses.pop(i)
                rethose.withdraw()
                return True
        raise PoolNotAGangMemberException()

    def withdraw(self):
        """
        Call withdraw() on all hoses, and remove them from the gang.
        """
        for hose in self.__hoses:
            hose.withdraw()
        self.__hoses = list()

    def hose_count(self):
        """
        Return the number (int64) of hoses participating in the gang.
        """
        return int64(len(self.__hoses))

    def nth_hose(self, n):
        """
        Return the nth hose in the gang.
        """
        return self.__hoses[n]

    def next(self):
        """
        Return the next available protein from the gang's hoses.  If 
        """
        for h in self.__hoses:
            try:
                return h.next()
            except PoolNoSuchProteinException:
                pass
        raise PoolNoSuchProteinException('no new proteins in gang hoses')

    def await_next(self, timeout=POOL_WAIT_FOREVER):
        awaiters = list(x.start_awaiter() for x in self.__hoses)
        try:
            if timeout == POOL_WAIT_FOREVER:
                timeout = None
            elif timeout == POOL_NO_WAIT:
                timeout = 0
            (rlist, wlist, elist) = select.select(awaiters, [], [], timeout)
            if rlist is None or len(rlist) <= 0:
                for x in self.__hoses:
                    x.cancel_awaiter()
                raise PoolAwaitTimedoutException('no new proteins arrived in gang hoses')
            ret = None
            for i in range(self.hose_count()):
                if ret is not None:
                    self.__hoses[i].cancel_awaiter()
                elif awaiters[i].fileno() == rlist[0].fileno():
                    ret = self.__hoses[i].read_awaiter()
                else:
                    self.__hoses[i].cancel_awaiter()
            return ret
        except PoolAwaitTimedoutException:
            raise
        except:
            for x in self.__hoses:
                x.cancel_awaiter()
            raise

    def Next(self, timeout=POOL_WAIT_FOREVER):
        ## pool_timestamp
        pass

    def Name(self):
        pass

    def SetName(self, name):
        ## Str
        pass

    def AppendTributary(self, hose_or_name):
        ## Hose or Str
        if type(hose_or_name) == plasma.hose.Hose:
            hose = hose_or_name
        else:
            hose = plasma.hose.Hose(hose_or_name)
        pass

    def RemoveTributary(self, hose_or_name):
        ## Hose or Str
        if type(hose_or_name) == plasma.hose.Hose:
            hose = hose_or_name
        else:
            hose = plasma.hose.Hose(hose_or_name)
        pass

    def NumTributaries(self):
        pass

    def NthTributary(self, index):
        ## int64
        pass

    def FindTributary(self, name):
        ## Str
        pass

    def WakeUp(self):
        pass

