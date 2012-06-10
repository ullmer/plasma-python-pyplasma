import datetime, time
from loam.obnum import unt8, int16, unt32, float64
from loam.obstr import obstring

class obtimestamp(datetime.datetime):
    """
    obtimestamps behave just time python datetime.datetime objects, but
    have a to_slaw method attached to them.  All of the usual
    datetime.datetime methods work here, and return loam objects.

    Note that this is not actually a libLoam type, and that only by
    knowing the protocol can we differentiate between obtimestamp's
    and float64's in slaw.
    """

    __timefields = {
        'year': int16,
        'month': unt8,
        'day': unt8,
        'hour': unt8,
        'minute': unt8,
        'second': unt8,
        'microsecond': unt32
    }

    def __new__(cls, *args):
        """
        If called with no arguments, returns an object representing
        the current date and time.  If called with a single numeric
        argument, returns an object representing the date and time
        as an number of seconds since Jan 1, 1970.  If called with
        a single date or datetime argument, that date is converted
        to an obtimestamp object.  When called with multiple arguments,
        those arguments are assumed to be year, month, day, hour,
        minute, second, microsecond and time zone as with the built-in
        datetime constructor.
        """
        if len(args) == 0:
            return cls(datetime.datetime.now())
        if len(args) == 1:
            if type(args[0]) == datetime.date:
                xargs = (args[0].year, args[0].month, args[0].day, 0, 0, 0)
                return super(obtimestamp, cls).__new__(cls, *xargs)
            if isinstance(args[0], datetime.datetime):
                xargs = (args[0].year, args[0].month, args[0].day,
                         args[0].hour, args[0].minute, args[0].second,
                         args[0].microsecond, args[0].tzinfo)
                return super(obtimestamp, cls).__new__(cls, *xargs)
            if isinstance(args[0], (int, float)):
                return cls(datetime.datetime.fromtimestamp(args[0]))
        return super(obtimestamp, cls).__new__(cls, *args)

    def __getattribute__(self, key):
        if key.startswith('_'):
            return super(obtimestamp, self).__getattribute__(key)
        if key not in self.__timefields:
            return super(obtimestamp, self).__getattribute__(key)
        return self.__timefields[key](super(obtimestamp, self).__getattribute__(key))

    def __add__(self, other):
        """
        Adds a number of days (which may be fractional) to this date.  
        """
        if type(other) == tuple:
            other = datetime.timedelta(*other)
        elif isinstance(other, int):
            other = datetime.timedelta(other)
        elif isinstance(other, float):
            days = int(other)
            seconds = int((other - days) * 86400)
            microseconds = int((((other - days) * 86400) % 86400) * 1E6)
            other = datetime.timedelta(days, seconds, microseconds)
        return obtimestamp(datetime.datetime.__add__(self, other))

    def __radd__(self, other):
        """
        x.__radd__(y) <==> y+x
        """
        return self + other

    def __sub__(self, other):
        """
        If subtracting a (possibly fracional) number, returns the date
        this many days prior.  If subtracting a date, returns the (possibly
        fractional) number of days between the two dates.
        """
        if type(other) == tuple:
            other = datetime.timedelta(*other)
        elif isinstance(other, int):
            other = datetime.timedelta(other)
        elif isinstance(other, float):
            days = int(other)
            seconds = int((other - days) * 86400)
            microseconds = int((((other - days) * 86400) % 86400) * 1E6)
            other = datetime.timedelta(days, seconds, microseconds)
        ret = datetime.datetime.__sub__(self, other)
        if type(ret) == datetime.timedelta:
            return float64(ret.days + ((ret.seconds + (ret.microseconds / 1E6)) / 86400.0))
        return obtimestamp(datetime.datetime.__sub__(self, other))

    def __rsub__(self, other):
        """
        x.__rsub__(y) <==> y-x
        """
        ret = datetime.datetime.__rsub__(self, other)
        if type(ret) == datetime.timedelta:
            return float64(ret.days + ((ret.seconds + (ret.microseconds / 1E6)) / 86400.0))
        return obtimestamp(datetime.datetime.__rsub__(self, other))

    def __str__(self):
        """
        obstring version of datetime.datetime.__str__
        """
        return obstring(datetime.datetime.__str__(self))

    def __repr__(self):
        """
        obstring version of datetime.datetime.__repr__
        """
        return obstring(datetime.datetime.__repr__(self))

    def astimezone(self, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.astimezone
        """
        return obtimestamp(datetime.datetime.astimezone(self, *args, **kwargs))

    def ctime(self):
        """
        obstring version of datetime.datetime.ctime
        """
        return obstring(datetime.datetime.ctime(self))

    def isoformat(self, sep='T'):
        """
        obstring version of datetime.datetime.isoformat
        """
        return obstring(datetime.datetime.isoformat(self, sep))

    def replace(self, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.replace
        """
        return obtimestamp(datetime.datetime.replace(self, *args, **kwargs))

    def strftime(self, *args, **kwargs):
        """
        obstring version of datetime.datetime.strftime
        """
        return obstring(datetime.datetime.strftime(self, *args, **kwargs))

    def weekday(self):
        """
        unt8 version of datetime.datetime.weekday
        """
        return unt8(datetime.datetime.weekday(self))

    def isoweekday(self):
        """
        unt8 version of datetime.datetime.isoweekday
        """
        return unt8(datetime.datetime.isoweekday(self))

    @classmethod
    def now(cls):
        """
        obtimestamp version of datetime.datetime.now
        """
        return obtimestamp()

    @classmethod
    def today(cls, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.today
        """
        return obtimestamp(datetime.date.today(*args, **kwargs))

    @classmethod
    def strptime(cls, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.strptime
        """
        return obtimestamp(datetime.datetime.strptime(*args, **kwargs))

    def timestamp(self):
        """
        Returns a float64 of the number of seconds since Jan 1, 1970 for
        this object.
        """
        return float64(time.mktime(self.timetuple()) + (self.microsecond / 1E6))

    @classmethod
    def fromtimestamp(self, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.fromtimestamp
        """
        return obtimestamp(datetime.datetime.fromtimestamp(*args, **kwargs))

    def toordinal(self):
        """
        int32 version of datetime.datetime.toordinal
        """
        return int32(datetime.datetime.toordinal(self))

    @classmethod
    def fromordinal(cls, *args, **kwargs):
        """
        obtimestamp version of datetime.datetime.fromordinal
        """
        return obtimestamp(datetime.datetime.fromordinal(*args, **kwargs))

    def to_slaw_v1(self, *args, **kwargs):
        """
        Since obtimestamp isn't a real loam type, this just converts
        to float64 epoch time, and returns the to_slaw value of that
        """
        return self.timestamp().to_slaw_v1(*args, **kwargs)

    def to_slaw_v2(self, *args, **kwargs):
        """
        Since obtimestamp isn't a real loam type, this just converts
        to float64 epoch time, and returns the to_slaw value of that
        """
        return self.timestamp().to_slaw_v2(*args, **kwargs)

    def to_slaw(self, *args, **kwargs):
        """
        Since obtimestamp isn't a real loam type, this just converts
        to float64 epoch time, and returns the to_slaw value of that
        """
        return self.timestamp().to_slaw(*args, **kwargs)

    def to_json(self, degrade=False):
        if degrade:
            return self.timestamp().to_json(True)
        return { 'json_class': 'obtimestamp', 'v': self.timestamp().to_json(True) }

    def to_yaml(self, indent=''):
        return self.timestamp().to_yaml(indent)

obtimestamp.max = obtimestamp(datetime.datetime.max)
obtimestamp.min = obtimestamp(datetime.datetime.min)
