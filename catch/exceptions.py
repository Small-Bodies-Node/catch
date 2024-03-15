# Licensed with the 3-clause BSD license.  See LICENSE for details.


class CatchException(Exception):
    pass


class InvalidSessionID(CatchException):
    pass


class InvalidSourceName(CatchException):
    pass


class DateRangeError(CatchException):
    pass


class FindObjectError(CatchException):
    pass


class EphemerisError(CatchException):
    pass


class DataSourceError(CatchException):
    pass


class DataSourceWarning(CatchException):
    pass
