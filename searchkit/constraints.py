import abc
import re
import uuid
import bisect

from enum import Enum
from datetime import datetime, timedelta
from functools import cached_property

from searchkit.log import log


class TimestampMatcherBase(object):
    """
    Match start of line timestamps in a standard way.

    Files containing lines starting with timestamps allow us to find a line
    that is before/after a specific time. This class is implemented to provides
    a common way to identify timestamps of varying format.
    """

    # used when converting a string to datetime.datetime
    DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, line):
        self.result = None
        for expr in self.patterns:
            ret = re.match(expr, line)
            if ret:
                self.result = ret
                break
        else:
            log.debug("failed to identify constraint datetime")

    @property
    @abc.abstractmethod
    def patterns(self):
        """
        List of regex patterns used to match a timestamp at the start of lines.

        Patterns *must* use named groups according to types i.e. year,
        month etc. See https://docs.python.org/3/library/re.html for format
        options.

        If the format of the timestamp is non-standard and the result needs
        post-processing before being used, a property with the group name
        can be added to implementations of this class and that will be used
        rather than extracting the value directly from the result.
        """

    @property
    def matched(self):
        """ Return True if a timestamp has been matched. """
        return self.result is not None

    @property
    def strptime(self):
        """
        Converts the extracted timestamp into a datetime.datetime object.

        Group names are extracted directly from the result unless an override
        property has been defined.

        @return: datetime.datetime object
        """
        vals = {}
        for key in ['day', 'month', 'year', 'hours', 'minutes', 'seconds']:
            if hasattr(self, key):
                vals[key.rstrip('s')] = int(getattr(self, key))
            else:
                vals[key.rstrip('s')] = int(self.result.group(key))

        return datetime(**vals)


class ConstraintBase(abc.ABC):

    @cached_property
    def id(self):
        """
        A unique identifier for this constraint.
        """
        return uuid.uuid4()

    @abc.abstractmethod
    def apply_to_line(self, line):
        """
        Apply constraint to a single line.

        @param fd: file descriptor
        """

    @abc.abstractmethod
    def apply_to_file(self, fd):
        """
        Apply constraint to an entire file.

        @param fd: file descriptor
        """

    @abc.abstractmethod
    def stats(self):
        """ provide runtime stats for this object. """

    @abc.abstractmethod
    def __repr__(self):
        """ provide string repr of this object. """


class BinarySeekSearchBase(ConstraintBase):
    """
    Provides a way to seek to a point in a file using a binary search and a
    given condition.
    """

    def __init__(self, allow_constraints_for_unverifiable_logs=True):
        self.allow_unverifiable_logs = allow_constraints_for_unverifiable_logs

    @abc.abstractmethod
    def extracted_datetime(self, line):
        """
        Extract timestamp from start of line.

        @param line: text line to extract a datetime from.
        @return: datetime.datetime object or None
        """

    @abc.abstractproperty
    def _since_date(self):
        """ A datetime.datetime object representing the "since" date/time """

    def _line_date_is_valid(self, extracted_datetime):
        """
        Validate if the given line falls within the provided constraint. In
        this case that's whether it has a datetime that is >= to the "since"
        date.
        """
        ts = extracted_datetime
        if ts is None:
            # log.info("s:%s: failed to extract datetime from "
            #          "using expressions %s - assuming line is not valid",
            #          unique_search_id, ', '.join(self.exprs))
            return False

        if ts < self._since_date:
            # log.debug("%s < %s at (%s) i.e. False", ts, self._since_date,
            #           line[-3:].strip())
            return False

        # log.debug("%s >= %s at (%s) i.e. True", ts, self._since_date,
        #           line[-3:].strip())

        return True


class ValidLinesNotFound(Exception):
    """Raised when a log file contains proper timestamps but
    no log lines after the since date."""


class ValidFormattedDateNotFound(Exception):
    """Raised when a log file does not contain any line with
    date suitable to specified date format"""


class DateNotFoundInLine(Exception):
    """Raised when searcher has encountered a line with no date
    and performed forward-backward searches, but still yet, could
    not found a line with date."""


class InvalidSearchState(Exception):
    """Raised when a variable dependent on another variable (e.g.
    the variable x only has value when y is True) is accessed without
    checking the prerequisite variable."""


class FindTokenStatus(Enum):
    FOUND = 1,
    REACHED_EOF = 2,
    FAILED = 3


class SearchState(object):
    def __init__(self, status: FindTokenStatus, offset=0):
        """
        @param status: current status of search
        @param offset: current position in file from which next search will be
                       started.
        """
        self._status = status
        self._offset = offset

    @property
    def status(self):
        return self._status

    @property
    def offset(self):
        if self.status == FindTokenStatus.FAILED:
            raise InvalidSearchState()

        return self._offset


class NonDestructiveFileRead(object):
    """ Context manager class that saves current position at start and restores
        once finished. """
    def __init__(self, file):
        self.file = file
        self.original_position = file.tell()

    def __enter__(self):
        return self.file

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.file.seek(self.original_position)


class LogLine(object):
    """Class representing a line in a log file.

    Keeps the start/end offsets of the line. Line content is lazy-loaded on
    demand by calling the `text` method.
    """

    MAX_DATETIME_READ_BYTES = 64

    def __init__(self, file, constraint, line_start_lf, line_end_lf):
        assert line_start_lf
        assert line_end_lf
        assert line_start_lf.offset is not None
        assert line_end_lf.offset is not None
        self._file = file
        self._constraint = constraint
        self._line_start_lf = line_start_lf
        self._line_end_lf = line_end_lf

    def __len__(self):
        return (self.end_offset - self.start_offset) + 1

    @property
    def start_offset(self):
        """Offset of the log line's first character (excluding \n)"""
        # Depending on whether we found the start line feed
        # or not, we discard one character at the beginning
        # (being the \n)
        if self.start_lf.status == FindTokenStatus.FOUND:
            return self.start_lf.offset + 1

        return self.start_lf.offset

    @property
    def end_offset(self):
        """Offset of the log line's last character (excluding \n)"""
        # Depending on whether we found the end line feed
        # or not, we discard one character at the end (being
        # the \n)
        if self.end_lf.status == FindTokenStatus.FOUND:
            return self.end_lf.offset - 1

        return self.end_lf.offset

    @property
    def start_lf(self):
        """Offset of the log line's starting line feed."""
        return self._line_start_lf

    @property
    def end_lf(self):
        """Offset of the log line's ending line feed."""
        return self._line_end_lf

    @property
    def date(self):
        """Extract the date from the log line, if any.

        The function will use extracted_datetime function to parse
        the date/time.

        @return: datetime: if `text` contains a valid datetime otherwise None.
        """
        return self._constraint.extracted_datetime(
            self._read_line(self.MAX_DATETIME_READ_BYTES)
        )

    @property
    def text(self):
        return self._read_line(max_len=len(self))

    def _read_line(self, max_len):
        """Retrieve the line text.

        This function seeks to the line start and reads the line content
        on demand. The function will revert the file offset back after reading
        to where it was before.

        @return: the line text string
        """
        with NonDestructiveFileRead(self._file) as f:
            f.seek(self.start_offset)
            line_text = f.read(max_len)
            return line_text


class LogFileDateSinceSeeker(object):
    r"""Performs "since" date lookups with file offsets. This is
    useful for performing line-based binary date searches on a log file.

    Implements __len__ and __getitem__ methods in order to behave like a list.
    When __getitem__ is called with an offset the algorithm locates the
    rightmost and leftmost line feed '\n' to form a line. For example with the
    following file contents:

    13:15 AAAAAA\n13:16 BBBBBBB\n13:17 CCCCCC

    and assuming __getitem__ is called with offset 19 i.e.

    13:15 AAAAAA\n13:16 BBBBBBB\n13:17 CCCCCC
                        ^19

    The algorithm will first read SEEK_HORIZON bytes forward, starting
    from offset `19`, and then try to find the first line feed:

    13:15 AAAAAA\n13:16 BBBBBBB\n13:17 CCCCCC
                        ^19     ^r-lf

    Then the algorithm will seek SEEK_HORIZON bytes backward, starting from
    offset 19, read SEEK_HORIZON bytes and then try to find the first line feed
    scanning in reverse:

    13:15 AAAAAA\n13:16 BBBBBBB\n13:17 CCCCCC
                ^l-lf   ^19     ^r-lf

    Then, the algorithm will extract the characters between l-lf and r-lf
    to form a line. The line will be checked against the date matcher
    to extract the date. If the date matcher yields a valid date, __getitem__
    will return that date. Otherwise, the search will be extended to other
    nearby lines, prioritising the lines prior to the current, until either of
    the following is true:

        - a line with a timestamp is found
        - MAX_*_FALLBACK_LINES has been reached
    """

    # Number of characters to read while searching
    SEEK_HORIZON = 256

    # How many times we can expand the search horizon while trying to find a
    # line feed. This means the search will read SEEK_HORIZON times
    # MAX_SEEK_HORIZON_EXPAND bytes in total when a line feed character is not
    # found.
    MAX_SEEK_HORIZON_EXPAND = 4096

    # Number of lines to search forwards when the algorithm encounters lines
    # with no date.
    MAX_TRY_FIND_WITH_DATE_ATTEMPTS = 500

    LINE_FEED_TOKEN = b'\n'

    def __init__(self, fd, c):
        self.file = fd
        self.constraint = c
        self.line_info = None
        self.found_any_date = False
        self.lookup_times = 0
        with NonDestructiveFileRead(self.file) as f:
            self.length = f.seek(0, 2)

    def find_token_reverse(self, start_offset):
        r"""Find `token` in `file` starting from `start_offset` and backing off
        `LogFileDateSinceSeeker.SEEK_HORIZON` bytes on each iteration for
        maximum of `LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND` times.

        @param start_offset (int): start offset of search

        @return: SearchState object
        """
        attempts = LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND
        current_offset = -LogFileDateSinceSeeker.SEEK_HORIZON
        while True:
            attempts -= 1
            read_offset = start_offset + current_offset
            read_offset = read_offset if read_offset > 0 else 0
            read_size = LogFileDateSinceSeeker.SEEK_HORIZON
            if start_offset + current_offset <= 0:
                read_size = read_size + (start_offset + current_offset)

            self.file.seek(read_offset)
            chunk = self.file.read(read_size)
            if not chunk or len(chunk) == 0:
                # We've reached the start of the file and could not find the
                # token.
                return SearchState(status=FindTokenStatus.REACHED_EOF,
                                   offset=0)

            chunk_offset = chunk.rfind(self.LINE_FEED_TOKEN)

            if chunk_offset != -1:
                return SearchState(status=FindTokenStatus.FOUND,
                                   offset=read_offset + chunk_offset)

            if attempts <= 0:
                break

            current_offset = current_offset - len(chunk)
            if (start_offset + current_offset) < 0:
                return SearchState(status=FindTokenStatus.REACHED_EOF,
                                   offset=0)
        log.debug("reached max line length search without finding a line feed")
        return SearchState(FindTokenStatus.FAILED)

    def find_token(self, start_offset):
        r"""Find `token` in `file` starting from `start_offset` and moving
        forward `LogFileDateSinceSeeker.SEEK_HORIZON` bytes on each
        iteration for maximum of `LogFileDateSinceSeeker.MAX_SEEK_HORIZON_
        EXPAND` times.

        @param start_offset (int): start offset of search

        @return: SearchState object
        """
        attempts = LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND
        current_offset = 0
        # Seek to the initial starting position
        self.file.seek(start_offset)
        while attempts > 0:
            # Read `horizon` bytes from the file.
            chunk = self.file.read(LogFileDateSinceSeeker.SEEK_HORIZON)

            if not chunk or len(chunk) == 0:
                # Reached end of file
                return SearchState(status=FindTokenStatus.REACHED_EOF,
                                   offset=len(self))

            chunk_offset = chunk.find(self.LINE_FEED_TOKEN)
            if chunk_offset != -1:
                # We've found the token in the chunk.
                # As the chunk_offset is a relative offset to the chunk
                # translate it to file offset while returning.
                return SearchState(status=FindTokenStatus.FOUND,
                                   offset=(start_offset + current_offset +
                                           chunk_offset))
            # We failed to find the token in the chunk.
            # Progress the current offset forward by
            # chunk's length.
            current_offset = current_offset + len(chunk)
            attempts -= 1
        # Exhausted all the attempts and found nothing.
        log.debug("reached max line length search without finding a line feed")
        return SearchState(FindTokenStatus.FAILED)

    def try_find_line(self, epicenter, slf_off=None, elf_off=None):
        r"""Try to find a line at `epicenter`. This function allows extracting
        the corresponding line from a file offset. "Line" is a string between
        two line feed characters i.e.;

        - \nThis is a line\n

        ... except when the line starts at the start of the file or ends at the
        end of the file, where SOF/EOF are also accepted as line start/end:

        - This is line at SOF\n
        - \nThis is a line at EOF

        Assume that we have the following file content:

        11.01.2023 fine\n11.01.2023 a line\n11.01.2023 another line

        We have a file with three lines in above, and the offsets for these
        lines would be:

        -----------------------------------------------------
        Line  |                Line               | Line    |
        Nr.   |                Text               | Offsets |
        -----------------------------------------------------
        #0:   | "11.01.2023 fine"                 | [0,14]  |
        #1:   | "11.01.2023 a line"               | [16,33] |
        #2:   | "11.01.2023 another line"         | [35,58] |

        The function `try_find_line_w_date` would return the following for
        the calls:

        (0)  -> (11.01.2023,0-14)
        (7)  -> (11.01.2023,0-14)
        (18) -> (11.01.2023,16-33)
        (47) -> (11.01.2023,35-58)

        This function will try to locate first line feed characters from both
        left and right side of the position `epicenter`. Assume the file
        content we have above, and we want to extract the line at offset `18`,
        which corresponds to the first dot `.` of date of the line #1:

        11.01.2023 fine\n11.01.2023 a line\n11.01.2023 another line
                           ^epicenter

        The function will try to form a line by first searching for the first
        line feed character in the left (slf) (if slf_off is None):

        11.01.2023 fine\n11.01.2023 a line\n11.01.2023 another line
                       ^slf^epicenter

        and then the same for the right (elf) (if elf_off is None):

        11.01.2023 fine\n11.01.2023 a line\n11.01.2023 another line
                       ^slf^epicenter     ^elf

        The function will then extract the string between the `slf` and `elf`,
        which yields the string "11.01.2023 a line".

        The function will either return a valid LogLine object, or raise an
        exception.

        @param epicenter: Search start offset
        @param slf_off: Optional starting line feed offset, if known. Defaults
                        to None.
        @param elf_off: Optional ending line feed offset, if known. Defaults to
                        None.

        @raise ValueError: when ending line feed offset could not be found or
                           when starting line feed offset could not be found.

        @return: found logline
        """
        log.debug("    > EPICENTER: %d", epicenter)

        # Find the first LF token from the right of the epicenter
        # e.g. \nThis is a line\n
        #         ^epicenter    ^line end lf
        line_end_lf = self.find_token(
            epicenter
        ) if elf_off is None else SearchState(
            FindTokenStatus.FOUND, elf_off)

        if line_end_lf.status == FindTokenStatus.FAILED:
            raise ValueError("Could not find ending line feed "
                             f"offset at epicenter {epicenter}")

        # Find the first LF token from the left of the epicenter
        # e.g.          \nThis is a line\n
        # line start lf  ^ ^epicenter
        line_start_lf = self.find_token_reverse(
            epicenter
        ) if slf_off is None else SearchState(
            FindTokenStatus.FOUND, slf_off)

        if line_start_lf.status == FindTokenStatus.FAILED:
            raise ValueError("Could not find start line feed "
                             f"offset at epicenter {epicenter}")

        # Ensure that found offsets are in file range
        assert line_start_lf.offset <= len(self)
        assert line_start_lf.offset >= 0
        assert line_end_lf.offset <= len(self)
        assert line_end_lf.offset >= 0
        # Ensure that end lf offset is >= start lf offset
        assert line_end_lf.offset >= line_start_lf.offset

        return LogLine(file=self.file, constraint=self.constraint,
                       line_start_lf=line_start_lf, line_end_lf=line_end_lf)

    def try_find_line_with_date(self, start_offset, line_feed_offset=None,
                                forwards=True):
        r"""Try to fetch a line with date, starting from `start_offset`.

        The algorithm will try to fetch a new line searching for a valid date
        for a maximum of `attempts` times. The lines will be fetched from
        either prior or after `start_offset`, depending on the value of the
        `forwards` parameter.

        If `prev_offset` parameter is used, the value will be used as either
        fwd_line_feed or rwd_line_feed position depending on the value of the
        `forwards` parameter.

        @param start_offset: Where to begin searching
        @param line_feed_offset: Offset of the fwd_line_feed, or rwd_line_feed
                                 if known. Defaults to None.
        @param forwards: Search forwards, or backwards. Defaults to True
                         (forwards).

        @return: line if found otherwise None.
        """
        attempts = LogFileDateSinceSeeker.MAX_TRY_FIND_WITH_DATE_ATTEMPTS
        offset = start_offset
        log_line = None
        while attempts > 0:
            log_line = self.try_find_line(
                offset,
                (None, line_feed_offset)[forwards],
                (None, line_feed_offset)[not forwards]
            )

            log.debug(
                "    > TRY_FETCH, REMAINING_ATTEMPTS:%d, START_LF_OFFSET: %d, "
                "END_LF_OFFSET: %d >: on line -> %s",
                attempts,
                log_line.start_lf.offset,
                log_line.end_lf.offset,
                log_line.text,
            )

            # If the line has a valid date, return it.
            if log_line.date:
                return log_line

            # Set offset of the found line feed
            line_feed_offset = (log_line.start_lf,
                                log_line.end_lf)[forwards].offset
            # Set the next search starting point
            offset = line_feed_offset + (-1, +1)[forwards]
            if offset < 0 or offset > len(self):
                log.debug("    > TRY_FETCH EXIT EOF/SOF")
                break

            attempts -= 1
        return None

    def __len__(self):
        return self.length

    def __getitem__(self, offset):
        r"""Find the nearest line with a date at `offset` and return its date.

        To illustrate how this function works, let's assume that we have a file
        with the contents as follows:

        line\n01.01.1970 line\nthisisaline\nthisisline2\n01.01.1970 thisisline3

        -----------------------------------------------------------------------
        For a lookup at offset `13`:

        line\n01.01.1970 line\nthisisaline\nthisisline2\n01.01.1970 thisisline3
                      ^ offset(13)
              |--------------|
                             ^try_find_line_with_date(bwd, itr #1)

        The function will first call ^try_find_line_with_date, which will yield
        `01.01.1970 line` in turn. As the line contains a date, the return
        value will be datetime("01.01.1970")
        -----------------------------------------------------------------------
        For a lookup at offset `25`:

        line\n01.01.1970 line\nthisisaline\nthisisline2\n01.01.1970 thisisline3
                                   ^ offset(25)
                               |---------|
                                         ^try_find_line_with_date(bwd, itr #1)
              |--------------|
                             ^try_find_line_with_date(bwd, itr #2)

        The function will first call try_find_line, which will yield
        `thisisaline` in turn. As the line does not contain a valid date, the
        function will perform a backwards search to find first line that
        contains a date by caling `try_find_line_with_date`, which will yield
        the line
        "01.01.1970 line", which contains a valid date, and the return value
        will be datetime("01.01.1970").
        -----------------------------------------------------------------------
        For a lookup at offset `35`:

        line\n01.01.1970 line\nthisisaline\nthisisline2\n01.01.1970 thisisline3
                                              ^ offset(35)
                                            |---------|
                                                      ^try_find_line_with_date
                                                       (bwd,itr #1)
                               |---------|
                                         ^try_find_line_with_date(bwd, itr #2)
              |--------------|
                             ^try_find_line_with_date(bwd, itr #3)

        The function will first call try_find_line, which will yield
        `thisisaline2` in turn. As the line does not contain a valid
        date, the function will perform a backwards search to find first
        line that contains a date by caling `try_find_line_with_date`,
        which will yield the line "01.01.1970 line", (notice that it'll
        skip line `thisisaline`) which contains a valid date, and the return
        value will be datetime("01.01.1970").
        -----------------------------------------------------------------------
        For a lookup at offset `3`:

        line\n01.01.1970 line\nthisisaline\nthisisline2\n01.01.1970 thisisline3
           ^ offset(3)
        |--|
           ^try_find_line_with_date(bwd, itr #1)
              |-------------|
                            ^try_find_line_with_date(fwd, itr #1)

        The function will first call try_find_line_with_date, which will yield
        `line` in turn. As the line does not contain a valid date, the
        function will perform a backwards search to find first line that
        contains a date by caling `try_find_line_with_date`, which will
        fail since there'is no more lines before the line. The function
        will then perform a forwards search to find first line with a date
        by calling `try_find_line_with_date` in forwards search mode, which
        will yield `01.01.1970 line` in turn,  which contains a valid date,
        and the return value will be datetime("01.01.1970").

        This function is intended to be used with bisect.bisect*
        functions, so therefore it only returns the `date` for
        the comparison.

        @param offset: integer lookup offset
        @raise DateNotFoundInLine: When a line with a date could not
                                   be found.
        @return: Date of the line at `offset`
        """

        self.lookup_times += 1
        log.debug("-------------------------------------------")
        log.debug("-------------------------------------------")
        log.debug("-------------------------------------------")
        log.debug("-------------------------------------------")
        log.debug("LOOKUP (#%d) AT OFFSET: %d", self.lookup_times, offset)
        result = None

        # Try to search backwards first.
        # First call will effectively be a regular forward search given
        # that we're not passing a line feed offset to the function.
        # Any subsequent search attempt will be made backwards.
        log.debug("######### BACKWARDS SEARCH START #########")
        result = self.try_find_line_with_date(
            offset,
            None,
            False,
        )
        log.debug("######### BACKWARDS SEARCH END #########")

        # ... then, forwards.
        if not result or result.date is None:
            log.debug("######### FORWARDS SEARCH START #########")
            result = self.try_find_line_with_date(offset + 1, offset, True)

            log.debug("######### FORWARDS SEARCH END #########")

        if not result or result.date is None:
            raise DateNotFoundInLine(
                f"Date search failed at offset `{offset}`")

        # This is mostly for diagnostics. If we could not find
        # any valid date in given file, we throw a specific exception
        # to indicate that.

        self.found_any_date = True
        if result.date >= self.constraint._since_date:
            # Keep the matching line so we can access it
            # after the bisect without having to perform another
            # lookup.
            self.line_info = result

        log.debug(
            "    > EXTRACTED_DATE: `%s` >= SINCE DATE: `%s` == %s",
            result.date,
            self.constraint._since_date,
            (result.date >= self.constraint._since_date)
            if result.date else False,
        )
        return result.date

    def run(self):
        # bisect_left will give us the first occurenct of the date
        # that satisfies the constraint.
        # Similarly, bisect_right would allow the last occurence of
        # a date that satisfies the criteria.

        try:
            bisect.bisect_left(self, self.constraint._since_date)
        except DateNotFoundInLine as exc:
            if not self.found_any_date:
                raise ValidFormattedDateNotFound from exc

            raise

        if not self.line_info:
            raise ValidLinesNotFound

        log.debug(
            "RUN END, FOUND LINE(START:%d, END:%d, CONTENT:%s)"
            " IN %d LOOKUP(S)",
            self.line_info.start_offset,
            self.line_info.end_offset,
            self.line_info.text,
            self.lookup_times
        )

        return (self.line_info.start_offset, self.line_info.end_offset)


class SearchConstraintSearchSince(BinarySeekSearchBase):

    def __init__(self, current_date, cache_path, exprs=None,
                 ts_matcher_cls=None, days=0, hours=24, **kwargs):
        """
        A search expression is provided that allows us to identify a datetime
        on each line and check whether it is within a given time period. The
        time period used defaults to 24 hours if use_all_logs is false, 7 days
        if it is true and max_logrotate_depth is default otherwise whatever
        value provided. This can be overridden by providing a specific number
        of hours.

        @param current_date: cli.date(format="+{}".format(self.date_format))
        @param cache_path: path to location where we can create an MPCache
        @param exprs: [DEPRECATED] a list of search/regex expressions used to
                      identify a date/time in. This is deprecated, use
                      ts_matcher_cls.
        @param ts_matcher_cls: TimestampMatcherBase implementation used to
                               match timestamps at start if lines.
        @param days: override default period with number of days
        @param hours: override default period with number of hours
        """
        super().__init__(**kwargs)
        self.cache_path = cache_path
        self.ts_matcher_cls = ts_matcher_cls
        if ts_matcher_cls:
            self.date_format = ts_matcher_cls.DEFAULT_DATETIME_FORMAT
        else:
            log.warning("using patterns to identify timestamp is deprecated - "
                        "use ts_matcher_cls instead")
            self.date_format = TimestampMatcherBase.DEFAULT_DATETIME_FORMAT

        self.current_date = datetime.strptime(current_date, self.date_format)
        self._line_pass = 0
        self._line_fail = 0
        self.exprs = exprs
        self.days = days
        if days:
            self.hours = 0
        else:
            self.hours = hours

        self._results = {}

    def extracted_datetime(self, line):
        if type(line) == bytes:
            # need this for e.g. gzipped files
            line = line.decode("utf-8", errors='backslashreplace')

        if self.ts_matcher_cls:
            timestamp = self.ts_matcher_cls(line)
            if timestamp.matched:
                return timestamp.strptime

            return

        # NOTE: the following code can be removed once we remove the deprecated
        # exprs arg from this class.
        log.debug("using patterns to identify timestamp is deprecated - "
                  "use ts_matcher_cls instead")
        for expr in self.exprs:
            # log.debug("attempting to extract from line using expr '%s'",
            #           expr)
            ret = re.search(expr, line)
            if ret:
                # log.debug("expr '%s' successful", expr)
                break

        if not ret:
            # log.info("all exprs unsuccessful: %s", self.exprs)
            return

        str_date = ""
        for g in ret.groups():
            str_date += "{} ".format(g)

        str_date = str_date.strip()
        try:
            return datetime.strptime(str_date, self.date_format)
        except ValueError:
            # this can happen if the line is incomplete or does not contain a
            # timestamp.
            log.exception("")

    @property
    def _is_valid(self):
        return self._since_date is not None

    @cached_property
    def _since_date(self):  # pylint: disable=W0236
        """
        Reflects the date from which we will start to apply searches.
        """
        if not self.current_date:
            return

        return self.current_date - timedelta(days=self.days,
                                             hours=self.hours or 0)

    def apply_to_line(self, line):
        if not self._is_valid:
            log.warning("c:%s unable to apply constraint to line", self.id)
            self._line_pass += 1
            return True

        extracted_datetime = self.extracted_datetime(line)
        if not extracted_datetime:
            self._line_pass += 1
            return True

        ret = self._line_date_is_valid(extracted_datetime)
        if ret:
            self._line_pass += 1
        else:
            self._line_fail += 1

        return ret

    def apply_to_file(self, fd, destructive=True):
        if not self._is_valid:
            log.warning("c:%s unable to apply constraint to %s", self.id,
                        fd.name)
            return

        if fd.name in self._results:
            log.debug("ret cached")
            return self._results[fd.name]

        log.debug("c:%s: starting binary seek search to %s in file %s "
                  "(destructive=True)", self.id, self._since_date, fd.name)
        try:
            seeker = LogFileDateSinceSeeker(fd, self)
            result = seeker.run()
            fd.seek(result[0] if result and destructive else 0)

            if not result or result[0] == len(seeker):
                self._results[fd.name] = None
            else:
                self._results[fd.name] = result[0]
        except ValidFormattedDateNotFound:
            log.debug("c:%s No timestamp found in file", self.id)
            fd.seek(0)
            return fd.tell()
        except ValidLinesNotFound:
            log.debug("c:%s No date after found in file", self.id)
            fd.seek(0, 2)
            return fd.tell()
        except DateNotFoundInLine as ed:
            log.debug("c:%s Expanded date search failed for a line: %s",
                      self.id, ed)
            fd.seek(0)
            return fd.tell()

        log.debug("c:%s: finished binary seek search in file %s, offset %d",
                  self.id, fd.name, self._results[fd.name])
        return self._results[fd.name]

    def stats(self):
        _stats = {'line': {'pass': self._line_pass,
                           'fail': self._line_fail}}
        return _stats

    def __repr__(self):
        return ("id={}, since={}, current={}".
                format(self.id, self._since_date, self.current_date))
