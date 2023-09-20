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
    that is before/after a specific time. This class is implemented to provide
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
        """ Provide runtime stats for this object. """

    @abc.abstractmethod
    def __repr__(self):
        """ Provide string repr of this object. """


class BinarySeekSearchBase(ConstraintBase):
    """
    Provides a way to seek to a point in a file using a binary search and a
    given condition.
    """

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


class NoValidLinesFoundInFile(Exception):
    """ Log file contains timestamps but none that meet the constraint. """


class NoTimestampsFoundInFile(Exception):
    """ Could not match a timestamp on any line in the file. """


class TooManyLinesWithoutDate(Exception):
    """
    Binary search encounters a line with no date and searches lines either
    side within max limit and without success.
    """


class MaxSearchableLineLengthReached(Exception):
    """
    Exceeded number of characters we can search in a line before finding a line
    feed.
    """


class FindTokenStatus(Enum):
    FOUND = 1,
    REACHED_EOF = 2,


class SearchState(object):
    def __init__(self, status: FindTokenStatus, offset=0):
        """
        Representation of binary search state.

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
        return self._offset


class SavedFilePosition(object):
    """
    Context manager class that saves current position at start and restores
    once finished.
    """
    def __init__(self, file):
        self.file = file
        self.original_position = file.tell()

    def __enter__(self):
        return self.file

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.file.seek(self.original_position)


class LogLine(object):
    """
    Class representing a line extracted from a log file.

    Keeps the start/end offsets of the line. Line content is lazy-loaded on
    demand by calling the `text` method.
    """
    MAX_DATETIME_READ_BYTES = 64

    def __init__(self, file, constraint, line_start_lf, line_end_lf):
        """
        @param file: file descriptor
        @param constraint: constraint object
        @param line_start_lf: SearchState object for start of line line feed
        @param line_end_lf: SearchState object for end of line line feed
        """
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
        """
        Offset of the log line's first character (excluding \n)

        Depending on whether we found the start line feed or not, we discard
        one character at the beginning (i.e. the \n)
        """
        if self.start_lf.status == FindTokenStatus.FOUND:
            return self.start_lf.offset + 1

        return self.start_lf.offset

    @property
    def end_offset(self):
        """
        Offset of the log line's last character (excluding \n)

        Depending on whether we found the end line feed or not, we discard one
        character at the end (i.e. the \n)
        """
        if self.end_lf.status == FindTokenStatus.FOUND:
            return self.end_lf.offset - 1

        return self.end_lf.offset

    @property
    def start_lf(self):
        """ Offset of the log line's starting line feed. """
        return self._line_start_lf

    @property
    def end_lf(self):
        """ Offset of the log line's ending line feed. """
        return self._line_end_lf

    @property
    def date(self):
        """
        Extract the date from the log line, if any.

        The function will use extracted_datetime function to parse
        the date/time.

        @return: datetime: if `text` contains a valid datetime otherwise None.
        """
        line = self._read_line(self.MAX_DATETIME_READ_BYTES)
        return self._constraint.extracted_datetime(line)

    @property
    def text(self):
        return self._read_line(max_len=len(self))

    def _read_line(self, max_len):
        """
        Perform a non-destructive read of max_len bytes.

        This function seeks to the line start and reads the line content
        on demand.

        @param max_len: number of bytes to read
        @return: the line text string
        """
        with SavedFilePosition(self._file) as f:
            f.seek(self.start_offset)
            line_text = f.read(max_len)
            return line_text


class LogFileDateSinceSeeker(object):
    """
    Performs "since" date lookups with file offsets. This is
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

    MAX_SEARCHABLE_LINE_LENGTH = MAX_SEEK_HORIZON_EXPAND * SEEK_HORIZON
    LINE_FEED_TOKEN = b'\n'

    def __init__(self, fd, c):
        self.file = fd
        self.constraint = c
        self.line_info = None
        self.found_any_date = False
        self.lookup_count = 0
        self.lines_searched = 0
        with SavedFilePosition(self.file) as f:
            self.length = f.seek(0, 2)

    def find_token_reverse(self, start_offset):
        """
        Find `token` in `file` starting from `start_offset` and backing
        off `LogFileDateSinceSeeker.SEEK_HORIZON` bytes on each iteration for
        maximum of `LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND` times.

        @param start_offset: integer start offset of search
        @return: SearchState object
        """
        start_offset_saved = start_offset
        attempts = LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND
        current_offset = -LogFileDateSinceSeeker.SEEK_HORIZON
        while True:
            attempts -= 1
            read_offset = start_offset + current_offset
            read_offset = read_offset if read_offset > 0 else 0
            read_size = LogFileDateSinceSeeker.SEEK_HORIZON
            if start_offset + current_offset <= 0:
                read_size = read_size + (start_offset + current_offset)

            log.debug("seeking to %s", read_offset)
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

        msg = ("reached max line length ({}) search without finding a line "
               "feed (epicenter={})".format(self.MAX_SEARCHABLE_LINE_LENGTH,
                                            start_offset_saved))
        raise MaxSearchableLineLengthReached(msg)

    def find_token(self, start_offset):
        """
        Find `token` in `file` starting from `start_offset` and moving
        forward `LogFileDateSinceSeeker.SEEK_HORIZON` bytes on each
        iteration for maximum of `LogFileDateSinceSeeker.MAX_SEEK_HORIZON_
        EXPAND` times.

        @param start_offset: integer start offset of search
        @return: SearchState object
        """
        start_offset_saved = start_offset
        attempts = LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND
        current_offset = 0
        # Seek to the initial starting position
        log.debug("seeking to %s", start_offset)
        self.file.seek(start_offset)
        while attempts > 0:
            attempts -= 1
            # Read `horizon` bytes from the file.
            chunk = self.file.read(LogFileDateSinceSeeker.SEEK_HORIZON)
            if not chunk or len(chunk) == 0:
                # Reached end of file
                return SearchState(status=FindTokenStatus.REACHED_EOF,
                                   offset=len(self))

            chunk_offset = chunk.find(self.LINE_FEED_TOKEN)
            if chunk_offset != -1:
                found_offset = start_offset + current_offset + chunk_offset
                log.debug("found line feed at pos %s", found_offset)
                # We've found the token in the chunk.
                # As the chunk_offset is a relative offset to the chunk
                # translate it to file offset while returning.
                return SearchState(status=FindTokenStatus.FOUND,
                                   offset=found_offset)
            # We failed to find the token in the chunk.
            # Progress the current offset forward by
            # chunk's length.
            current_offset = current_offset + len(chunk)

        msg = ("reached max line length ({}) search without finding a line "
               "feed (epicenter={})".format(self.MAX_SEARCHABLE_LINE_LENGTH,
                                            start_offset_saved))
        raise MaxSearchableLineLengthReached(msg)

    def try_find_line(self, epicenter, slf_off=None, elf_off=None):
        """
        Try to find a line at `epicenter`. This function allows extracting
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
        @return: found logline
        """
        log.debug("searching either side of pos=%d", epicenter)

        # Find the first LF token from the right of the epicenter
        # e.g. \nThis is a line\n
        #         ^epicenter    ^line end lf
        if elf_off is None:
            line_end_lf = self.find_token(epicenter)
        else:
            line_end_lf = SearchState(FindTokenStatus.FOUND, elf_off)

        # Find the first LF token to the left of the epicenter
        # e.g.          \nThis is a line\n
        # line start lf  ^ ^epicenter
        if slf_off is None:
            line_start_lf = self.find_token_reverse(epicenter)
        else:
            line_start_lf = SearchState(FindTokenStatus.FOUND, slf_off)

        # Ensure that found offsets are in file range
        assert line_start_lf.offset <= len(self)
        assert line_start_lf.offset >= 0
        assert line_end_lf.offset <= len(self)
        assert line_end_lf.offset >= 0
        # Ensure that end lf offset is >= start lf offset
        assert line_end_lf.offset >= line_start_lf.offset

        self.lines_searched += 1
        return LogLine(file=self.file, constraint=self.constraint,
                       line_start_lf=line_start_lf, line_end_lf=line_end_lf)

    def try_find_line_with_date(self, start_offset, line_feed_offset=None,
                                forwards=True):
        """
        Try to fetch a line with date, starting from `start_offset`.

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
        log.debug("starting %s search from %s",
                  'forward' if forwards else 'reverse', start_offset)

        attempts = LogFileDateSinceSeeker.MAX_TRY_FIND_WITH_DATE_ATTEMPTS
        offset = start_offset
        log_line = None
        while attempts > 0:
            attempts -= 1
            log_line = self.try_find_line(
                offset,
                (None, line_feed_offset)[forwards],
                (None, line_feed_offset)[not forwards]
            )

            # If the line has a valid date, return it.
            if log_line.date:
                log.debug("finished %s search - date found",
                          'forward' if forwards else 'reverse')
                return log_line

            log.debug("looking further %s for date: attempts_remaining=%d, "
                      "start=%d, end=%d",
                      'forwards' if forwards else 'backwards',
                      attempts, log_line.start_lf.offset,
                      log_line.end_lf.offset)

            # Set offset of the found line feed
            line_feed_offset = (log_line.start_lf,
                                log_line.end_lf)[forwards].offset
            # Set the next search starting point
            offset = line_feed_offset + (-1, +1)[forwards]
            if offset < 0 or offset > len(self):
                log.debug("search hit eof/sof - exiting")
                break

        log.debug("finished %s search - no date found",
                  'forward' if forwards else 'reverse')
        return None

    def __len__(self):
        return self.length

    def __getitem__(self, offset):
        """
        Find the nearest line with a date at `offset` and return its date.

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
        @raise TooManyLinesWithoutDate: When a line with a date could not
                                        be found.
        @return: Date of the line at `offset`
        """

        self.lookup_count += 1
        log.debug("timestamp lookup attempt=%d, offset=%d", self.lookup_count,
                  offset)
        result = None

        # Try to search backwards first.
        # First call will effectively be a regular forward search given
        # that we're not passing a line feed offset to the function.
        # Any subsequent search attempt will be made backwards.
        result = self.try_find_line_with_date(offset, None, False)
        # ... then, forwards.
        if not result or result.date is None:
            result = self.try_find_line_with_date(offset + 1, offset, True)

        if not result or result.date is None:
            raise TooManyLinesWithoutDate(
                f"date search failed at offset `{offset}`")

        # This is mostly for diagnostics. If we could not find
        # any valid date in given file, we throw a specific exception
        # to indicate that.

        self.found_any_date = True
        if result.date >= self.constraint._since_date:
            # Keep the matching line so we can access it
            # after the bisect without having to perform another
            # lookup.
            self.line_info = result

        constraint_met = ((result.date >= self.constraint._since_date)
                          if result.date else False)
        log.debug("extracted_date='%s' >= since_date='%s' == %s", result.date,
                  self.constraint._since_date, constraint_met)
        return result.date

    def run(self):
        """
        Find first timstamp that is within constrain limit.

        Bisect_left will give us the first occurrence of the date
        that satisfies the constraint. Similarly, bisect_right would allow the
        last occurrence of a date that satisfies the criteria.
        """

        # Check last line in file first, if not valid we know rest is also not
        # valid.
        log.debug("checking last line")
        with SavedFilePosition(self.file):
            result = self.try_find_line_with_date(self.file.seek(0, 2), None,
                                                  False)
            if result and result.date is None:
                log.debug("last timestamp in file is not valid so assuming "
                          "same for rest of file")
                raise NoValidLinesFoundInFile()

        # Check first line in file, if valid we know rest of file is valid
        log.debug("checking first line")
        with SavedFilePosition(self.file):
            self.lines_searched += 1
            current = self.file.tell()
            # NOTE: the end lf offset 100 below is arbitrary
            result = LogLine(self.file, self.constraint,
                             SearchState(FindTokenStatus.FOUND, -1),
                             SearchState(FindTokenStatus.FOUND, 100))
            if result.date is not None:
                if result.date >= self.constraint._since_date:
                    log.debug("first line has date that is valid so assuming "
                              "rest of file is valid")
                    return current

        log.debug("starting full binary search")
        try:
            bisect.bisect_left(self, self.constraint._since_date)
        except TooManyLinesWithoutDate as exc:
            if not self.found_any_date:
                raise NoTimestampsFoundInFile from exc

            raise

        if not self.line_info:
            raise NoValidLinesFoundInFile

        log.debug("run end: found line start=%d, end=%d, content=%s in %d "
                  "lookup(s)", self.line_info.start_offset,
                  self.line_info.end_offset, self.line_info.text,
                  self.lookup_count)

        return self.line_info.start_offset


class SearchConstraintSearchSince(BinarySeekSearchBase):

    def __init__(self, current_date, ts_matcher_cls, days=0, hours=24,
                 **kwargs):
        """
        A search expression is provided that allows us to identify a datetime
        on each line and check whether it is within a given time period. The
        time period used defaults to 24 hours if use_all_logs is false, 7 days
        if it is true and max_logrotate_depth is default otherwise whatever
        value provided. This can be overridden by providing a specific number
        of hours.

        @param current_date: cli.date(format="+{}".format(self.date_format))
        @param ts_matcher_cls: TimestampMatcherBase implementation used to
                               match timestamps at start if lines.
        @param days: override default period with number of days
        @param hours: override default period with number of hours
        """
        super().__init__(**kwargs)
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
        self._lines_searched = 0
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

        timestamp = self.ts_matcher_cls(line)
        if timestamp.matched:
            return timestamp.strptime

        return

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
            log.debug("using cached offset")
            return self._results[fd.name]

        log.debug("c:%s: starting binary seek search to %s in file %s "
                  "(destructive=True)", self.id, self._since_date, fd.name)
        try:
            orig_offset = fd.tell()
            seeker = LogFileDateSinceSeeker(fd, self)
            new_offset = seeker.run()
            self._lines_searched += seeker.lines_searched
            if new_offset is None or not destructive:
                fd.seek(orig_offset)
            else:
                fd.seek(new_offset)

            if new_offset is None or new_offset == len(seeker):
                self._results[fd.name] = None
            else:
                self._results[fd.name] = new_offset
        except NoTimestampsFoundInFile:
            log.debug("c:%s no timestamp found in file", self.id)
            fd.seek(0)
            return fd.tell()
        except NoValidLinesFoundInFile:
            log.debug("c:%s no date after %s found in file - seeking to end",
                      self._since_date, self.id)
            fd.seek(0, 2)
            return fd.tell()
        except TooManyLinesWithoutDate as exc:
            log.warning("c:%s failed to find a line containing a date: %s",
                        self.id, exc)
            fd.seek(0)
            return fd.tell()
        except MaxSearchableLineLengthReached as exc:
            log.error("c:%s exceeded allowed line length search limit "
                      "before finding line feed: %s", self.id, exc)
            raise

        log.debug("c:%s: finished binary seek search in file %s, offset %d",
                  self.id, fd.name, self._results[fd.name])
        return self._results[fd.name]

    def stats(self):
        _stats = {'lines_searched': self._lines_searched,
                  'line': {'pass': self._line_pass,
                           'fail': self._line_fail}}
        return _stats

    def __repr__(self):
        return "id={}, since={}".format(self.id, self._since_date)
