import abc
import hashlib
import os
import re
import tempfile
import uuid

from datetime import datetime, timedelta
from functools import cached_property

from searchkit.utils import MPCacheSharded
from searchkit.log import log


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


class SkipRangeOverlapException(Exception):
    def __init__(self, ln):
        msg = ("the current and previous skip ranges overlap which "
               "suggests that we have re-entered a range by skipping "
               "line {}.".format(ln))
        super().__init__(msg)


class SkipRange(object):
    # skip directions
    SKIP_BWD = 0
    SKIP_FWD = 1

    def __init__(self):
        self.direction = self.SKIP_BWD
        self.current = set()
        self.prev = set()

    def re_entered(self):
        if self.prev.intersection(self.current):
            return True

        return False

    def add(self, ln):
        self.current.add(ln)
        if self.prev.intersection(self.current):
            raise SkipRangeOverlapException(ln)

    def __len__(self):
        return len(self.current)

    def save_and_reset(self):
        if self.current:
            self.prev = self.current
            self.current = set()
            self.direction = self.SKIP_BWD

    def __repr__(self):
        if self.current:
            _r = sorted(list(self.current))
            if self.direction == self.SKIP_BWD:
                _dir = '<-'
            else:
                _dir = '->'

            return "+skip({}{}{})".format(_r[0], _dir, _r[-1])

        return ""


class BinarySearchState(object):
    # max contiguous skip lines before bailing on file search
    SKIP_MAX = 500

    RC_FOUND_GOOD = 0
    RC_FOUND_BAD = 1
    RC_SKIPPING = 2
    RC_ERROR = 3

    def __init__(self, fd_info, cur_pos):
        self.fd_info = fd_info
        self.rc = self.RC_FOUND_GOOD
        self.cur_ln = 0
        self.cur_pos = cur_pos
        self.next_pos = 0

    def start(self):
        """ Must be called before starting searches beyond the first line of
        a file.

        This is not done in __init__ since it will load file markers, a
        potentially expensive operation that should only be done if necessary.
        """
        self.search_range_start = 0
        self.search_range_end = len(self.fd_info.markers) - 1
        self.update_pos_pointers()
        self.invalid_range = SkipRange()
        self.last_known_good_ln = None

    def save_last_known_good(self):
        self.last_known_good_ln = self.cur_ln

    def skip_current_line(self):
        if len(self.invalid_range) == self.SKIP_MAX - 1:
            self.rc = self.RC_ERROR
            log.warning("reached max contiguous skips (%d) - skipping "
                        "constraint for file %s", self.SKIP_MAX,
                        self.fd_info.fd.name)
            return

        self.rc = self.RC_SKIPPING
        try:
            self.invalid_range.add(self.cur_ln)
            if (self.invalid_range.direction == SkipRange.SKIP_BWD and
                    self.cur_ln > self.search_range_start):
                self.cur_ln -= 1
            elif self.cur_ln < self.search_range_end:
                if self.invalid_range.direction == SkipRange.SKIP_BWD:
                    log.debug("changing skip direction to fwd")

                self.invalid_range.direction = SkipRange.SKIP_FWD
                self.cur_ln += 1

            self.update_pos_pointers()
        except SkipRangeOverlapException:
            if self.last_known_good_ln is not None:
                self.rc = self.RC_FOUND_GOOD
                self.cur_ln = self.last_known_good_ln
                self.update_pos_pointers()
                log.debug("re-entered skip range so good line is %s",
                          self.cur_ln)
                self.fd_info.fd.seek(self.cur_pos)
            else:
                self.rc = self.RC_ERROR
                log.error("last known good not set so not sure where to "
                          "go after skip range overlap.")

    def update_pos_pointers(self):
        if len(self.fd_info.markers) == 0:
            log.debug("file %s has no markers - skipping update pos pointers",
                      self.fd_info.fd.name)
            return

        ln = self.cur_ln
        self.cur_pos = self.fd_info.markers[ln]
        if len(self.fd_info.markers) > ln + 1:
            self.next_pos = self.fd_info.markers[ln + 1]
        else:
            self.next_pos = self.fd_info.eof_pos

    def get_next_midpoint(self):
        """
        Given two line numbers in a file, find the mid point.
        """
        start = self.search_range_start
        end = self.search_range_end
        if start == end:
            return start, self.fd_info.markers[start]

        range = end - start
        mid = start + int(range / 2) + (range % 2)
        log.debug("midpoint: start=%s, mid=%s, end=%s", start, mid, end)
        self.cur_ln = mid

    def __repr__(self):
        return ("start={}{}, end={}, cur_pos={}, cur_ln={}, rc={}".format(
                self.search_range_start,
                self.invalid_range,
                self.search_range_end,
                self.cur_pos,
                self.cur_ln,
                self.rc))


class FileMarkers(object):
    INDEX_FSIZE_LIMIT = (1024 ** 3) * 10
    SYNC_LIMIT = 100000
    # NOTE: chunk much contain a newline
    BLOCK_SIZE_BASE = 4096
    CHUNK_SIZE = BLOCK_SIZE_BASE * 64

    def __init__(self, fd, eof_pos, cache_path=None):
        """
        Index start position for every line in file. Starts at current
        position and is non-destructive i.e. original position is
        restored.

        This is an expensive operation and we only want to do it once per
        file/path so the results are cached on disk and loaded when needed.

        NOTE: this is only safe to use if the file does not change between
              calls.
        """
        if cache_path is None:
            self.cache_path = tempfile.mkdtemp()
            log.debug("cache path not provided to filemarkers so using a "
                      "custom one (%s)", self.cache_path)
        else:
            self.cache_path = cache_path

        self.fd = fd
        self.file_path = self.fd.name
        hash = self._fname_hash(self.file_path)
        mtimesize = self._fmtime_size(self.file_path)
        self.cache_id = 'file_markers_{}_{}'.format(hash, mtimesize)
        self.cache_type = 'search_constraints'
        self.orig_pos = self.fd.tell()
        self.eof_pos = eof_pos
        self._primed = False
        self._num_lines = None
        self.chunk_size = self.CHUNK_SIZE

    def _do_chunk(self, cache, chunk, current_position, marker):
        if (self.fd.tell() < self.eof_pos) and '\n' not in chunk:
            log.warning("no newline found in chunk len=%s starting at "
                        "%s - increasing chunksize by %s bytes and "
                        "trying again", self.chunk_size, current_position,
                        self.BLOCK_SIZE_BASE)
            self.chunk_size += self.BLOCK_SIZE_BASE
            self.fd.seek(current_position)
            return -1, marker

        markers = {}
        chunkpos = 0
        while True:
            n = chunk[chunkpos:].find('\n')
            if n == -1:
                break

            chunkpos += n + 1
            if current_position + chunkpos == self.eof_pos:
                # don't save eof
                break

            marker += 1
            markers[marker] = current_position + chunkpos
            if marker % self.SYNC_LIMIT == 0:
                log.debug("indexed {0:.2f}% of file".format(
                          (100 / self._num_lines) * marker))

        cache.bulk_set(markers)
        return chunkpos, marker

    def _prime(self):
        self.fd.seek(self.orig_pos)
        self._num_lines = sum(1 for i in self.fd)
        self.fd.seek(self.orig_pos)
        log.debug("priming index cache for file %s in %s (numlines=%s)",
                  self.file_path,
                  self.cache_path, self._num_lines)
        with MPCacheSharded(self.cache_id, self.cache_type,
                            self.cache_path) as cache:
            if cache.get(self._num_lines - 1) is not None:
                log.debug("using existing file index from cache.")
                self._primed = True
                return

            main_marker = 0
            main_pos = self.orig_pos
            cache.set(main_marker, self.orig_pos)
            remainder = ''
            for chunk in self._readchunk(self.fd):
                if remainder != '':
                    chunk = remainder + chunk

                chunk_pos, main_marker = self._do_chunk(cache, chunk, main_pos,
                                                        main_marker)
                if chunk_pos >= 0:
                    remainder = chunk[chunk_pos:]
                    main_pos += chunk_pos
                else:
                    # this implies we are going around again
                    remainder = ''

        self.fd.seek(self.orig_pos)
        log.debug("finished creating index. (lines=%s)", self._num_lines)
        self._primed = True

    def _fname_hash(self, path):
        hash = hashlib.sha256()
        hash.update(path.encode('utf-8'))
        return hash.hexdigest()

    def _fmtime_size(self, path):
        """
        Criteria used to determine if file contents changed since markers were
        last generated.
        """
        if not os.path.exists(path):
            return 0

        mtime = os.path.getmtime(path)
        size = os.path.getsize(path)
        return "{}+{}".format(mtime, size)

    def _readchunk(self, fd):
        while True:
            data = fd.read(self.chunk_size).decode()
            if data == '':
                break

            yield data

    def __getitem__(self, key):
        if not self._primed:
            self._prime()

        with MPCacheSharded(self.cache_id, self.cache_type,
                            self.cache_path) as cache:
            return cache.get(key)

    def __iter__(self):
        if not self._primed:
            self._prime()

        with MPCacheSharded(self.cache_id, self.cache_type,
                            self.cache_path) as cache:
            for value in sorted(cache):
                yield value

    def __len__(self):
        if not self._primed:
            self._prime()

        return self._num_lines


class SeekInfo(object):

    def __init__(self, fd, cache_path=None):
        """
        @param cache_path: provide a path to save cache info if you want it to
                          persist.
        """
        self.fd = fd
        self.iterations = 0
        self._orig_pos = self.fd.tell()
        self.markers = FileMarkers(fd, self.eof_pos, cache_path)

    @cached_property
    def eof_pos(self):
        """
        Returns file EOF position.
        """
        orig = self.fd.tell()
        eof = self.fd.seek(0, 2)
        self.fd.seek(orig)
        return eof

    @cached_property
    def orig_pos(self):
        """
        The original position of the file descriptor.

        NOTE: cannot be called when iterating over an fd. Must be called before
        any destructive operations take place.
        """
        return self._orig_pos

    def reset(self):
        log.debug("restoring file position to start (%s)",
                  self.orig_pos)
        self.fd.seek(self.orig_pos)


class BinarySeekSearchBase(ConstraintBase):
    """
    Provides a way to seek to a point in a file using a binary search and a
    given condition.
    """

    def __init__(self, allow_constraints_for_unverifiable_logs=True):
        self.fd_info = None
        self.allow_unverifiable_logs = allow_constraints_for_unverifiable_logs

    @abc.abstractmethod
    def extracted_datetime(self, line):
        """
        Extract datetime from line. Returns a datetime object or None if unable
        to extract one from the line.

        @param line: text line to extract a datetime from.
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

    def _seek_and_validate(self, datetime_obj):
        """
        Seek to position and validate. If the line at pos is valid the new
        position is returned otherwise None.

        NOTE: this operation is destructive and will always point to the next
              line after being called.

        @param pos: position in a file.
        """
        if self._line_date_is_valid(datetime_obj):
            return self.fd_info.fd.tell()

    def _check_line(self, search_state):
        """
        Attempt to read and validate datetime from line.

        @return new position or -1 if we were not able to validate the line.
        """
        self.fd_info.fd.seek(search_state.cur_pos)
        # don't read the whole line since we only need the date at the start.
        # hopefully 64 bytes is enough for any date+time format.
        datetime_obj = self.extracted_datetime(self.fd_info.fd.read(64))
        self.fd_info.fd.seek(search_state.next_pos)
        if datetime_obj is None:
            return -1

        return self._seek_and_validate(datetime_obj)

    def _seek_next(self, state):
        log.debug("seek %s", state)
        newpos = self._check_line(state)
        if newpos == -1:
            if not self.allow_unverifiable_logs:
                log.info("file contains unverifiable lines and "
                         "allow_constraints_for_unverifiable_logs is False  "
                         "- aborting constraints for this file.")
                state.rc = state.RC_ERROR
                return state

            # until we get out of a skip range we want to leave the pos at the
            # start but we rely on the caller to enforce this so that we don't
            # have to seek(0) after every skip.
            state.skip_current_line()
            return state

        if newpos is None:
            state.rc = state.RC_FOUND_BAD
            if state.cur_ln == 0:
                log.debug("first line is not valid, checking last line")
                state.cur_ln = state.search_range_end
                state.update_pos_pointers()
            elif (state.search_range_end - state.search_range_start) >= 1:
                # _start_ln = state.search_range_start
                state.search_range_start = state.cur_ln
                # log.debug("going forwards (%s->%s:%s)", _start_ln,
                #           state.search_range_start, state.search_range_end)
                state.invalid_range.save_and_reset()
                state.get_next_midpoint()
                state.update_pos_pointers()
        else:
            state.save_last_known_good()
            state.rc = state.RC_FOUND_GOOD
            if state.cur_ln == 0:
                log.debug("first line is valid so assuming same for rest of "
                          "file")
                self.fd_info.reset()
            elif state.search_range_end - state.search_range_start <= 1:
                log.debug("found final good ln=%s", state.cur_ln)
                self.fd_info.fd.seek(state.cur_pos)
            elif (len(state.invalid_range) > 0 and
                  state.invalid_range.direction == SkipRange.SKIP_FWD):
                log.debug("found good after skip range")
                self.fd_info.fd.seek(state.cur_pos)
            else:
                # set rc to bad since we are going to a new range
                state.rc = state.RC_FOUND_BAD
                # _end_ln = state.search_range_end
                state.search_range_end = state.cur_ln
                # log.debug("going backwards (%s:%s->%s)",
                #           state.search_range_start, _end_ln,
                #           state.search_range_end)
                self.fd_info.fd.seek(state.cur_pos)
                state.invalid_range.save_and_reset()
                state.get_next_midpoint()
                state.update_pos_pointers()

        return state

    def _seek_to_first_valid(self, destructive=True):
        """
        Find first valid line in file using binary search. By default this is a
        destructive and will actually seek to the line. If no line is found the
        descriptor will be at EOF.

        Returns offset at which valid line was found.

        @param destructive: by default this seek operation is destructive i.e.
                            it will find the least valid point and stay there.
                            If that is not desired this can be set to False.
        """
        search_state = BinarySearchState(self.fd_info, self.fd_info.fd.tell())
        offset = 0

        # check first line before going ahead with full search which requires
        # generating file markers that is expensive for large files.
        if self._check_line(search_state) == search_state.next_pos:
            self.fd_info.reset()
            log.debug("first line is valid so assuming same for rest of "
                      "file")
            log.debug("seek %s finished (skipped %d lines) current_pos=%s, "
                      "offset=%s iterations=%s",
                      self.fd_info.fd.name, offset,
                      self.fd_info.fd.tell(), offset, self.fd_info.iterations)

            return offset

        log.debug("first line is not valid - checking rest of file")
        self.fd_info.reset()
        search_state.start()
        num_lines = len(self.fd_info.markers)
        if num_lines > 0:
            while True:
                if search_state.cur_ln >= num_lines:
                    log.debug("reached eof - no more lines to check")
                    break

                self.fd_info.iterations += 1
                search_state = self._seek_next(search_state)
                if search_state.rc == search_state.RC_ERROR:
                    offset = 0
                    self.fd_info.reset()
                    break

                if (search_state.search_range_end -
                        search_state.search_range_start) < 1:
                    # we've reached the end of all ranges but the result in
                    # undetermined.
                    if search_state.rc != search_state.RC_FOUND_BAD:
                        self.fd_info.reset()
                        offset = 0
                    else:
                        offset = search_state.cur_ln

                    break

                # log.debug(search_state)
                if search_state.rc == search_state.RC_FOUND_GOOD:
                    # log.debug("seek ended at offset=%s", search_state.cur_ln)
                    offset = search_state.cur_ln
                    break

                if search_state.rc == search_state.RC_SKIPPING:
                    if ((search_state.cur_ln >= search_state.search_range_end)
                            and (len(search_state.invalid_range) ==
                                 search_state.search_range_end)):
                        # offset and pos should still be SOF so we
                        # make this the same
                        search_state.cur_ln = 0
                        self.fd_info.reset()
                        break

                if self.fd_info.iterations >= len(self.fd_info.markers):
                    log.warning("exiting seek loop since limit reached "
                                "(eof=%s)", self.fd_info.eof_pos)
                    offset = 0
                    self.fd_info.reset()
                    break
        else:
            log.debug("file %s is empty", self.fd_info.fd.name)

        if not destructive:
            self.fd_info.fd.reset()

        log.debug("seek %s finished (skipped %d lines) current_pos=%s, "
                  "offset=%s iterations=%s",
                  self.fd_info.fd.name, offset,
                  self.fd_info.fd.tell(), offset, self.fd_info.iterations)

        return offset


class SearchConstraintSearchSince(BinarySeekSearchBase):

    def __init__(self, current_date, cache_path, exprs=None, days=0, hours=24,
                 **kwargs):
        """
        A search expression is provided that allows us to identify a datetime
        on each line and check whether it is within a given time period. The
        time period used defaults to 24 hours if use_all_logs is false, 7 days
        if it is true and max_logrotate_depth is default otherwise whatever
        value provided. This can be overridden by providing a specific number
        of hours.

        @param current_date: cli.date(format="+{}".format(self.date_format))
        @param exprs: a list of search/regex expressions used to identify a
                      date/time in.
        each line in the file we are applying this constraint to.
        @param days: override default period with number of days
        @param hours: override default period with number of hours
        """
        super().__init__(**kwargs)
        self.cache_path = cache_path
        self.date_format = '%Y-%m-%d %H:%M:%S'
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
        """
        Validate if the given line falls within the provided constraint. In
        this case that's whether it has a datetime that is >= to the "since"
        date.

        @param line: text line to extract a datetime from.
        """
        if type(line) == bytes:
            # need this for e.g. gzipped files
            line = line.decode("utf-8")

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
        self.fd_info = SeekInfo(fd, cache_path=self.cache_path)
        if not self._is_valid:
            log.warning("c:%s unable to apply constraint to %s", self.id,
                        fd.name)
            return

        # indexing files larger than this takes too long and is too resource
        # intensive so best to fall back to per-line check.
        if os.path.getsize(fd.name) >= FileMarkers.INDEX_FSIZE_LIMIT:
            log.debug("s:%s: file %s too big to perform binary search - "
                      "skipping", self.id, fd.name)
            return

        if fd.name in self._results:
            return self._results[fd.name]

        log.debug("c:%s: starting binary seek search to %s in file %s "
                  "(destructive=True)", self.id, self._since_date, fd.name)
        self._results[fd.name] = self._seek_to_first_valid(destructive)
        log.debug("c:%s: finished binary seek search in file %s", self.id,
                  fd.name)
        return self._results[fd.name]

    def stats(self):
        _stats = {'line': {'pass': self._line_pass,
                           'fail': self._line_fail}}
        if self.fd_info:
            _stats['file'] = {'name': self.fd_info.fd.name,
                              'iterations': self.fd_info.iterations}
        return _stats

    def __repr__(self):
        return ("id={}, since={}, current={}".
                format(self.id, self._since_date, self.current_date))
