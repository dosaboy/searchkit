import os
import re
import tempfile
import shutil
import subprocess

from unittest import mock

from . import utils
from searchkit.constraints import (
    BinarySearchState,
    TimestampMatcherBase,
    FileMarkers,
    SearchConstraintSearchSince,
)


class TimestampSimple(TimestampMatcherBase):

    @property
    def patterns(self):
        return [r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})+\s+'
                r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d+)']


LOGS_W_TS = """2022-01-01 00:00:00.00 L0
2022-01-01 01:00:00.00 L1
2022-01-02 00:00:00.00 L2
2022-01-02 01:00:00.00 L3
2022-01-03 00:00:00.00 L4
"""

LOGS_W_TS_AND_UNMATCABLE_LINES = """blah 1
2022-01-01 00:00:00.00 L0
blah 2
blah 3
2022-01-01 01:00:00.00 L1
blah 4
2022-01-02 00:00:00.00 L2
blah 5
blah 6
blah 7
2022-01-02 01:00:00.00 L3
blah 8
2022-01-03 00:00:00.00 L4
blah 9
"""


class TestSearchKitBase(utils.BaseTestCase):

    def get_date(self, date):
        cmd = ["date", "--utc", "--date={}".format(date), "+%Y-%m-%d %H:%M:%S"]
        out = subprocess.check_output(cmd)
        out = re.compile(r"\s+").sub(' ', out.decode('UTF-8')).strip()
        return out

    def setUp(self):
        super().setUp()
        self.current_date = self.get_date("Thu Feb 10 16:19:17 UTC 2022")
        self.data_root = tempfile.mkdtemp()
        self.constraints_cache_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.data_root)
        shutil.rmtree(self.constraints_cache_path)
        super().tearDown()


class TestSearchConstraints(TestSearchKitBase):

    def test_constraints_file_markers_w_trailing_newline(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache_path = os.path.join(dtmp, 'cache')
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'w') as fd:
                fd.write('this is\n')
                fd.write('some\n')
                fd.write('file content\n')

            with open(fpath, 'rb') as fd:
                orig_pos = fd.tell()
                eof_pos = fd.seek(0, 2)
                fd.seek(orig_pos)
                markers = FileMarkers(fd, eof_pos, cache_path)
                self.assertEqual(len(markers), 3)
                self.assertEqual(list(markers), [0, 8, 13])
                full_cache_path = os.path.join(markers.cache_path, 'caches',
                                               'search_constraints')
                self.assertEqual(len(os.listdir(full_cache_path)), 1)
                cache_file = os.path.join(full_cache_path,
                                          os.listdir(full_cache_path)[0])
                mtime1 = os.path.getmtime(cache_file)
                # re-run and this time should use existing cache
                markers = FileMarkers(fd, eof_pos, cache_path)
                self.assertEqual(len(markers), 3)
                self.assertEqual(list(markers), [0, 8, 13])
                mtime2 = os.path.getmtime(cache_file)
                full_cache_path = os.path.join(markers.cache_path, 'caches',
                                               'search_constraints')
                self.assertEqual(len(os.listdir(full_cache_path)), 1)
                self.assertEqual(mtime1, mtime2)
                self.assertEqual(orig_pos, fd.tell())

                self.assertEqual(markers[0], 0)
                self.assertEqual(markers[1], 8)
                self.assertEqual(markers[2], 13)
                self.assertEqual(markers[3], None)

    def test_constraints_file_markers_no_trailing_newline(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache_path = os.path.join(dtmp, 'cache')
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'w') as fd:
                fd.write('this is\n')
                fd.write('some\n')
                fd.write('file content')

            with open(fpath, 'rb') as fd:
                orig_pos = fd.tell()
                eof_pos = fd.seek(0, 2)
                fd.seek(orig_pos)
                markers = FileMarkers(fd, eof_pos, cache_path)
                self.assertEqual(len(markers), 3)
                self.assertEqual(list(markers), [0, 8, 13])

                self.assertEqual(markers[0], 0)
                self.assertEqual(markers[1], 8)
                self.assertEqual(markers[2], 13)
                self.assertEqual(markers[3], None)

    @mock.patch.object(FileMarkers, 'CHUNK_SIZE', 14)
    def test_constraints_file_markers_gt_block(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache_path = os.path.join(dtmp, 'cache')
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'w') as fd:
                fd.write('this is\n')
                fd.write('some\n')
                fd.write('file content\n')

            with open(fpath, 'rb') as fd:
                orig_pos = fd.tell()
                eof_pos = fd.seek(0, 2)
                fd.seek(orig_pos)
                markers = FileMarkers(fd, eof_pos, cache_path)
                self.assertEqual(len(markers), 3)
                self.assertEqual(list(markers), [0, 8, 13])
                self.assertEqual(markers[0], 0)
                self.assertEqual(markers[1], 8)
                self.assertEqual(markers[2], 13)
                self.assertEqual(markers[3], None)

    @mock.patch.object(FileMarkers, 'CHUNK_SIZE', 7)
    @mock.patch.object(FileMarkers, 'BLOCK_SIZE_BASE', 1)
    def test_constraints_file_markers_chunk_too_small(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache_path = os.path.join(dtmp, 'cache')
            fpath = os.path.join(dtmp, 'f1')
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'w') as fd:
                fd.write('this is\n')
                fd.write('some\n')
                fd.write('file content')

            with open(fpath, 'rb') as fd:
                orig_pos = fd.tell()
                eof_pos = fd.seek(0, 2)
                fd.seek(orig_pos)
                markers = FileMarkers(fd, eof_pos, cache_path)
                self.assertEqual(len(markers), 3)
                self.assertEqual(list(markers), [0, 8, 13])
                self.assertEqual(markers[0], 0)
                self.assertEqual(markers[1], 8)
                self.assertEqual(markers[2], 13)
                self.assertEqual(markers[3], None)

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 0)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 1)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 499 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, BinarySearchState.SKIP_MAX - 1)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 500 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, 0)
