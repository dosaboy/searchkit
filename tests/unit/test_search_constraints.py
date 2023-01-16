import os
import re
import tempfile
import shutil
import subprocess

from . import utils
from searchkit.constraints import (
    BinarySearchState,
    SearchConstraintSearchSince,
)


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

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        datetime_expr = r"^([\d-]+\s+[\d:]+)"
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        exprs=[datetime_expr], days=7)
        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 0)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        exprs=[datetime_expr], days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 1)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        exprs=[datetime_expr], days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 499 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, BinarySearchState.SKIP_MAX - 1)

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        cache_path=self.constraints_cache_path,
                                        exprs=[datetime_expr], days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 500 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, 0)
