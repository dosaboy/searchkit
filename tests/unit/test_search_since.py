""" Searchkit "search since" constraint unit tests. """
import os

from searchkit import (
    FileSearcher,
    SearchDef,
)
from searchkit.constraints import SearchConstraintSearchSince

from . import utils
from .utils import TimestampSimple

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


class TestSearchSince(utils.TestSearchKitBase):
    """ Unit tests for searches constrained by a "since" date. """

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_single_valid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Mon Jan 10 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd',
                       constraints=[c])
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L4'])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_first_valid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sat Jan 1 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd',
                       constraints=[c])
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results],
                         [f"L{i}" for i in range(5)])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_multi_valid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sun Jan 09 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L2', 'L3', 'L4'])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_all_valid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sat Jan 01 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results],
                         [f"L{i}" for i in range(5)])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_all_invalid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sat Jan 15 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], [])

    @utils.create_files({'atestfile': "\n" + LOGS_W_TS})
    def test_logs_since_junk_at_start_of_file(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sat Jan 01 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results],
                         [f"L{i}" for i in range(5)])

    @utils.create_files({'atestfile': LOGS_W_TS + "\n"})
    def test_logs_since_junk_at_end_of_file(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sat Jan 01 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results],
                         [f"L{i}" for i in range(5)])

    @utils.create_files({'atestfile': LOGS_W_TS + "\n"})
    def test_logs_since_junk_at_end_of_file_and_start_invalid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Mon Jan 03 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=1)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L2', 'L3', 'L4'])

    @utils.create_files({'atestfile': LOGS_W_TS_AND_UNMATCABLE_LINES})
    def test_logs_since_file_valid_with_unmatchable_lines(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Sun Jan 09 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L2', 'L3', 'L4'])

    @utils.create_files({'atestfile': 'L0'})
    def test_logs_since_single_junk(self):
        """
        Test scenario: file contains a single unverifiable line and we expect
        pointers to be reset to start of file.
        """
        self.current_date = self.get_date('Sun Jan 09 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(r"(.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(1) for r in results], ['L0'])

    @utils.create_files({'atestfile':
                         "L0\nL1\nL2\nL3\nL4\nL5\nL6\nL7\nL8\n"})
    def test_logs_since_all_junk(self):
        """
        Test scenario: file contains no matchable/verifiable lines i.e.
        we are not able to match a timestamp to verify on any line then we have
        to deem the file contents as valid.
        """
        self.current_date = self.get_date('Sun Jan 09 00:00:00 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        s = FileSearcher(constraint=c)
        sd = SearchDef(r"(.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(1) for r in results],
                         [f"L{i}" for i in range(9)])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_hours(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Mon Jan 03 00:00:01 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        hours=24,
                                        ts_matcher_cls=TimestampSimple)
        s = FileSearcher(constraint=c)
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd')
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L3', 'L4'])

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_hours_sd(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Mon Jan 03 00:00:01 UTC 2022')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        hours=24,
                                        ts_matcher_cls=TimestampSimple)
        s = FileSearcher()
        sd = SearchDef(rf"{self.datetime_expr}\S+ (.+)", tag='mysd',
                       constraints=[c])
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sd, path=fname)
        results = s.run()
        results = results.find_by_tag('mysd')
        self.assertEqual([r.get(2) for r in results], ['L3', 'L4'])
