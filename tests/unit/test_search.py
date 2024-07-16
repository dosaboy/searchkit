import glob
import os
import re
import tempfile
import shutil
import subprocess
from collections import OrderedDict
from unittest import mock

from searchkit import (
    FileSearcher,
    ResultFieldInfo,
    SearchDef,
    SequenceSearchDef,
)
from searchkit.search import (
    logrotate_log_sort,
    SearchCatalog,
    SearchResultsCollection,
    ResultStoreSimple,
)
from searchkit.result import SearchResult
from searchkit.constraints import (
    TimestampMatcherBase,
    SearchConstraintSearchSince,
)

from . import utils

SEQ_TEST_1 = """a start point
leads to
an ending
"""

SEQ_TEST_2 = """a start point
another start point
leads to
an ending
"""

SEQ_TEST_3 = """a start point
another start point
leads to
an ending
a start point
"""

SEQ_TEST_4 = """a start point
another start point
value is 3
"""

SEQ_TEST_5 = """a start point
another start point
value is 3

another start point
value is 4
"""

SEQ_TEST_6 = """section 1
1_1
1_2
section 2
2_1
"""

SEQ_TEST_7 = """section 1
1_1
1_2
section 2
2_1
section 3
3_1
"""

MULTI_SEQ_TEST = """
sectionB 1
1_1
sectionA 1
1_1
sectionB 2
2_2
sectionA 2
2_1
"""

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


class TestFailedError(Exception):
    """ Raised when an error is identified in a test. """


class TimestampSimple(TimestampMatcherBase):
    """ Test timestamp implementation. """

    @property
    def patterns(self):
        return [r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})+\s+'
                r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d+)']


class TestSearchKitBase(utils.BaseTestCase):
    """ Base class for tests. """

    @property
    def datetime_expr(self):
        return r"^([\d-]+\s+[\d:]+)"

    @staticmethod
    def get_date(date):
        cmd = ["date", "--utc", f"--date={date}",
               '+' + TimestampSimple.DEFAULT_DATETIME_FORMAT]
        out = subprocess.check_output(cmd)
        out = re.compile(r"\s+").sub(' ', out.decode('UTF-8')).strip()
        return out

    def setUp(self):
        super().setUp()
        self.current_date = self.get_date("Thu Feb 10 16:19:17 UTC 2022")
        self.data_root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.data_root)
        super().tearDown()


class TestSearchKit(TestSearchKitBase):  # noqa,pylint: disable=too-many-public-methods
    """ Unit tests for searchkit. """

    def test_resultscollection(self):
        catalog = SearchCatalog()
        sd = SearchDef(r'.+ (\S+) \S+$')
        catalog.register(sd, 'a/path')
        rs = ResultStoreSimple()
        results = SearchResultsCollection(catalog, rs)
        self.assertEqual(len(results), 0)
        results.add(SearchResult(0, catalog.get_source_id('a/path'),
                                 re.match(sd.patterns[0], '1 2 3'),
                                 search_def=sd, results_store=rs).export)
        self.assertEqual(len(results), 1)
        for path, _results in results.items():
            self.assertEqual(path, 'a/path')
            self.assertEqual(_results[0].get(1), '2')

    def test_simple_search(self):
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            fpaths = [os.path.join(dtmp, fname) for fname in ['f1', 'f2']]
            for fpath in fpaths:
                with open(fpath, 'w', encoding='utf-8') as fd:
                    fd.write("a key: some value\n")
                    fd.write("a key: another value\n")

                    f.add(SearchDef(r'.+:\s+(\S+) \S+', tag='simple'), fpath)

            results = f.run()

            self.assertEqual(len(results), 4)
            self.assertEqual(len(results.find_by_tag('simple')), 4)
            for path, _results in results.items():
                self.assertTrue(path in fpaths)
                self.assertEqual(len(_results), 2)

                self.assertEqual(len(_results[0]), 1)
                self.assertEqual(len(_results[1]), 1)

                self.assertEqual(repr(_results[0]),
                                 "ln:1 1='some' (section=None)")
                self.assertEqual(repr(_results[1]),
                                 "ln:2 1='another' (section=None)")

                self.assertEqual(_results[0].get(1), "some")
                self.assertEqual(_results[1].get(1), "another")

                # test iterating over results
                for r in _results[0]:
                    self.assertEqual(r, "some")

                for r in _results[1]:
                    self.assertEqual(r, "another")

    def test_simple_search_zero_length_files_only(self):
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            for i in range(30):
                with open(os.path.join(dtmp, str(i)), 'w',
                          encoding='utf-8') as fd:
                    fd.write('')

            f.add(SearchDef(r'.+:\s+(\S+) \S+', tag='simple'), dtmp + '/*')
            results = f.run()

        self.assertEqual(len(results), 0)
        self.assertEqual(len(results.find_by_tag('simple')), 0)

    def test_simple_search_zero_length_files_mixed(self):
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            for i in range(30):
                with open(os.path.join(dtmp, str(i)), 'w',
                          encoding='utf-8') as fd:
                    fd.write('')

            for i in range(30, 60):
                with open(os.path.join(dtmp, str(i)), 'w',
                          encoding='utf-8') as fd:
                    fd.write("a key: foo bar\n")

            f.add(SearchDef(r'.+:\s+(\S+) \S+', tag='simple'), dtmp + '/*')
            results = f.run()

        self.assertEqual(len(results), 30)
        self.assertEqual(len(results.find_by_tag('simple')), 30)

    def test_simple_search_many_files(self):
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            for i in range(1000):
                with open(os.path.join(dtmp, str(i)), 'w',
                          encoding='utf-8') as fd:
                    fd.write("a key: foo bar\n")
                    for i in range(1000):
                        fd.write("some extra text\n")
                    fd.write("a key: bar foo\n")

            f.add(SearchDef(r'.+:\s+(\S+) \S+', tag='simple'), dtmp + '/*')
            results = f.run()

        self.assertEqual(len(results), 2000)
        self.assertEqual(len(results.find_by_tag('simple')), 2000)

    def test_simple_search_named_fields_no_types(self):
        f = FileSearcher()
        with tempfile.NamedTemporaryFile() as ftmp:
            with open(ftmp.name, 'w', encoding='utf-8') as fd:
                fd.write("  PID TTY          TIME CMD\n"
                         "49606 pts/2    00:00:00 bash\n"
                         "49613 pts/2    00:00:00 ps\n")

            fields = ResultFieldInfo(['PID', 'TTY', 'TIME', 'CMD'])
            f.add(SearchDef(r'\s*(\d+)\s+(\S+)\s+([0-9:]+)\s+(\S+)',
                            tag='simple', field_info=fields), ftmp.name)
            results = f.run()

            self.assertEqual(len(results), 2)
            self.assertEqual(len(results.find_by_tag('simple')), 2)
            self.assertEqual(len(results.find_by_path(fd.name)), 2)
            for r in results.find_by_tag('simple'):
                for pid, tty, time, cmd in [r, (r.PID, r.TTY, r.TIME, r.CMD)]:
                    self.assertTrue(pid in ['49606', '49613'])
                    self.assertEqual(tty, 'pts/2')
                    self.assertEqual(time, '00:00:00')
                    self.assertTrue(cmd in ['bash', 'ps'])

    def test_simple_search_named_fields_w_types(self):
        f = FileSearcher()
        with tempfile.NamedTemporaryFile() as ftmp:
            with open(ftmp.name, 'w', encoding='utf-8') as fd:
                fd.write("  PID TTY          TIME CMD\n"
                         "49606 pts/2    00:00:00 bash\n"
                         "49613 pts/2    00:00:00 ps\n")

            fields = ResultFieldInfo({'PID': int, 'TTY': str, 'TIME': str,
                                      'CMD': str})
            self.assertEqual(set(fields.values()), set([str, int]))
            f.add(SearchDef(r'\s*(\d+)\s+(\S+)\s+([0-9:]+)\s+(\S+)',
                            tag='simple', field_info=fields), ftmp.name)
            results = f.run()

            self.assertEqual(len(results), 2)
            self.assertEqual(len(results.find_by_tag('simple')), 2)
            self.assertEqual(len(results.find_by_path(fd.name)), 2)
            for r in results.find_by_tag('simple'):
                for pid, tty, time, cmd in [r, (r.PID, r.TTY, r.TIME, r.CMD)]:
                    self.assertTrue(pid in [49606, 49613])
                    self.assertEqual(tty, 'pts/2')
                    self.assertEqual(time, '00:00:00')
                    self.assertTrue(cmd in ['bash', 'ps'])

    def test_simple_search_named_fields_w_types_orderered_dict(self):
        f = FileSearcher()
        with tempfile.NamedTemporaryFile() as ftmp:
            with open(ftmp.name, 'w', encoding='utf-8') as fd:
                fd.write("  PID TTY          TIME CMD\n"
                         "49606 pts/2    00:00:00 bash\n"
                         "49613 pts/2    00:00:00 ps\n")

            fields = ResultFieldInfo(OrderedDict({'PID': int, 'TTY': str,
                                                  'TIME': str,
                                                  'CMD': str}))
            self.assertEqual(set(fields.values()), set([str, int]))
            f.add(SearchDef(r'\s*(\d+)\s+(\S+)\s+([0-9:]+)\s+(\S+)',
                            tag='simple', field_info=fields), ftmp.name)
            results = f.run()

            self.assertEqual(len(results), 2)
            self.assertEqual(len(results.find_by_tag('simple')), 2)
            self.assertEqual(len(results.find_by_path(fd.name)), 2)
            for r in results.find_by_tag('simple'):
                for pid, tty, time, cmd in [r, (r.PID, r.TTY, r.TIME, r.CMD)]:
                    self.assertTrue(pid in [49606, 49613])
                    self.assertEqual(tty, 'pts/2')
                    self.assertEqual(time, '00:00:00')
                    self.assertTrue(cmd in ['bash', 'ps'])

    def test_large_sequence_search(self):
        seq = SequenceSearchDef(start=SearchDef(r'(HEADER)'),
                                body=SearchDef(r'(\d+)'),
                                end=SearchDef(r'(FOOTER)'),
                                tag='myseq')
        simple = SearchDef(r'(\d+)', tag='simple')
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            try:
                for i in range(20):
                    fpath = os.path.join(dtmp, f'f{i}')
                    with open(fpath, 'w', encoding='utf-8') as fd:
                        fd.write('HEADER\n')
                        for _ in range(1000):
                            # this should be almost 100% deduped
                            fd.write(f'{1234}\n')

                        fd.write('FOOTER\n')

                    # simple search
                    f.add(simple, fpath)

                    # sequence search
                    f.add(seq, fpath)

                results = f.run()
            finally:
                shutil.rmtree(dtmp)

            self.assertEqual(f.stats['parts_deduped'], 40037)
            self.assertEqual(f.stats['parts_non_deduped'], 3)

        self.assertEqual(len(results), 40040)
        self.assertEqual(len(results.find_by_tag('simple')), 20000)
        self.assertEqual(len(results.find_sequence_by_tag('myseq')), 20)
        for section in results.find_sequence_by_tag('myseq').values():
            for r in section:
                if r.tag == seq.start_tag:
                    self.assertEqual(r.get(1), 'HEADER')
                elif r.tag == seq.end_tag:
                    self.assertEqual(r.get(1), 'FOOTER')
                elif r.tag != seq.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")
                else:
                    self.assertEqual(r.get(1), '1234')

    @mock.patch.object(os, "environ", {})
    @mock.patch.object(os, "cpu_count")
    def test_num_parallel_tasks_no_override(self, mock_cpu_count):
        mock_cpu_count.return_value = 3
        with mock.patch.object(FileSearcher, 'files', range(4)):
            s = FileSearcher()
            self.assertEqual(s.num_parallel_tasks, 3)

    @mock.patch.object(os, "environ", {})
    @mock.patch.object(os, "cpu_count")
    def test_num_parallel_tasks_files_capped(self, mock_cpu_count):
        mock_cpu_count.return_value = 3
        with mock.patch.object(FileSearcher, 'files', range(2)):
            s = FileSearcher()
            self.assertEqual(s.num_parallel_tasks, 2)

    @mock.patch.object(os, "cpu_count")
    def test_num_parallel_tasks_w_override(self, mock_cpu_count):
        mock_cpu_count.return_value = 3
        with mock.patch.object(FileSearcher, 'files', range(4)):
            s = FileSearcher(max_parallel_tasks=2)
            self.assertEqual(s.num_parallel_tasks, 2)

    def test_filesearcher_error(self):
        s = FileSearcher()
        with mock.patch.object(SearchResult, '__init__') as mock_init:

            def fake_init(*args, **kwargs):
                raise EOFError("some error")

            mock_init.side_effect = fake_init
            path = os.path.join(self.data_root)
            s.add(SearchDef("."), path)
            s.run()

    def test_logrotatelogsort(self):
        ordered_contents = []
        with tempfile.TemporaryDirectory() as dtmp:
            os.mknod(os.path.join(dtmp, "my-test-agent.log"))
            ordered_contents.append("my-test-agent.log")
            os.mknod(os.path.join(dtmp, "my-test-agent.log.1"))
            ordered_contents.append("my-test-agent.log.1")
            # add in an erroneous file that does not follow logrotate format
            os.mknod(os.path.join(dtmp, "my-test-agent.log.tar.gz"))
            for i in range(2, 100):
                fname = f"my-test-agent.log.{i}.gz"
                os.mknod(os.path.join(dtmp, fname))
                ordered_contents.append(fname)
                self.assertEqual(logrotate_log_sort(fname), i)

            ordered_contents.append("my-test-agent.log.tar.gz")

            contents = os.listdir(dtmp)
            self.assertEqual(sorted(contents,
                                    key=logrotate_log_sort),
                             ordered_contents)

    def test_catalog_user_paths_overlap(self):
        with tempfile.TemporaryDirectory() as dtmp:
            logspath = os.path.join(dtmp, 'var/log')
            os.makedirs(logspath)
            logpath = os.path.join(logspath, 'foo.log')
            with open(logpath, 'w', encoding='utf-8') as fd:
                fd.write('blah')

            catalog = SearchCatalog(max_logrotate_depth=1)
            s1 = SearchDef(r'.+', tag=1)
            catalog.register(s1, os.path.join(logspath, '*.log'))
            s2 = SearchDef(r'.+', tag=2)
            catalog.register(s2, os.path.join(logspath, 'foo*.log'))
            self.assertEqual(len(catalog), 1)
            self.assertEqual(list(catalog),
                             [{'source_id': catalog.get_source_id(logpath),
                               'path': logpath,
                               'searches': [s1, s2]}])

    def test_catalog_glob_filesort(self):
        dir_contents = []
        with tempfile.TemporaryDirectory() as dtmp:
            dir_contents.append(os.path.join(dtmp, "my-test-agent.0.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.1.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.1.log.1"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.2.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.16.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.49.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.49.log.1"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.77.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.100.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.100.log.1"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.110.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.142.log"))
            dir_contents.append(os.path.join(dtmp, "my-test-agent.183.log"))
            for e in dir_contents:
                os.mknod(e)

            max_logrotate_depth = 7
            for i in range(2, max_logrotate_depth + 10):
                fname = os.path.join(dtmp, f"my-test-agent.1.log.{i}.gz")
                os.mknod(fname)
                if i <= max_logrotate_depth:
                    dir_contents.append(fname)

            for i in range(2, max_logrotate_depth + 10):
                fname = os.path.join(dtmp, f"my-test-agent.49.log.{i}.gz")
                os.mknod(fname)
                if i <= max_logrotate_depth:
                    dir_contents.append(fname)

            for i in range(2, max_logrotate_depth + 10):
                fname = os.path.join(dtmp, f"my-test-agent.100.log.{i}.gz")
                os.mknod(fname)
                if i <= max_logrotate_depth:
                    dir_contents.append(fname)

            exp = sorted(dir_contents)
            path = os.path.join(dtmp, 'my-test-agent*.log*')
            depth = max_logrotate_depth
            act = sorted(SearchCatalog(max_logrotate_depth=depth).  # noqa,pylint: disable=protected-access
                         _filtered_dir(glob.glob(path)))
            self.assertEqual(act, exp)

    @utils.create_files({'atestfile': SEQ_TEST_1})
    def test_sequence_searcher(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                             r"^a\S* (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test1")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test1')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "start")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_2,
                         'atestfile2': SEQ_TEST_2})
    def test_sequence_searcher_overlapping(self):
        """
        NOTE: tests searches in parallel.
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test2")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        s.add(sd, path=os.path.join(self.data_root, 'atestfile2'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test2')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_3,
                         'atestfile2': SEQ_TEST_3})
    def test_sequence_searcher_overlapping_incomplete(self):
        """
        NOTE: tests searches in parallel.
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test3")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        s.add(sd, path=os.path.join(self.data_root, 'atestfile2'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test3')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_4})
    def test_sequence_searcher_incomplete_eof_match(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"value is (\S+)"),
                               end=SearchDef(r"^$"),
                               tag="seq-search-test4")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test4')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.body_tag:
                    self.assertEqual(r.get(1), "3")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(0), "")
                else:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_5})
    def test_sequence_searcher_multiple_sections(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"value is (\S+)"),
                               end=SearchDef(r"^$"),
                               tag="seq-search-test5")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test5')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.body_tag:
                    self.assertTrue(r.get(1) in ["3", "4"])
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(0), "")
                else:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_6})
    def test_sequence_searcher_eof(self):
        """
        Test scenario:
         * multiple sections that end with start of the next
         * start def matches any start
         * end def matches any start
         * file ends before start of next
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(r"^section (\d+)"),
                               body=SearchDef(r"\d_\d"),
                               tag="seq-search-test6")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test6')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    section = r.get(1)
                    self.assertTrue(r.get(1) in ["1", "2"])
                elif r.tag == sd.body_tag:
                    if section == "1":
                        self.assertTrue(r.get(0) in ["1_1", "1_2"])
                    else:
                        self.assertTrue(r.get(0) in ["2_1"])
                elif r.tag != sd.end_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_7})
    def test_sequence_searcher_section_start_end_same(self):
        """
        Test scenario:
         * multiple sections that end with start of the next
         * start def matches unique start
         * end def matches any start
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(r"^section (2)"),
                               body=SearchDef(r"\d_\d"),
                               end=SearchDef(r"^section (\d+)"),
                               tag="seq-search-test7")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test7')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "2")
                elif r.tag == sd.body_tag:
                    self.assertTrue(r.get(0) in ["2_1"])
                elif r.tag != sd.end_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': MULTI_SEQ_TEST})
    def test_sequence_searcher_multi_sequence(self):
        """
        Test scenario:
         * search containing multiple seqeunce definitions
         * data containing 2 results of each where one is incomplete
         * test that single incomplete result gets removed
        """
        s = FileSearcher()
        sda = SequenceSearchDef(start=SearchDef(r"^sectionA (\d+)"),
                                body=SearchDef(r"\d_\d"),
                                end=SearchDef(
                                            r"^section\S+ (\d+)"),
                                tag="seqA-search-test")
        sdb = SequenceSearchDef(start=SearchDef(r"^sectionB (\d+)"),
                                body=SearchDef(r"\d_\d"),
                                end=SearchDef(
                                            r"^section\S+ (\d+)"),
                                tag="seqB-search-test")
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sda, path=fname)
        s.add(sdb, path=fname)
        results = s.run()
        sections = results.find_sequence_by_tag('seqA-search-test')
        self.assertEqual(len(sections), 1)
        sections = results.find_sequence_by_tag('seqB-search-test')
        self.assertEqual(len(sections), 2)

    @utils.create_files({'atestfile': LOGS_W_TS})
    def test_logs_since_single_valid(self):
        """
        Test scenario:
        """
        self.current_date = self.get_date('Wed Jan 10 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Wed Jan 1 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 09 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 01 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 15 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 01 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 01 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 03 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 09 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 09 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 09 00:00:00 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
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
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
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

    def test_search_result_index(self):
        sri = ResultStoreSimple()
        for val in ['foo', 'bar', 'foo']:
            sri.add('atag', None, val)

        self.assertEqual(sri, ['foo', 'bar'])
        self.assertEqual(sri.counters, {0: 2, 1: 1})
        self.assertEqual(sri.tag_store, ['atag'])
        self.assertEqual(sri[0], 'foo')

        self.assertEqual(sri[1], 'bar')
        self.assertEqual(sri[2], None)
        self.assertEqual(sri.parts_deduped, 1)
        self.assertEqual(sri.parts_non_deduped, 2)

    def test_search_unicode_decode_w_error(self):
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'wb') as fd:
                fd.write(b'\xe2')

            f.add(SearchDef(r'(.+)', tag='simple'), fpath)
            with self.assertRaises(UnicodeDecodeError):
                f.run()

    @staticmethod
    def test_search_unicode_decode_no_error():
        f = FileSearcher(decode_errors='backslashreplace')
        with tempfile.TemporaryDirectory() as dtmp:
            fpath = os.path.join(dtmp, 'f1')
            with open(fpath, 'wb') as fd:
                fd.write(b'\xe2')

            f.add(SearchDef(r'(.+)', tag='simple'), fpath)
            f.run()
