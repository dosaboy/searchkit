""" Searchkit unit tests. """
import glob
import logging
import multiprocessing
import os
import queue
import re
import signal
import tempfile
import threading
from collections import OrderedDict
from unittest import mock

import psutil
from searchkit import (
    FileSearcher,
    ResultFieldInfo,
    SearchDef,
)
from searchkit.search import (
    logrotate_log_sort,
    SearchCatalog,
    SearchResultsCollection,
    ThreadManager,
)
from searchkit.results_store import (
    ResultStoreParallel,
    ResultStoreSimple,
)
from searchkit.exception import FileSearchException, ResultsPurgeTimeout
from searchkit.log import log
from searchkit.result import SearchResult
from searchkit.task import SearchTaskStats

from . import utils


class TestSearchKit(utils.TestSearchKitBase):  # noqa,pylint: disable=too-many-public-methods
    """ Unit tests for searchkit. """

    def test_resultscollection(self):
        catalog = SearchCatalog()
        sd = SearchDef(r'.+ (\S+) \S+$')
        catalog.register(sd, 'a/path')
        rs = ResultStoreSimple()
        results = SearchResultsCollection(catalog, rs)
        self.assertEqual(len(results), 0)
        result = SearchResult(0, catalog.get_source_id('a/path'),
                              re.match(sd.patterns[0], '1 2 3'),
                              search_def=sd, results_store=rs).export
        results.add([result])
        self.assertEqual(len(results), 1)
        for path, _results in results.items():
            self.assertEqual(path, 'a/path')
            self.assertEqual(_results[0].get(1), '2')

    @staticmethod
    def _purge_setup(num_queued):
        catalog = SearchCatalog()
        sd = SearchDef(r'.+ (\S+) \S+$')
        catalog.register(sd, 'a/path')
        rs = ResultStoreSimple()
        results = SearchResultsCollection(catalog, rs)
        batches = []
        for i in range(num_queued):
            result = SearchResult(i, catalog.get_source_id('a/path'),
                                  re.match(sd.patterns[0], '1 2 3'),
                                  search_def=sd, results_store=rs).export
            batches.append([result])

        return results, batches

    def test_purge_results_drains_to_expected(self):
        results, batches = self._purge_setup(3)
        results_queue = mock.MagicMock()
        results_queue.get.side_effect = batches

        FileSearcher._purge_results(results, results_queue, 3)  # noqa,pylint: disable=protected-access
        self.assertEqual(len(results), 3)
        self.assertEqual(results_queue.get.call_count, 3)

    def test_purge_results_already_complete(self):
        results, batches = self._purge_setup(2)
        results.add(batches[0])
        results.add(batches[1])
        results_queue = mock.MagicMock()

        FileSearcher._purge_results(results, results_queue, 2)  # noqa,pylint: disable=protected-access
        self.assertEqual(results_queue.get.call_count, 0)

    def test_purge_results_timeout(self):
        results, batches = self._purge_setup(1)
        results_queue = mock.MagicMock()
        results_queue.get.side_effect = [batches[0], queue.Empty]

        with self.assertRaises(ResultsPurgeTimeout):
            FileSearcher._purge_results(results, results_queue, 3)  # noqa,pylint: disable=protected-access

        # one result drained, then a single timeout and no retry
        self.assertEqual(len(results), 1)
        self.assertEqual(results_queue.get.call_count, 2)

    def test_purge_results_timeout_empty_queue(self):
        results, _ = self._purge_setup(0)
        results_queue = mock.MagicMock()
        results_queue.get.side_effect = queue.Empty

        with self.assertRaises(ResultsPurgeTimeout):
            FileSearcher._purge_results(results, results_queue, 1)  # noqa,pylint: disable=protected-access

        self.assertEqual(results_queue.get.call_count, 1)

    def test_info_thread_stops_while_store_lock_held(self):
        """
        Ensure the info thread can still be stopped when the results store
        lock is held indefinitely, as happens when a worker is killed while
        holding it. The thread's lock wait must be bounded so that it notices
        the stop event instead of blocking forever.
        """
        with multiprocessing.Manager() as mgr:
            rs = ResultStoreParallel(mgr)
            real_lock = rs.lock
            attempted = threading.Event()

            def spy_acquire(*args, **kwargs):
                """ Record that an acquisition was attempted, then delegate to
                the real lock so the held-lock behaviour is preserved. """
                attempted.set()
                return real_lock.acquire(*args, **kwargs)

            rs.lock = mock.MagicMock(wraps=real_lock)
            rs.lock.acquire.side_effect = spy_acquire
            results = SearchResultsCollection(SearchCatalog(), rs)
            thread = ThreadManager(
                'info', FileSearcher._get_info,  # noqa,pylint: disable=protected-access
                [results, rs, SearchTaskStats(), psutil.Process()])
            # simulate a lock abandoned by a killed worker
            real_lock.acquire()
            try:
                thread.start()
                self.assertTrue(attempted.wait(timeout=10))
                thread.event.set()
                thread.thread.join(timeout=10)
                self.assertFalse(thread.thread.is_alive())
            finally:
                real_lock.release()
                thread.event.set()
                thread.thread.join(timeout=10)

    def test_run_mp_error_kills_workers(self):
        for name in ['f1', 'f2']:
            with open(os.path.join(self.data_root, name), 'w',
                      encoding='utf-8') as fd:
                fd.write("a key: some value\n")

        s = FileSearcher()
        s.add(SearchDef(r'.+:\s+(\S+) \S+'), self.data_root + '/*')
        killer = mock.MagicMock(
            side_effect=FileSearcher._ensure_worker_processes_killed)  # noqa,pylint: disable=protected-access
        with mock.patch.object(FileSearcher, '_ensure_worker_processes_killed',
                               killer):
            with mock.patch.object(SearchTaskStats, 'update',
                                   side_effect=RuntimeError('boom')):
                with self.assertRaises(RuntimeError):
                    s.run()

        self.assertEqual(killer.call_count, 2)

        # a subsequent search must not be affected by the failed one
        s = FileSearcher()
        s.add(SearchDef(r'.+:\s+(\S+) \S+'), self.data_root + '/*')
        self.assertEqual(len(s.run()), 2)

    def test_run_mp_worker_dies_holding_store_lock(self):
        """
        A worker that exits abruptly while holding the results store lock must
        result in run() raising FileSearchException (via BrokenProcessPool)
        rather than hanging, and must not affect a subsequent search.
        """
        for name in ['f1', 'f2']:
            with open(os.path.join(self.data_root, name), 'w',
                      encoding='utf-8') as fd:
                fd.write("a key: some value\n")

        def die_holding_lock(store):
            store.lock.acquire()
            os.kill(os.getpid(), signal.SIGKILL)

        def on_timeout(_signum, _frame):
            raise AssertionError("run() did not terminate in time")

        old_handler = signal.signal(signal.SIGALRM, on_timeout)
        self.addCleanup(signal.signal, signal.SIGALRM, old_handler)
        self.addCleanup(signal.alarm, 0)

        s = FileSearcher()
        s.add(SearchDef(r'.+:\s+(\S+) \S+'), self.data_root + '/*')
        # workers are forked so they inherit the patched sync()
        with mock.patch.object(ResultStoreParallel, 'sync',
                               autospec=True,
                               side_effect=die_holding_lock):
            signal.alarm(60)
            try:
                with self.assertRaises(FileSearchException):
                    s.run()
            finally:
                signal.alarm(0)

        # a subsequent search must not be affected by the abandoned lock
        signal.alarm(60)
        s = FileSearcher()
        s.add(SearchDef(r'.+:\s+(\S+) \S+'), self.data_root + '/*')
        self.assertEqual(len(s.run()), 2)
        signal.alarm(0)

    def _force_log_level(self, level):
        """ Set the searchkit log level for this test only, restoring the
        original on cleanup. This keeps tests independent of the
        TESTS_LOG_LEVEL_DEBUG environment setting. """
        self.addCleanup(log.setLevel, log.level)
        log.setLevel(level)

    @staticmethod
    def _run_timing_search(path):
        """ Search path for lines ending in a multiple of 1000 using a single
        process.

        @param path: file to search
        @return: tuple of (list of (line number, value) results,
                 lines_searched)
        """
        s = FileSearcher()
        s.add(SearchDef(r'^line (\d*000)$', tag='t'), path)
        results = s.run()
        found = [(r.linenumber, r.get(1)) for r in results.find_by_tag('t')]
        return found, s.stats['lines_searched']

    def test_search_timings_debug_matches_non_debug(self):
        """
        Check the DEBUG-only per-line timing path using a mocked clock: the
        max/avg output must be correct either side of the 100000 line chunk
        boundary and results/lines_searched must match the non-debug path.
        """
        num_lines = 100001
        path = os.path.join(self.data_root, 'big')
        with open(path, 'w', encoding='utf-8') as fd:
            for i in range(1, num_lines + 1):
                fd.write(f"line {i}\n")

        # per-line durations (ms); everything else takes 0.5ms
        durations = {5: 2.0, 99999: 3.0, 100000: 5.0, 100001: 1.0}

        def fake_clock():
            """ Yield a start and finish time for each line such that the
            elapsed time of each line is as defined in durations. """
            for ln in range(1, num_lines + 1):
                start = float(ln)
                yield start
                yield start + durations.get(ln, 0.5) / 1000

        self._force_log_level(logging.INFO)
        expected, expected_lines = self._run_timing_search(path)
        self.assertEqual(expected_lines, num_lines)
        self.assertEqual(len(expected), 100)

        with mock.patch('searchkit.task.time') as mock_time:
            mock_time.perf_counter.side_effect = fake_clock()
            with self.assertLogs('searchkit', level='DEBUG') as logs:
                found, lines = self._run_timing_search(path)

        self.assertEqual(found, expected)
        self.assertEqual(lines, expected_lines)

        # two clock reads per line, none more
        self.assertEqual(mock_time.perf_counter.call_count, num_lines * 2)

        chunk_msgs = [m for m in logs.output if ' lines searched in ' in m]
        self.assertEqual(len(chunk_msgs), 1)
        self.assertIn('100000 lines searched in ', chunk_msgs[0])
        # only lines 1..99999 are included in the first chunk
        self.assertIn('(slowest (avg/max) 3.0/3.0ms)', chunk_msgs[0])

        final_msgs = [m for m in logs.output if 'completed search of' in m]
        self.assertEqual(len(final_msgs), 1)
        self.assertIn('completed search of 100001 lines', final_msgs[0])
        # chunk maxes are 3.0 and 5.0 hence avg 4.0 and overall max 5.0
        self.assertIn('(slowest (avg/max) 4.0/5.0ms', final_msgs[0])

    def test_search_timings_not_used_when_not_debug(self):
        """
        Check that with INFO logging the per-line timing path is skipped
        (the clock is never read) while results and lines_searched are still
        correct.
        """
        self._force_log_level(logging.INFO)
        path = os.path.join(self.data_root, 'small')
        with open(path, 'w', encoding='utf-8') as fd:
            fd.write("line 1000\nline 2\n")

        with mock.patch('searchkit.task.time') as mock_time:
            found, lines = self._run_timing_search(path)

        self.assertEqual(found, [(1, '1000')])
        self.assertEqual(lines, 2)
        self.assertFalse(mock_time.perf_counter.called)

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

    def test_search_result_index(self):
        sri = ResultStoreSimple()
        for val in ['foo', 'bar', 'foo']:
            sri.add('atag', None, val)

        self.assertEqual(list(sri.values()), ['foo', 'atag', 'bar'])
        self.assertEqual(sri.tag_store, {'atag': 1})
        self.assertEqual(sri[0], 'foo')

        self.assertEqual(sri[2], 'bar')
        self.assertEqual(sri.get(109238), None)

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
