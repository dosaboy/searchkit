""" Searchkit unit tests. """
import multiprocessing
import os

from searchkit.results_store import (
    ResultStoreSimple,
    ResultStoreParallel,
)

from . import utils


class TestResultStore(utils.BaseTestCase):
    """ Test results store implementations. """

    def rs_tests(self, rs):
        self.assertEqual(rs.add(None, None, 'foo'), (None, None, 0))
        self.assertEqual(rs.add(None, None, 'bar'), (None, None, 1))
        self.assertEqual(rs.add(None, None, 'foo'), (None, None, 0))
        self.assertEqual(rs.add('tag1', None, 'foo'), (2, None, 0))
        self.assertEqual(rs.add('tag1', 'seq1', 'foo'), (2, 3, 0))
        self.assertEqual(rs.add('tag1', 'seq1', 'foo'), (2, 3, 0))
        self.assertEqual(rs.add(None, None, 'xxx'), (None, None, 4))
        rs.sync()

    def test_resultstore_simple(self):
        rs = ResultStoreSimple()
        self.rs_tests(rs)
        self.assertEqual(rs[0], 'foo')
        self.assertEqual(rs[1], 'bar')
        self.assertEqual(rs[2], 'tag1')
        self.assertEqual(rs[3], 'seq1')

    def test_resultstore_parallel(self):
        with multiprocessing.Manager() as mgr:
            rs = ResultStoreParallel(mgr, prealloc_block_size=2)
            p = multiprocessing.Process(target=self.rs_tests, args=(rs, ))
            p.start()
            p.join()
            rs.unproxy_results()
            self.assertEqual(rs[0], 'foo')
            self.assertEqual(rs[1], 'bar')
            self.assertEqual(rs[4], 'xxx')
            self.assertEqual(rs[2], 'tag1')
            self.assertEqual(rs[3], 'seq1')

    def test_resultstore_parallel_prealloc(self):
        with multiprocessing.Manager() as mgr:
            rs = ResultStoreParallel(mgr, prealloc_block_size=10)
            rs.local  # pylint: disable=pointless-statement
            self.assertEqual(rs.preallocate(rs.prealloc_block_size),
                             list(range(10)))
            self.assertEqual(rs.preallocate(rs.prealloc_block_size),
                             list(range(10, 20)))

    def test_resultstore_parallel_alloc_next(self):
        # pylint: disable=protected-access
        with multiprocessing.Manager() as mgr:
            rs = ResultStoreParallel(mgr, prealloc_block_size=10)
            self.assertEqual(rs._allocate_next('foo'), 0)
            self.assertEqual(rs._allocate_next('foo'), 0)
            self.assertEqual(rs._allocate_next('bar'), 1)

    def test_resultstore_parallel_local(self):
        # pylint: disable=protected-access
        with multiprocessing.Manager() as mgr:
            rs = ResultStoreParallel(mgr, prealloc_block_size=10)
            self.assertEqual(rs.local, {})
            self.assertEqual(rs._local_store['owner'], os.getpid())
            self.assertEqual(rs.local, {})
