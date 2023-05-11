import os
import shelve
import tempfile

from searchkit.utils import MPCache, MPCacheSharded

from . import utils


class TestUtils(utils.BaseTestCase):

    def test_mpcache_simple(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache = MPCache('testtype', 'testcache', dtmp)
            cache.set('key1', 'value1')
            cache.set('key2', 'value2')
            cache_path = os.path.join(dtmp, 'caches', 'testcache', 'testtype')
            self.assertTrue(os.path.isdir(cache_path))
            self.assertEqual(len(os.listdir(cache_path)), 2)
            self.assertTrue(os.path.isfile(os.path.join(cache_path, 'key1')))
            self.assertTrue(os.path.isfile(os.path.join(cache_path, 'key2')))
            self.assertEqual(cache.get('key1'), 'value1')
            self.assertEqual(cache.get('key2'), 'value2')
            self.assertEqual(cache.get('key3'), None)

    def test_mpcache_sharded(self):
        with tempfile.TemporaryDirectory() as dtmp:
            with MPCacheSharded('testtype', 'testcache', dtmp) as cache:
                cache.set(1, 'value1')
                cache.set(2, 'value2')
                cache.set(64 + 2, 'value64+2')
                cache.set(3, 'value3')
                cache.set(4, 'value4')
                cache.unset(4)
                self.assertEqual(len(cache), 4)
                self.assertEqual(sorted(list(cache)),
                                 sorted(['value1', 'value64+2', 'value2',
                                         'value3']))
                cache_path = os.path.join(dtmp, 'caches', 'testcache',
                                          'testtype')
                self.assertTrue(os.path.isdir(cache_path))
                self.assertEqual(len(os.listdir(cache_path)), 64)
                self.assertTrue(os.path.isfile(os.path.join(cache_path, '1')))
                self.assertTrue(os.path.isfile(os.path.join(cache_path, '2')))
                self.assertEqual(cache.get(1), 'value1')
                self.assertEqual(cache.get(2), 'value2')
                self.assertEqual(cache.get(3), 'value3')
                self.assertEqual(cache.get(4), None)
                cache.close()
                with shelve.open(os.path.join(cache_path, '1')) as db:
                    self.assertEqual(dict(db), {'1': 'value1'})

                with shelve.open(os.path.join(cache_path, '2')) as db:
                    self.assertEqual(dict(db), {'2': 'value2',
                                                '66': 'value64+2'})
