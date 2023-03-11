import os
import tempfile

from . import utils
from searchkit.utils import MPCache


class TestUtils(utils.BaseTestCase):

    def test_mpcache_single_file(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache = MPCache('testtype', 'testcache', dtmp,
                            use_single_file=True)
            cache.set('key1', 'value1')
            cache.set('key2', 'value2')
            cache_path = os.path.join(dtmp, 'caches', 'testcache', 'testtype')
            self.assertTrue(os.path.isfile(cache_path))
            self.assertEqual(cache.get('key1'), 'value1')
            self.assertEqual(cache.get('key2'), 'value2')
            self.assertEqual(cache.get('key3'), None)

    def test_mpcache_file_per_key(self):
        with tempfile.TemporaryDirectory() as dtmp:
            cache = MPCache('testtype', 'testcache', dtmp)
            cache.set('key1', 'value1')
            cache.set('key2', 'value2')
            cache_path = os.path.join(dtmp, 'caches', 'testcache', 'testtype')
            self.assertTrue(os.path.isdir(cache_path))
            self.assertTrue(len(os.listdir(cache_path)), 2)
            self.assertTrue(os.path.isfile(os.path.join(cache_path, 'key1')))
            self.assertTrue(os.path.isfile(os.path.join(cache_path, 'key2')))
            self.assertEqual(cache.get('key1'), 'value1')
            self.assertEqual(cache.get('key2'), 'value2')
            self.assertEqual(cache.get('key3'), None)
