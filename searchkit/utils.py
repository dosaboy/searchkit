""" Search utilities. """
import abc
import dbm
import os
import shelve
import time
from contextlib import ContextDecorator
from functools import cached_property

import fasteners
from searchkit.log import log


class MPCacheBase(ContextDecorator):
    """
    A multiprocessing safe key/value store cache.

    Saves data to disk and coordinates access using a lock that exists in a
    path that must be global to all process using this cache.
    """
    def __init__(self, cache_id, cache_type, global_path):
        """
        @param cache_id: A unique name for this cache.
        @param cache_type: A name given to this type of cache.
        @param global_path: Path shared across all processes using this cache.
        """
        self.cache_id = cache_id
        self.cache_type = cache_type
        self.global_path = global_path
        locks_path = os.path.join(self.global_path, 'locks')
        path = os.path.join(locks_path, 'cache_all_global.lock')
        self.global_lock = fasteners.InterProcessLock(path)
        path = os.path.join(locks_path, f'cache_{self.cache_id}.lock')
        self.cache_lock = fasteners.InterProcessLock(path)

    def __enter__(self):
        return self

    @abc.abstractmethod
    def __exit__(self, *exc_info):
        pass

    @cached_property
    def cache_base_path(self):
        path = os.path.join(self.global_path, 'caches', self.cache_type,
                            self.cache_id)
        with self.global_lock:
            if not os.path.isdir(path):
                os.makedirs(path)

        return path

    @abc.abstractmethod
    def get(self, key):
        """ Get value from cache using key. """

    @abc.abstractmethod
    def set(self, key, value):
        """ Set value in cache using key. """

    @abc.abstractmethod
    def bulk_set(self, data):
        """ Set one or more key/value in cache. """

    @abc.abstractmethod
    def unset(self, key):
        """ Remove key/value from cache. """

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass


class MPCacheSimple(MPCacheBase):
    """ Multiprocessing safe simple key/value store used to cache values. """
    def __exit__(self, *exc_info):
        """ noop. """

    def get(self, key):
        """
        Python shelve is not MP safe so if two processes try to access the same
        cache at once only one will succeed. We retry a fixed number of times
        and bail if not successful.
        """
        max_open_retry = 10
        with self.cache_lock:
            path = os.path.join(self.cache_base_path, key)
            attempt = 0
            while True:
                try:
                    with shelve.open(path) as db:
                        return db.get('0')
                except dbm.gnu.error:
                    log.debug("error opening cache %s - sleeping 10s then "
                              "retrying (attempt %s/%s)", path, attempt,
                              max_open_retry)
                    time.sleep(10)
                    attempt += 1
                    if attempt > max_open_retry:
                        raise

    def bulk_set(self, data):
        with self.cache_lock:
            for key, value in data.items():
                with shelve.open(os.path.join(self.cache_base_path,
                                              key)) as db:
                    db['0'] = value

    def set(self, key, value):
        with self.cache_lock:
            with shelve.open(os.path.join(self.cache_base_path, key)) as db:
                db['0'] = value

    def unset(self, key):
        with self.cache_lock:
            with shelve.open(os.path.join(self.cache_base_path, key)) as db:
                del db[key]

    def __iter__(self):
        with self.cache_lock:
            for path in os.listdir(self.cache_base_path):
                with shelve.open(path) as db:
                    yield db.get('0')

    def __len__(self):
        with self.cache_lock:
            return len(os.listdir(self.cache_base_path))


class MPCache(MPCacheSimple):
    """ Denotes the default cache type. """
