import fasteners
import os
import pickle

from functools import cached_property

from searchkit.log import log


class MPCache(object):
    """
    A multiprocessing safe cache.

    Saves data to disk and coordinates access using a lock that exists in a
    path that much be global to all process using this cache. Cache content is
    structured as a dictionary and content is pickled before saving to disk.
    """
    def __init__(self, cache_id, cache_type, global_path,
                 use_single_file=False):
        """
        @param cache_id: A unique name for this cache.
        @param cache_type: A name given to this type of cache.
        @param global_path: Path shared across all processes using this cache.
        @param use_single_file: By default each key is stored as its own file
                                but if this is set to True a single file is
                                used to store all keys.
        """
        self.cache_id = cache_id
        self.cache_type = cache_type
        self.global_path = global_path
        self.file_per_key = not use_single_file

    @cached_property
    def _global_cache_lock(self):
        """ Inter-process lock for all caches. """
        path = os.path.join(self.global_path, 'locks', 'cache_all_global.lock')
        return fasteners.InterProcessLock(path)

    @cached_property
    def _cache_lock(self):
        """ Inter-process lock for this cache. """
        path = os.path.join(self.global_path, 'locks',
                            'cache_{}.lock'.format(self.cache_id))
        return fasteners.InterProcessLock(path)

    @cached_property
    def _cache_path(self):
        """
        Get cache path. Takes global cache lock to check root is created.
        """
        if self.global_path is None:
            log.warning("global path '%s' not setup - could not determine "
                        "cache path")
            return

        path = os.path.join(self.global_path, 'caches', self.cache_type,
                            self.cache_id)
        with self._global_cache_lock:
            if self.file_per_key:
                _dir = path
            else:
                _dir = os.path.dirname(path)

            if not os.path.isdir(_dir):
                os.makedirs(_dir)

        return path

    def _get_unsafe(self, path):
        """
        Unlocked get not to be used without having first acquired the lock.

        @param path: path to cache contents file.
        """
        if not path or not os.path.exists(path):
            log.debug("no cache found at '%s'", path)
            return

        with open(path, 'rb') as fd:
            try:
                contents = pickle.load(fd)
            except Exception:
                log.exception("failed to load contents from cache '%s'", path)
                contents = None

            if not contents:
                return

            return contents

    def get(self, key):
        """
        Get value for key

        @param key: key to lookup in cache.
        @return: value or None.
        """
        path = self._cache_path
        with self._cache_lock:
            log.debug("load from cache '%s' (key='%s')", path, key)
            if self.file_per_key:
                return self._get_unsafe(os.path.join(path, key))

            contents = self._get_unsafe(path)
            if contents:
                return contents.get(key)

    def set(self, key, value):
        """
        Set value for key.

        Cache contents are update as read-modify-write of entire contents.

        @param key: key under which we will store value.
        @param value: value we want to store.
        """
        path = self._cache_path
        if not path:
            log.warning("invalid path '%s' - cannot save to cache", path)
            return

        with self._cache_lock:
            if self.file_per_key:
                path = os.path.join(path, key)
                log.debug("saving to cache '%s' (key=%s)", path, key)
                contents = value
            else:
                contents = self._get_unsafe(path)
                if contents:
                    contents[key] = value
                else:
                    contents = {key: value}

                log.debug("saving to cache '%s' (key=%s, items=%s)", path, key,
                          len(contents))

            with open(path, 'wb') as fd:
                try:
                    pickle.dump(contents, fd)
                except Exception:
                    log.exception("failed to save contents to cache '%s'",
                                  path)

            log.debug("cache id=%s size=%s", self.cache_id,
                      os.path.getsize(path))
