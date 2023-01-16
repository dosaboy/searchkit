import fasteners
import os
import pickle

from functools import cached_property

from searchkit.log import log


class MPCache(object):
    """
    A multiprocessing safe cache.
    """
    def __init__(self, id, root_dir, tmp_dir):
        self.id = id
        self.root_dir = root_dir
        self.tmp_dir = tmp_dir

    @property
    def _global_lock(self):
        """ Inter-process lock for all caches. """
        path = os.path.join(self.tmp_dir, 'locks',
                            'cache_all.lock')
        return fasteners.InterProcessLock(path)

    @property
    def _cache_lock(self):
        """ Inter-process lock for this cache. """
        path = os.path.join(self.tmp_dir, 'locks',
                            'cache_{}.lock'.format(self.id))
        return fasteners.InterProcessLock(path)

    @cached_property
    def cache_path(self):
        """
        Get cache path. Takes global cache lock to check root is created.
        """
        globaltmp = self.tmp_dir
        if globaltmp is None:
            log.warning("global tmp dir '%s' not setup")
            return

        dir = os.path.join(globaltmp, 'cache/{}'.format(self.root_dir))
        with self._global_lock:
            if not os.path.isdir(dir):
                os.makedirs(dir)

        return os.path.join(dir, self.id)

    def _get_unsafe(self, path):
        """
        Unlocked get not to be used without having first acquired the lock.
        """
        if not path or not os.path.exists(path):
            log.debug("no cache found at '%s'", path)
            return

        with open(path, 'rb') as fd:
            contents = pickle.load(fd)
            if not contents:
                return

            return contents

    def get(self, key):
        """ Get value for key. If not found returns None. """
        path = self.cache_path
        with self._cache_lock:
            log.debug("load from cache '%s' (key='%s')", path, key)
            contents = self._get_unsafe(path)
            if contents:
                return contents.get(key)

    def set(self, key, data):
        """ Set value for key. """
        path = self.cache_path
        with self._cache_lock:
            if not path:
                log.warning("invalid path '%s' - cannot save to cache", path)

            contents = self._get_unsafe(path)
            if contents:
                contents[key] = data
            else:
                contents = {key: data}

            log.debug("saving to cache '%s' (key=%s, items=%s)", path, key,
                      len(contents))
            with open(path, 'wb') as fd:
                pickle.dump(contents, fd)

            log.debug("cache id=%s size=%s", self.id, os.path.getsize(path))
