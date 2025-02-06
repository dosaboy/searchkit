""" Search result store implementations. """
import copy
import multiprocessing
import os
from collections import UserDict

from searchkit.log import log
from searchkit.exception import ResultStoreException

RESULTS_STORE_LOCK = multiprocessing.Lock()


def rs_locked(f):
    def _rs_locked_inner(*args, **kwargs):
        with RESULTS_STORE_LOCK:
            return f(*args, **kwargs)

    return _rs_locked_inner


class ResultStoreBase(UserDict):
    """
    This class is used to de-duplicate values stored in search results such
    that allowing their reference to be saved in the result for later lookup.
    """
    def __init__(self, f_preallocator=None, prealloc_block_size=1000):
        super().__init__({})
        self.value_store = {}
        self.tag_store = {}
        self.sequence_id_store = {}
        self._allocations = None
        self.prealloc_block_size = prealloc_block_size
        self.f_preallocator = f_preallocator

    @property
    def allocations(self):
        if not self.f_preallocator:
            return None

        if self._allocations is None:
            self._allocations = self.f_preallocator(self.prealloc_block_size)
        elif (self.data and len(self.data) % self.prealloc_block_size == 0 and
              self._allocations[-1] in self.data):
            self._allocations = self.f_preallocator(
                self.prealloc_block_size)

        return self._allocations

    def _allocate_next(self, value):
        """
        Add to store and return position. If not already exists use next
        available slot.

        If t
        """
        for idx, _value in self.data.items():
            if value == _value:
                return idx

        if self.allocations:
            for idx in self.allocations:
                if idx not in self.data:
                    current = idx
                    break
            else:
                raise ResultStoreException("failed to get store allocation")
        else:
            current = len(self.data)

        self.data[current] = value
        # log.debug(f"{os.getpid()} allocated {current} for '{value}'")
        return current

    def _add_to_store(self, value, store, idx=None):
        """
        Add value to the provided store and return its position. If the value
        is None do not save in the store and return None.

        @param value: arbitrary value to store
        @param store: implementation of ResultStoreBase
        """
        if value is None:
            return None

        if value in store:
            return store[value]

        if idx is None:
            idx = self._allocate_next(value)

        store[value] = idx
        return idx

    def sync(self):
        """ Only required for parallel. """

    def add(self, tag, sequence_id, value):
        """
        Ensure the given values are saved in the store and return their
        position.

        Returns a tuple of references to the position in the store of each
        value. A ref value of None indicates that the value does not exist and
        is not stored.

        @param tag: optional search tag
        @param sequence_id: optional sequence search id
        @param value: search result value
        """
        value_idx = self._add_to_store(value, self.value_store)
        tag_idx = self._add_to_store(tag, self.tag_store)
        sequence_id_idx = self._add_to_store(sequence_id,
                                             self.sequence_id_store)
        return tag_idx, sequence_id_idx, value_idx


class ResultStoreSimple(ResultStoreBase):
    """ Store for use when sharing between processes is not needed. """


class ResultStoreParallel(ResultStoreBase):
    """ Store for use when sharing between processes is required. """

    def __init__(self, mgr, **kwargs):
        super().__init__(**kwargs)
        # Replace super attributes with MP-safe equivalents
        self.alloc_pointer = mgr.Value('i', 0)
        self.data = mgr.dict()
        self.value_store = mgr.dict()
        self.tag_store = mgr.dict()
        self.sequence_id_store = mgr.dict()
        # Set my worker process to a local ResultStoreSimple st6re. This is
        # used store/collect results locally so that we do not need to sync()
        # with the main store until the end which is a slow action.
        self._local_store = None

    def preallocate(self, size):
        with RESULTS_STORE_LOCK:
            current = self.alloc_pointer.value
            allocated = list(range(current, current + size))
            self.alloc_pointer.value += size
            log.debug("owner=%s allocated range: %s:%s",
                      self._local_store['owner'],
                      allocated[0], allocated[-1])

        return allocated

    def _allocate_next(self, value):
        """
        We allocate from the shared store but save to the local store. This way
        allocations are global/shared between all workers. When we finally sync
        the local store to the shared store the allocations will not conflict.
        """
        with RESULTS_STORE_LOCK:
            return super()._allocate_next(value)

    @property
    def local(self):
        """ Each worker process will end up with its own local store.

        Contents must be copied into the the shared store once finished by
        calling sync().
        """
        pid = os.getpid()
        if self._local_store is None:
            log.debug("new local store for pid=%s", pid)
            store = ResultStoreSimple(
                f_preallocator=self.preallocate,
                prealloc_block_size=self.prealloc_block_size)
            self._local_store = {'store': store, 'owner': pid}
        elif self._local_store['owner'] != pid:
            raise ResultStoreException("local store not created by current "
                                       f"pid ({pid})")

        return self._local_store['store']

    def add(self, *args, **kwargs):
        """ Add to local store. """
        return self.local.add(*args, **kwargs)

    @rs_locked
    def sync(self):
        """
        Sync local store data into the global/shared store. This must be called
        by the worker process that owns the local store once it has finished
        adding to the store.
        """
        log.debug("syncing local results to shared store")
        for idx, value in self.local.data.items():
            if value is not None:
                self.data[idx] = value

        for value, idx in self.local.value_store.items():
            self._add_to_store(value, self.value_store, idx=idx)

        for value, idx in self.local.tag_store.items():
            self._add_to_store(value, self.tag_store, idx=idx)

        for value, idx in self.local.sequence_id_store.items():
            self._add_to_store(value, self.sequence_id_store, idx=idx)

    @rs_locked
    def unproxy_results(self):
        """
        Converts internal stores to unproxied types so they can be accessed
        once their manager is gone.
        """
        log.debug("unproxying results store (values=%s, data=%s)",
                  len(self.value_store), len(self.data))
        self.data = copy.deepcopy(self.data)
        self.value_store = copy.deepcopy(self.value_store)
        self.tag_store = copy.deepcopy(self.tag_store)
        self.sequence_id_store = copy.deepcopy(self.sequence_id_store)
