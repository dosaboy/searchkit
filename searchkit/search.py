""" Search implementations.

Searchkit supports searching one or more file in parallel using multiple
processes to perform the searches and then collecting their results.

Searches can be single or multi-line.
"""
import abc
import concurrent.futures
import copy
import glob
import multiprocessing
import os
import queue
import re
import signal
import subprocess
import threading
import time
from collections import namedtuple, UserDict
from datetime import datetime, timedelta

import psutil
from searchkit.log import log
from searchkit.constraints import CouldNotApplyConstraint
from searchkit.exception import FileSearchException, ResultStoreException
from searchkit.task import (
    RESULTS_QUEUE_SIZE,
    RESULTS_QUEUE_TIMEOUT,
    SearchTask,
    SearchTaskStats,
    SearchTaskResultsManager,
)
from searchkit.searchdef import SequenceSearchDef


RESULTS_COLLECTION_LOCK = multiprocessing.Lock()
RESULTS_STORE_LOCKED = multiprocessing.Lock()


def rs_locked(f):
    def _rs_locked_inner(*args, **kwargs):
        with RESULTS_STORE_LOCKED:
            return f(*args, **kwargs)

    return _rs_locked_inner


class ResultStoreBase(UserDict):
    """
    This class is used to de-duplicate values stored in search results such
    that allowing their reference to be saved in the result for later lookup.
    """

    def __init__(self, f_allocator=None):
        super().__init__({})
        self.value_store = {}
        self.tag_store = {}
        self.sequence_id_store = {}
        self.f_allocator = f_allocator or self._allocate_next

    def _allocate_next(self, value):
        for idx, _value in self.data.items():
            if value == _value:
                return idx

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
            idx = self.f_allocator(value)

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

    def __init__(self, mgr):
        super().__init__()
        # Replace super attributes with MP-safe equivalents
        self.data = mgr.dict()
        self.value_store = mgr.dict()
        self.tag_store = mgr.dict()
        self.sequence_id_store = mgr.dict()
        # Set my worker process to a local ResultStoreSimple st6re. This is
        # used store/collect results locally so that we do not need to sync()
        # with the main store until the end which is a slow action.
        self.local_store = None

    def _allocate_next(self, value):
        """
        We allocate from the shared store but save to the local store. This way
        allocations are global/shared between all workers. When we finally sync
        the local store to the shared store the allocations will not conflict.
        """
        with RESULTS_STORE_LOCKED:
            return super()._allocate_next(value)

    @property
    def local(self):
        """ Each worker process will end up with its own local store.

        Contents must be copied into the the shared store once finished by
        calling sync().
        """
        pid = os.getpid()
        if self.local_store is None:
            log.debug("new local store for pid=%s", pid)
            store = ResultStoreSimple(f_allocator=self._allocate_next)
            self.local_store = {'store': store, 'owner': pid}
        elif self.local_store['owner'] != pid:
            raise ResultStoreException("local store not created by current "
                                       f"pid ({pid})")

        return self.local_store['store']

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
        log.debug("unproxying results store (data=%s)", len(self.value_store))
        self.data = copy.deepcopy(self.data)
        self.value_store = copy.deepcopy(self.value_store)
        self.tag_store = copy.deepcopy(self.tag_store)
        self.sequence_id_store = copy.deepcopy(self.sequence_id_store)


class ResultFieldInfo(UserDict):
    """ Supports defining result field names to allow results to be retrieved
    by name as opposed to index. """
    def __init__(self, fields):
        """
        @param fields: list or dictionary of field names. If a dictionary is
                       provided, the values are expected to be functions that
                       the field value will be cast to. In other words these
                       should typically be standard or custom types.
        """
        if issubclass(fields.__class__, dict):
            data = fields
        else:
            data = {f: None for f in fields}

        super().__init__(data)

    def ensure_type(self, name, value):
        """
        If our fields have associated type functions, cast the value to
        its expected type.
        """
        if name not in self.data or self.data[name] is None:
            return value

        return self.data[name](value)

    def index_to_name(self, index):
        """ Retrieve a field name using the result group index. """
        for i, _field in enumerate(self.data):
            if index == i:
                return _field

        raise FileSearchException(f"field with index {index} not found in "
                                  "mapping")


class SearchResultsCollection(UserDict):
    """ Store for all search results received from worker jobs. Provides
    methods for searching for results. """
    def __init__(self, search_catalog, results_store):
        super().__init__()
        self.search_catalog = search_catalog
        self.results_store = results_store
        self.reset()

    def __getattribute__(self, name):
        if name != 'data':
            return super().__getattribute__(name)

        results = {}
        for path, _results in self._results_by_path.items():
            results[path] = _results

        return results

    @property
    def all(self):
        for results in self._results_by_path.values():
            yield from results

    def reset(self):
        self._results_by_path = {}

    @property
    def files(self):
        return list(self._results_by_path.keys())

    def add(self, results):
        for result in results:
            result.register_results_store(self.results_store)
            # resolve
            path = self.search_catalog.source_id_to_path(result.source_id)
            if path not in self._results_by_path:
                self._results_by_path[path] = [result]
            else:
                self._results_by_path[path].append(result)

    def find_by_path(self, path):
        """ Return results for a given path. """
        return self._results_by_path.get(path, [])

    def find_by_tag(self, tag, path=None):
        """ Return results matched by tag.

        @param tag: tag used to identify search results.
        @param path: optional path used to filter results to only include those
                     matched from a given path.
        """
        if path:
            paths = [path]
        else:
            paths = list(self._results_by_path.keys())

        results = []
        for _path in paths:
            for result in self.find_by_path(_path):
                if result.tag != tag:
                    continue

                results.append(result)

        return results

    def _get_all_sequence_results(self, path=None):
        """ Return a list of ids for all sequence match results.

        @param path: optionally filter results for a given path.
        """
        if path:
            paths = [path]
        else:
            paths = list(self._results_by_path.keys())

        sequences = []
        for _path in paths:
            for result in self.find_by_path(_path):
                if result.sequence_id is None:
                    continue

                sequences.append(result)

        return sequences

    def find_sequence_by_tag(self, tag, path=None):
        """ Find results for the sequence search(es) identified from tag.

        Returns a dictionary of "sections" i.e. complete sequences matched
        using associated SequenceSearchDef objects. Each section is a list of
        SearchResult objects representing start/body/end for that section.

        @param tag: tag used to identify sequence results.
        @param path: optionally filter results for a given path.
        """
        sections = {}
        for seq_obj in self.search_catalog.resolve_from_tag(tag):
            sections.update(self.find_sequence_sections(seq_obj, path))

        return sections

    def find_sequence_sections(self, sequence_obj, path=None):
        """ Find results for the given sequence search.

        Returns a dictionary of "sections" i.e. complete sequences matched
        using the associated SequenceSearchDef object. Each section is a list
        of SearchResult objects representing start/body/end for that section.

        @param sequence_obj: SequenceSearch object
        @param path: optionally filter results for a given path.
        """
        _results = {}
        for result in self._get_all_sequence_results(path=path):
            s_id = result.sequence_id
            if s_id != sequence_obj.id:
                continue

            section_id = result.section_id
            if section_id not in _results:
                _results[section_id] = []

            _results[section_id].append(result)

        return _results

    def __len__(self):
        """ Returns total number of search results. """
        _count = 0
        for f in self.files:
            _count += len(self.find_by_path(f))

        return _count


def logrotate_log_sort(fname):
    """
    Sort contents of a directory by passing the function as the key to a
    list sort. Directory is expected to contain logfiles with extensions
    used by logrotate e.g. .log, .log.1, .log.2.gz etc.
    """
    filters = [r"\S+\.log$",
               r"\S+\.log\.(\d+)$",
               r"\S+\.log\.(\d+)\.gz?$"]
    for f in filters:
        ret = re.compile(f).match(fname)
        if ret:
            break

    # files that don't follow logrotate naming format go to the end.
    if not ret:
        # put at the end
        return 100000

    if len(ret.groups()) == 0:
        return 0

    return int(ret.group(1))


class SearchCatalog():
    """ Catalog to register all searches to be performed. """
    def __init__(self, max_logrotate_depth=7):
        self.max_logrotate_depth = max_logrotate_depth
        self._source_ids = {}
        self._search_tags = {}
        self._simple_searches = {}
        self._sequence_searches = {}
        self._entries = {}

    def register(self, search, user_path):
        """
        Register a search against a path.

        The same search can be registered against more than one path.

        @param search: object implemented from SearchDefBase.
        @param user_path: directory or file path.
        """
        if search.tag is not None:
            if search.tag in self._search_tags:
                if search.id not in self._search_tags[search.tag]:
                    log.debug("one or more search tagged '%s' has already "
                              "been registered against path '%s'",
                              search.tag, user_path)
                    self._search_tags[search.tag].append(search.id)
            else:
                self._search_tags[search.tag] = [search.id]

        if isinstance(search, SequenceSearchDef):
            self._sequence_searches[search.id] = search
        else:
            self._simple_searches[search.id] = search

        for path in self._expand_path(user_path):
            if path in self._entries:
                entry = self._entries[path]
                entry['searches'].append(search)
            else:
                self._entries[path] = {'source_id': self.get_source_id(path),
                                       'path': path,
                                       'searches': [search]}

    def resolve_from_id(self, search_id):
        """ Resolve search definition from unique id. """
        if search_id in self._simple_searches:
            return self._simple_searches[search_id]

        return self._sequence_searches[search_id]

    def resolve_from_tag(self, tag):
        """ Resolve search definition from tag.

        Returns a list of resolved searches.
        """
        searches = []
        for search_id in self._search_tags[tag]:
            searches.append(self.resolve_from_id(search_id))

        return searches

    @staticmethod
    def _filtered_dir(contents, max_logrotate_depth=7):
        """ Filter contents of a directory. Directories are ignored and if any
        files look like logrotated log files they are sorted and only
        max_logrotate_depth are kept.
        """
        logrotated = {}
        new_contents = []
        for path in contents:
            if not os.path.isfile(path):
                continue

            ret = re.compile(r"(\S+)\.log\S*").match(path)
            if not ret:
                new_contents.append(path)
                continue

            fnamepfix = ret.group(1)
            if path.endswith('.log'):
                new_contents.append(fnamepfix + '.log')
            else:
                if fnamepfix not in logrotated:
                    logrotated[fnamepfix] = [path]
                else:
                    logrotated[fnamepfix].append(path)

        limit = max_logrotate_depth
        for logrotated in logrotated.values():
            capped = sorted(logrotated,
                            key=logrotate_log_sort)[:limit]
            new_contents += capped

        return new_contents

    def _expand_path(self, path):
        if os.path.isfile(path):
            return [path]

        if os.path.isdir(path):
            return self._filtered_dir(os.listdir(path),
                                      self.max_logrotate_depth)

        return self._filtered_dir(glob.glob(path), self.max_logrotate_depth)

    def source_id_to_path(self, s_id):
        try:
            return self._source_ids[s_id]
        except KeyError:
            log.exception("ALL PATHS:")
            log.error('\n'.join(list(self._source_ids.keys())))

        return None

    def get_source_id(self, path):
        if not self._source_ids:
            source_id = 0
        else:
            for source_id, _path in self._source_ids.items():
                if _path == path:
                    return source_id

            source_id = max(list(self._source_ids)) + 1

        log.debug("path=%s source_id=%s", path, source_id)
        self._source_ids[source_id] = path
        return source_id

    def __len__(self):
        return len(self._entries)

    def __iter__(self):
        yield from self._entries.values()

    def __repr__(self):
        info = ""
        for path, searches in self._entries.items():
            info += f"\n{path}:\n    "
            entries = []
            for key, val in searches.items():
                entries.append(f"{key}={val}")

            info += '\n    '.join(entries)

        return info


class SearcherBase(abc.ABC):
    """ Base class for searcher implementations. """
    @property
    @abc.abstractmethod
    def files(self):
        """ Returns a list of files we will be searching. """

    @property
    @abc.abstractmethod
    def num_parallel_tasks(self):
        """
        Returns an integer representing the maximum number of tasks we can
        run in parallel. This will typically be bound by the number of
        cpu threads available.
        """

    @abc.abstractmethod
    def add(self, searchdef):
        """
        Add a search criterea.

        @param searchdef: SearchDef object
        """

    @abc.abstractmethod
    def run(self):
        """
        Execute all searches.
        """


class SearchConstraintsManager():
    """ Manager for any search constraints being applied to searches. """
    def __init__(self, search_catalog):
        self.search_catalog = search_catalog
        self.global_constraints = []
        self.global_restrictions = set()

    def apply_global(self, search_ids, fd):
        """ Apply any global constraints to the entire file. """
        offset = 0
        if not self.global_constraints:
            log.debug("no global constraint to apply to %s", fd.name)
            return offset

        if self.global_restrictions.intersection(search_ids):
            log.debug("skipping global constraint for %s", fd.name)
            return offset

        for c in self.global_constraints:
            log.debug("applying task global constraint %s to %s", c.id,
                      fd.name)
            _offset = c.apply_to_file(fd)
            if _offset is not None:
                return _offset

        return offset

    @staticmethod
    def apply_single(searchdef, line):
        """ Apply any constraints for this searchdef to the give line.

        @param searchdef: SearchDef object
        @param line: string line we want to validate
        @return: tuple of showing if line passes and if so, did all
                 constraints applied pass.
        """
        Result = namedtuple('result',
                            ('line_is_valid', 'all_constraints_passed'))

        if not searchdef.constraints:
            return Result(True, True)

        any_passed = False
        all_passed = True
        for c in searchdef.constraints.values():
            try:
                if c.apply_to_line(line):
                    any_passed = True
                    continue
            except CouldNotApplyConstraint:
                all_passed = False
                continue

            return Result(False, False)

        return Result(any_passed, all_passed)


class ThreadManager():
    """ Manage threads used by the searcher. """
    def __init__(self, name, func, args):
        log.debug("creating %s thread", name)
        self.name = name
        self.event = threading.Event()
        self.event.clear()
        self.thread = threading.Thread(target=func, args=[self.event, *args])
        self.running = False

    def start(self):
        self.thread.start()
        self.running = True

    def stop(self):
        if self.running:
            log.debug("joining/stopping queue %s thread", self.name)
            self.event.set()
            self.thread.join()
            self.running = False
            log.debug("%s thread stopped successfully", self.name)


class FileSearcher(SearcherBase):
    """ Searcher implementation used to search filesystem locations. """
    def __init__(self, max_parallel_tasks=8, max_logrotate_depth=7,
                 constraint=None, decode_errors=None):
        """
        @param max_parallel_tasks: max number of search tasks that can run in
                                   parallel.
        @param max_logrotate_depth: used by SearchCatalog to filter logfiles
                                    based on their name if it matches a
                                    logrotate format and want to constrain how
                                    much history we search.
        @param constraint: constraint to be used with this
                                   searcher that applies to all files searched.
        @param decode_errors: unicode decode error handling. This usually
                              defaults to "strict". See
                              https://docs.python.org/3/howto/unicode.html
                              for more options.
        """
        self.max_parallel_tasks = max_parallel_tasks
        self._stats = SearchTaskStats()
        self.catalog = SearchCatalog(max_logrotate_depth)
        self.constraints_manager = SearchConstraintsManager(self.catalog)
        self.decode_errors = decode_errors
        if constraint:
            self.constraints_manager.global_constraints.append(constraint)

    @property
    def files(self):
        return [e['path'] for e in self.catalog]

    def resolve_source_id(self, source_id):
        return self.catalog.source_id_to_path(source_id)

    def add(self, searchdef, path, allow_global_constraints=True):  # noqa, pylint: disable=arguments-differ
        """
        Add a search definition.

        @param searchdef: a SearchDef or SequenceSearchDef object.
        @param path: path we want to search. this can be a file, dir or glob.
        @param allow_global_constraints: boolean determining whether we want
                                         any global constraints available to be
                                         applied to this path.
        """
        if not allow_global_constraints:
            self.constraints_manager.global_restrictions.add(searchdef.id)

        self.catalog.register(searchdef, path)

    @property
    def num_parallel_tasks(self):
        if self.max_parallel_tasks == 0:
            cpus = 1  # i.e. no parallelism
        else:
            cpus = min(self.max_parallel_tasks, os.cpu_count())

        return min(len(self.files) or 1, cpus)

    @property
    def stats(self):
        """
        Provide stats for the last search run.

        @return: SearchTaskStats object
        """
        return self._stats

    @staticmethod
    def _get_info(event, results, results_store, stats, proc):
        """
        Collect results from all search task processes.

        @param event: threading.Event() object used to stop this thread.
        @param results: SearchResultsCollection object.
        @param results_store: ResultStore* holding results from workers.
        @param stats: SearchTaskStats object
        @param proc: psutil.Process() object for local process.
        """
        then = None
        while True:
            if event.is_set():
                log.debug("exiting info thread")
                break

            if not then or datetime.now() >= then + timedelta(seconds=5):
                with RESULTS_STORE_LOCKED:
                    allocations = len(results_store)

                with RESULTS_COLLECTION_LOCK:
                    num_results = len(results)

                rss = int(proc.memory_info().rss / 1024 ** 2)
                log.debug("total %s results received (rss=%sM, "
                          "store_allocations=%s), "
                          "%s/%s jobs completed - "
                          "waiting for more", num_results, rss,
                          allocations,
                          stats['jobs_completed'], stats['total_jobs'])

                then = datetime.now()

            time.sleep(1)

    @staticmethod
    def _get_results(event, results, results_queue):
        """
        Collect results from all search task processes.

        @param event: threading.Event() object used to stop this thread.
        @param results: SearchResultsCollection object.
        @param results_queue: results queue used to collect results from
                              workers.
        """
        log.debug("fetching results from worker queues")
        while True:
            if not results_queue.empty():
                with RESULTS_COLLECTION_LOCK:
                    results.add(results_queue.get())

                # if len(results) % 1000 == 0:
                #     log.debug("received %s results", len(results))
            elif event.is_set():
                log.debug("exiting results thread")
                break
            else:
                # yield
                time.sleep(0.1)

        with RESULTS_COLLECTION_LOCK:
            log.debug("stopped fetching results (total received=%s)",
                      len(results))

    @staticmethod
    def _purge_results(results, results_queue, expected):
        """
        Purge results from all search task processes.

        @param results: SearchResultsCollection object.
        @param results_queue: results queue used for this search session.
        @param expected: number of results we expect to receive. this is used
                         to do a final sweep once all search tasks are complete
                         to ensure all results have been collected.
        """
        log.debug("purging results (expected=%s)", expected)

        while True:
            with RESULTS_COLLECTION_LOCK:
                if not results_queue.empty():
                    results.add(results_queue.get())
                elif expected > len(results):
                    try:
                        results.add(results_queue.get(
                            timeout=RESULTS_QUEUE_TIMEOUT))
                    except queue.Empty:
                        log.info("timeout waiting > %s secs to receive "
                                 "results - expected=%s, actual=%s",
                                 RESULTS_QUEUE_TIMEOUT, expected, len(results))
                else:
                    break

        log.debug("stopped purging results (total received=%s)",
                  len(results))

    @staticmethod
    def _ensure_worker_processes_killed():
        """
        For some reason it is sometimes possible to for pool termination to
        hang indefinitely because one or more worker process fails to
        terminate. This method ensures that all extant worker child processes
        are killed so that pool termination is guaranteed to complete.
        """
        log.debug("ensuring all pool workers killed")
        worker_pids = []
        for child in multiprocessing.active_children():
            if isinstance(child, multiprocessing.context.ForkProcess):
                if 'ForkProcess' in child.name:
                    worker_pids.append(child.pid)

        ps_out = subprocess.check_output(['ps', '-opid', '--no-headers',
                                          '--ppid',
                                          str(os.getpid())], encoding='utf8')
        child_pids = [int(line.strip()) for line in ps_out.splitlines()]
        log.debug("process has child pids: %s", child_pids)
        for wpid in worker_pids:
            if int(wpid) not in child_pids:
                log.error("worker pid %s no longer a child of this process "
                          "(%s)", wpid, os.getpid())
                continue

            try:
                log.debug('sending SIGKILL to worker process %s', wpid)
                os.kill(wpid, signal.SIGILL)
            except ProcessLookupError:
                log.debug('worker process %s already killed', wpid)

    def _run_single(self, results_collection, results_store):
        """ Run a single search using this process.

        @param results_collection: SearchResultsCollection object
        @param results_store: ResultsStoreSimple object
        """
        results_manager = SearchTaskResultsManager(
                            results_store,
                            results_collection=results_collection)
        for info in self.catalog:
            task = SearchTask(info,
                              constraints_manager=self.constraints_manager,
                              results_manager=results_manager,
                              decode_errors=self.decode_errors)
            self.stats.update(task.execute())

        self.stats['jobs_completed'] = 1
        self.stats['total_jobs'] = 1

    def _run_mp(self, mgr, results, results_store):  # noqa,pylint: disable=too-many-locals
        """ Run searches in parallel.

        @param mgr: multiprocessing.Manager object
        @param results: SearchResultsCollection object
        """
        results_queue = mgr.Queue(RESULTS_QUEUE_SIZE)
        info_thread = ThreadManager('info', self._get_info,
                                    [results, results_store, self.stats,
                                     psutil.Process()])
        results_thread = ThreadManager('results', self._get_results,
                                       [results, results_queue])
        results_manager = SearchTaskResultsManager(
                            results_store,
                            results_queue=results_queue)
        try:
            num_workers = self.num_parallel_tasks
            with concurrent.futures.ProcessPoolExecutor(
                                    max_workers=num_workers) as executor:
                jobs = {}
                for info in self.catalog:
                    c_mgr = self.constraints_manager
                    task = SearchTask(info,
                                      constraints_manager=c_mgr,
                                      results_manager=results_manager,
                                      decode_errors=self.decode_errors)
                    job = executor.submit(task.execute)
                    jobs[job] = info['path']
                    self.stats['total_jobs'] += 1

                log.debug("filesearcher: syncing %s job(s)", len(jobs))
                info_thread.start()
                results_thread.start()
                try:
                    for future in concurrent.futures.as_completed(jobs):
                        self.stats.update(future.result())
                        self.stats['jobs_completed'] += 1
                except concurrent.futures.process.BrokenProcessPool as exc:
                    msg = ("one or more worker processes has died - "
                           "aborting search")
                    raise FileSearchException(msg) from exc

                log.debug("all workers synced")
                # double check nothing is running anymore
                for job, path in jobs.items():
                    log.debug("worker for path '%s' has state: %s", path,
                              repr(job))
                    if job.running():
                        log.info("job for path '%s' still running when "
                                 "not expected to be", path)

                results_thread.stop()
                info_thread.stop()
                log.debug("purging remaining results (expected=%s, "
                          "remaining=%s)", self.stats['results'],
                          self.stats['results'] - len(results))
                self._purge_results(results, results_queue,
                                    self.stats['results'])

                self._ensure_worker_processes_killed()
                log.debug("terminating pool")
        finally:
            results_thread.stop()
            info_thread.stop()

    def run(self):
        """ Run all searches.

        @return: SearchResultsCollection object
        """
        log.debug("filesearcher: starting")
        self.stats.reset()
        if len(self.catalog) == 0:
            log.debug("catalog is empty - nothing to run")
            return SearchResultsCollection(self.catalog, ResultStoreSimple())

        self.stats['searches'] = sum((len(p['searches'])
                                      for p in self.catalog))
        self.stats['searches_by_job'] = [len(p['searches'])
                                         for p in self.catalog]
        log.debug(repr(self.catalog))
        if len(self.files) > 1:
            log.debug("running searches (parallel=True)")
            with multiprocessing.Manager() as mgr:
                rs = ResultStoreParallel(mgr)
                results = SearchResultsCollection(self.catalog, rs)
                self._run_mp(mgr, results, rs)
                rs.unproxy_results()
        else:
            log.debug("running searches (parallel=False)")
            rs = ResultStoreSimple()
            results = SearchResultsCollection(self.catalog, rs)
            self._run_single(results, rs)

        log.debug("filesearcher: completed (%s)", self.stats)
        return results
