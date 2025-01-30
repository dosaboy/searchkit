""" Search result handlers. """
import abc
from functools import cached_property
from collections import UserList, UserDict

from searchkit.log import log
from searchkit.exception import FileSearchException


class SearchResultBase(UserList):
    """ Base class for search result implementations. """
    META_OFFSET_TAG = 0
    META_OFFSET_SEQ_ID = 1

    PART_OFFSET_IDX = 0
    PART_OFFSET_VALUE = 1
    PART_OFFSET_FIELD = 2

    @abc.abstractmethod
    def __init__(self):
        super().__init__()
        self.results_store = None
        self.linenumber = None
        self.section_id = None

    def _get_store_id(self, field):
        """
        Get the position in the results store of the value corresponding to
        field. A return value of None indicates that the value does not exist
        in the store.
        """
        for part in self.data:
            if len(part) > self.PART_OFFSET_FIELD and isinstance(field, str):
                if part[self.PART_OFFSET_FIELD] != field:
                    continue
            elif part[self.PART_OFFSET_IDX] != field:
                continue

            store_id = part[self.PART_OFFSET_VALUE]
            if store_id is not None:
                return store_id

        return None

    def get(self, field):
        """
        Retrieve result part value by index or name.

        @param field: integer index of string field name.
        """
        store_id = self._get_store_id(field)
        if store_id is not None:
            return self.results_store[store_id]

        return None

    def __iter__(self):
        """ Only return part values when iterating over this object. """
        for part in self.data:
            yield self.results_store.get(part[self.PART_OFFSET_VALUE])

    def __repr__(self):
        if self.results_store is None:
            r_list = []
        else:
            r_list = [f"{rp[self.PART_OFFSET_IDX]}="
                      f"'{self.results_store.get(rp[self.PART_OFFSET_VALUE])}'"
                      for rp in self.data]
        return (f"ln:{self.linenumber} {', '.join(r_list)} "
                f"(section={self.section_id})")


class SearchResultMinimal(SearchResultBase):
    """
    Minimal search result implementation optimised for IPC transfer between
    worker tasks and the main collector process.
    """
    def __init__(self, data, metadata, linenumber, source_id,  # noqa,pylint: disable=too-many-arguments
                 sequence_section_id, field_info):
        """
        This is a minimised representation of a SearchResult object so as to
        reduce its size as much as possible before putting on the results
        queue.

        IMPORTANT: this class must contain as few attributes as possible and
        their values must be as small as possible. For large values that need
        to be shared, we de-duplicate using ResultStoreBase implementations.

        Do not store references to shared objects.
        """
        self.data = data[:]
        self.metadata = metadata[:]
        self.linenumber = linenumber
        self.source_id = source_id
        self.section_id = sequence_section_id
        if field_info:
            self.field_names = list(field_info)
        else:
            self.field_names = None

        self.results_store = None

    def __getattr__(self, name):
        if name != 'field_names':
            if self.field_names and name in self.field_names:
                return self.get(name)

        raise AttributeError(f"'{self.__class__.__name__}' object has "
                             f"no attribute '{name}'")

    @property
    def tag(self):
        idx = self.metadata[self.META_OFFSET_TAG]
        if idx is None:
            return None

        return self.results_store.get(idx)

    @property
    def sequence_id(self):
        idx = self.metadata[self.META_OFFSET_SEQ_ID]
        if idx is None:
            return None

        return self.results_store.get(idx)

    def register_results_store(self, store):
        """
        Register a ResultsStore with this result. This is used to re-register
        the store once the result has been received by the main process.

        @param store: ResultsStore object
        """
        self.results_store = store


class SearchResult(SearchResultBase):  # noqa,pylint: disable=too-many-instance-attributes
    """ Search result.

    This implementation is not optimised to be transfered over the results
    queue and is only intended to be used locally to the search task before
    sending the compressed result back to the collector.
    """
    def __init__(self, linenumber, source_id, result, search_def,  # noqa,pylint: disable=too-many-arguments
                 results_store, sequence_section_id=None):
        """
        @param linenumber: line number that produced a match.
        @param source_id: data source id - resolves to a path in the
                          SearchCatalog.
        @param result: python.re.match object.
        @param search_def: SearchDef object.
        @param results_store: ResultsStore object
        @param sequence_section_id: if this result is part of a sequence the
                                    section ID must be provided.
        """
        self.results_store = results_store
        self.data = []
        self.linenumber = linenumber
        self.source_id = source_id
        self.tag = search_def.tag
        self.section_id = sequence_section_id
        self.sequence_id = None
        if search_def.sequence_def:
            if sequence_section_id is None:
                raise FileSearchException("sequence section result saved "
                                          "but no section id provided")

            self.sequence_id = search_def.sequence_def.id

        self.field_info = search_def.field_info

        if not search_def.store_result_contents:
            log.debug("store_contents is False - skipping save value")
            return

        self.store_result(result)

    def store_result(self, result):
        num_groups = len(result.groups())
        # NOTE: this does not include group(0)
        if num_groups:
            # To reduce memory footprint, don't store group(0) i.e. the whole
            # line, if there are actual groups in the result.
            for i in range(1, num_groups + 1):
                self._save_part(i, result.group(i))
        else:
            log.debug("Saving full search result is inefficient and can lead "
                      "to high memory usage. Use sub groups if possible. "
                      "(tag=%s)", self.tag)
            self._save_part(0, result.group(0))

    @property
    def metadata(self):
        tag_id, seq_id, _ = self.results_store.add(self.tag, self.sequence_id,
                                                   value=None)
        return (tag_id, seq_id)

    def _save_part(self, part_index, value=None):
        name = None
        if value is not None:
            if self.field_info:
                name = self.field_info.index_to_name(part_index - 1)
                value = self.field_info.ensure_type(name, value)

        _, _, store_id = self.results_store.add(self.tag, self.sequence_id,
                                                value)

        # REMEMBER:
        #   PART_OFFSET_IDX = 0
        #   PART_OFFSET_VALUE = 1
        #   PART_OFFSET_FIELD = 2
        if name is None:
            entry = (part_index, store_id)
        else:
            entry = (part_index, store_id, name)

        self.data.append(entry)

    @cached_property
    def export(self):
        """ Export the smallest possible representation of this object. """
        return SearchResultMinimal(self.data, self.metadata,
                                   self.linenumber, self.source_id,
                                   self.section_id, self.field_info)


class SequenceSearchResults(UserDict):
    """ Captures results from sequence searches.

    Sequence searches require one or more search to match in any particular
    sequence to get an overall match. These results are stored independently
    then aggregated once all searches are finished to identify which complete
    sequences matched.
    """
    def add(self, result):
        sid = result.sequence_id
        if sid in self.data:
            self.data[sid].append(result)
        else:
            self.data[sid] = [result]

    def remove(self, sid):
        if sid in self.data:
            del self.data[sid]
