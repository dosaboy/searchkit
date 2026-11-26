""" Searchkit sequence search unit tests. """
import os
import shutil
import tempfile

from searchkit import (
    FileSearcher,
    SearchDef,
    SequenceSearchDef,
)

from . import utils

SEQ_TEST_1 = """a start point
leads to
an ending
"""

SEQ_TEST_2 = """a start point
another start point
leads to
an ending
"""

SEQ_TEST_3 = """a start point
another start point
leads to
an ending
a start point
"""

SEQ_TEST_4 = """a start point
another start point
value is 3
"""

SEQ_TEST_5 = """a start point
another start point
value is 3

another start point
value is 4
"""

SEQ_TEST_6 = """section 1
1_1
1_2
section 2
2_1
"""

SEQ_TEST_7 = """section 1
1_1
1_2
section 2
2_1
section 3
3_1
"""

MULTI_SEQ_TEST = """
sectionB 1
1_1
sectionA 1
1_1
sectionB 2
2_2
sectionA 2
2_1
"""


class TestFailedError(Exception):
    """ Raised when an error is identified in a test. """


class TestSequenceSearch(utils.TestSearchKitBase):
    """ Unit tests for sequence searches. """

    def test_large_sequence_search(self):
        seq = SequenceSearchDef(start=SearchDef(r'(HEADER)'),
                                body=SearchDef(r'(\d+)'),
                                end=SearchDef(r'(FOOTER)'),
                                tag='myseq')
        simple = SearchDef(r'(\d+)', tag='simple')
        f = FileSearcher()
        with tempfile.TemporaryDirectory() as dtmp:
            try:
                for i in range(20):
                    fpath = os.path.join(dtmp, f'f{i}')
                    with open(fpath, 'w', encoding='utf-8') as fd:
                        fd.write('HEADER\n')
                        for _ in range(1000):
                            # this should be almost 100% deduped
                            fd.write(f'{1234}\n')

                        fd.write('FOOTER\n')

                    # simple search
                    f.add(simple, fpath)

                    # sequence search
                    f.add(seq, fpath)

                results = f.run()
            finally:
                shutil.rmtree(dtmp)

        self.assertEqual(len(results), 40040)
        self.assertEqual(len(results.find_by_tag('simple')), 20000)
        self.assertEqual(len(results.find_sequence_by_tag('myseq')), 20)
        for section in results.find_sequence_by_tag('myseq').values():
            for r in section:
                if r.tag == seq.start_tag:
                    self.assertEqual(r.get(1), 'HEADER')
                elif r.tag == seq.end_tag:
                    self.assertEqual(r.get(1), 'FOOTER')
                elif r.tag != seq.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")
                else:
                    self.assertEqual(r.get(1), '1234')

    @utils.create_files({'atestfile': SEQ_TEST_1})
    def test_sequence_searcher(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                             r"^a\S* (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test1")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test1')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "start")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_2,
                         'atestfile2': SEQ_TEST_2})
    def test_sequence_searcher_overlapping(self):
        """
        NOTE: tests searches in parallel.
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test2")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        s.add(sd, path=os.path.join(self.data_root, 'atestfile2'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test2')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_3,
                         'atestfile2': SEQ_TEST_3})
    def test_sequence_searcher_overlapping_incomplete(self):
        """
        NOTE: tests searches in parallel.
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"leads to"),
                               end=SearchDef(r"^an (ending)$"),
                               tag="seq-search-test3")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        s.add(sd, path=os.path.join(self.data_root, 'atestfile2'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test3')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(1), "ending")
                elif r.tag != sd.body_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_4})
    def test_sequence_searcher_incomplete_eof_match(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"value is (\S+)"),
                               end=SearchDef(r"^$"),
                               tag="seq-search-test4")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test4')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.body_tag:
                    self.assertEqual(r.get(1), "3")
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(0), "")
                else:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_5})
    def test_sequence_searcher_multiple_sections(self):
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(
                                           r"^(a\S*) (start\S*) point\S*"),
                               body=SearchDef(r"value is (\S+)"),
                               end=SearchDef(r"^$"),
                               tag="seq-search-test5")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test5')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "another")
                elif r.tag == sd.body_tag:
                    self.assertTrue(r.get(1) in ["3", "4"])
                elif r.tag == sd.end_tag:
                    self.assertEqual(r.get(0), "")
                else:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_6})
    def test_sequence_searcher_eof(self):
        """
        Test scenario:
         * multiple sections that end with start of the next
         * start def matches any start
         * end def matches any start
         * file ends before start of next
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(r"^section (\d+)"),
                               body=SearchDef(r"\d_\d"),
                               tag="seq-search-test6")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test6')
        self.assertEqual(len(sections), 2)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    section = r.get(1)
                    self.assertTrue(r.get(1) in ["1", "2"])
                elif r.tag == sd.body_tag:
                    if section == "1":
                        self.assertTrue(r.get(0) in ["1_1", "1_2"])
                    else:
                        self.assertTrue(r.get(0) in ["2_1"])
                elif r.tag != sd.end_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': SEQ_TEST_7})
    def test_sequence_searcher_section_start_end_same(self):
        """
        Test scenario:
         * multiple sections that end with start of the next
         * start def matches unique start
         * end def matches any start
        """
        s = FileSearcher()
        sd = SequenceSearchDef(start=SearchDef(r"^section (2)"),
                               body=SearchDef(r"\d_\d"),
                               end=SearchDef(r"^section (\d+)"),
                               tag="seq-search-test7")
        s.add(sd, path=os.path.join(self.data_root, 'atestfile'))
        results = s.run()
        sections = results.find_sequence_by_tag('seq-search-test7')
        self.assertEqual(len(sections), 1)
        for section_info in sections.values():
            for r in section_info:
                if r.tag == sd.start_tag:
                    self.assertEqual(r.get(1), "2")
                elif r.tag == sd.body_tag:
                    self.assertTrue(r.get(0) in ["2_1"])
                elif r.tag != sd.end_tag:
                    raise TestFailedError(f"error - tag is '{r.tag}'")

    @utils.create_files({'atestfile': MULTI_SEQ_TEST})
    def test_sequence_searcher_multi_sequence(self):
        """
        Test scenario:
         * search containing multiple seqeunce definitions
         * data containing 2 results of each where one is incomplete
         * test that single incomplete result gets removed
        """
        s = FileSearcher()
        sda = SequenceSearchDef(start=SearchDef(r"^sectionA (\d+)"),
                                body=SearchDef(r"\d_\d"),
                                end=SearchDef(
                                            r"^section\S+ (\d+)"),
                                tag="seqA-search-test")
        sdb = SequenceSearchDef(start=SearchDef(r"^sectionB (\d+)"),
                                body=SearchDef(r"\d_\d"),
                                end=SearchDef(
                                            r"^section\S+ (\d+)"),
                                tag="seqB-search-test")
        fname = os.path.join(self.data_root, 'atestfile')
        s.add(sda, path=fname)
        s.add(sdb, path=fname)
        results = s.run()
        sections = results.find_sequence_by_tag('seqA-search-test')
        self.assertEqual(len(sections), 1)
        sections = results.find_sequence_by_tag('seqB-search-test')
        self.assertEqual(len(sections), 2)
