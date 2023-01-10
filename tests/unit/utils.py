import os
import unittest

# disable debug for stestr otherwise output is much too verbose
from searchtools.log import log, logging

# Must be set prior to other imports
TESTS_DIR = os.environ["TESTS_DIR"]
DEFS_TESTS_DIR = os.path.join(os.environ['TESTS_DIR'], 'defs', 'tests')


def create_files(files_to_create):
    """
    Decorator helper to create any number of files with provided content within
    a temporary data_root.

    @param files_to_create: a dictionary of <filename>: <contents> pairs.
    """

    def create_files_inner1(f):
        def create_files_inner2(self, *args, **kwargs):
            if files_to_create is None:
                return f(self, *args, **kwargs)

            for path, content in files_to_create.items():
                path = os.path.join(self.data_root, path)
                if not os.path.exists(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path))

                log.debug("creating test file %s", path)
                with open(path, 'w') as fd:
                    fd.write(content)

                ret = f(self, *args, **kwargs)
                return ret

        return create_files_inner2

    return create_files_inner1


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        log.setLevel(logging.INFO)
