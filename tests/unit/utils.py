import os
import unittest

# disable debug for stestr otherwise output is much too verbose
from searchkit.log import log, logging, set_log_level


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
        if os.environ.get('TESTS_LOG_LEVEL_DEBUG', 'no') == 'yes':
            set_log_level(logging.DEBUG)
        else:
            set_log_level(logging.INFO)
