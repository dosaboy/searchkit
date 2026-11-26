""" Searchkit unit tests utilities. """
import os
import re
import shutil
import subprocess
import tempfile
import warnings
import unittest

from searchkit.constraints import TimestampMatcherBase
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
                with open(path, 'w', encoding='utf-8') as fd:
                    fd.write(content)

            ret = f(self, *args, **kwargs)
            return ret

        return create_files_inner2

    return create_files_inner1


class BaseTestCase(unittest.TestCase):
    """ Custom test case for all unit tests. """
    def setUp(self):
        warnings.simplefilter('default', DeprecationWarning)
        self.maxDiff = None  # pylint: disable=invalid-name
        if os.environ.get('TESTS_LOG_LEVEL_DEBUG', 'no') == 'yes':
            set_log_level(logging.DEBUG)
        else:
            set_log_level(logging.INFO)

    # For Python >= 3.12
    def _addDuration(self, *args, **kwargs):  # noqa,pylint: disable=invalid-name
        """ Python 3.12 needs subclasses of unittest.TestCase to implement
        this in order to record times and execute any cleanup actions once
        a test completes regardless of success. Otherwise it emits a warning.
        This generic implementation helps suppress it and possibly others in
        the future. """


class TimestampSimple(TimestampMatcherBase):
    """ Test timestamp implementation. """

    @property
    def patterns(self):
        return [r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})+\s+'
                r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d+)']


class TestSearchKitBase(BaseTestCase):
    """ Base class for tests that search files under a temporary data_root. """

    @property
    def datetime_expr(self):
        return r"^([\d-]+\s+[\d:]+)"

    @staticmethod
    def get_date(date):
        cmd = ["date", "--utc", f"--date={date}",
               '+' + TimestampSimple.DEFAULT_DATETIME_FORMAT]
        out = subprocess.check_output(cmd)
        out = re.compile(r"\s+").sub(' ', out.decode('UTF-8')).strip()
        return out

    def setUp(self):
        super().setUp()
        self.current_date = self.get_date("Thu Feb 10 16:19:17 UTC 2022")
        self.data_root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.data_root)
        super().tearDown()
