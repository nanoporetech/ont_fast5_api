import os
import shutil
import tempfile
import unittest

test_data = os.path.join(os.path.dirname(__file__), 'data')


class TemporaryDirectoryPy2:
    """ This class exists only for python2 compatibility
    It can be switched out for tempfile.TemporaryDirectory() in future
    """

    def __init__(self):
        self.name = tempfile.mkdtemp()

    def cleanup(self):
        shutil.rmtree(self.name)


try:
    from tempfile import TemporaryDirectory
except ImportError:
    TemporaryDirectory = TemporaryDirectoryPy2


class TestFast5ApiHelper(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory()
        self.save_path = self._tmp_dir.name

    def tearDown(self):
        self._tmp_dir.cleanup()

    def generate_temp_filename(self):
        return os.path.join(self.save_path, next(tempfile._get_candidate_names()))
