import os
from tempfile import TemporaryDirectory, _get_candidate_names
import unittest

test_data = os.path.join(os.path.dirname(__file__), 'data')


class TestFast5ApiHelper(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory()
        self.save_path = self._tmp_dir.name

    def tearDown(self):
        self._tmp_dir.cleanup()

    def generate_temp_filename(self):
        return os.path.join(self.save_path, next(_get_candidate_names()))
