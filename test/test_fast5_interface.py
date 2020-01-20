import os
import unittest

from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.fast5_interface import get_fast5_file, check_file_type, MULTI_READ, SINGLE_READ
from ont_fast5_api.multi_fast5 import MultiFast5File
from test.helpers import test_data



class TestFast5Interface(unittest.TestCase):

    def test_correct_type(self):
        single_read_path = os.path.join(test_data, "single_reads", "read0.fast5")
        single_read_id = Fast5File(single_read_path).get_read_id()
        with get_fast5_file(single_read_path) as f5:
            self.assertTrue(isinstance(f5, Fast5File))
            self.assertEqual(check_file_type(f5), SINGLE_READ)
            self.assertEqual(len(f5.get_read_ids()), 1)
            self.assertEqual(single_read_id, f5.get_read_ids()[0])
            self.get_raw(f5)

        multi_read_path = os.path.join(test_data, "multi_read", "batch_0.fast5")
        with get_fast5_file(multi_read_path) as f5:
            self.assertTrue(isinstance(f5, MultiFast5File))
            self.assertEqual(check_file_type(f5), MULTI_READ)
            self.assertTrue(len(f5.get_read_ids()) >= 1)
            self.get_raw(f5)

    def get_raw(self, f5):
        # Test we can get raw data using the same method for single and multi
        raw_data = f5.get_read(f5.get_read_ids()[0]).get_raw_data()
        self.assertTrue(len(raw_data) >= 0)
