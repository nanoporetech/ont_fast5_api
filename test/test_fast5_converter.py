from __future__ import division

try:
    from unittest.mock import Mock, patch
except ImportError:  # python2 compatibility
    from mock import Mock, patch

import h5py
import os
import shutil
import unittest

from ont_fast5_api.conversion_tools.multi_to_single_fast5 import convert_multi_to_single
from ont_fast5_api.conversion_tools.single_to_multi_fast5 import batch_convert_single_to_multi, get_fast5_file_list
from ont_fast5_api.multi_fast5 import MultiFast5File

test_data = os.path.join(os.path.dirname(__file__), 'data')
save_path = os.path.join(os.path.dirname(__file__), 'tmp')


class TestFast5Converter(unittest.TestCase):
    def setUp(self):
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.makedirs(save_path)

    @patch('ont_fast5_api.conversion_tools.single_to_multi_fast5.get_progress_bar')
    def test_single_to_multi(self, mock_pbar):
        input_folder = os.path.join(test_data, "single_reads")
        batch_size = 3
        file_count = len(os.listdir(input_folder))
        batch_convert_single_to_multi(input_folder, save_path, filename_base="batch", batch_size=batch_size,
                                      recursive=False)

        expected_output_reads = {"filename_mapping.txt": 0,
                                 "batch_0.fast5": batch_size,
                                 "batch_1.fast5": file_count % batch_size}
        self.assertEqual(sorted(os.listdir(save_path)), sorted(list(expected_output_reads.keys())))
        for file, read_count in expected_output_reads.items():
            if read_count > 0:
                with h5py.File(os.path.join(save_path, file), 'r') as f5:
                    self.assertEqual(len(f5), read_count)

    def test_multi_to_single(self):
        input_file = os.path.join(test_data, "multi_read", "batch_0.fast5")
        with MultiFast5File(input_file, 'r') as f5:
            read_count = len(f5.handle)
            expected_files = sorted([os.path.join(save_path, "{}", i + '.fast5') for i in f5.get_read_ids()])

        # Large batch size should all be in a single folder
        batch_size = 10
        subfolder = 0
        mock_pbar = Mock()
        mock_pbar.currval = 0
        convert_multi_to_single(input_file, save_path, batch_size=batch_size,
                                pbar=mock_pbar, output_table=Mock())
        out_files = sorted(get_fast5_file_list(save_path, recursive=True))
        self.assertEqual(len(out_files), read_count)
        self.assertEqual(out_files, [f.format(subfolder) for f in expected_files])

        # Small batch size should be split across multiple folders
        shutil.rmtree(save_path)
        batch_size = 2
        expected = [f.format(i // batch_size) for i, f in enumerate(expected_files)]
        mock_pbar.currval = 0
        convert_multi_to_single(input_file, save_path, batch_size=batch_size,
                                pbar=mock_pbar, output_table=Mock())
        out_files = sorted(get_fast5_file_list(save_path, recursive=True))
        self.assertEqual(len(out_files), read_count)
        self.assertEqual(out_files, expected)
