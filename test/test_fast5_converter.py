from unittest.mock import patch

import os
import h5py
import numpy

from ont_fast5_api.conversion_tools.multi_to_single_fast5 import convert_multi_to_single, try_multi_to_single_conversion
from ont_fast5_api.conversion_tools.single_to_multi_fast5 import batch_convert_single_to_multi, get_fast5_file_list, \
    create_multi_read_file
from ont_fast5_api.conversion_tools.fast5_subset import MultiFast5File
from ont_fast5_api.fast5_file import Fast5FileTypeError, Fast5File
from test.helpers import TestFast5ApiHelper, test_data, disable_logging


class TestFast5Converter(TestFast5ApiHelper):

    @patch('ont_fast5_api.conversion_tools.single_to_multi_fast5.get_progress_bar')
    def test_single_to_multi(self, mock_pbar):
        input_folder = os.path.join(test_data, "single_reads")
        batch_size = 3
        file_count = len(os.listdir(input_folder))
        batch_convert_single_to_multi(input_folder, self.save_path, filename_base="batch", batch_size=batch_size,
                                      threads=1, recursive=False, follow_symlinks=False, target_compression=None)

        expected_output_reads = {"filename_mapping.txt": 0,
                                 "batch_0.fast5": batch_size,
                                 "batch_1.fast5": file_count % batch_size}
        self.assertEqual(sorted(os.listdir(self.save_path)), sorted(list(expected_output_reads.keys())))
        for file, read_count in expected_output_reads.items():
            if read_count > 0:
                with h5py.File(os.path.join(self.save_path, file), 'r') as f5:
                    self.assertEqual(len(f5), read_count)

    def test_multi_to_single(self):
        input_file = os.path.join(test_data, "multi_read", "batch_0.fast5")
        with MultiFast5File(input_file, 'r') as f5:
            read_count = len(f5.handle)
            expected_files = sorted([os.path.join(self.save_path, "{}", i + '.fast5') for i in f5.get_read_ids()])

        subfolder = '0'
        convert_multi_to_single(input_file, self.save_path, subfolder)

        out_files = sorted(get_fast5_file_list(self.save_path, recursive=True, follow_symlinks=True))
        self.assertEqual(len(out_files), read_count)
        self.assertEqual(out_files, [f.format(subfolder) for f in expected_files])

    @disable_logging
    def test_single_to_multi_incorrect_types(self):
        input_files = [os.path.join(test_data, "multi_read", "batch_0.fast5")]
        with self.assertRaises(Fast5FileTypeError):
            create_multi_read_file(input_files, self.generate_temp_filename(), target_compression=None)

    def test_multi_to_single_incorrect_types(self):
        input_folder = os.path.join(test_data, "single_reads")
        input_file = os.path.join(input_folder, os.listdir(input_folder)[0])
        with self.assertRaises(Fast5FileTypeError):
            try_multi_to_single_conversion(input_file, self.save_path, subfolder='0')

    def test_add_read_to_multi(self):
        with Fast5File(os.path.join(test_data, "single_reads", "read0.fast5"), 'r') as single_fast5, \
                MultiFast5File(self.generate_temp_filename(), 'w') as multi_out:
            multi_out.add_existing_read(single_fast5)
            expected_raw = single_fast5.get_raw_data()
            actual_raw = multi_out.get_read(single_fast5.get_read_id()).get_raw_data()
            self.assertTrue(numpy.array_equal(actual_raw, expected_raw))
