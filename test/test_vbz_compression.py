import os
import shutil
import unittest

import h5py

from ont_fast5_api.compression_settings import VBZ
from ont_fast5_api.conversion_tools.compress_fast5 import compress_read_from_multi, compress_read_from_single, \
    compress_batch
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list
from ont_fast5_api.fast5_file import Fast5File, EmptyFast5
from ont_fast5_api.fast5_info import ReadInfo
from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File

try:
    from unittest.mock import patch
except ImportError:  # python2 compatibility
    from mock import patch

test_data = os.path.join(os.path.dirname(__file__), 'data')
save_path = os.path.join(os.path.dirname(__file__), 'tmp')


class TestVbzReadWrite(unittest.TestCase):

    def setUp(self):
        self.run_id = "123abc"
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.makedirs(save_path)

    def test_write_vbz_directly(self):
        input_data = range(10)
        with h5py.File(os.path.join(save_path, 'h5py.fast5'), 'w') as fast5:
            fast5.create_dataset('Signal', data=input_data, **vars(VBZ))
            raw = fast5['Signal']

            self.assertTrue(str(VBZ.compression) in raw._filters)
            self.assertEqual(VBZ.compression_opts, raw._filters[str(VBZ.compression)])
            self.assertEqual(list(input_data), list(raw))

    def test_read_vbz_using_api(self):
        with MultiFast5File(os.path.join(test_data, 'vbz_reads', 'vbz_reads.fast5'), 'r') as fast5:
            read_count = 0
            for read in fast5.get_reads():
                # This input file was created to have 4 reads with 20 samples per read
                read_count += 1
                raw_data = read.get_raw_data()
                self.assertEqual(20, len(raw_data))
            self.assertEqual(4, read_count)

    def test_write_vbz_using_api(self):
        input_data = list(range(5))
        read_id = "0a1b2c3d"
        with MultiFast5File(os.path.join(save_path, 'api_write.fast5'), 'w') as fast5:
            fast5.create_read(read_id, self.run_id)
            read = fast5.get_read(read_id)
            read.add_raw_data(input_data, attrs={}, compression=VBZ)
            raw = read.get_raw_data()
            # First check the data comes back in an appropriate form
            self.assertEqual(input_data, list(raw))
            # Then check the types are as they should be under the hood
            filters = read.compression_filters
            self.assertTrue(str(VBZ.compression) in filters)
            self.assertEqual(VBZ.compression_opts, filters[str(VBZ.compression)])

    def test_write_vbz_using_api_single_read(self):
        input_data = list(range(5))
        read_id = "0a1b2c3d"
        read_number = 0
        with Fast5File(os.path.join(save_path, 'api_write_single.fast5'), 'w') as fast5:
            fast5.status.read_number_map[read_number] = read_number
            fast5.status.read_info = [ReadInfo(read_number=read_number, read_id=read_id,
                                               start_time=0, duration=len(input_data))]
            fast5.add_raw_data(data=input_data, attrs={}, compression=VBZ)
            raw = fast5.get_raw_data()
            # First check the data comes back in an appropriate form
            self.assertEqual(input_data, list(raw))

            # Then check the types are as they should be under the hood
            filters = fast5.compression_filters
            self.assertTrue(str(VBZ.compression) in filters)
            self.assertEqual(VBZ.compression_opts, filters[str(VBZ.compression)])


class TestVbzConvert(unittest.TestCase):

    def setUp(self):
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.makedirs(save_path)

    def assertUncompressed(self, read):
        filters = read.compression_filters
        # Check we have don't have VBZ filters
        ## TODO we should be able to check what the compression is from an AbstractCompression object
        self.assertFalse(str(VBZ.compression) in filters)

    def assertCompressed(self, read):
        filters = read.compression_filters
        # Check we only have 1 filter
        self.assertEqual(1, len(filters))
        # Check it is the VBZ filter
        self.assertTrue(str(VBZ.compression) in filters)
        # Check it has the correct opts
        self.assertEqual(VBZ.compression_opts, filters[str(VBZ.compression)])

    def test_compress_read_from_multi(self):
        target_compression = VBZ
        with get_fast5_file(os.path.join(test_data, "multi_read", "batch_0.fast5"), "r") as input_f5, \
                MultiFast5File(os.path.join(save_path, 'compress_multi_out.fast5'), 'w') as output_f5:
            read_id = input_f5.get_read_ids()[0]
            input_read = input_f5.get_read(read_id)

            # Input read should be uncompressed on the way in:
            self.assertUncompressed(input_read)

            compress_read_from_multi(output_f5, input_read, target_compression)

            output_read = output_f5.get_read(read_id)
            self.assertCompressed(output_read)

    def test_compress_read_from_single(self):
        with get_fast5_file(os.path.join(test_data, "single_reads", "read0.fast5"), "r") as input_f5, \
                EmptyFast5(os.path.join(save_path, 'compress_single_out.fast5'), 'w') as output_f5:
            read_id = input_f5.get_read_ids()[0]
            input_read = input_f5.get_read(read_id)

            # Input read should be uncompressed on the way in:
            self.assertUncompressed(input_read)

            compress_read_from_single(output_f5, input_read, target_compression=VBZ)

            output_read = output_f5.get_read(read_id)
            self.assertCompressed(output_read)

    @patch('ont_fast5_api.conversion_tools.compress_fast5.get_progress_bar')
    def test_conversion_script_multi(self, mock_pbar):
        input_folder = os.path.join(test_data, 'multi_read')
        output_folder = save_path
        compress_batch(input_folder=input_folder, output_folder=output_folder, target_compression=VBZ)

        count_files = 0
        count_reads = 0
        for out_file in get_fast5_file_list(save_path, recursive=True):
            count_files += 1
            with get_fast5_file(out_file) as f5:
                self.assertTrue(isinstance(f5, MultiFast5File))
                for read in f5.get_reads():
                    self.assertCompressed(read)
                    count_reads += 1
        self.assertEqual(1, count_files)
        self.assertEqual(4, count_reads)

    @patch('ont_fast5_api.conversion_tools.compress_fast5.get_progress_bar')
    def test_conversion_script_single(self, mock_pbar):
        input_folder = os.path.join(test_data, 'single_reads')
        compress_batch(input_folder=input_folder, output_folder=save_path, target_compression=VBZ)

        count_files = 0
        count_reads = 0
        for out_file in get_fast5_file_list(save_path, recursive=True):
            count_files += 1
            with get_fast5_file(out_file) as f5:
                self.assertTrue(isinstance(f5, Fast5File))
                for read in f5.get_reads():
                    self.assertCompressed(read)
                    count_reads += 1

        self.assertEqual(4, count_files)
        self.assertEqual(4, count_reads)
