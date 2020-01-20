import h5py
import os
import shutil
from unittest.mock import patch

from ont_fast5_api.compression_settings import VBZ, GZIP
from ont_fast5_api.conversion_tools.check_file_compression import check_read_compression, check_compression
from ont_fast5_api.conversion_tools.compress_fast5 import compress_read_from_multi, compress_read_from_single, \
    compress_batch
from ont_fast5_api.fast5_file import Fast5File, EmptyFast5
from ont_fast5_api.fast5_info import ReadInfo
from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File
from test.helpers import TestFast5ApiHelper, test_data


class TestVbzReadWrite(TestFast5ApiHelper):
    run_id = "123abc"

    def test_write_vbz_directly(self):
        input_data = range(10)
        with h5py.File(os.path.join(self.save_path, 'h5py.fast5'), 'w') as fast5:
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
        with MultiFast5File(os.path.join(self.save_path, 'api_write.fast5'), 'w') as fast5:
            fast5.create_read(read_id, self.run_id)
            read = fast5.get_read(read_id)
            read.add_raw_data(input_data, attrs={}, compression=VBZ)
            raw = read.get_raw_data()
            # First check the data comes back in an appropriate form
            self.assertEqual(input_data, list(raw))
            # Then check the types are as they should be under the hood
            filters = read.raw_compression_filters
            self.assertTrue(str(VBZ.compression) in filters)
            self.assertEqual(VBZ.compression_opts, filters[str(VBZ.compression)])

    def test_write_vbz_using_api_single_read(self):
        input_data = list(range(5))
        read_id = "0a1b2c3d"
        read_number = 0
        with Fast5File(os.path.join(self.save_path, 'api_write_single.fast5'), 'w') as fast5:
            fast5.status.read_number_map[read_number] = read_number
            fast5.status.read_info = [ReadInfo(read_number=read_number, read_id=read_id,
                                               start_time=0, duration=len(input_data))]
            fast5.add_raw_data(data=input_data, attrs={}, compression=VBZ)
            raw = fast5.get_raw_data()
            # First check the data comes back in an appropriate form
            self.assertEqual(input_data, list(raw))

            # Then check the types are as they should be under the hood
            filters = fast5.raw_compression_filters
            self.assertTrue(str(VBZ.compression) in filters)
            self.assertEqual(VBZ.compression_opts, filters[str(VBZ.compression)])


class TestVbzConvert(TestFast5ApiHelper):
    run_id = "123abc"

    def assertCompressed(self, data_path, expected_compression, read_count, file_count):
        files = set()
        read_ids = set()
        for compression, read_id, filepath in check_compression(data_path, False, False, check_all_reads=True):
            self.assertEqual(expected_compression, compression)
            read_ids.add(read_id)
            files.add(filepath)
        self.assertEqual(read_count, len(read_ids))
        self.assertEqual(file_count, len(files))

    def test_compress_read_from_multi(self):
        target_compression = VBZ
        with get_fast5_file(os.path.join(test_data, "multi_read", "batch_0.fast5"), "r") as input_f5, \
                MultiFast5File(os.path.join(self.save_path, 'compress_multi_out.fast5'), 'w') as output_f5:
            read_id = input_f5.get_read_ids()[0]
            input_read = input_f5.get_read(read_id)

            # Input read should be uncompressed on the way in:
            self.assertEqual(check_read_compression(input_read), GZIP)

            compress_read_from_multi(output_f5, input_read, target_compression)

            output_read = output_f5.get_read(read_id)
            self.assertEqual(check_read_compression(output_read), VBZ)

    def test_compress_read_from_single(self):
        with get_fast5_file(os.path.join(test_data, "single_reads", "read0.fast5"), "r") as input_f5, \
                EmptyFast5(os.path.join(self.save_path, 'compress_single_out.fast5'), 'w') as output_f5:
            read_id = input_f5.get_read_ids()[0]
            input_read = input_f5.get_read(read_id)

            # Input read should be uncompressed on the way in:
            self.assertEqual(check_read_compression(input_read), GZIP)

            compress_read_from_single(output_f5, input_read, target_compression=VBZ)

            output_read = output_f5.get_read(read_id)
            self.assertEqual(check_read_compression(output_read), VBZ)

    @patch('ont_fast5_api.conversion_tools.compress_fast5.get_progress_bar')
    def test_conversion_script_multi(self, mock_pbar):
        input_folder = os.path.join(test_data, 'multi_read')
        compress_batch(input_folder=input_folder, output_folder=self.save_path, target_compression=VBZ)
        self.assertCompressed(self.save_path, VBZ, read_count=4, file_count=1)

    @patch('ont_fast5_api.conversion_tools.compress_fast5.get_progress_bar')
    def test_conversion_script_single(self, mock_pbar):
        input_folder = os.path.join(test_data, 'single_reads')
        compress_batch(input_folder=input_folder, output_folder=self.save_path, target_compression=VBZ)

        self.assertCompressed(self.save_path, VBZ, read_count=4, file_count=4)

    @patch('ont_fast5_api.conversion_tools.compress_fast5.get_progress_bar')
    def test_compress_in_place(self, mock_pbar):
        for input_file in os.listdir(os.path.join(test_data, 'single_reads')):
            # We copy file by file as copytree won't work to an existing directory
            shutil.copy(os.path.join(test_data, 'single_reads', input_file), self.save_path)

        self.assertCompressed(self.save_path, GZIP, read_count=4, file_count=4)
        in_files = set(os.listdir(self.save_path))
        compress_batch(self.save_path, output_folder=None, target_compression=VBZ, in_place=True)
        self.assertCompressed(self.save_path, VBZ, read_count=4, file_count=4)
        self.assertEqual(in_files, set(os.listdir(self.save_path)))
