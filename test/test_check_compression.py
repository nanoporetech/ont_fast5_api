import os
import unittest

import numpy

from ont_fast5_api.compression_settings import GZIP, VBZ
from ont_fast5_api.conversion_tools.check_file_compression import check_read_compression, check_compression
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File
from test.helpers import test_data


class TestCheckCompression(unittest.TestCase):

    def test_check_read_compression_single_read(self):
        with get_fast5_file(os.path.join(test_data, 'single_reads', 'read0.fast5'), 'r') as f5:
            for read in f5.get_reads():
                compression = check_read_compression(read)
                self.assertEqual(compression, GZIP)

    def test_check_read_compression_multi_read(self):
        with get_fast5_file(os.path.join(test_data, 'multi_read', 'batch_0.fast5'), 'r') as f5:
            for read in f5.get_reads():
                compression = check_read_compression(read)
                self.assertEqual(compression, GZIP)

    def test_check_read_compression_vbz(self):
        with get_fast5_file(os.path.join(test_data, 'vbz_reads', 'vbz_reads.fast5'), 'r') as f5:
            for read in f5.get_reads():
                compression = check_read_compression(read)
                self.assertEqual(compression, VBZ)

    def test_check_single_read_folder(self):
        input_folder = os.path.join(test_data, 'single_reads')
        compression_results = list(check_compression(input_folder, recursive=False,
                                                     follow_symlinks=False, check_all_reads=False))

        ## expected
        expected_results = []
        for input_file in os.listdir(input_folder):
            input_path = os.path.join(input_folder, input_file)
            with Fast5File(input_path, 'r') as f5:
                expected_results.append((GZIP, f5.read_id, input_path))

        self.assertTrue(numpy.array_equal(expected_results, compression_results))

    def test_check_multi_read(self):
        input_folder = os.path.join(test_data, 'vbz_reads')
        ## expected results
        expected_results = []
        for input_file in os.listdir(input_folder):
            input_path = os.path.join(input_folder, input_file)
            with MultiFast5File(input_path, 'r') as f5:
                for read in f5.get_reads():
                    expected_results.append((VBZ, read.read_id, input_path))

        # Test check all reads True
        compression_results = list(check_compression(input_folder, recursive=False, follow_symlinks=False,
                                                     check_all_reads=True))
        self.assertTrue(numpy.array_equal(expected_results, compression_results))

        ## check one read only
        compression_results = list(check_compression(input_folder, recursive=False, follow_symlinks=False,
                                                     check_all_reads=False))
        self.assertTrue(len(compression_results) == len(os.listdir(input_folder)))
        self.assertTrue(compression_results[0] in expected_results)
