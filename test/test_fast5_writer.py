import os
import shutil
import unittest
import filecmp
import numpy as np
from ont_fast5_api.helpers import compare_hdf_files
from ont_fast5_api.fast5_writer import Fast5Writer

test_data = os.path.join(os.path.dirname(__file__), 'data')
save_path = os.path.join(os.path.dirname(__file__), 'tmp')


class TestFast5Writer(unittest.TestCase):
    def setUp(self):
        self._data = test_data
        self._path = save_path
        if not os.path.exists(self._path):
            os.makedirs(self._path)

    def tearDown(self):
        shutil.rmtree(self._path)

    def test_001_single_read(self):
        read_attrs = {'read_number': 0,
                      'start_mux': 1,
                      'start_time': 0,
                      'duration': 10,
                      'read_id': 'abcd',
                      'median_before': 100.0,
                      'scaling_used': 1}
        strand = {'run_id': 1, 'strand_id': 1, 'channel': 1,
                  'offset': 0, 'range': 1000.0, 'digitisation': 8192,
                  'sampling_rate': 5000, 'read_attrs': read_attrs}
        names = ['mean', 'start', 'length', 'stdv', 'pos', 'labl']
        formats = [float, float, float, float, int, (str, 32)]
        raw = np.zeros(100, dtype=np.int16)
        data = np.empty(3, dtype=list(zip(names, formats)))
        data['mean'] = [1.5, 2.7, 3.3]
        data['start'] = [0.0, 1.5, 3.5]
        data['length'] = [1.5, 2.0, 2.5]
        data['stdv'] = [1.0, 1.2, 1.4]
        data['pos'] = [0, 1, 3]
        data['labl'] = ['TTT', 'TTC', 'CGA']
        strand['event_data'] = data
        strand['raw_data'] = raw
        with Fast5Writer(self._path, 'writer_test1', 1) as writer:
            writer.write_strand(strand)
            strand['read_attrs']['read_number'] = 1
            writer.write_strand(strand)
            strand['read_attrs']['read_number'] = 2
            writer.write_strand(strand)
            strand['channel'] = 2
            strand['read_attrs']['read_number'] = 3
            writer.write_strand(strand)
        self.assertTrue(filecmp.cmp(os.path.join(self._path,
                                                 'writer_test1_index.txt'),
                                    os.path.join(self._data,
                                                 'writer_test1_index.txt')))
        for fname in ['writer_test1_ch1_read0_strand.fast5',
                      'writer_test1_ch1_read1_strand.fast5',
                      'writer_test1_ch1_read2_strand.fast5',
                      'writer_test1_ch2_read3_strand.fast5']:
            result = compare_hdf_files(os.path.join(self._path, fname),
                                       os.path.join(self._data, fname))
            self.assertTrue(result,
                            'File {} failed the regression test.'.format(fname))

    def test_002_multi_read(self):
        read_attrs = {'read_number': 0,
                      'start_mux': 1,
                      'start_time': 0,
                      'duration': 10,
                      'read_id': 'abcd',
                      'median_before': 100.0,
                      'scaling_used': 1}
        strand = {'run_id': 1, 'strand_id': 1, 'channel': 1,
                  'offset': 0, 'range': 1000.0, 'digitisation': 8192,
                  'sampling_rate': 5000, 'read_attrs': read_attrs}
        names = ['mean', 'start', 'length', 'stdv', 'pos', 'labl']
        formats = [float, float, float, float, int, (str, 32)]
        raw = np.zeros(100, dtype=np.int16)
        data = np.empty(3, dtype=list(zip(names, formats)))
        data['mean'] = [1.5, 2.7, 3.3]
        data['start'] = [0.0, 1.5, 3.5]
        data['length'] = [1.5, 2.0, 2.5]
        data['stdv'] = [1.0, 1.2, 1.4]
        data['pos'] = [0, 1, 3]
        data['labl'] = ['TTT', 'TTC', 'CGA']
        strand['event_data'] = data
        strand['raw_data'] = raw
        with Fast5Writer(self._path, 'writer_test2', 2) as writer:
            writer.write_strand(strand)
            strand['read_attrs']['read_number'] = 1
            writer.write_strand(strand)
            strand['read_attrs']['read_number'] = 2
            writer.write_strand(strand)
            strand['channel'] = 2
            strand['read_attrs']['read_number'] = 3
            writer.write_strand(strand)
        self.assertTrue(filecmp.cmp(os.path.join(self._path,
                                                 'writer_test2_index.txt'),
                                    os.path.join(self._data,
                                                 'writer_test2_index.txt')))
        for fname in ['writer_test2_ch1_read0_strand.fast5',
                      'writer_test2_ch1_read2_strand.fast5',
                      'writer_test2_ch2_read3_strand.fast5']:
            result = compare_hdf_files(os.path.join(self._path, fname),
                                       os.path.join(self._data, fname))
            self.assertTrue(result,
                            'File {} failed the regression test.'.format(fname))
