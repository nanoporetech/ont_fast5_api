import numpy as np
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.basecall_1d import Basecall1DTools
from test.helpers import TestFast5ApiHelper


class TestBasecall1DTools(TestFast5ApiHelper):

    def test_001_put_and_retrieve(self):
        fname = self.generate_temp_filename()
        dtypes = [('mean', float),
                  ('start', float),
                  ('stdv', float),
                  ('length', float),
                  ('called_state', '<U5'),
                  ('move', int)]
        data1 = np.zeros(10, dtype=dtypes)
        data1['mean'] = [10.0, 15.0, 8.5, 7.2, 13.6, 9.4, 11.8, 10.1, 4.2, 10.9]
        data1['stdv'] = [0.7, 0.9, 1.0, 1.1, 0.75, 0.6, 0.83, 1.12, 9.45, 2.9]
        data1['start'] = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        data1['length'] = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]
        data1['move'] = [1, 1, 0, 1, 0, 2, 1, 1, 1, 0]
        data1['called_state'] = ['AAAAA', 'AAAAT', 'AAAAT', 'AAATC', 'AAATC',
                                 'ATCCG', 'TCCGT', 'CCGTT', 'CGTTA', 'CGTTA']
        data2 = data1[::-1]
        seq1 = 'AAAAATCCGTTA'
        seq2 = 'TAACGGATTTTT'
        qstring1 = 'blahblahblah'
        qstring2 = 'halbhalbhalb'
        with Fast5File(fname, mode='w') as fh:
            fh.add_channel_info({'channel_number': 1,
                                 'sampling_rate': 4000,
                                 'digitisation': 8192,
                                 'range': 819.2,
                                 'offset': 0})
            fh.add_read(12, 'unique_snowflake', 12345, 1000, 0, 120.75)
            attrs = {'name': 'test', 'version': 0, 'time_stamp': 'just now'}
            fh.add_analysis('basecall_1d', 'Basecall_1D_000', attrs)
            with Basecall1DTools(fh, group_name='Basecall_1D_000') as basecall:
                basecall.add_event_data('template', data1)
                basecall.add_event_data('complement', data2)
                basecall.add_called_sequence('template', 'template', seq1, qstring1)
                basecall.add_called_sequence('complement', 'complement', seq2, qstring2)
        with Fast5File(fname, mode='r') as fh:
            with Basecall1DTools(fh, group_name='Basecall_1D_000') as basecall:
                events1 = basecall.get_event_data('template')
                np.testing.assert_array_equal(events1, data1)
                events2 = basecall.get_event_data('complement')
                np.testing.assert_array_equal(events2, data2)
                n1, s1, q1 = basecall.get_called_sequence('template')
                self.assertEqual(n1, 'template')
                self.assertEqual(s1, seq1)
                self.assertEqual(q1, qstring1)
                n2, s2, q2 = basecall.get_called_sequence('complement')
                self.assertEqual(n2, 'complement')
                self.assertEqual(s2, seq2)
                self.assertEqual(q2, qstring2)
