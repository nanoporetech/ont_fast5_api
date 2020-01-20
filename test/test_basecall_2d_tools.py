import numpy as np
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.basecall_2d import Basecall2DTools
from test.helpers import TestFast5ApiHelper


class TestBasecall2DTools(TestFast5ApiHelper):

    def test_001_put_and_retrieve(self):
        fname = self.generate_temp_filename()
        dtypes = [('template', int), ('complement', int)]
        data1 = np.zeros(10, dtype=dtypes)
        data1['template'] = [0, 1, 2, 2, 3, 4, 5, 6, 7, 8]
        data1['complement'] = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
        dtypes.append(('model_state', '<U5'))
        data2 = np.zeros(10, dtype=dtypes)
        data2['template'] = data1['template']
        data2['complement'] = data1['complement']
        data2['model_state'] = ['AAAAA', 'AAAAT', 'AAATC', 'AAATC', 'ATCCG',
                                'TCCGT', 'CCGTT', 'CGTTA', 'CGTTA', 'GTTAC']
        seq = 'AAAAATCCGTTAC'
        qstring = 'blahblahblahb'
        with Fast5File(fname, mode='w') as fh:
            fh.add_channel_info({'channel_number': 1,
                                 'sampling_rate': 4000,
                                 'digitisation': 8192,
                                 'range': 819.2,
                                 'offset': 0})
            fh.add_read(12, 'unique_snowflake', 12345, 1000, 0, 120.75)
            attrs = {'name': 'test', 'version': 0, 'time_stamp': 'just now'}
            fh.add_analysis('basecall_2d', 'Basecall_2D_000', attrs)
            with Basecall2DTools(fh, group_name='Basecall_2D_000') as basecall:
                basecall.add_prior_alignment(data1)
                basecall.add_2d_call_alignment(data2)
                basecall.add_called_sequence("2D", 'test_2d', seq, qstring)
        with Fast5File(fname, mode='r') as fh:
            with Basecall2DTools(fh, group_name='Basecall_2D_000') as basecall:
                hp_align = basecall.get_prior_alignment()
                np.testing.assert_array_equal(hp_align, data1)
                bc2d = basecall.get_2d_call_alignment()
                np.testing.assert_array_equal(bc2d, data2)
                n, s, q = basecall.get_called_sequence("2D")
                self.assertEqual(n, 'test_2d')
                self.assertEqual(s, seq)
                self.assertEqual(q, qstring)
