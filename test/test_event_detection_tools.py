import os
import numpy as np
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.event_detection import EventDetectionTools
from test.helpers import TestFast5ApiHelper, test_data


class TestEventDetectionTools(TestFast5ApiHelper):

    def test_001_read_events(self):
        # Check that it is recognized properly.
        fname = os.path.join(test_data, 'read_file_v1.0_single.fast5')
        with EventDetectionTools(fname, mode='r', ) as fh:
            self.assertTrue(fh.has_event_data)
            self.assertTrue(fh.has_event_data(read_number=59))
            self.assertEqual('EventDetection_000', fh.group_name)
            data, attrs = fh.get_event_data()
            self.assertDictEqual({'read_number': 59,
                                  'strand_id': 60,
                                  'start_mux': 1,
                                  'end_mux': 1,
                                  'start_time': 32463855,
                                  'duration': 729468}, attrs)
            self.assertEqual(7875, data.size)
            self.assertEqual(118, data[0]['length'])
            data, attrs = fh.get_event_data(time_in_seconds=True)
            self.assertEqual(0.0236, data[0]['length'])

    def test_002_write_events(self):
        fname = os.path.join(self.save_path, 'test.fast5')
        with Fast5File(fname, 'w') as fh:
            fh.add_channel_info({'channel_number': 1, 'sampling_rate': 4000})
            fh.add_read(12, 'unique_snowflake', 12345, 111, 0, 120.75)
            with EventDetectionTools(fh, group_name='EventDetection_000',
                                     meta={'name': 'test', 'version': '0.1.0'}) as evdet:
                data = np.zeros(100, dtype=[('start', int), ('length', int), ('mean', float), ('stdv', float)])
                read_attrs = {'read_number': 12}
                evdet.set_event_data(data, read_attrs)
        with Fast5File(fname, 'r') as fh:
            self.assertEqual(1, len(fh.status.read_info))
            read_info = fh.status.read_info[0]
            self.assertEqual(12, read_info.read_number)
            group = fh.get_latest_analysis('EventDetection')
            self.assertEqual('EventDetection_000', group)
            with EventDetectionTools(fh) as evdet:
                self.assertTrue(evdet.has_event_data())
                data, attrs = evdet.get_event_data()
                self.assertDictEqual({u'read_number': 12,
                                      u'read_id': 'unique_snowflake',
                                      u'start_time': 12345,
                                      u'duration': 111,
                                      u'start_mux': 0,
                                      u'median_before': 120.75}, attrs)
                self.assertEqual(100, data.size)
