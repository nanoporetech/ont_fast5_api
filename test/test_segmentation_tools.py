import os
import numpy as np

from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.event_detection import EventDetectionTools
from ont_fast5_api.analysis_tools.segmentation import SegmentationTools
from test.helpers import TestFast5ApiHelper


class TestSegmentationTools(TestFast5ApiHelper):

    def test_001_raw_only(self):
        fname = self.generate_temp_filename()
        with Fast5File(fname, mode='w') as fh:
            fh.add_channel_info({'channel_number': 1,
                                 'sampling_rate': 4000,
                                 'digitisation': 8192,
                                 'range': 819.2,
                                 'offset': 0})
            fh.add_read(12, 'unique_snowflake', 12345, 1000, 0, 120.75)
            raw = np.empty(1000, dtype=np.int16)
            raw[:] = range(1000)
            fh.add_raw_data(raw)
            attrs = {'name': 'test', 'version': 0, 'time_stamp': 'just now'}
            fh.add_analysis('segmentation', 'Segmentation_000', attrs)
            segment_data = {'has_template': 1,
                            'has_complement': 1,
                            'first_sample_template': 10,
                            'duration_template': 470,
                            'first_sample_complement': 520,
                            'duration_complement': 460}
            fh.set_summary_data('Segmentation_000', 'segmentation', segment_data)
            with SegmentationTools(fh, group_name='Segmentation_000') as segment:
                results = segment.get_results()
                self.assertDictEqual({'has_template': True,
                                      'has_complement': True,
                                      'first_sample_template': 10,
                                      'duration_template': 470,
                                      'first_sample_complement': 520,
                                      'duration_complement': 460}, results)
                temp_raw = segment.get_raw_data('template', scale=False)
                np.testing.assert_array_equal(temp_raw, raw[10:480])
                comp_raw = segment.get_raw_data('complement', scale=False)
                np.testing.assert_array_equal(comp_raw, raw[520:980])
                temp_raw, comp_raw = segment.get_raw_data('both', scale=False)
                np.testing.assert_array_equal(temp_raw, raw[10:480])
                np.testing.assert_array_equal(comp_raw, raw[520:980])
                temp_raw, comp_raw = segment.get_raw_data('both', scale=True)
                scaled_temp = raw[10:480] * 0.1
                scaled_comp = raw[520:980] * 0.1
                np.testing.assert_array_almost_equal(temp_raw, scaled_temp, decimal=5)
                np.testing.assert_array_almost_equal(comp_raw, scaled_comp, decimal=5)

    def test_002_events_only(self):
        fname = self.generate_temp_filename()
        with Fast5File(fname, mode='w') as fh:
            fh.add_channel_info({'channel_number': 1,
                                 'sampling_rate': 4000,
                                 'digitisation': 8192,
                                 'range': 819.2,
                                 'offset': 0})
            fh.add_read(12, 'unique_snowflake', 10000, 1000, 0, 120.75)
            with EventDetectionTools(fh, group_name='EventDetection_000', meta={'name': 'test'}) as evdet:
                data = np.zeros(100, dtype=[('start', int), ('length', int), ('mean', float), ('stdv', float)])
                data['start'][2] = 10010
                data['start'][46] = 10470
                data['length'][46] = 10
                data['start'][53] = 10520
                data['start'][97] = 10960
                data['length'][97] = 20
                read_attrs = {'read_number': 12}
                evdet.set_event_data(data, read_attrs)
            attrs = {'name': 'test', 'version': 0, 'time_stamp': 'just now',
                     'event_detection': 'Analyses/EventDetection_000'}
            fh.add_analysis('segmentation', 'Segmentation_000', attrs)
            segment_data = {'has_template': 1,
                            'has_complement': 1,
                            'start_event_template': 2,
                            'end_event_template': 47,
                            'start_event_complement': 53,
                            'end_event_complement': 98}
            fh.set_summary_data('Segmentation_000', 'segmentation', segment_data)
            with SegmentationTools(fh, group_name='Segmentation_000') as segment:
                results = segment.get_results()
                self.assertDictEqual({'has_template': True,
                                      'has_complement': True,
                                      'start_event_template': 2,
                                      'end_event_template': 47,
                                      'start_event_complement': 53,
                                      'end_event_complement': 98,
                                      'first_sample_template': 10,
                                      'duration_template': 470,
                                      'first_sample_complement': 520,
                                      'duration_complement': 460}, results)
