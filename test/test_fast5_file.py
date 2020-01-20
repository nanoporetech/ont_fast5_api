try:
    from ConfigParser import ConfigParser
except:  # python3
    from configparser import ConfigParser

import os
from shutil import copyfile
from tempfile import NamedTemporaryFile

from ont_fast5_api import CURRENT_FAST5_VERSION
from ont_fast5_api.fast5_info import Fast5Info, _clean
from ont_fast5_api.fast5_file import Fast5File
from test.helpers import TestFast5ApiHelper, test_data


class TestFast5File(TestFast5ApiHelper):

    def test_001_get_latest_analysis(self):
        test_file = os.path.join(test_data, 'basecall_2d_file_v1.0.fast5')
        with Fast5File(test_file, mode='r') as fh:
            group_name = fh.get_latest_analysis('Basecall_2D')
            self.assertEqual('Basecall_2D_000', group_name)
            # Test a non-existent group.
            group_name = fh.get_latest_analysis('Garbage_5D')
            self.assertEqual(None, group_name)

    def test_002_read_summary_data(self):
        test_file = os.path.join(test_data, 'telemetry_test.fast5')
        summary = Fast5File.read_summary_data(test_file, 'segmentation')
        expected = {'filename': 'telemetry_test.fast5',
                    'channel_id': {u'channel_number': 129,
                                   u'range': 10000.0,
                                   u'sampling_rate': 5000,
                                   u'digitisation': 10000,
                                   u'offset': 0.0},
                    'reads': [{'duration': 755.79559999999947,
                               'start_time': 4034.6948000000002,
                               'read_id': 'telemetry_test.fast5',
                               'start_mux': 1,
                               'read_number': 199}],
                    'tracking_id': {u'device_id': '445444'},
                    'data': {u'split_hairpin': {u'median_sd_comp': 1.4719812720343015,
                                                u'range_comp': 3.965029408419298,
                                                u'median_level_temp': 88.66729546440973,
                                                u'duration_temp': 327.82499999999936,
                                                u'num_temp': 10773,
                                                u'num_events': 24091,
                                                u'median_sd_temp': 1.328457722537222,
                                                u'range_temp': 4.01780031383548,
                                                u'median_level_comp': 89.8680971725336,
                                                u'split_index': 10903,
                                                u'duration_comp': 422.3665999999994,
                                                u'num_comp': 13158},
                             u'empty': {}},
                    'software': {u'time_stamp': '2014-Jun-04 16:28:31',
                                 u'version': '0.5.4',
                                 'component': u'Validation'}}
        self.assertEqual(expected, summary)

    def test_002_add_analysis_group(self):
        fname = self.generate_temp_filename()
        with Fast5File(fname, mode='w') as fast5:
            att = {'foo': 1, 'bar': 2}
            fast5.add_analysis('test', 'Test_000', att)
            att_in = fast5.get_analysis_attributes('Test_000')
            att['component'] = 'test'
            self.assertEqual(att, att_in)
            att2 = {'Bob': 'your uncle'}
            fast5.add_analysis_attributes('Test_000', att2)
            att_in = fast5.get_analysis_attributes('Test_000')
            att.update(att2)
            self.assertEqual(att, att_in)

    def test_003_add_analysis_subgroup(self):
        fname = self.generate_temp_filename()
        with Fast5File(fname, mode='w') as fast5:
            fast5.add_analysis('test', 'Test_000', attrs={})
            fast5.add_analysis_subgroup('Test_000', 'Sub1', attrs={'foo': 'bar', 'monkey': 1})
            att_in = fast5.get_analysis_attributes('Test_000/Sub1')
            self.assertEqual({'foo': 'bar', 'monkey': 1}, att_in)

    def test_010_v0_6_single(self):
        # Check that it is recognized properly.
        fname = os.path.join(test_data, 'read_file_v0.6_single.fast5')
        result = Fast5Info(fname)
        self.assertEqual(0.6, result.version)
        # Copy file and Update to current format.
        new_file = os.path.join(self.save_path, 'single_read_v0.6_test.fast5')
        copyfile(fname, new_file)
        Fast5File.update_legacy_file(new_file)
        result = Fast5Info(new_file)
        self.assertEqual(CURRENT_FAST5_VERSION, result.version)
        self.assertEqual(1, len(result.read_info))
        self.assertEqual(5804, result.read_info[0].read_number)
        # Load the event data.
        with Fast5File(new_file, mode='r') as fh:
            analist = fh.list_analyses('event_detection')
            self.assertEqual(1, len(analist))
            group = '{}/Reads/Read_5804'.format(analist[0][1])
            data = fh.get_analysis_dataset(group, 'Events')
            self.assertEqual(8433, data.size)
            self.assertEqual(set(('mean', 'stdv', 'start', 'length')), set(data.dtype.names))
            read_info = fh.status.read_info[0]
            self.assertEqual(8433, read_info.event_data_count)
            channel_info = fh.get_channel_info()
            self.assertEqual(102, channel_info['channel_number'])

    def test_011_v0_6_raw(self):
        # Check that it is recognized properly.
        fname = os.path.join(test_data, 'read_file_v0.6_raw.fast5')
        result = Fast5Info(fname)
        self.assertEqual(0.6, result.version)
        # Copy file and Update to current format.
        new_file = self.generate_temp_filename()
        copyfile(fname, new_file)
        Fast5File.update_legacy_file(new_file)
        result = Fast5Info(new_file)
        self.assertEqual(CURRENT_FAST5_VERSION, result.version)
        self.assertEqual(1, len(result.read_info))
        self.assertEqual(627, result.read_info[0].read_number)
        # Load the event data.
        with Fast5File(new_file, mode='r') as fh:
            analist = fh.list_analyses('event_detection')
            self.assertEqual(1, len(analist))
            group = '{}/Reads/Read_627'.format(analist[0][1])
            data = fh.get_analysis_dataset(group, 'Events')
            self.assertEqual(2337, data.size)
            self.assertEqual(set(('mean', 'stdv', 'start', 'length')), set(data.dtype.names))
            read_info = fh.status.read_info[0]
            self.assertEqual(2337, read_info.event_data_count)
            channel_info = fh.get_channel_info()
            self.assertEqual(118, channel_info['channel_number'])
            raw = fh.get_raw_data(read_number=627)
            self.assertEqual(46037, raw.size)
            self.assertEqual(46037, read_info.duration)

    def test_012_v1_0_single(self):
        # Check that it is recognized properly.
        fname = os.path.join(test_data, 'read_file_v1.0_single.fast5')
        result = Fast5Info(fname)
        self.assertEqual(1.0, result.version)
        # Copy file and Update to current format.
        new_file = self.generate_temp_filename()
        copyfile(fname, new_file)
        Fast5File.update_legacy_file(new_file)
        result = Fast5Info(new_file)
        self.assertEqual(CURRENT_FAST5_VERSION, result.version)
        self.assertEqual(1, len(result.read_info))
        self.assertEqual(59, result.read_info[0].read_number)
        # Load the event data.
        with Fast5File(new_file, mode='r') as fh:
            analist = fh.list_analyses('event_detection')
            self.assertEqual(1, len(analist))
            group = '{}/Reads/Read_59'.format(analist[0][1])
            data = fh.get_analysis_dataset(group, 'Events')
            self.assertEqual(7875, data.size)
            self.assertEqual(set(('mean', 'stdv', 'start', 'length')), set(data.dtype.names))
            read_info = fh.status.read_info[0]
            self.assertEqual(7875, read_info.event_data_count)
            channel_info = fh.get_channel_info()
            self.assertEqual(1, channel_info['channel_number'])

    def test_fast5_add_and_get_chain(self):
        fname = self.generate_temp_filename()
        group_name1 = 'First_000'
        component1 = 'first'
        component1_path = 'Analyses/{}'.format(group_name1)
        group_name2 = 'Second_000'
        component2 = 'second'

        # Add fake group
        with Fast5File(fname=fname, mode='w') as fast5:
            fast5.add_analysis(component1, group_name1, attrs={})
            fast5.add_analysis(component2, group_name2, attrs={})

            # Check group was added successfully
            target_list_of_analyses = [(component1, group_name1),
                                       (component2, group_name2)]
            self.assertEqual(fast5.list_analyses(), target_list_of_analyses)

            # Check fake group has chain including itself
            target_chain = [(component2, group_name2)]
            self.assertEqual(fast5.get_chain(group_name2), target_chain)

            # Add component chain
            fake_component_map = {component1: group_name1}
            fast5.add_chain(group_name=group_name2,
                            component_map=fake_component_map)

            # Check attributes are as expected
            attr = {'component': component2, component1: component1_path}
            self.assertEqual(fast5.get_analysis_attributes(group_name2), attr)
            # Check chain is as expected
            chain = [(component2, group_name2), (component1, group_name1)]
            self.assertEqual(fast5.get_chain(group_name2), chain)

    def test_fast5_add_group(self):
        fname = self.generate_temp_filename()
        group_name = 'First_000'
        component = 'first'
        component_path = 'Analyses/{}'.format(group_name)

        with Fast5File(fname, mode='w') as fast5:
            # No analyses in file
            self.assertEqual(fast5.list_analyses(), [])
            self.assertEqual(fast5.get_latest_analysis(component), None)
            # Add group to file & check it's there
            fast5._add_group(component_path, {'component': component})
            self.assertEqual(fast5.list_analyses(), [(component, group_name)])
            # Check latest analyses
            target = {group_name: {'component': component}}
            self.assertEqual(fast5._parse_attribute_tree('Analyses'), target)
            # Get latest analyses
            self.assertEqual(fast5.get_latest_analysis('First'), group_name)

    def test_fast5_add_analysis_dataset(self):
        fname = self.generate_temp_filename()
        group_name = 'First_000'
        component = 'first'

        with Fast5File(fname, mode='w') as fast5:
            self.assertFalse(fast5.list_analyses())
            with self.assertRaises(KeyError):
                fast5.add_analysis_dataset(group_name=group_name,
                                           dataset_name='Example',
                                           data='hello',
                                           attrs=None)

            fast5.add_analysis(component=component,
                               group_name=group_name,
                               attrs={})
            fast5.add_analysis_dataset(group_name=group_name,
                                       dataset_name='Example',
                                       data='hello',
                                       attrs=None)
            answer = fast5.get_analysis_dataset(group_name=group_name,
                                                dataset_name='Example')
            self.assertEqual(answer, 'hello')

    def test_fast5_set_analysis_config(self):
        fname = self.generate_temp_filename()
        group_name = 'First_000'
        component = 'first'
        with Fast5File(fname, mode='w') as fast5:
            self.assertFalse(fast5.list_analyses())
            with NamedTemporaryFile(dir=self.save_path,
                                    delete=False, mode='w+t') as f:
                config_str = "[section]\nkey=value\nkey1=value1\n"
                f.write(config_str)
                config_path = f.name

            config = ConfigParser()
            config.read(config_path)
            self.assertTrue(config)
            self.assertEqual(config.get('section', 'key'), 'value')
            with self.assertRaises(KeyError):
                fast5.set_analysis_config(group_name, config)

            fast5.add_analysis(component, group_name, {})
            fast5.set_analysis_config(group_name, config)
            get_config = fast5.get_analysis_config(group_name)
            self.assertEqual(get_config['section']['key'], 'value')

        os.remove(fname)
        with Fast5File(fname, mode='w') as fast5:
            self.assertFalse(fast5.list_analyses())
            config = {'section': {'key': 'value', 'key1': 'value1'}}
            with self.assertRaises(KeyError):
                fast5.set_analysis_config(group_name, config)
            fast5.add_analysis(component, group_name, {})
            fast5.set_analysis_config(group_name, config)
            get_config = fast5.get_analysis_config(group_name)
            self.assertEqual(get_config['section']['key'], 'value')
