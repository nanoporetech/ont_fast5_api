import numpy
import os
import random

from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.fast5_read import Fast5Read
from ont_fast5_api.multi_fast5 import MultiFast5File
from test.helpers import TestFast5ApiHelper

hexdigits = "0123456789abcdef"
run_id = "123abc"


class TestMultiFast5(TestFast5ApiHelper):

    def create_multi_file(self, read_ids):
        filename = self.generate_temp_filename()
        with MultiFast5File(filename, 'w') as multi_f5:
            for read_id in read_ids:
                multi_f5.create_empty_read(read_id, run_id)
        return filename

    def test_read_interface(self):
        read_ids = generate_read_ids(6)
        f5_file = self.create_multi_file(read_ids)

        with MultiFast5File(f5_file, 'a') as multi_f5:
            # Check we have the read_ids we expect
            self.assertEqual(sorted(read_ids), sorted(multi_f5.get_read_ids()))

            # Try and add another read with the same read_id and expect error
            with self.assertRaises(ValueError):
                multi_f5.create_empty_read(read_ids[0], run_id)

            # Test we can get a read from the file and it has the interface we expect
            read_0 = multi_f5.get_read(read_ids[0])
            self.assertTrue(isinstance(read_0, Fast5Read))

            # Test we cannot get a read which doesn't exit
            with self.assertRaises(KeyError):
                multi_f5.get_read("0123")

    def test_raw_data(self):
        f5_file = self.create_multi_file(generate_read_ids(4))
        data = list(range(10))
        raw_attrs = {
            "duration": 1,
            "median_before": 2.5,
            "read_id": "abcd",
            "read_number": 8,
            "start_mux": 2,
            "start_time": 99
        }
        with MultiFast5File(f5_file, 'a') as multi_f5:
            read0 = multi_f5.get_read(multi_f5.get_read_ids()[0])
            read0.add_raw_data(data, attrs=raw_attrs)
            output_data = read0.get_raw_data()
            numpy.testing.assert_array_equal(output_data, data)

    def test_channel_info(self):
        f5_file = self.create_multi_file(generate_read_ids(4))
        channel_info = {
            "digitisation": 2048,
            "offset": -119.5,
            "range": 74.2,
            "sampling_rate": 4000,
            "channel_number": "72"
        }
        # Fast5File explicitly casts the channel number on reading
        expected_out = channel_info.copy()
        expected_out['channel_number'] = int(channel_info['channel_number'])
        with MultiFast5File(f5_file, 'a') as multi_f5:
            read0 = multi_f5.get_read(multi_f5.get_read_ids()[0])
            read0.add_channel_info(channel_info)
            output_data = read0.get_channel_info()
            self.assertEqual(output_data, expected_out)

    def test_tracking_id(self):
        f5_file = self.create_multi_file(generate_read_ids(4))
        tracking_id = {
            "asic_id_eeprom": "some string",
            "device_id": "some string",
            "exp_script_name": "some string",
            "exp_script_purpose": "some string",
            "exp_start_time": "some string",
            "flow_cell_id": "some string",
            "hostname": "some string",
            "protocol_run_id": "some string",
            "protocols_version": "some string",
            "run_id": "some string",
            "version": "some string",
        }

        with MultiFast5File(f5_file, 'a') as multi_f5:
            read0 = multi_f5.get_read(multi_f5.get_read_ids()[0])
            read0.add_tracking_id(tracking_id)
            output_data = read0.get_tracking_id()
            self.assertEqual(output_data, tracking_id)

    def test_add_analysis(self):
        f5_file = self.create_multi_file(generate_read_ids(4))
        group = "Test"
        component = "test_component"
        attrs = {"attribute": 1}

        # Fast5File.add_analysis includes the component name in the analysis attributes
        expected_attributes = attrs.copy()
        expected_attributes['component'] = component
        with MultiFast5File(f5_file, 'a') as multi_f5:
            read0 = multi_f5.get_read(multi_f5.get_read_ids()[0])
            self.assertEqual(read0.list_analyses(), [])
            read0.add_analysis(component, group, attrs)
            self.assertEqual(read0.list_analyses(), [(component, group)])
            self.assertEqual(read0.get_analysis_attributes(group), expected_attributes)


def generate_read_ids(num_ids, id_len=8):
    return ["".join(random.choice(hexdigits) for _ in range(id_len)) for _ in range(num_ids)]
