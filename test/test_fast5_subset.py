import os
import numpy
from glob import glob
from unittest.mock import patch

from ont_fast5_api.compression_settings import VBZ_V0
from ont_fast5_api.conversion_tools.fast5_subset import Fast5Filter, read_generator, extract_selected_reads
from ont_fast5_api.multi_fast5 import MultiFast5File
from ont_fast5_api.fast5_file import Fast5File
from test.helpers import TestFast5ApiHelper, test_data


class TestFast5Subset(TestFast5ApiHelper):
    input_multif5_path = os.path.join(test_data, "multi_read", "batch_0.fast5")
    read_set = {"fe85b517-62ee-4a33-8767-41cab5d5ab39", "fe9374ee-b86a-4ca4-81dc-ac06e3297728"}

    def test_read_generator(self):
        count = 0
        for read_id, read in read_generator(input_file=self.input_multif5_path, read_set=self.read_set):
            assert read_id in self.read_set
            count += 1

        assert len(self.read_set) == count

    def _create_read_list_file(self, read_ids):
        output_path = os.path.join(self.save_path, 'read_list.txt')
        with open(output_path, 'w') as fh:
            for read_id in read_ids:
                fh.write(read_id + "\n")
        return output_path

    @patch('ont_fast5_api.conversion_tools.fast5_subset.logging')
    @patch('ont_fast5_api.conversion_tools.fast5_subset.get_progress_bar')
    def test_subset_from_single(self, mock_log, mock_pbar):
        input_path = os.path.join(test_data, "single_reads")
        read_list = self._create_read_list_file(self.read_set)
        f5_filter = Fast5Filter(input_folder=input_path,
                                output_folder=self.save_path,
                                read_list_file=read_list)
        f5_filter.run_batch()

        count = 0
        with MultiFast5File(os.path.join(self.save_path, 'batch0.fast5'), 'r') as output_f5:
            for input_file in os.listdir(input_path):
                with Fast5File(os.path.join(input_path, input_file), 'r') as input_f5:
                    read_id = input_f5.get_read_id()
                    if read_id in self.read_set:
                        read_in = input_f5.get_read(read_id)
                        read_out = output_f5.get_read(read_id)
                        self.assertTrue(numpy.array_equal(read_in.get_raw_data(), read_out.get_raw_data()))
                        count += 1
        self.assertEqual(len(self.read_set), count)

    @patch('ont_fast5_api.conversion_tools.fast5_subset.logging')
    @patch('ont_fast5_api.conversion_tools.fast5_subset.get_progress_bar')
    def test_subset_from_multi(self, mock_log, mock_pbar):
        read_list = self._create_read_list_file(self.read_set)
        f5_filter = Fast5Filter(input_folder=os.path.dirname(self.input_multif5_path),
                                output_folder=self.save_path,
                                read_list_file=read_list)
        f5_filter.run_batch()
        with MultiFast5File(self.input_multif5_path, 'r') as input_f5, \
                MultiFast5File(os.path.join(self.save_path, 'batch0.fast5'), 'r') as output_f5:
            self.assertEqual(len(self.read_set), len(output_f5.get_read_ids()))
            for read_id in self.read_set:
                read_in = input_f5.get_read(read_id)
                read_out = output_f5.get_read(read_id)
                self.assertTrue(numpy.array_equal(read_in.get_raw_data(), read_out.get_raw_data()))

    def test_extract_selected_reads(self):
        # three test for count below, equaling and above number of read in input file
        for count in (1, 2, 3):
            temp_file_name = self.generate_temp_filename()
            found_reads, output_file, input_file = extract_selected_reads(input_file=self.input_multif5_path,
                                                                          output_file=temp_file_name,
                                                                          count=count, read_set=self.read_set)
            if count < len(self.read_set):
                assert found_reads.issubset(self.read_set)
                assert input_file == self.input_multif5_path
            elif count == len(self.read_set):
                assert found_reads == self.read_set
                assert input_file == self.input_multif5_path
            elif count >= len(self.read_set):
                assert found_reads == self.read_set
                assert input_file is None

            assert output_file == temp_file_name
            # verify that resulting output file is a legal MultiFast5 with desired reads in it
            with MultiFast5File(output_file) as multi_file:
                readlist = multi_file.get_read_ids()
                self.assertTrue(set(readlist).issubset(self.read_set))

    @patch('ont_fast5_api.conversion_tools.fast5_subset.get_progress_bar')
    def test_selector_args_generator(self, mock_pbar):
        single_reads = os.path.join(test_data, "single_reads")
        assert os.path.isdir(single_reads), single_reads

        input_f5s = glob(os.path.join(single_reads, '*.fast5'))  # list(single_reads.glob('*'))
        batch_size = 1

        # create mock read id list file
        temp_file_name = self.generate_temp_filename()
        with open(temp_file_name, 'w') as temp_file:
            for read in self.read_set:
                temp_file.write(read + '\n')

        f = Fast5Filter(input_folder=single_reads, output_folder=self.save_path, read_list_file=temp_file_name,
                        batch_size=batch_size, filename_base="batch", target_compression=VBZ_V0)

        args_combos = list(f._args_generator())
        # there should be two tuples of arguments
        assert len(args_combos) == len(self.read_set) / batch_size

        num_files_queued = len(f.input_f5s)
        assert num_files_queued == (len(input_f5s) - len(args_combos)), f.input_f5s
        assert len(f.available_out_files) == 0

        # "exhaust" an input file and put output file back on queue
        input_file, output_file, reads, count, compression = args_combos[0]
        f._update_file_lists(reads={}, in_file=None, out_file=output_file)
        assert len(f.input_f5s) == num_files_queued
        assert len(f.available_out_files) == 1
        self.assertEqual(compression, VBZ_V0)

        # this results in another args tuple generated
        new_args_combos = list(f._args_generator())
        assert len(new_args_combos) == 1, len(new_args_combos)
