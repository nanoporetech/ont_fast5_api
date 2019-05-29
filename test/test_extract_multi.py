from tempfile import _get_candidate_names, mkdtemp
from ont_fast5_api.conversion_tools.multi_fast5_subset import Fast5Filter, read_generator, extract_selected_reads
import unittest

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path


class ExtractMultiTest(unittest.TestCase):
    def setUp(self):
        self.multifast5 = Path(__file__).parent / "data" / "multi_read" / "batch_0.fast5"
        self.read_set = {"568b93db", "9171d66b"}

    def test_read_generator(self):
        test_read_set = {item for item in self.read_set}  # copy

        for read, group in read_generator(input_file=self.multifast5, read_set=self.read_set):
            assert read in self.read_set
            test_read_set.remove(read)

        assert len(test_read_set) == 0

    def test_extract_selected_reads(self):
        test_read_set = {item for item in self.read_set}  # copy to be modified

        # three test for count below, equaling and above number of read in input file
        for count in (1, 2, 3):
            temp_file_name = next(_get_candidate_names())
            found_reads, output_file, input_file = extract_selected_reads(input_file=self.multifast5,
                                                                          output_file=temp_file_name,
                                                                          count=count, read_set=self.read_set)
            if count < len(test_read_set):
                assert found_reads.issubset(test_read_set)
                assert input_file == self.multifast5
            elif count == len(test_read_set):
                assert found_reads == test_read_set
                assert input_file == self.multifast5
            elif count >= len(test_read_set):
                assert found_reads == test_read_set
                assert input_file is None

            assert output_file == temp_file_name
            Path(temp_file_name).unlink()

    def test_selector_args_generator(self):

        test_read_set = {item for item in self.read_set}  # copy to be modified
        base_path = Path(__file__).parent
        single_reads = base_path / "data" / "single_reads"

        input_f5s = list(single_reads.glob('*'))
        assert single_reads.is_dir(), single_reads
        batch_size = 1

        # create mock read id list file
        temp_file_name = next(_get_candidate_names())
        with open(temp_file_name, 'w') as temp_file:
            for read in test_read_set:
                temp_file.write(read + '\n')
            temp_file.flush()
            temp_file.seek(0)

        # catch exceptions to make sure temp_file is deleted after
        try:
            temp_dir = mkdtemp()
            f = Fast5Filter(input_folder=single_reads, output_folder=temp_dir, read_list_file=temp_file_name,
                            batch_size=batch_size,
                            filename_base="batch")

            args_combos = list(f._args_generator())
            # there should be two tuples of arguments
            assert len(args_combos) == len(test_read_set) / batch_size

            num_files_queued = len(f.input_f5s)
            assert num_files_queued == (len(input_f5s) - len(args_combos)), f.input_f5s
            assert len(f.available_out_files) == 0

            # "exhaust" an input file and put output file back on queue
            input_file, output_file, reads, count = args_combos[0]
            f._update_file_lists(reads={}, in_file=None, out_file=output_file)
            assert len(f.input_f5s) == num_files_queued
            assert len(f.available_out_files) == 1

            # this results in another args tuple generated
            new_args_combos = list(f._args_generator())
            assert len(new_args_combos) == 1, len(new_args_combos)
            Path(temp_file_name).unlink()

        except Exception as e:
            Path(temp_file_name).unlink()
            raise
