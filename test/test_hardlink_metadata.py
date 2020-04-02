import os

from ont_fast5_api.compression_settings import VBZ
from ont_fast5_api.conversion_tools.compress_fast5 import compress_file
from ont_fast5_api.conversion_tools.fast5_subset import extract_selected_reads
from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File
from ont_fast5_api.static_data import HARDLINK_GROUPS
from test.helpers import TestFast5ApiHelper, test_data


class TestHardlinkMetaData(TestFast5ApiHelper):
    read_subset = {'00031f3e-415c-4ab5-9c16-fb6fe45ff519',
                   "000c0b4e-46c2-4fb5-9b17-d7031eefb975",
                   '000ebd63-3e1a-4499-9ded-26af3225a022',
                   '002ad0e4-c6bb-4eff-a30f-5fec01475ab8',
                   '0059d270-3238-4413-b38b-f588e28326df'}

    def test_create_read(self):
        input_path = os.path.join(test_data, 'hardlink', 'unlinked', 'batch0.fast5')
        output_path = self.generate_temp_filename()
        compress_file(input_path, output_path, target_compression=VBZ)
        new_read_id = "123456789abcdef"
        with MultiFast5File(output_path, 'a') as f5:
            # Test we can hardlink to existing metadata when creating an new empty read
            run_id = list(f5.run_id_map.keys())[0]
            master_read_id = f5.run_id_map[run_id]
            f5.create_empty_read(new_read_id, run_id)
            for group in HARDLINK_GROUPS:
                self.assertTrue(self.is_read_hardlinked(f5, new_read_id, master_read_id, group))

            # Test we don't explode if there is no metadata
            f5.create_empty_read(new_read_id[::-1], "not an existing run_id")

    def test_hardlink_multi_compression(self):
        input_path = os.path.join(test_data, 'hardlink', 'unlinked', 'batch0.fast5')
        output_path = self.generate_temp_filename()

        self.assertFalse(self.is_file_hardlinked(input_path))
        compress_file(input_path, output_path, target_compression=VBZ)
        self.assertTrue(self.is_file_hardlinked(output_path))

    def test_hardlink_subset(self):
        input_path = os.path.join(test_data, 'hardlink', 'unlinked', 'batch0.fast5')
        output_path = self.generate_temp_filename()

        self.assertFalse(self.is_file_hardlinked(input_path))
        extract_selected_reads(input_path, output_path, self.read_subset, count=len(self.read_subset))
        self.assertTrue(self.is_file_hardlinked(output_path))

    def test_hardlink_subset_single_reads(self):
        input_path = os.path.join(test_data, 'hardlink', 'single_reads')
        output_path = self.generate_temp_filename()

        for single_read_file in os.listdir(input_path):
            extract_selected_reads(os.path.join(input_path, single_read_file), output_path, self.read_subset, count=1)
        self.assertTrue(self.is_file_hardlinked(output_path))

    def test_hardlink_single_to_multi(self):
        input_folder = os.path.join(test_data, 'hardlink', 'single_reads')
        input_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder)]
        output_path = self.generate_temp_filename()

        with MultiFast5File(output_path, 'a') as multi_f5:
            for input_file in input_files:
                with get_fast5_file(input_file, 'r') as f5_file:
                    for read in f5_file.get_reads():
                        multi_f5.add_existing_read(read)

        with MultiFast5File(output_path, 'r') as multi_f5:
            self.assertEqual(len(input_files), len(multi_f5.get_read_ids()))
        self.assertTrue(self.is_file_hardlinked(output_path))

    def is_file_hardlinked(self, input_path):
        file_hardlinked = True
        with MultiFast5File(input_path, 'r') as f5_file:
            for read in f5_file.get_reads():
                master_read_id = f5_file.run_id_map[read.get_run_id()]
                for group in HARDLINK_GROUPS:
                    file_hardlinked &= self.is_read_hardlinked(f5_file, read.read_id, master_read_id, group)
        return file_hardlinked

    def is_read_hardlinked(self, f5_handle, read_id1, read_id2, group):
        if read_id1 == read_id2:
            return True
        group1 = f5_handle.get_read(read_id1).handle[group]
        group2 = f5_handle.get_read(read_id2).handle[group]
        return group1 == group2
