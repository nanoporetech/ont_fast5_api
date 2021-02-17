from pathlib import Path
from ont_fast5_api.fast5_read import Fast5Read
from test.helpers import test_data, TestFast5ApiHelper

from ont_fast5_api.conversion_tools.conversion_utils import yield_fast5_files, yield_fast5_reads

class TestFast5ConversionUtilities(TestFast5ApiHelper):
    """
    Test the convenience functions yield_fast5_files and yield_fast5_reads
    """

    def setUp(self) -> None:
        super().setUp()

        # Known good read_ids from test_data/multi_read/batch_0.fast5
        self.read_id_set = {'fe849dd3-63bc-4044-8910-14e1686273bb',
                            'fe85b517-62ee-4a33-8767-41cab5d5ab39'}
        self.read_id_list = ['fe849dd3-63bc-4044-8910-14e1686273bb',
                            'fe85b517-62ee-4a33-8767-41cab5d5ab39']
        self.fast5_path = test_data + "/multi_read/batch_0.fast5"


    def test_yield_fast5_files_from_fast5_file(self):
        f5_gen = yield_fast5_files(self.fast5_path, recursive=False)
        f5_path = next(f5_gen)
        self.assertTrue(Path(f5_path).is_file(), "Filepath is not a file")
        self.assertTrue(f5_path.endswith('.fast5'), "Filepath does not end with fast5 extension")
        self.assertTrue(Path(f5_path).absolute() == Path(self.fast5_path).absolute(),
                        "Direct path did not return itself")

    def test_yield_fast5_files_from_dir(self):
        f5_gen = yield_fast5_files(test_data, recursive=False)

        for f5_path in f5_gen:
            self.assertTrue(Path(f5_path).is_file(), "Filepath is not a file")
            self.assertTrue(f5_path.endswith('.fast5'), "Filepath does not end with fast5 extension")

    def test_yield_fast5_reads_from_fast5_file(self):
        f5_read_gen = yield_fast5_reads(self.fast5_path, recursive=False)
        read_id, read_data = next(f5_read_gen)
        self.assertTrue(read_id is not None, "read_id is None")
        self.assertTrue(isinstance(read_data, Fast5Read), "Return is not Fast5Read instance")

    def test_yield_fast5_reads_from_dir(self):
        f5_read_gen = yield_fast5_reads(test_data, recursive=False)
        read_id, read_data = next(f5_read_gen)
        self.assertTrue(read_id is not None, "read_id is None")
        self.assertTrue(isinstance(read_data, Fast5Read), "Return is not Fast5Read instance")

    def test_yield_fast5_reads_with_set(self):
        f5_read_gen = yield_fast5_reads(self.fast5_path,
                                        recursive=False,
                                        read_ids=self.read_id_set)
        f5_reads = list(f5_read_gen)
        self.assertTrue(len(f5_reads) == len(self.read_id_set))

        for read_id, read_data in f5_reads:
            self.assertTrue(read_id in self.read_id_set, "A read_id is not a member of read_ids")
            self.assertTrue(isinstance(read_data, Fast5Read), "Return is not Fast5Read instance")

    def test_yield_fast5_reads_with_list(self):
        f5_read_gen = yield_fast5_reads(self.fast5_path,
                                        recursive=False,
                                        read_ids=self.read_id_set)
        f5_reads = list(f5_read_gen)
        self.assertTrue(len(f5_reads) == len(self.read_id_list))

        for read_id, read_data in f5_reads:
            self.assertTrue(read_id in self.read_id_set, "A read_id is not a member of read_id_list")
            self.assertTrue(isinstance(read_data, Fast5Read), "Return is not Fast5Read instance")

    def test_yield_fast5_reads_set_versus_list_equality(self):
        f5_read_gen_by_id_set = yield_fast5_reads(self.fast5_path,
                                                  recursive=False,
                                                  read_ids=self.read_id_set)

        f5_read_gen_by_id_list = yield_fast5_reads(self.fast5_path,
                                                   recursive=False,
                                                   read_ids=self.read_id_list)

        # Consume the generators into sets
        ids_by_set = set(rid for rid, _ in f5_read_gen_by_id_set)
        ids_by_list = set(rid for rid, _  in f5_read_gen_by_id_list)
        self.assertTrue(ids_by_list == ids_by_set, 'Ids differ when using read_id list versus set')


    def test_yield_fast5_reads_with_empty_set(self):
        f5_read_gen = yield_fast5_reads(self.fast5_path,
                                        recursive=False,
                                        read_ids=set([]))

        self.assertTrue(len(list(f5_read_gen)) != 0, "Empty read_ids resulted in zero returned reads")

    def test_yield_fast5_reads_with_garbage_set(self):
        f5_read_gen = yield_fast5_reads(self.fast5_path,
                                        recursive=False,
                                        read_ids={'_g4rbag£_'})
        f5_reads = list(f5_read_gen)
        self.assertTrue(len(f5_reads) == 0, "Garbage read_ids returned non-zero reads")

    def test_yield_fast5_reads_type_error(self):
        with self.assertRaisesRegex(TypeError, 'read_ids'):
            f5_read_gen = yield_fast5_reads(self.fast5_path,
                                            recursive=False,
                                            read_ids=int(1))
            next(f5_read_gen)