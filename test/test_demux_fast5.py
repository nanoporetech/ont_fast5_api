from pathlib import Path
from test.helpers import TestFast5ApiHelper, test_data
from unittest.mock import patch
from ont_fast5_api.conversion_tools.demux_fast5 import Fast5Demux
from ont_fast5_api.multi_fast5 import MultiFast5File


class TestDemuxFast5(TestFast5ApiHelper):
    multi_fast5 = Path(test_data) / "multi_read" / "batch_0.fast5"
    summary = Path(test_data) / "summaries" / "two_barcode_summary.txt"
    barcode01 = {"fe85b517-62ee-4a33-8767-41cab5d5ab39", "fe8a3026-d1f4-46b3-8daa-e610f27acde1"}
    barcode02 = {"fe9374ee-b86a-4ca4-81dc-ac06e3297728", "fe849dd3-63bc-4044-8910-14e1686273bb"}
    barcodes = (barcode01, barcode02)

    @patch('ont_fast5_api.conversion_tools.demux_fast5.logging')
    @patch('ont_fast5_api.conversion_tools.conversion_utils.ProgressBar')
    def test_demux_1t(self, mock_pbar, mock_logger):
        # given 4 read multi fast5 file and a summary, bin it in two barcode directories
        output_dir = Path(self.save_path) / "1t"
        output_dir.mkdir()
        demux = Fast5Demux(input_dir=self.multi_fast5, output_dir=output_dir, summary_file=self.summary,
                           demultiplex_column="barcode_arrangement",threads=1)
        demux.run_batch()
        self.check_output(output_dir)

    @patch('ont_fast5_api.conversion_tools.demux_fast5.logging')
    @patch('ont_fast5_api.conversion_tools.conversion_utils.ProgressBar')
    def test_demux_8t(self, mock_pbar, mock_logger):
        # given 4 read multi fast5 file and a summary, bin it in two barcode directories
        output_dir = Path(self.save_path) / "8t"
        output_dir.mkdir()
        demux = Fast5Demux(input_dir=self.multi_fast5, output_dir=output_dir, summary_file=self.summary,
                           demultiplex_column="barcode_arrangement",threads=8)
        demux.workers_setup()
        # even with 8 threads allocated, only max 2 can be used
        self.assertEqual(demux.max_threads, 2)
        demux.run_batch()
        self.check_output(output_dir)

    def check_output(self, result_path):
        output_dir1 = result_path / "barcode01"
        output_dir2 = result_path / "barcode02"

        for directory, barcodes in zip((output_dir1, output_dir2), self.barcodes):
            self.assertTrue(directory.exists())
            self.assertTrue(directory.is_dir())
            batch_file = directory / "batch0.fast5"
            self.assertTrue(batch_file.exists())
            self.assertTrue(batch_file.is_file())
            with MultiFast5File(batch_file, 'r') as fast5_in:
                read_ids = set(fast5_in.get_read_ids())
                self.assertEqual(read_ids, barcodes)
            summary_file = directory / "filename_mapping.txt"
            self.assertTrue(summary_file.exists())
            self.assertTrue(summary_file.is_file())

    @patch('ont_fast5_api.conversion_tools.demux_fast5.logging')
    @patch('ont_fast5_api.conversion_tools.conversion_utils.ProgressBar')
    def test_parse_summary(self, mock_pbar, mock_logger):
        # create a summary file with standard column names
        summary_file = Path(self.generate_temp_filename())
        truth = {"barcode01": self.barcode01, "barcode02": self.barcode02}
        with open(summary_file, 'w') as summ:
            header = "read_id\tbarcode_arrangement\n"
            summ.write(header)
            for barcode, read_ids in truth.items():
                for read_id in read_ids:
                    line = read_id + "\t" + barcode + "\n"
                    summ.write(line)

        demux = Fast5Demux(input_dir=self.multi_fast5, output_dir=Path(self.save_path), summary_file=summary_file,
                           demultiplex_column="barcode_arrangement")
        demux.workers_setup()

        self.assertEqual(demux.read_sets, truth)

        # create a summary file with non-standard column names
        truth = {"genome1": self.barcode01, "genome2": self.barcode02}
        summary_file = Path(self.generate_temp_filename())
        with open(summary_file, 'w') as summ:
            header = "genome\tread_name\n"
            summ.write(header)
            for genome, read_ids in truth.items():
                for read_id in read_ids:
                    line = genome + "\t" + read_id + "\n"
                    summ.write(line)

        demux = Fast5Demux(input_dir=self.multi_fast5, output_dir=Path(self.save_path), summary_file=summary_file,
                           demultiplex_column="genome", read_id_column="read_name")
        demux.workers_setup()

        self.assertEqual(demux.read_sets, truth)
