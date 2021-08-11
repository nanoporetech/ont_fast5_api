"""
Script for binning fast5 reads into separate directories based on column value in summary file
Inteded for demultiplexing reads using barcoding summary file.
"""
from pathlib import Path
from typing import Union, Dict, Set, List
from multiprocessing import Pool
import logging
from csv import reader
from collections import defaultdict
from time import sleep
from math import ceil
from argparse import ArgumentParser

from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import (
    get_fast5_file_list,
    get_progress_bar,
    Fast5FilterWorker,
    READS_PER_FILE,
    FILENAME_BASE,
    ProgressBar,
)

DEMULTIPLEX_COLUMN = "barcode_arrangement"
READ_ID_COLUMN = "read_id"


class Fast5Demux:
    """
    Bin reads from directory of fast5 files according to demultiplex_column in sequencing_summary path
    :param input_dir: Path to input Fast5 file or directory of Fast5 files
    :param output_dir: Path to output directory
    :param summary_file: Path to TSV summary file
    :param demultiplex_column: str name of column with demultiplex values
    :param read_id_column: str name of column with read ids
    :param filename_base: str prefix for output Fast5 files
    :param batch_size: int maximum number of reads per output file
    :param threads: int maximum number of worker processes
    :param recursive: bool flag to search recursively through input_dir for Fast5 files
    :param follow_symlinks: bool flag to follow symlinks in input_dir
    :param target_compression: str compression type in output Fast5 files
    """

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        summary_file: Path,
        demultiplex_column: str,
        read_id_column: str = READ_ID_COLUMN,
        filename_base: str = FILENAME_BASE,
        batch_size: int = READS_PER_FILE,
        threads: int = 1,
        recursive: bool = False,
        follow_symlinks: bool = True,
        target_compression: Union[str, None] = None,
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.summary = summary_file
        self.demultiplex_column = demultiplex_column
        self.read_id_column = read_id_column
        self.filename_base = filename_base
        self.batch_size = batch_size
        self.threads = threads
        self.recursive = recursive
        self.follow_symlinks = follow_symlinks
        self.target_compression = target_compression

        self.read_sets: Dict[str, Set[str]] = {}
        self.input_fast5s: List[Path] = []
        self.max_threads: int = 0
        self.workers: List = []
        self.progressbar: Union[ProgressBar, None] = None
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

    def create_output_dirs(self) -> None:
        """
        In output directory create a subdirectory per demux category
        :return:
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for demux in self.read_sets:
            out_dir = self.output_dir / demux
            out_dir.mkdir(exist_ok=True)

    def run_batch(self) -> None:
        """
        Run workers in pool or sequentially
        Starts multiprocessing pool if max_threads allows it
        :return:
        """
        self.workers_setup()

        if self.max_threads > 1:
            with Pool(self.max_threads) as pool:
                for worker in self.workers:
                    worker.run_batch(pool=pool)
                while any(worker.tasks for worker in self.workers):
                    sleep(1)

            pool.join()
            pool.close()
        else:
            for worker in self.workers:
                worker.run_batch(pool=None)

        self.progressbar.finish()

    def workers_setup(self) -> None:
        """
        Parse input summary and input file list to determine amount of work
        Create output directories and initialise workers
        :return:
        """
        self.read_sets = self.parse_summary_demultiplex()
        self.input_fast5s = get_fast5_file_list(
            input_path=self.input_dir,
            recursive=self.recursive,
            follow_symlinks=self.follow_symlinks,
        )
        self.max_threads = self.calculate_max_threads()
        # progressbar length is total numbers of reads to be extracted plus total number of files to be read
        total_progress = sum(len(item) for item in self.read_sets.values()) + (
            len(self.input_fast5s) * len(self.read_sets)
        )
        self.progressbar = get_progress_bar(num_reads=total_progress)
        self.create_output_dirs()
        for demux in sorted(self.read_sets):
            self.workers.append(
                Fast5FilterWorker(
                    input_file_list=self.input_fast5s,
                    output_dir=self.output_dir / demux,
                    read_set=self.read_sets[demux],
                    progressbar=self.progressbar,
                    logger=self.logger,
                    filename_base=self.filename_base,
                    batch_size=self.batch_size,
                    target_compression=self.target_compression,
                )
            )

    def report(self) -> None:
        """
        Log summary of work done
        :return:
        """
        total_reads = 0
        for idx, _ in enumerate(sorted(self.read_sets)):
            worker = self.workers[idx]
            for file, reads in worker.out_files.items():
                total_reads += len(reads)

        self.logger.info("{} reads extracted".format(total_reads))

        # report reads not found
        reads_to_extract = sum(len(item) for item in self.read_sets.values())
        if reads_to_extract > total_reads:
            self.logger.warning(
                "{} reads not found!".format(reads_to_extract - total_reads)
            )

    def calculate_max_threads(self) -> int:
        """
        Calculate max number of workers based on number of output files, input files and threads argument
        :return: int
        """
        max_inputs_per_worker = len(self.input_fast5s)
        total_outputs = 0
        for read_set in self.read_sets.values():
            outputs = int(ceil(len(read_set) / float(self.batch_size)))
            total_outputs += min(outputs, max_inputs_per_worker)

        return min(self.threads, total_outputs)

    def parse_summary_demultiplex(self) -> Dict[str, Set[str]]:
        """
        Open a TSV file and parse read_id and demultiplex columns into dict {demultiplex: read_id_set}
        :return:
        """
        read_sets = defaultdict(set)
        with open(str(self.summary), "r") as fh:
            read_list_tsv = reader(fh, delimiter="\t")
            header = next(read_list_tsv)

            if self.read_id_column in header:
                read_id_col_idx = header.index(self.read_id_column)
            else:
                raise ValueError(
                    "No '{}' read_id column in header: {}".format(
                        self.read_id_column, header
                    )
                )

            if self.demultiplex_column in header:
                demultiplex_col_idx = header.index(self.demultiplex_column)
            else:
                raise ValueError(
                    "No '{}' demultiplex column in header: {}".format(
                        self.demultiplex_column, header
                    )
                )

            for line in read_list_tsv:
                read_id = line[read_id_col_idx]
                demux = line[demultiplex_col_idx]
                read_sets[demux].add(read_id)

        return read_sets


def create_arg_parser():
    parser = ArgumentParser(
        "Tool for binning reads from a multi_read_fast5_file by column value in summary file"
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        type=Path,
        help="Path to Fast5 file or directory of Fast5 files",
    )
    parser.add_argument(
        "-s",
        "--save_path",
        required=True,
        type=Path,
        help="Directory to output MultiRead subset to",
    )
    parser.add_argument(
        "-l",
        "--summary_file",
        required=True,
        type=Path,
        help="TSV file containing read_id column (sequencing_summary.txt file)",
    )
    parser.add_argument(
        "-f",
        "--filename_base",
        default="batch",
        required=False,
        help="Root of output filename, default='{}' -> '{}0.fast5'".format(
            FILENAME_BASE, FILENAME_BASE
        ),
    )
    parser.add_argument(
        "-n",
        "--batch_size",
        type=int,
        default=READS_PER_FILE,
        required=False,
        help="Number of reads per multi-read file (default {})".format(READS_PER_FILE),
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=1,
        required=False,
        help="Maximum number of parallel processes to use (default 1)",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        required=False,
        default=False,
        help="Flag to search recursively through input directory for MultiRead fast5 files",
    )
    parser.add_argument(
        "--ignore_symlinks",
        action="store_true",
        help="Ignore symlinks when searching recursively for fast5 files",
    )
    parser.add_argument(
        "-c",
        "--compression",
        required=False,
        default=None,
        choices=list(COMPRESSION_MAP.keys()) + [None],
        help="Target output compression type. If omitted - don't change compression type",
    )
    parser.add_argument(
        "--demultiplex_column",
        type=str,
        default=DEMULTIPLEX_COLUMN,
        required=False,
        help="Name of column for demultiplexing in summary file (default '{}'".format(
            DEMULTIPLEX_COLUMN
        ),
    )
    parser.add_argument(
        "--read_id_column",
        type=str,
        default=READ_ID_COLUMN,
        required=False,
        help="Name of read_id column in summary file (default '{}'".format(
            READ_ID_COLUMN
        ),
    )
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    if args.compression is not None:
        args.compression = COMPRESSION_MAP[args.compression]

    demux = Fast5Demux(
        input_dir=args.input,
        output_dir=args.save_path,
        summary_file=args.summary_file,
        demultiplex_column=args.demultiplex_column,
        read_id_column=args.read_id_column,
        filename_base=args.filename_base,
        batch_size=args.batch_size,
        threads=args.threads,
        recursive=args.recursive,
        follow_symlinks=not args.ignore_symlinks,
        target_compression=args.compression,
    )
    demux.run_batch()
    demux.report()


if __name__ == "__main__":
    main()
