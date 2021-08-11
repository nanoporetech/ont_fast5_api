"""Filter Fast5 files based on read_id list
"""
import csv
import logging
from argparse import ArgumentParser
from math import ceil
from multiprocessing import Pool
from os import makedirs, path
from pathlib import Path
from time import sleep

from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar, Fast5FilterWorker
from ont_fast5_api.conversion_tools.conversion_utils import READS_PER_FILE, FILENAME_BASE

logging.basicConfig(level=logging.DEBUG)


class Fast5Filter:
    """
    Extract reads listed read_list_file from fast5 files in input_folder, write to multi-fast5 files in
    output_folder
    """

    def __init__(self, input_folder, output_folder, read_list_file, filename_base=FILENAME_BASE,
                 batch_size=READS_PER_FILE, threads=1, recursive=False, file_list_file=None, follow_symlinks=True,
                 target_compression=None):
        assert path.isdir(input_folder)
        assert path.isfile(read_list_file)
        assert isinstance(filename_base, str)
        assert isinstance(batch_size, int)
        assert isinstance(threads, int)
        assert isinstance(recursive, bool)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.read_set = parse_summary_file(read_list_file)
        self.input_f5s = get_fast5_file_list(str(input_folder), recursive, follow_symlinks=follow_symlinks)
        makedirs(output_folder, exist_ok=True)

        if len(self.read_set) < 1:
            raise ValueError("No reads in read list file {}".format(read_list_file))

        if len(self.input_f5s) < 1:
            raise ValueError(
                "No input fast5 files found in {}. Recursion is set to {}".format(str(input_folder), recursive))

        if batch_size < 1:
            raise ValueError("Batch size (--batch_size) must be a positive integer, not {}".format(batch_size))

        if threads < 1:
            raise ValueError("Max number of threads (--threads) must be a positive integer, not {}".format(threads))

        if file_list_file:
            file_set = parse_summary_file(file_list_file)
            for file in file_set:
                assert path.exists(file), "{} from file list doesn't exist".format(file)
            self.input_f5s = list(file_set.intersection(self.input_f5s))

        # determine max number of workers
        num_outputs = int(ceil(len(self.read_set) / float(batch_size)))
        self.num_workers = min(threads, num_outputs, len(self.input_f5s))

        # progressbar total is number of reads in read_set plus number of input files
        # (to see progress while scanning files that don't have any relevant reads)
        self.pbar = get_progress_bar(len(self.read_set) + len(self.input_f5s))

        self.worker = Fast5FilterWorker(
            input_file_list=self.input_f5s,
            output_dir=Path(output_folder),
            logger=self.logger,
            progressbar=self.pbar,
            read_set=self.read_set,
            filename_base=filename_base,
            batch_size=batch_size,
            target_compression=target_compression
        )

    def run_batch(self):

        if self.num_workers == 1:
            self.worker.run_batch(pool=None)
        else:
            with Pool(self.num_workers) as pool:
                self.worker.run_batch(pool=pool)

                while self.worker.tasks:
                    sleep(1)

                pool.close()
                pool.join()

        self.pbar.finish()
        self.logger.info("{} reads extracted".format(sum(len(v) for v in self.worker.out_files.values())))

        # report reads not found
        if len(self.worker.read_set) > 0:
            self.logger.warning("{} reads not found!".format(len(self.worker.read_set)))


def parse_summary_file(read_list_file):
    """
    Opens a text file and returns set of read_ids
    Expects either a single column file where every line is read_id or
      multi-column Tab-separated CSV, that contains a column read_id
    :param read_list_file: path to file
    :return: set
    """
    reads = set()
    with open(str(read_list_file), 'r') as fh:
        read_list_tsv = csv.reader(fh, delimiter='\t')
        header = next(read_list_tsv)

        if "read_id" in header:
            col_idx = header.index("read_id")
        else:
            if len(header) == 1:
                reads.add(header[0].strip())
                col_idx = 0
            else:
                raise TypeError("multi-column file without 'read_id' column")

        for line in read_list_tsv:
            reads.add(line[col_idx].strip())
    if len(reads) < 1:
        raise ValueError("No reads in read list file {}".format(read_list_file))
    return reads


def main():
    parser = ArgumentParser("Tool for extracting reads from a multi_read_fast5_file by read_id")
    parser.add_argument('-i', '--input', required=True,
                        help="Path to Fast5 file or directory of Fast5 files")
    parser.add_argument('-s', '--save_path', required=True,
                        help="Folder to output MultiRead subset to")
    parser.add_argument('-l', '--read_id_list', required=True,
                        help="File containing list of read ids to extract (or sequencing_summary.txt file)")
    parser.add_argument('-f', '--filename_base', default=FILENAME_BASE, required=False,
                        help="Root of output filename, default='{}' -> '{}0.fast5'".format(FILENAME_BASE, FILENAME_BASE))
    parser.add_argument('-n', '--batch_size', type=int, default=READS_PER_FILE, required=False,
                        help="Number of reads per multi-read file (default {}".format(READS_PER_FILE))
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Maximum number of threads to use")
    parser.add_argument('-r', '--recursive', action='store_true', required=False, default=False,
                        help="Search recursively through folders for MultiRead fast5 files")
    parser.add_argument('--ignore_symlinks', action='store_true',
                        help="Ignore symlinks when searching recursively for fast5 files")
    parser.add_argument('-c', '--compression', required=False, default=None,
                        choices=list(COMPRESSION_MAP.keys()) + [None], help="Target output compression type")
    parser.add_argument('--file_list', required=False,
                        help="File containing names of files to search in")
    args = parser.parse_args()

    if args.compression is not None:
        args.compression = COMPRESSION_MAP[args.compression]

    multifilter = Fast5Filter(input_folder=args.input,
                              output_folder=args.save_path,
                              filename_base=args.filename_base,
                              read_list_file=args.read_id_list,
                              batch_size=args.batch_size,
                              threads=args.threads,
                              recursive=args.recursive,
                              file_list_file=args.file_list,
                              follow_symlinks=not args.ignore_symlinks,
                              target_compression=args.compression)

    multifilter.run_batch()


if __name__ == '__main__':
    main()
