"""Filter Fast5 files based on read_id list
"""
import csv
import logging
from argparse import ArgumentParser
from math import ceil
from multiprocessing import Pool
from os import makedirs, path, unlink
from time import sleep

from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar
from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File

logging.basicConfig(level=logging.DEBUG)


class Fast5Filter:
    """
    Extract reads listed read_list_file from fast5 files in input_folder, write to multi-fast5 files in
    output_folder
    Max number of output files is calculated from length of read_list divided by batch_size
    Every worker receives a single input and single output file (and full set of read_ids to extract)
    If input file is not exhausted before output file has reached batch_size limit, it is given to next worker.

    Single-process case:
      Is triggered if threads is set to 1 or there is only a single input file or number of reads in read_list_file
      does not exceed batch_size, so there will only be single output file.
    """

    def __init__(self, input_folder, output_folder, read_list_file, filename_base="batch",
                 batch_size=4000, threads=1, recursive=False, file_list_file=None, follow_symlinks=True,
                 target_compression=None):
        assert path.isdir(input_folder)
        assert path.isfile(read_list_file)
        assert isinstance(filename_base, str)
        assert isinstance(batch_size, int)
        assert isinstance(threads, int)
        assert isinstance(recursive, bool)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.read_set = get_filter_reads(read_list_file)
        self.input_f5s = get_fast5_file_list(str(input_folder), recursive, follow_symlinks=follow_symlinks)
        self.target_compression = target_compression

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
            file_set = get_filter_reads(file_list_file)
            for file in file_set:
                assert path.exists(file), "{} from file list doesn't exist".format(file)
            self.input_f5s = list(file_set.intersection(self.input_f5s))

        # determine max number of workers
        self.batch_size = batch_size
        num_outputs = int(ceil(len(self.read_set) / float(batch_size)))
        self.num_workers = min(threads, num_outputs, len(self.input_f5s))

        makedirs(output_folder, exist_ok=True)
        self.filename_mapping_file = path.join(output_folder, "filename_mapping.txt")
        if path.exists(self.filename_mapping_file):
            self.logger.info("overwriting filename mapping file {}".format(self.filename_mapping_file))
            unlink(self.filename_mapping_file)

        # dict where key=filename value=read_set
        self.out_files = {}

        out_file_names = []
        for i in range(num_outputs):
            filename = filename_base + str(i) + ".fast5"
            output_file_name = path.join(output_folder, filename)

            if path.exists(output_file_name):
                self.logger.info("overwriting multiread file {}".format(output_file_name))
                unlink(output_file_name)

            self.out_files[output_file_name] = set()
            out_file_names.append(output_file_name)

        # reversing so that first item to be popped is lower idx
        self.available_out_files = out_file_names[::-1]
        self.tasks = []
        self.pool = None
        # progressbar total is number of reads in read_set plus number of input files
        # (to see progress while scanning files that don't have any relevant reads)
        self.pbar = get_progress_bar(len(self.read_set) + len(self.input_f5s))

    def run_batch(self):

        if self.num_workers == 1:
            self._launch_sync_tasks()
        else:
            with Pool(self.num_workers) as pool:
                self.pool = pool
                self._launch_async_tasks()

                while self.tasks or (self.input_f5s and self.read_set):
                    sleep(1)

                self.pool.close()
                self.pool.join()
            self.pool = None

        self.pbar.finish()
        self.logger.info("{} reads extracted".format(sum(len(v) for v in self.out_files.values())))

        # report reads not found
        if len(self.read_set) > 0:
            self.logger.warning("{} reads not found!".format(len(self.read_set)))

    def _launch_sync_tasks(self):
        """
        Run tasks sequentially
        :return:
        """
        for args_tuple in self._args_generator():
            reads, out_file, in_file = extract_selected_reads(*args_tuple)
            self._update_file_lists(reads=reads, out_file=out_file, in_file=in_file)

    def _launch_async_tasks(self):
        """
        Launch an async task for every input-output pair
        self.tasks is just for keeping track of number of tasks still running
        :return:
        """
        for args_tuple in self._args_generator():
            self.pool.apply_async(func=extract_selected_reads, args=args_tuple, callback=self._callback)

            self.tasks.append(0)
            if len(self.tasks) > self.num_workers:
                break

    def _callback(self, result):
        """
        Once a thread finishes, decrement self.tasks, update available files and reads and trigger scan for new tasks
        :param result: tuple
        :return:
        """
        self._update_file_lists(*result)
        self._launch_async_tasks()
        self.tasks.pop()

    def _update_file_lists(self, reads, out_file, in_file):
        """
        Update read sets and files available for processing
        :param reads:
        :param out_file:
        :param in_file:
        :return:
        """
        in_file_exhausted = 1
        if in_file is not None:
            # in_file was not exhausted
            in_file_exhausted = 0
            self.input_f5s.append(in_file)

        self.out_files[out_file].update(reads)
        self.read_set.difference_update(reads)

        if len(self.out_files[out_file]) < self.batch_size:
            # out_file has not reached batch limit
            self.available_out_files.append(out_file)

        # print filename - read table
        with open(str(self.filename_mapping_file), 'a') as output_table:
            for read in reads:
                output_table.write("{}\t{}\n".format(read, path.basename(out_file)))

        # increment progressbar by number of reads found and by number of files processed
        self.pbar.update(self.pbar.currval + len(reads) + in_file_exhausted)

    def _args_generator(self):
        """
        If there are possible pairs of input and output files, yield tuples that are suitable inputs to
         extract_selected_reads
        :return:
        """
        while self.available_out_files and self.input_f5s:
            out_file = self.available_out_files.pop()
            in_file = self.input_f5s.pop()
            count = self.batch_size - len(self.out_files[out_file])
            yield in_file, out_file, self.read_set, count, self.target_compression


def get_filter_reads(read_list_file):
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


def extract_selected_reads(input_file, output_file, read_set, count, target_compression=None):
    """
    Take reads from input file if read_id id is in read_set
    Write to output file, at most count times
    return tuple (found_reads, output_file, input_file)
    If input file was exhausted, the third item in return is None
    :param input_file:
    :param output_file:
    :param read_set:
    :param count:
    :return:
    """
    found_reads = set()
    with MultiFast5File(str(output_file), 'a') as output_f5:
        reads_present = set(output_f5.get_read_ids())
        for read_id, read in read_generator(input_file, read_set):
            found_reads.add(read_id)

            if read_id in reads_present:
                continue

            output_f5.add_existing_read(read, target_compression=target_compression)
            reads_present.add(read_id)

            if len(found_reads) >= count:
                return found_reads, output_file, input_file

    return found_reads, output_file, None


def read_generator(input_file, read_set):
    """
    Open input_file as Fast5, yield tuples (read_id, fast5_read) for every read_id that is present in read_set
    :param input_file:
    :param read_set:
    :return:
    """
    with get_fast5_file(str(input_file)) as input_f5:
        read_ids = input_f5.get_read_ids()
        for read_id in read_set.intersection(read_ids):
            read = input_f5.get_read(read_id)
            yield read_id, read


def main():
    parser = ArgumentParser("Tool for extracting reads from a multi_read_fast5_file by read_id")
    parser.add_argument('-i', '--input', required=True,
                        help="Path to Fast5 file or directory of Fast5 files")
    parser.add_argument('-s', '--save_path', required=True,
                        help="Folder to output MultiRead subset to")
    parser.add_argument('-l', '--read_id_list', required=True,
                        help="File containing list of read ids to extract (or sequencing_summary.txt file)")
    parser.add_argument('-f', '--filename_base', default='batch', required=False,
                        help="Root of output filename, default='batch' -> 'batch_0.fast5'")
    parser.add_argument('-n', '--batch_size', type=int, default=4000, required=False,
                        help="Number of reads per multi-read file")
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
