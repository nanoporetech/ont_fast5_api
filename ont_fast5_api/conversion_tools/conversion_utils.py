import multiprocessing
import os

from glob import glob
from math import ceil
from pathlib import Path
from typing import List, Optional, Set, Tuple, Iterator

from progressbar import RotatingMarker, ProgressBar, SimpleProgress, Bar, Percentage, ETA
from logging import Logger

from ont_fast5_api.fast5_interface import get_fast5_file
from ont_fast5_api.multi_fast5 import MultiFast5File
from ont_fast5_api.fast5_read import Fast5Read


READS_PER_FILE = 4000
FILENAME_BASE = "batch"


def batcher(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def yield_fast5_files(input_path, recursive, follow_symlinks=True):
    """
    Yield fast5 file paths within a given directory.
    Optionally search recursively and follow symbolic links

    :param input_path: Path
    :param recursive: bool
    :param follow_symlinks: bool
    :return:
    """
    if os.path.isfile(input_path):
        yield input_path
        return

    if recursive:
        for root, _, filenames in os.walk(input_path, followlinks=follow_symlinks):
            for filename in filenames:
                if not filename.endswith('.fast5'): continue
                yield os.path.join(root, filename)
    else:
        for filename in glob(os.path.join(input_path, '*.fast5')):
            yield filename
    return


def yield_fast5_reads(input_path, recursive, follow_symlinks=True, read_ids=None):
    """
    Iterate over reads in fast5 files and yield read_ids and fast5 read objects.
    If read_id_set is defined, skip reads which are not in this set/list. An empty set/list returns all.

    :param input_path: Path
    :param recursive: bool
    :param follow_symlinks: bool
    :param read_ids: set or list
    :raise TypeError: read_id_set must be of type set or list'
    :return: yielded tuple (read_id, fast5_read_object)
    """
    if not isinstance(read_ids, (list, set)) and read_ids is not None:
        raise TypeError('read_ids must be of type set or list or none')

    if read_ids and isinstance(read_ids, list):
        read_ids = set(read_ids)

    for fast5_path in yield_fast5_files(input_path=input_path, recursive=recursive, follow_symlinks=follow_symlinks):
        fast5_file = get_fast5_file(fast5_path)
        if read_ids:
            selected_reads = read_ids.intersection(fast5_file.get_read_ids())
        else:
            selected_reads = fast5_file.get_read_ids()

        for read_id in selected_reads:
            yield read_id, fast5_file.get_read(read_id)


def get_fast5_file_list(input_path, recursive, follow_symlinks=True):
    # NB this method is provided for compatibility with use cases where
    # generator behaviour is not appropriate.
    # E.g. where we need to know the file_list length or be able to sample from it
    return list(yield_fast5_files(input_path, recursive, follow_symlinks))


def get_progress_bar(num_reads):
    bar_format = [RotatingMarker(), " ", SimpleProgress(), Bar(), Percentage(), " ", ETA()]
    progress_bar = ProgressBar(maxval=num_reads, widgets=bar_format)
    bad_progressbar_version = False
    try:
        progress_bar.currval
    except AttributeError as e:
        bad_progressbar_version = True
        pass
    if bad_progressbar_version:
        raise RuntimeError('Wrong progressbar package detected, likely '
                           '"progressbar2". Please uninstall that package and '
                           'install "progressbar33" instead.')

    return progress_bar.start()


class Fast5FilterWorker:
    """
    Common worker for fast5_subset and demux_fast5
    Extract reads in [read_set] from fast5 files in [input_file_list] to [output_dir]
    Implements single process and multi process (multiprocessing.Pool object must be provided as argument to run_batch
    to run in multi process mode)
    Max number of output files is calculated from length of read_list divided by batch_size
    Every worker receives a single input and single output file (and full set of read_ids to extract)
    If input file is not exhausted before output file has reached batch_size limit, it is given to next worker.
    """
    def __init__(
        self,
        input_file_list: List[Path],
        output_dir: Path,
        read_set: Set[str],
        progressbar: ProgressBar,
        logger: Logger,
        filename_base: str="batch",
        batch_size: int=READS_PER_FILE,
        target_compression: Optional[str]=None,
    ):
        self.input_f5s = input_file_list.copy()  # this list will be modified to track progress
        self.read_set = read_set
        self.target_compression = target_compression
        self.batch_size = batch_size
        self.filename_base = filename_base
        self.output_dir = output_dir
        self.pbar = progressbar
        self.logger = logger

        self.filename_mapping_file = self.output_dir / "filename_mapping.txt"
        if self.filename_mapping_file.exists():
            self.logger.info("overwriting filename mapping file {}".format(self.filename_mapping_file))
            self.filename_mapping_file.unlink()

        self.out_files = {}  # dict where key=filename value=read_set
        self.available_out_files = []
        self.populate_out_files()
        self.tasks = []
        self.pool = None

    def populate_out_files(self) -> None:
        """
        Calculate number of output files based on batch size.
        Delete existing files and initialise dict that keeps track of reads added to each file
        :return:
        """
        num_outputs = int(ceil(len(self.read_set) / float(self.batch_size)))
        for i in range(num_outputs):
            filename = self.filename_base + str(i) + ".fast5"
            output_file_name = self.output_dir / filename

            if output_file_name.exists():
                self.logger.info("overwriting multiread file {}".format(output_file_name))
                output_file_name.unlink()

            self.out_files[output_file_name] = set()
        # reversing so that first item to be popped is lower idx
        self.available_out_files = sorted(self.out_files.keys(), reverse=True)

    def run_batch(self, pool: multiprocessing.Pool=None) -> None:
        """
        Choose sync or async (if multiprocessing.Pool is provided) running mode, launch tasks.
        :param pool:
        :return:
        """
        if pool is None:
            self._launch_sync_tasks()
        else:
            self.pool = pool
            self._launch_async_tasks()

    def _launch_sync_tasks(self) -> None:
        """
        Run tasks sequentially
        :return:
        """
        for args_tuple in self._args_generator():
            reads, out_file, in_file = extract_selected_reads(*args_tuple)
            self._update_file_lists(reads=reads, out_file=out_file, in_file=in_file)

    def _launch_async_tasks(self) -> None:
        """
        Launch an async task for every input-output pair
        self.tasks is just for keeping track of number of tasks still running
        :return:
        """
        for args_tuple in self._args_generator():
            self.pool.apply_async(func=extract_selected_reads, args=args_tuple,
                                  callback=self._callback, error_callback=self._error_callback)

            self.tasks.append(0)

    def _callback(self, result) -> None:
        """
        Once a thread finishes, decrement self.tasks, update available files and reads and trigger scan for new tasks
        :param result: tuple
        :return:
        """
        self._update_file_lists(*result)
        self._launch_async_tasks()
        self.tasks.pop()

    def _error_callback(self, result) -> None:
        self.logger.error(result.original_exception)
        self._update_file_lists(set(), result.output_file, None)
        self._launch_async_tasks()
        self.tasks.pop()

    def _update_file_lists(self, reads, out_file, in_file) -> None:
        """
        Update read sets and files available for processing
        Update progressbar
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
                output_table.write("{}\t{}\n".format(read, out_file.name))

        # increment progressbar by number of reads found and by number of files processed
        self.pbar.update(self.pbar.currval + len(reads) + in_file_exhausted)

    def _args_generator(self) -> Iterator[Tuple[Path, Path, Set[str], int, Optional[str]]]:
        """
        If there are possible pairs of input and output files, yield tuples that are suitable inputs to
         extract_selected_reads
        :return: a generator of tuples(in_file, out_file, read_set, count, compression)
        """
        while self.available_out_files and self.input_f5s:
            out_file = self.available_out_files.pop()
            in_file = self.input_f5s.pop()
            count = self.batch_size - len(self.out_files[out_file])
            yield in_file, out_file, self.read_set, count, self.target_compression


def extract_selected_reads(
        input_file: Path,
        output_file: Path,
        read_set: Set[str],
        count: int,
        target_compression: Optional[str]=None,
) -> Tuple[Set[str], Path, Optional[Path]]:
    """
    Take reads from input file if read_id id is in read_set
    Write to output file, at most count times
    return tuple (found_reads, output_file, input_file)
    If input file was exhausted, the third item in return is None
    :param input_file: Path to input Fast5 file
    :param output_file: Path to output Fast5 file (will be appended to if already exists)
    :param read_set: set of read_ids to extract
    :param count: int max number of reads to be added to output_file
    :param target_compression: str type of compression for output fast5 file
    :return: tuple of: found read set, path to output file, path to input file (if not exhausted)
    """
    try:
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
    except Exception as e:
        exception = type(e)("Error processing file {}: {}".format(input_file,
                                                                  e.args))
        raise ExtractionException(exception, output_file)

    return found_reads, output_file, None


class ExtractionException(Exception):
    def __init__(self, original_exception, output_file):
        self.original_exception = original_exception
        self.output_file = output_file


def read_generator(input_file: Path, read_set: Set[str]) -> Iterator[Tuple[str, Fast5Read]]:
    """
    Open input_file as Fast5, yield tuples (read_id, fast5_read) for every read_id that is present in read_set
    :param input_file: Path to input Fast5File
    :param read_set: set of read_ids to look for
    :return: tuples of (read_id, read object)
    """
    with get_fast5_file(str(input_file)) as input_f5:
        read_ids = input_f5.get_read_ids()
        for read_id in read_set.intersection(read_ids):
            read = input_f5.get_read(read_id)
            yield read_id, read
